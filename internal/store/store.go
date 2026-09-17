// Package store 提供灭菌柜卸载窗口的 SQLite 持久化层。
//
// API 进程与扫描 worker 进程共享同一个 SQLite 数据库文件，数据库是两个
// 进程对窗口状态（open / confirmed / quarantined）进行裁决的唯一依据：
// 所有终态写入都是带 state='open' 前置条件的单条原子 UPDATE，并发竞争时
// 只有一条语句能命中目标行，因此一台设备的同一窗口只会提交一个终态。
//
// 卸载确认时限按设备策略（device_policies）确定，未配置的设备使用默认
// 30 秒。登记窗口时由同一条 INSERT 读取当前策略、生成截止时刻并把采用的
// 秒数快照到窗口的 ttl_seconds，策略后续变化不影响已登记窗口。
//
// 时间规则：所有参与裁决的时刻一律取自数据库自身的 UTC 当前时间
// （SQLite 的 strftime('%Y-%m-%dT%H:%M:%fZ','now')，毫秒精度），
// 客户端与应用进程的时钟不参与裁决。SQLite 保证同一语句内多次调用
// 'now' 返回完全相同的值，因此同一条 UPDATE 里的判定与 closed_at
// 使用的是同一个数据库时刻。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"modernc.org/sqlite"
)

// 窗口状态机：open 是唯一非终态；confirmed 与 quarantined 是终态，
// 终态之后不存在任何迁移。
const (
	StateOpen        = "open"
	StateConfirmed   = "confirmed"
	StateQuarantined = "quarantined"
)

// 终态关闭原因。
const (
	ReasonDoorOpenConfirmed    = "door_open_confirmed"    // 截止时刻之前收到开门确认
	ReasonConfirmAfterDeadline = "confirm_after_deadline" // 确认到达时数据库时间已不早于截止时刻
	ReasonScanTimeout          = "scan_timeout"           // worker 扫描发现窗口已到期
)

// DefaultWindowTTLSeconds 是未配置策略设备的默认卸载窗口确认时限（秒）：
// 登记时由数据库 UTC 当前时间加该秒数生成截止时刻。配置了策略的设备以
// device_policies 中的 ttl_seconds 为准。
const DefaultWindowTTLSeconds = 30

// 卸载时限策略的合法取值范围（秒）。
const (
	MinPolicyTTLSeconds = 1
	MaxPolicyTTLSeconds = 600
)

var (
	ErrDeviceNotFound    = errors.New("device not found")
	ErrDeviceExists      = errors.New("device already exists")
	ErrWindowAlreadyOpen = errors.New("device already has an open unload window")
	ErrNoOpenWindow      = errors.New("device has no open unload window")
	ErrWindowNotFound    = errors.New("unload window not found")
	ErrInvalidPolicy     = errors.New("policy ttl_seconds must be an integer between 1 and 600")
)

// Device 是一台灭菌柜设备。
type Device struct {
	ID        string `json:"device_id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
}

// Window 是一次卸载窗口。OpenedAt / Deadline / ClosedAt 均为数据库生成的
// UTC 时间串，格式为 YYYY-MM-DDTHH:MM:SS.SSSZ（毫秒精度，字典序即时间序）。
// TTLSeconds 是登记该窗口时采用的确认时限快照（秒）：它来自登记那一刻的
// 设备策略（未配置时为默认值 30），策略后续变化不会改写已登记窗口。
type Window struct {
	ID          int64   `json:"id"`
	DeviceID    string  `json:"device_id"`
	State       string  `json:"state"`
	OpenedAt    string  `json:"opened_at"`
	Deadline    string  `json:"deadline"`
	TTLSeconds  int     `json:"ttl_seconds"`
	ClosedAt    *string `json:"closed_at"`
	CloseReason *string `json:"close_reason"`
}

// 策略来源：SourceConfigured 表示设备显式配置过策略，SourceDefault 表示
// 未配置、使用默认时限。
const (
	SourceConfigured = "configured"
	SourceDefault    = "default"
)

// DevicePolicy 是设备的卸载确认时限策略。未配置的设备没有策略行，
// GetDevicePolicy 返回 SourceDefault 与默认秒数；UpdatedAt 仅在
// SourceConfigured 时有值（数据库生成的 UTC 时间串）。
type DevicePolicy struct {
	DeviceID   string  `json:"device_id"`
	TTLSeconds int     `json:"ttl_seconds"`
	Source     string  `json:"source"`
	UpdatedAt  *string `json:"updated_at,omitempty"`
}

const schema = `
CREATE TABLE IF NOT EXISTS devices (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS unload_windows (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL REFERENCES devices(id),
    state        TEXT NOT NULL CHECK (state IN ('open', 'confirmed', 'quarantined')),
    opened_at    TEXT NOT NULL,
    deadline     TEXT NOT NULL,
    ttl_seconds  INTEGER NOT NULL DEFAULT 30,
    closed_at    TEXT,
    close_reason TEXT
);

-- 每台设备至多一条卸载时限策略；没有策略行的设备使用默认 30 秒。
-- 取值范围由 CHECK 兜底（写入前 store 层已校验）。
CREATE TABLE IF NOT EXISTS device_policies (
    device_id   TEXT PRIMARY KEY REFERENCES devices(id),
    ttl_seconds INTEGER NOT NULL CHECK (ttl_seconds BETWEEN 1 AND 600),
    updated_at  TEXT NOT NULL
);

-- 同一设备至多一个 open 窗口，由数据库部分唯一索引强制保证；
-- 并发登记时第二个 INSERT 必然因唯一约束失败。
CREATE UNIQUE INDEX IF NOT EXISTS ux_unload_windows_open_device
    ON unload_windows (device_id) WHERE state = 'open';

-- worker 按 (state='open' AND now >= deadline) 扫描，加速到期窗口定位。
CREATE INDEX IF NOT EXISTS ix_unload_windows_open_deadline
    ON unload_windows (deadline) WHERE state = 'open';
`

// windowColumns 中 ttl_seconds 是登记窗口时快照的确认时限（秒）。
const windowColumns = "id, device_id, state, opened_at, deadline, ttl_seconds, closed_at, close_reason"

// registerWindowSQL 在单条 INSERT 内完成：读取设备当前策略（无策略行时取
// 默认值）、用数据库 UTC 当前时间生成 opened_at 与 deadline（now + 采用的
// 秒数）、并把采用的秒数快照到 ttl_seconds。单条语句即一个事务，策略读取
// 与窗口写入不可分割；设备不存在时 SELECT 产出零行，INSERT 不写入任何行。
const registerWindowSQL = `
INSERT INTO unload_windows (device_id, state, opened_at, deadline, ttl_seconds)
SELECT d.id, 'open',
       strftime('%Y-%m-%dT%H:%M:%fZ','now'),
       strftime('%Y-%m-%dT%H:%M:%fZ','now',
                printf('+%d seconds', COALESCE(p.ttl_seconds, ?1))),
       COALESCE(p.ttl_seconds, ?1)
FROM devices d
LEFT JOIN device_policies p ON p.device_id = d.id
WHERE d.id = ?2
RETURNING ` + windowColumns

// confirmWindowSQL 在单条 UPDATE 内完成裁决：数据库 UTC 当前时间严格早于
// 截止时刻时写入 confirmed，等于或晚于截止时刻时写入 quarantined。
// WHERE state='open' 是原子前置条件：与 worker 的到期扫描竞争时，两条
// UPDATE 被 SQLite 串行化，只有先执行的一条能命中该行。
const confirmWindowSQL = `
UPDATE unload_windows
SET state = CASE
                WHEN strftime('%Y-%m-%dT%H:%M:%fZ','now') < deadline THEN 'confirmed'
                ELSE 'quarantined'
            END,
    closed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
    close_reason = CASE
                WHEN strftime('%Y-%m-%dT%H:%M:%fZ','now') < deadline THEN 'door_open_confirmed'
                ELSE 'confirm_after_deadline'
            END
WHERE device_id = ? AND state = 'open'
RETURNING ` + windowColumns

// quarantineExpiredSQL 与确认使用相同的时间边界：数据库 UTC 当前时间等于
// 或晚于截止时刻的 open 窗口被原子地写入 quarantined。
const quarantineExpiredSQL = `
UPDATE unload_windows
SET state = 'quarantined',
    closed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
    close_reason = 'scan_timeout'
WHERE state = 'open'
  AND strftime('%Y-%m-%dT%H:%M:%fZ','now') >= deadline`

// Store 封装对 SQLite 数据库的访问。
type Store struct {
	db *sql.DB
}

// Open 打开（必要时创建）SQLite 数据库并执行 schema 迁移。
// busy_timeout 让两个进程的写冲突在驱动层自动重试；WAL 允许读写并发。
func Open(ctx context.Context, path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	// 兼容旧库：为策略快照前创建的 unload_windows 表补充 ttl_seconds 列，
	// 既有行回填默认值 30（与它们登记时的实际时限一致）。
	if err := ensureColumn(ctx, db, "unload_windows", "ttl_seconds",
		"ALTER TABLE unload_windows ADD COLUMN ttl_seconds INTEGER NOT NULL DEFAULT 30"); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return &Store{db: db}, nil
}

// ensureColumn 在目标表缺少指定列时执行补充 DDL。SQLite 的 ADD COLUMN 不支持
// IF NOT EXISTS，因此先查 PRAGMA；api 与 worker 同时启动时可能重复执行，
// 对 “duplicate column” 错误按已成功处理。
func ensureColumn(ctx context.Context, db *sql.DB, table, column, ddl string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return fmt.Errorf("inspect table %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return fmt.Errorf("inspect table %s: %w", table, err)
		}
		if name == column {
			return rows.Close()
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect table %s: %w", table, err)
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil && !strings.Contains(err.Error(), "duplicate column") {
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
}

// Close 关闭底层数据库连接池。
func (s *Store) Close() error { return s.db.Close() }

// Ping 检查数据库连通性，供健康检查使用。
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// CreateDevice 登记一台设备；设备已存在时返回 ErrDeviceExists。
func (s *Store) CreateDevice(ctx context.Context, id, name string) (Device, error) {
	var d Device
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO devices (id, name, created_at)
		VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		RETURNING id, name, created_at`, id, name).
		Scan(&d.ID, &d.Name, &d.CreatedAt)
	switch {
	case err == nil:
		return d, nil
	case isUniqueViolation(err):
		return Device{}, ErrDeviceExists
	default:
		return Device{}, fmt.Errorf("create device: %w", err)
	}
}

// GetDevice 按 ID 查询设备；不存在时返回 ErrDeviceNotFound。
func (s *Store) GetDevice(ctx context.Context, id string) (Device, error) {
	var d Device
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, created_at FROM devices WHERE id = ?`, id).
		Scan(&d.ID, &d.Name, &d.CreatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Device{}, ErrDeviceNotFound
	case err != nil:
		return Device{}, fmt.Errorf("get device: %w", err)
	default:
		return d, nil
	}
}

// RegisterWindow 为设备登记一个卸载窗口：同一条 INSERT 内读取设备当前策略
// （未配置时为默认 30 秒），由数据库 UTC 当前时间生成 opened_at 与 deadline，
// 并把采用的秒数快照到窗口的 ttl_seconds——策略后续变化不会改动该窗口。
// 设备不存在时返回 ErrDeviceNotFound；设备已有 open 窗口时返回
// ErrWindowAlreadyOpen（由部分唯一索引在并发下同样成立）。
func (s *Store) RegisterWindow(ctx context.Context, deviceID string) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx, registerWindowSQL, DefaultWindowTTLSeconds, deviceID))
	switch {
	case err == nil:
		return w, nil
	case errors.Is(err, sql.ErrNoRows):
		// INSERT...SELECT 未命中设备行，未写入任何窗口。
		return Window{}, ErrDeviceNotFound
	case isUniqueViolation(err):
		return Window{}, ErrWindowAlreadyOpen
	case isForeignKeyViolation(err):
		return Window{}, ErrDeviceNotFound
	default:
		return Window{}, fmt.Errorf("register window: %w", err)
	}
}

// SetDevicePolicy 设置（或覆盖）设备的卸载确认时限，ttlSeconds 必须在
// [MinPolicyTTLSeconds, MaxPolicyTTLSeconds] 内，否则返回 ErrInvalidPolicy。
// 设备不存在时返回 ErrDeviceNotFound。写入是单条 UPSERT：要么完整生效，
// 要么不留下任何配置，且不影响该设备已登记的任何窗口。
func (s *Store) SetDevicePolicy(ctx context.Context, deviceID string, ttlSeconds int) (DevicePolicy, error) {
	if ttlSeconds < MinPolicyTTLSeconds || ttlSeconds > MaxPolicyTTLSeconds {
		return DevicePolicy{}, ErrInvalidPolicy
	}
	var p DevicePolicy
	var updatedAt string
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO device_policies (device_id, ttl_seconds, updated_at)
		VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT (device_id) DO UPDATE
		SET ttl_seconds = excluded.ttl_seconds,
		    updated_at  = excluded.updated_at
		RETURNING device_id, ttl_seconds, updated_at`, deviceID, ttlSeconds).
		Scan(&p.DeviceID, &p.TTLSeconds, &updatedAt)
	switch {
	case err == nil:
		p.Source = SourceConfigured
		p.UpdatedAt = &updatedAt
		return p, nil
	case isForeignKeyViolation(err):
		return DevicePolicy{}, ErrDeviceNotFound
	default:
		return DevicePolicy{}, fmt.Errorf("set device policy: %w", err)
	}
}

// GetDevicePolicy 返回设备当前生效的卸载确认时限：显式配置过时返回
// SourceConfigured 及配置值，否则返回 SourceDefault 与默认 30 秒。
// 设备不存在时返回 ErrDeviceNotFound。
func (s *Store) GetDevicePolicy(ctx context.Context, deviceID string) (DevicePolicy, error) {
	var (
		id        string
		ttl       sql.NullInt64
		updatedAt sql.NullString
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT d.id, p.ttl_seconds, p.updated_at
		FROM devices d
		LEFT JOIN device_policies p ON p.device_id = d.id
		WHERE d.id = ?`, deviceID).
		Scan(&id, &ttl, &updatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return DevicePolicy{}, ErrDeviceNotFound
	case err != nil:
		return DevicePolicy{}, fmt.Errorf("get device policy: %w", err)
	}
	p := DevicePolicy{DeviceID: id, TTLSeconds: DefaultWindowTTLSeconds, Source: SourceDefault}
	if ttl.Valid {
		p.TTLSeconds = int(ttl.Int64)
		p.Source = SourceConfigured
		p.UpdatedAt = &updatedAt.String
	}
	return p, nil
}

// ConfirmWindow 处理开门确认：数据库 UTC 当前时间严格早于截止时刻时写入
// confirmed，等于或晚于截止时刻时写入 quarantined，并返回终态窗口。
// 设备不存在时返回 ErrDeviceNotFound；没有可确认的 open 窗口（从未登记或
// 已被确认/隔离）时返回 ErrNoOpenWindow。
func (s *Store) ConfirmWindow(ctx context.Context, deviceID string) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx, confirmWindowSQL, deviceID))
	switch {
	case err == nil:
		return w, nil
	case errors.Is(err, sql.ErrNoRows):
		if _, derr := s.GetDevice(ctx, deviceID); derr != nil {
			if errors.Is(derr, ErrDeviceNotFound) {
				return Window{}, ErrDeviceNotFound
			}
			return Window{}, derr
		}
		return Window{}, ErrNoOpenWindow
	default:
		return Window{}, fmt.Errorf("confirm window: %w", err)
	}
}

// QuarantineExpired 由 worker 周期调用：把数据库 UTC 当前时间等于或晚于
// 截止时刻的 open 窗口原子地写入 quarantined，返回隔离的窗口数量。
// 与 ConfirmWindow 竞争同一窗口时，只有一条语句能命中 state='open' 的行。
func (s *Store) QuarantineExpired(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, quarantineExpiredSQL)
	if err != nil {
		return 0, fmt.Errorf("quarantine expired windows: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("quarantine expired windows: %w", err)
	}
	return n, nil
}

// GetWindow 按窗口 ID 查询；不存在时返回 ErrWindowNotFound。
func (s *Store) GetWindow(ctx context.Context, id int64) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx,
		`SELECT `+windowColumns+` FROM unload_windows WHERE id = ?`, id))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Window{}, ErrWindowNotFound
	case err != nil:
		return Window{}, fmt.Errorf("get window: %w", err)
	default:
		return w, nil
	}
}

// LatestWindow 返回设备最近一个窗口；从未登记过时返回 ErrWindowNotFound。
func (s *Store) LatestWindow(ctx context.Context, deviceID string) (Window, error) {
	w, err := scanWindow(s.db.QueryRowContext(ctx,
		`SELECT `+windowColumns+` FROM unload_windows WHERE device_id = ? ORDER BY id DESC LIMIT 1`, deviceID))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Window{}, ErrWindowNotFound
	case err != nil:
		return Window{}, fmt.Errorf("latest window: %w", err)
	default:
		return w, nil
	}
}

func scanWindow(row *sql.Row) (Window, error) {
	var w Window
	var closedAt, closeReason sql.NullString
	if err := row.Scan(&w.ID, &w.DeviceID, &w.State, &w.OpenedAt, &w.Deadline, &w.TTLSeconds, &closedAt, &closeReason); err != nil {
		return Window{}, err
	}
	if closedAt.Valid {
		w.ClosedAt = &closedAt.String
	}
	if closeReason.Valid {
		w.CloseReason = &closeReason.String
	}
	return w, nil
}

// isUniqueViolation 判定 SQLite 唯一约束冲突（SQLITE_CONSTRAINT_UNIQUE=2067）。
func isUniqueViolation(err error) bool {
	var serr *sqlite.Error
	if errors.As(err, &serr) {
		return serr.Code() == 2067
	}
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// isForeignKeyViolation 判定 SQLite 外键约束冲突（SQLITE_CONSTRAINT_FOREIGNKEY=787）。
func isForeignKeyViolation(err error) bool {
	var serr *sqlite.Error
	if errors.As(err, &serr) {
		return serr.Code() == 787
	}
	return err != nil && strings.Contains(err.Error(), "FOREIGN KEY constraint failed")
}
