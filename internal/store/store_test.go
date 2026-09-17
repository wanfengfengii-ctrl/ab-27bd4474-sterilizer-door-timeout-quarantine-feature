package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func mustDevice(t *testing.T, st *Store, id string) {
	t.Helper()
	if _, err := st.CreateDevice(context.Background(), id, "device "+id); err != nil {
		t.Fatalf("create device %s: %v", id, err)
	}
}

func mustWindow(t *testing.T, st *Store, deviceID string) Window {
	t.Helper()
	w, err := st.RegisterWindow(context.Background(), deviceID)
	if err != nil {
		t.Fatalf("register window for %s: %v", deviceID, err)
	}
	return w
}

// setDeadline 用数据库自身的时间函数把窗口截止时刻改写为 now+offset，
// 使测试可以精确地把窗口放到边界上或过去，客户端时钟不参与。
func setDeadline(t *testing.T, st *Store, id int64, offset string) string {
	t.Helper()
	var deadline string
	err := st.db.QueryRow(`
		UPDATE unload_windows
		SET deadline = strftime('%Y-%m-%dT%H:%M:%fZ','now', ?)
		WHERE id = ?
		RETURNING deadline`, offset, id).Scan(&deadline)
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	return deadline
}

// 及时确认：数据库时间严格早于截止时刻 → confirmed。
func TestConfirmWithinDeadline(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-01")

	w := mustWindow(t, st, "STER-01")
	if w.State != StateOpen {
		t.Fatalf("new window state = %q, want %q", w.State, StateOpen)
	}
	opened, err := time.Parse(time.RFC3339Nano, w.OpenedAt)
	if err != nil {
		t.Fatalf("parse opened_at %q: %v", w.OpenedAt, err)
	}
	deadline, err := time.Parse(time.RFC3339Nano, w.Deadline)
	if err != nil {
		t.Fatalf("parse deadline %q: %v", w.Deadline, err)
	}
	if got := deadline.Sub(opened); got != DefaultWindowTTLSeconds*time.Second {
		t.Fatalf("deadline - opened_at = %v, want %v", got, DefaultWindowTTLSeconds*time.Second)
	}
	if w.TTLSeconds != DefaultWindowTTLSeconds {
		t.Fatalf("ttl_seconds snapshot = %d, want %d", w.TTLSeconds, DefaultWindowTTLSeconds)
	}

	done, err := st.ConfirmWindow(ctx, "STER-01")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if done.State != StateConfirmed {
		t.Fatalf("state = %q, want %q", done.State, StateConfirmed)
	}
	if done.CloseReason == nil || *done.CloseReason != ReasonDoorOpenConfirmed {
		t.Fatalf("close_reason = %v, want %q", done.CloseReason, ReasonDoorOpenConfirmed)
	}
	if done.ClosedAt == nil {
		t.Fatal("closed_at must be set on terminal window")
	}
	if !(*done.ClosedAt < done.Deadline) {
		t.Fatalf("closed_at %q must be strictly before deadline %q", *done.ClosedAt, done.Deadline)
	}

	// 通过查询断言唯一最终状态。
	got, err := st.GetWindow(ctx, done.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if got.State != StateConfirmed {
		t.Fatalf("persisted state = %q, want %q", got.State, StateConfirmed)
	}
	latest, err := st.LatestWindow(ctx, "STER-01")
	if err != nil {
		t.Fatalf("latest window: %v", err)
	}
	if latest.ID != done.ID || latest.State != StateConfirmed {
		t.Fatalf("latest = %+v, want id=%d confirmed", latest, done.ID)
	}
}

// 无人确认：worker 扫描把到期窗口写入 quarantined，且扫描幂等。
func TestScanQuarantinesExpiredWindow(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-02")

	w := mustWindow(t, st, "STER-02")
	deadline := setDeadline(t, st, w.ID, "-1 seconds")

	n, err := st.QuarantineExpired(ctx)
	if err != nil {
		t.Fatalf("quarantine expired: %v", err)
	}
	if n != 1 {
		t.Fatalf("quarantined %d windows, want 1", n)
	}

	got, err := st.GetWindow(ctx, w.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if got.State != StateQuarantined {
		t.Fatalf("state = %q, want %q", got.State, StateQuarantined)
	}
	if got.CloseReason == nil || *got.CloseReason != ReasonScanTimeout {
		t.Fatalf("close_reason = %v, want %q", got.CloseReason, ReasonScanTimeout)
	}
	if got.ClosedAt == nil {
		t.Fatal("closed_at must be set on terminal window")
	}
	if *got.ClosedAt < deadline {
		t.Fatalf("closed_at %q must be at or after deadline %q", *got.ClosedAt, deadline)
	}

	// 再次扫描不应命中任何窗口：终态不会重复迁移。
	n, err = st.QuarantineExpired(ctx)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if n != 0 {
		t.Fatalf("second scan quarantined %d windows, want 0", n)
	}
}

// 确认到达时数据库时间已不早于截止时刻 → quarantined（等号归属隔离侧）。
func TestConfirmAfterDeadlineWritesQuarantined(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-03")

	w := mustWindow(t, st, "STER-03")
	setDeadline(t, st, w.ID, "-1 seconds")

	done, err := st.ConfirmWindow(ctx, "STER-03")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if done.State != StateQuarantined {
		t.Fatalf("state = %q, want %q", done.State, StateQuarantined)
	}
	if done.CloseReason == nil || *done.CloseReason != ReasonConfirmAfterDeadline {
		t.Fatalf("close_reason = %v, want %q", done.CloseReason, ReasonConfirmAfterDeadline)
	}
	if done.ClosedAt == nil || *done.ClosedAt < done.Deadline {
		t.Fatalf("closed_at %v must be at or after deadline %q", done.ClosedAt, done.Deadline)
	}
}

// 边界并发裁决：截止时刻被精确设置为数据库当前时间，确认请求与超时扫描
// 同时到达。两侧按相同边界都只能写 quarantined，且带 open 前置条件的原子
// UPDATE 保证只有一个终态被提交。
func TestConcurrentArbitrationAtDeadline(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-RACE")

	const rounds = 25
	for i := 0; i < rounds; i++ {
		w := mustWindow(t, st, "STER-RACE")
		// 把截止时刻精确放到数据库当前时间上：确认与扫描都落在边界。
		setDeadline(t, st, w.ID, "+0 seconds")

		var (
			confirmWin Window
			confirmErr error
			scanned    int64
			scanErr    error
		)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			confirmWin, confirmErr = st.ConfirmWindow(ctx, "STER-RACE")
		}()
		go func() {
			defer wg.Done()
			<-start
			scanned, scanErr = st.QuarantineExpired(ctx)
		}()
		close(start)
		wg.Wait()

		if scanErr != nil {
			t.Fatalf("round %d: scan: %v", i, scanErr)
		}
		if scanned != 0 && scanned != 1 {
			t.Fatalf("round %d: scan affected %d windows, want 0 or 1", i, scanned)
		}
		confirmWon := confirmErr == nil
		scanWon := scanned == 1
		if confirmWon == scanWon {
			t.Fatalf("round %d: exactly one side must commit the terminal state, confirmWon=%v scanWon=%v (confirmErr=%v)",
				i, confirmWon, scanWon, confirmErr)
		}
		if !confirmWon && !errors.Is(confirmErr, ErrNoOpenWindow) {
			t.Fatalf("round %d: losing confirm must return ErrNoOpenWindow, got %v", i, confirmErr)
		}
		if confirmWon {
			if confirmWin.State != StateQuarantined {
				t.Fatalf("round %d: confirm at the boundary must quarantine, got %q", i, confirmWin.State)
			}
			if confirmWin.CloseReason == nil || *confirmWin.CloseReason != ReasonConfirmAfterDeadline {
				t.Fatalf("round %d: close_reason = %v, want %q", i, confirmWin.CloseReason, ReasonConfirmAfterDeadline)
			}
		}

		// 通过查询断言唯一最终状态：该窗口有且仅有一条记录，且为 quarantined。
		var count int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM unload_windows WHERE id = ?`, w.ID).Scan(&count); err != nil {
			t.Fatalf("round %d: count windows: %v", i, err)
		}
		if count != 1 {
			t.Fatalf("round %d: found %d rows for window %d, want exactly 1", i, count, w.ID)
		}
		final, err := st.GetWindow(ctx, w.ID)
		if err != nil {
			t.Fatalf("round %d: get window: %v", i, err)
		}
		if final.State != StateQuarantined {
			t.Fatalf("round %d: final state = %q, want %q", i, final.State, StateQuarantined)
		}
		if final.ClosedAt == nil {
			t.Fatalf("round %d: closed_at must be set", i)
		}
		if final.CloseReason == nil ||
			(*final.CloseReason != ReasonConfirmAfterDeadline && *final.CloseReason != ReasonScanTimeout) {
			t.Fatalf("round %d: close_reason = %v, want %q or %q",
				i, final.CloseReason, ReasonConfirmAfterDeadline, ReasonScanTimeout)
		}
	}
}

// 活动窗口冲突：同一设备只允许一个 open 窗口；并发登记时恰好一个成功。
func TestOneOpenWindowPerDevice(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-04")

	mustWindow(t, st, "STER-04")
	if _, err := st.RegisterWindow(ctx, "STER-04"); !errors.Is(err, ErrWindowAlreadyOpen) {
		t.Fatalf("second register = %v, want ErrWindowAlreadyOpen", err)
	}

	if _, err := st.ConfirmWindow(ctx, "STER-04"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// 终态之后允许登记下一个窗口。
	if _, err := st.RegisterWindow(ctx, "STER-04"); err != nil {
		t.Fatalf("register after close: %v", err)
	}
	if _, err := st.ConfirmWindow(ctx, "STER-04"); err != nil {
		t.Fatalf("confirm second window: %v", err)
	}

	// 并发登记：N 个 goroutine 同时登记，数据库唯一索引保证恰好一个成功。
	mustDevice(t, st, "STER-05")
	const n = 8
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = st.RegisterWindow(ctx, "STER-05")
		}(i)
	}
	close(start)
	wg.Wait()

	successes := 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrWindowAlreadyOpen):
		default:
			t.Fatalf("unexpected register error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent register successes = %d, want exactly 1", successes)
	}

	// 通过查询断言：该设备当前有且仅有一个 open 窗口。
	var openCount int
	if err := st.db.QueryRow(
		`SELECT COUNT(*) FROM unload_windows WHERE device_id = 'STER-05' AND state = 'open'`).Scan(&openCount); err != nil {
		t.Fatalf("count open windows: %v", err)
	}
	if openCount != 1 {
		t.Fatalf("open windows = %d, want 1", openCount)
	}
}

// 未知设备的登记与确认都返回 ErrDeviceNotFound。
func TestUnknownDevice(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	if _, err := st.RegisterWindow(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("register on unknown device = %v, want ErrDeviceNotFound", err)
	}
	if _, err := st.ConfirmWindow(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("confirm on unknown device = %v, want ErrDeviceNotFound", err)
	}
	if _, err := st.GetDevice(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("get unknown device = %v, want ErrDeviceNotFound", err)
	}
}

// 策略快照语义：未配置设备用默认 30 秒；配置后新窗口采用新时限；再次改策
// 不回改活动窗口（deadline 与 ttl_seconds 快照均不变）；活动窗口关闭后
// 登记的下一个窗口采用最新策略。
func TestPolicySnapshotSemantics(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-P1")

	// 未配置：默认策略。
	p, err := st.GetDevicePolicy(ctx, "STER-P1")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.TTLSeconds != DefaultWindowTTLSeconds || p.Source != PolicySourceDefault || p.UpdatedAt != nil {
		t.Fatalf("default policy = %+v, want ttl=%d source=default updated_at=nil",
			p, DefaultWindowTTLSeconds)
	}
	w0 := mustWindow(t, st, "STER-P1")
	if w0.TTLSeconds != DefaultWindowTTLSeconds {
		t.Fatalf("default window ttl_seconds = %d, want %d", w0.TTLSeconds, DefaultWindowTTLSeconds)
	}
	if _, err := st.ConfirmWindow(ctx, "STER-P1"); err != nil {
		t.Fatalf("confirm default window: %v", err)
	}

	// 配置 10 秒：新窗口采用新时限。
	p, err = st.SetDevicePolicy(ctx, "STER-P1", 10)
	if err != nil {
		t.Fatalf("set policy 10: %v", err)
	}
	if p.TTLSeconds != 10 || p.Source != PolicySourceCustom || p.UpdatedAt == nil {
		t.Fatalf("custom policy = %+v, want ttl=10 source=custom updated_at set", p)
	}
	w1 := mustWindow(t, st, "STER-P1")
	assertWindowTTL(t, w1, 10)

	// 再次改策为 600 秒：活动窗口 w1 的 deadline 与快照不得改变。
	if _, err := st.SetDevicePolicy(ctx, "STER-P1", MaxWindowTTLSeconds); err != nil {
		t.Fatalf("set policy 600: %v", err)
	}
	cur, err := st.GetWindow(ctx, w1.ID)
	if err != nil {
		t.Fatalf("get active window: %v", err)
	}
	if cur.TTLSeconds != 10 || cur.Deadline != w1.Deadline || cur.State != StateOpen {
		t.Fatalf("active window changed after policy update: %+v, want ttl=10 deadline=%q open",
			cur, w1.Deadline)
	}
	// 当前策略查询反映最新配置。
	p, err = st.GetDevicePolicy(ctx, "STER-P1")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.TTLSeconds != MaxWindowTTLSeconds || p.Source != PolicySourceCustom {
		t.Fatalf("policy = %+v, want ttl=%d source=custom", p, MaxWindowTTLSeconds)
	}

	// 活动窗口仍在 10 秒时限内，确认成功。
	done, err := st.ConfirmWindow(ctx, "STER-P1")
	if err != nil {
		t.Fatalf("confirm w1: %v", err)
	}
	if done.State != StateConfirmed || done.TTLSeconds != 10 {
		t.Fatalf("confirmed window = %+v, want confirmed ttl=10", done)
	}

	// 下一个窗口采用最新策略 600 秒。
	w2 := mustWindow(t, st, "STER-P1")
	assertWindowTTL(t, w2, MaxWindowTTLSeconds)
}

// assertWindowTTL 断言窗口快照了 want 秒时限，且 deadline = opened_at + want。
func assertWindowTTL(t *testing.T, w Window, want int) {
	t.Helper()
	if w.TTLSeconds != want {
		t.Fatalf("window %d ttl_seconds = %d, want %d", w.ID, w.TTLSeconds, want)
	}
	opened, err := time.Parse(time.RFC3339Nano, w.OpenedAt)
	if err != nil {
		t.Fatalf("parse opened_at %q: %v", w.OpenedAt, err)
	}
	deadline, err := time.Parse(time.RFC3339Nano, w.Deadline)
	if err != nil {
		t.Fatalf("parse deadline %q: %v", w.Deadline, err)
	}
	if got := deadline.Sub(opened); got != time.Duration(want)*time.Second {
		t.Fatalf("window %d deadline - opened_at = %v, want %v", w.ID, got, time.Duration(want)*time.Second)
	}
}

// 策略校验：越界值返回 ErrInvalidPolicyTTL 且不留下半写入配置；未知设备
// 返回 ErrDeviceNotFound；边界值 1 与 600 合法。
func TestSetDevicePolicyValidation(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-P2")

	for _, ttl := range []int{0, -1, 601, 1 << 30} {
		if _, err := st.SetDevicePolicy(ctx, "STER-P2", ttl); !errors.Is(err, ErrInvalidPolicyTTL) {
			t.Fatalf("set policy %d = %v, want ErrInvalidPolicyTTL", ttl, err)
		}
	}
	// 越界写入不得留下任何配置：仍为默认策略。
	p, err := st.GetDevicePolicy(ctx, "STER-P2")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.TTLSeconds != DefaultWindowTTLSeconds || p.Source != PolicySourceDefault {
		t.Fatalf("policy after rejected writes = %+v, want default %d", p, DefaultWindowTTLSeconds)
	}

	for _, ttl := range []int{MinWindowTTLSeconds, MaxWindowTTLSeconds} {
		if _, err := st.SetDevicePolicy(ctx, "STER-P2", ttl); err != nil {
			t.Fatalf("set boundary policy %d: %v", ttl, err)
		}
	}
	// 最后一次成功写入生效。
	p, err = st.GetDevicePolicy(ctx, "STER-P2")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.TTLSeconds != MaxWindowTTLSeconds || p.Source != PolicySourceCustom {
		t.Fatalf("policy = %+v, want ttl=%d source=custom", p, MaxWindowTTLSeconds)
	}
	// 越界更新不得破坏已有配置。
	if _, err := st.SetDevicePolicy(ctx, "STER-P2", 0); !errors.Is(err, ErrInvalidPolicyTTL) {
		t.Fatalf("set policy 0 = %v, want ErrInvalidPolicyTTL", err)
	}
	p, err = st.GetDevicePolicy(ctx, "STER-P2")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.TTLSeconds != MaxWindowTTLSeconds || p.Source != PolicySourceCustom {
		t.Fatalf("policy after failed update = %+v, want ttl=%d source=custom", p, MaxWindowTTLSeconds)
	}

	if _, err := st.SetDevicePolicy(ctx, "NOPE", 30); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("set policy on unknown device = %v, want ErrDeviceNotFound", err)
	}
	if _, err := st.GetDevicePolicy(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("get policy on unknown device = %v, want ErrDeviceNotFound", err)
	}
}

// 既有数据库迁移：旧 schema（unload_windows 无 ttl_seconds 列、无
// device_policies 表）的库文件在 Open 后自动补列建表，存量窗口回填默认
// 时限，新登记与策略功能正常。
func TestOpenMigratesLegacyDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")

	// 用上线前的旧 schema 建库并写入存量数据。
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	_, err = raw.ExecContext(ctx, `
		CREATE TABLE devices (
		    id         TEXT PRIMARY KEY,
		    name       TEXT NOT NULL,
		    created_at TEXT NOT NULL
		);
		CREATE TABLE unload_windows (
		    id           INTEGER PRIMARY KEY AUTOINCREMENT,
		    device_id    TEXT NOT NULL REFERENCES devices(id),
		    state        TEXT NOT NULL CHECK (state IN ('open', 'confirmed', 'quarantined')),
		    opened_at    TEXT NOT NULL,
		    deadline     TEXT NOT NULL,
		    closed_at    TEXT,
		    close_reason TEXT
		);
		CREATE UNIQUE INDEX ux_unload_windows_open_device
		    ON unload_windows (device_id) WHERE state = 'open';
		INSERT INTO devices (id, name, created_at)
		    VALUES ('LEGACY', 'legacy sterilizer', '2026-01-01T00:00:00.000Z');
		INSERT INTO unload_windows (device_id, state, opened_at, deadline, closed_at, close_reason)
		    VALUES ('LEGACY', 'confirmed',
		            '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:30.000Z',
		            '2026-01-01T00:00:05.000Z', 'door_open_confirmed');`)
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	st, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	// 存量窗口回填默认时限 30 秒。
	legacy, err := st.GetWindow(ctx, 1)
	if err != nil {
		t.Fatalf("get legacy window: %v", err)
	}
	if legacy.TTLSeconds != DefaultWindowTTLSeconds {
		t.Fatalf("legacy window ttl_seconds = %d, want backfilled %d",
			legacy.TTLSeconds, DefaultWindowTTLSeconds)
	}
	if legacy.State != StateConfirmed {
		t.Fatalf("legacy window state = %q, want %q", legacy.State, StateConfirmed)
	}

	// 新登记窗口快照默认时限；策略功能在迁移后的库上正常。
	w := mustWindow(t, st, "LEGACY")
	assertWindowTTL(t, w, DefaultWindowTTLSeconds)
	if _, err := st.SetDevicePolicy(ctx, "LEGACY", 45); err != nil {
		t.Fatalf("set policy on migrated db: %v", err)
	}
	p, err := st.GetDevicePolicy(ctx, "LEGACY")
	if err != nil {
		t.Fatalf("get policy on migrated db: %v", err)
	}
	if p.TTLSeconds != 45 || p.Source != PolicySourceCustom {
		t.Fatalf("policy = %+v, want ttl=45 source=custom", p)
	}

	// 重复打开已迁移的库是空操作（模拟进程重启）。
	st2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	st2.Close()
}

// 并发迁移：api 与 worker 两进程可能同时启动并打开同一个旧库，
// 两边的 Open 都必须成功（重复列迁移被识别为已完成）。
func TestConcurrentOpenMigratesLegacyDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-race.db")

	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	_, err = raw.ExecContext(ctx, `
		CREATE TABLE devices (
		    id         TEXT PRIMARY KEY,
		    name       TEXT NOT NULL,
		    created_at TEXT NOT NULL
		);
		CREATE TABLE unload_windows (
		    id           INTEGER PRIMARY KEY AUTOINCREMENT,
		    device_id    TEXT NOT NULL REFERENCES devices(id),
		    state        TEXT NOT NULL CHECK (state IN ('open', 'confirmed', 'quarantined')),
		    opened_at    TEXT NOT NULL,
		    deadline     TEXT NOT NULL,
		    closed_at    TEXT,
		    close_reason TEXT
		);
		CREATE UNIQUE INDEX ux_unload_windows_open_device
		    ON unload_windows (device_id) WHERE state = 'open';
		INSERT INTO devices (id, name, created_at)
		    VALUES ('LEGACY-RACE', 'legacy sterilizer', '2026-01-01T00:00:00.000Z');`)
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	const n = 4
	stores := make([]*Store, n)
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			stores[i], errs[i] = Open(ctx, path)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent open %d: %v", i, err)
		}
	}
	defer func() {
		for _, st := range stores {
			if st != nil {
				st.Close()
			}
		}
	}()

	// 迁移恰好发生一次：列存在，且每个打开者都能正常使用策略与登记。
	w, err := stores[0].RegisterWindow(ctx, "LEGACY-RACE")
	if err != nil {
		t.Fatalf("register after concurrent migrate: %v", err)
	}
	assertWindowTTL(t, w, DefaultWindowTTLSeconds)
	if _, err := stores[n-1].SetDevicePolicy(ctx, "LEGACY-RACE", 20); err != nil {
		t.Fatalf("set policy after concurrent migrate: %v", err)
	}
}
