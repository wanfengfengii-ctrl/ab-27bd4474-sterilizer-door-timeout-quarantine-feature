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
		t.Fatalf("window ttl_seconds = %d, want default %d", w.TTLSeconds, DefaultWindowTTLSeconds)
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
	if _, err := st.GetDevicePolicy(ctx, "NOPE"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("get policy on unknown device = %v, want ErrDeviceNotFound", err)
	}
	if _, err := st.SetDevicePolicy(ctx, "NOPE", 60); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("set policy on unknown device = %v, want ErrDeviceNotFound", err)
	}
}

// mustWindowTTL 断言窗口快照的时限与 deadline-opened_at 间隔一致。
func mustWindowTTL(t *testing.T, w Window, wantSeconds int) {
	t.Helper()
	if w.TTLSeconds != wantSeconds {
		t.Fatalf("window %d ttl_seconds = %d, want %d", w.ID, w.TTLSeconds, wantSeconds)
	}
	opened, err := time.Parse(time.RFC3339Nano, w.OpenedAt)
	if err != nil {
		t.Fatalf("parse opened_at %q: %v", w.OpenedAt, err)
	}
	deadline, err := time.Parse(time.RFC3339Nano, w.Deadline)
	if err != nil {
		t.Fatalf("parse deadline %q: %v", w.Deadline, err)
	}
	if got := deadline.Sub(opened); got != time.Duration(wantSeconds)*time.Second {
		t.Fatalf("window %d deadline - opened_at = %v, want %ds", w.ID, got, wantSeconds)
	}
}

// 未配置策略的设备：策略查询返回默认 30 秒，登记的窗口快照 30 秒。
func TestDefaultPolicyAppliesThirtySeconds(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-POL-DEFAULT")

	p, err := st.GetDevicePolicy(ctx, "STER-POL-DEFAULT")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.Source != SourceDefault || p.TTLSeconds != DefaultWindowTTLSeconds || p.UpdatedAt != nil {
		t.Fatalf("policy = %+v, want default %d seconds without updated_at", p, DefaultWindowTTLSeconds)
	}

	w := mustWindow(t, st, "STER-POL-DEFAULT")
	mustWindowTTL(t, w, DefaultWindowTTLSeconds)
}

// 策略更新只影响之后登记的窗口：新窗口按新时限生成截止时刻并快照秒数，
// 再次改策后下一个窗口使用最新值。
func TestPolicyUpdateAppliesToNewWindows(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-POL-1")

	p, err := st.SetDevicePolicy(ctx, "STER-POL-1", 45)
	if err != nil {
		t.Fatalf("set policy: %v", err)
	}
	if p.Source != SourceConfigured || p.TTLSeconds != 45 || p.UpdatedAt == nil {
		t.Fatalf("policy = %+v, want configured 45 seconds with updated_at", p)
	}

	w1 := mustWindow(t, st, "STER-POL-1")
	mustWindowTTL(t, w1, 45)
	if _, err := st.ConfirmWindow(ctx, "STER-POL-1"); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// 再次改策：下一个窗口使用新值。
	if _, err := st.SetDevicePolicy(ctx, "STER-POL-1", 10); err != nil {
		t.Fatalf("update policy: %v", err)
	}
	p, err = st.GetDevicePolicy(ctx, "STER-POL-1")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.TTLSeconds != 10 || p.Source != SourceConfigured {
		t.Fatalf("policy = %+v, want configured 10 seconds", p)
	}
	w2 := mustWindow(t, st, "STER-POL-1")
	mustWindowTTL(t, w2, 10)
	if w2.ID == w1.ID {
		t.Fatalf("expected a new window, got id %d twice", w2.ID)
	}
}

// 策略变化不得改动已登记窗口：活动窗口的快照秒数与截止时刻保持不变，
// 确认仍按原截止时刻裁决；终态后的下一个窗口才采用新策略。
func TestPolicyChangeDoesNotAffectRegisteredWindow(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-POL-2")

	before := mustWindow(t, st, "STER-POL-2")
	mustWindowTTL(t, before, DefaultWindowTTLSeconds)

	if _, err := st.SetDevicePolicy(ctx, "STER-POL-2", MaxPolicyTTLSeconds); err != nil {
		t.Fatalf("set policy: %v", err)
	}

	// 已登记窗口原封不动：快照秒数与截止时刻都与改策前一致。
	after, err := st.GetWindow(ctx, before.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if after.TTLSeconds != before.TTLSeconds || after.Deadline != before.Deadline || after.State != StateOpen {
		t.Fatalf("registered window changed after policy update: before=%+v after=%+v", before, after)
	}

	// 确认仍按原截止时刻裁决（原 30 秒窗口内确认成功）。
	done, err := st.ConfirmWindow(ctx, "STER-POL-2")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if done.State != StateConfirmed || done.TTLSeconds != DefaultWindowTTLSeconds || done.Deadline != before.Deadline {
		t.Fatalf("confirmed window = %+v, want original deadline and ttl %d", done, DefaultWindowTTLSeconds)
	}

	// 下一个窗口采用新策略。
	next := mustWindow(t, st, "STER-POL-2")
	mustWindowTTL(t, next, MaxPolicyTTLSeconds)
}

// 非法策略值被拒绝且不留下任何配置、不影响已有窗口。
func TestSetDevicePolicyValidation(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-POL-3")
	open := mustWindow(t, st, "STER-POL-3")

	for _, bad := range []int{0, -1, MinPolicyTTLSeconds - 1, MaxPolicyTTLSeconds + 1, 601, 1 << 20} {
		if _, err := st.SetDevicePolicy(ctx, "STER-POL-3", bad); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("set policy %d = %v, want ErrInvalidPolicy", bad, err)
		}
	}

	// 失败的更新没有留下半写入配置：设备仍是默认策略。
	p, err := st.GetDevicePolicy(ctx, "STER-POL-3")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.Source != SourceDefault || p.TTLSeconds != DefaultWindowTTLSeconds {
		t.Fatalf("policy = %+v after rejected updates, want default %d", p, DefaultWindowTTLSeconds)
	}
	var policyRows int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM device_policies WHERE device_id = 'STER-POL-3'`).Scan(&policyRows); err != nil {
		t.Fatalf("count policies: %v", err)
	}
	if policyRows != 0 {
		t.Fatalf("rejected updates left %d policy row(s), want 0", policyRows)
	}

	// 已有窗口不受失败的更新影响。
	cur, err := st.GetWindow(ctx, open.ID)
	if err != nil {
		t.Fatalf("get window: %v", err)
	}
	if cur.State != StateOpen || cur.TTLSeconds != open.TTLSeconds || cur.Deadline != open.Deadline {
		t.Fatalf("window changed after rejected updates: before=%+v after=%+v", open, cur)
	}

	// 边界值可用。
	for _, ok := range []int{MinPolicyTTLSeconds, MaxPolicyTTLSeconds} {
		if _, err := st.SetDevicePolicy(ctx, "STER-POL-3", ok); err != nil {
			t.Fatalf("set boundary policy %d: %v", ok, err)
		}
	}
}

// 旧库迁移：没有 ttl_seconds 列与 device_policies 表的数据库在 Open 时被
// 补充迁移；既有窗口回填默认 30 秒，登记、确认与策略功能照常工作。
func TestOpenMigratesLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")

	// 用策略特性之前的旧 schema 建库并写入一台设备与一个已确认窗口。
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	_, err = db.Exec(`
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
		INSERT INTO devices (id, name, created_at) VALUES ('LEGACY-1', 'legacy sterilizer', '2026-01-01T00:00:00.000Z');
		INSERT INTO unload_windows (device_id, state, opened_at, deadline, closed_at, close_reason)
		VALUES ('LEGACY-1', 'confirmed', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:30.000Z',
		        '2026-01-01T00:00:05.000Z', 'door_open_confirmed');`)
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open store over legacy db: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	ctx := context.Background()

	// 既有窗口回填默认 30 秒。
	legacy, err := st.LatestWindow(ctx, "LEGACY-1")
	if err != nil {
		t.Fatalf("latest window: %v", err)
	}
	if legacy.TTLSeconds != DefaultWindowTTLSeconds || legacy.State != StateConfirmed {
		t.Fatalf("legacy window = %+v, want ttl_seconds %d confirmed", legacy, DefaultWindowTTLSeconds)
	}
	p, err := st.GetDevicePolicy(ctx, "LEGACY-1")
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if p.Source != SourceDefault || p.TTLSeconds != DefaultWindowTTLSeconds {
		t.Fatalf("legacy device policy = %+v, want default %d", p, DefaultWindowTTLSeconds)
	}

	// 迁移后策略与新登记照常工作。
	if _, err := st.SetDevicePolicy(ctx, "LEGACY-1", 7); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	w := mustWindow(t, st, "LEGACY-1")
	mustWindowTTL(t, w, 7)
	if _, err := st.ConfirmWindow(ctx, "LEGACY-1"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
}
// 确认与 worker 竞争的回归：配置过策略的设备上，竞争仍只产生一个终态。
func TestConcurrentArbitrationWithPolicy(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	mustDevice(t, st, "STER-POL-RACE")
	if _, err := st.SetDevicePolicy(ctx, "STER-POL-RACE", 5); err != nil {
		t.Fatalf("set policy: %v", err)
	}

	const rounds = 10
	for i := 0; i < rounds; i++ {
		w := mustWindow(t, st, "STER-POL-RACE")
		mustWindowTTL(t, w, 5)
		setDeadline(t, st, w.ID, "+0 seconds")

		var (
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
			_, confirmErr = st.ConfirmWindow(ctx, "STER-POL-RACE")
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
		confirmWon := confirmErr == nil
		scanWon := scanned == 1
		if confirmWon == scanWon {
			t.Fatalf("round %d: exactly one side must commit the terminal state, confirmWon=%v scanWon=%v (confirmErr=%v)",
				i, confirmWon, scanWon, confirmErr)
		}
		final, err := st.GetWindow(ctx, w.ID)
		if err != nil {
			t.Fatalf("round %d: get window: %v", i, err)
		}
		if final.State != StateQuarantined {
			t.Fatalf("round %d: final state = %q, want %q", i, final.State, StateQuarantined)
		}
		if final.TTLSeconds != 5 {
			t.Fatalf("round %d: final ttl_seconds = %d, want snapshot 5", i, final.TTLSeconds)
		}
	}
}
