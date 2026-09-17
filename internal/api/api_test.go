package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"cssd-unload/internal/store"
)

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "api-test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewServer(st).Handler()
}

func do(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("%s %s: response is not JSON: %v: %s", method, path, err, rec.Body.String())
	}
	return rec.Code, parsed
}

func errCode(t *testing.T, resp map[string]any) string {
	t.Helper()
	e, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("response has no error object: %v", resp)
	}
	code, _ := e["code"].(string)
	return code
}

func windowOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	w, ok := resp["window"].(map[string]any)
	if !ok {
		t.Fatalf("response has no window object: %v", resp)
	}
	return w
}

func TestHTTPConfirmFlow(t *testing.T) {
	h := newTestHandler(t)

	status, _ := do(t, h, http.MethodPost, "/devices",
		map[string]any{"device_id": "STER-HTTP-1", "name": "HTTP Sterilizer 1"})
	if status != http.StatusCreated {
		t.Fatalf("create device status = %d, want 201", status)
	}

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-1/windows", map[string]any{})
	if status != http.StatusCreated {
		t.Fatalf("register window status = %d, want 201 (%v)", status, resp)
	}
	if got := windowOf(t, resp)["state"]; got != store.StateOpen {
		t.Fatalf("window state = %v, want open", got)
	}

	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-1/confirm", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("confirm status = %d, want 200 (%v)", status, resp)
	}
	if got := windowOf(t, resp)["state"]; got != store.StateConfirmed {
		t.Fatalf("window state = %v, want confirmed", got)
	}

	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-1/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	if got := windowOf(t, resp)["state"]; got != store.StateConfirmed {
		t.Fatalf("queried state = %v, want confirmed", got)
	}
}

func TestHTTPRegisterConflictReturnsStructuredError(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-2", "name": "HTTP Sterilizer 2"})

	status, _ := do(t, h, http.MethodPost, "/devices/STER-HTTP-2/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("first register status = %d, want 201", status)
	}

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-2/windows", nil)
	if status != http.StatusConflict {
		t.Fatalf("second register status = %d, want 409 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeWindowAlreadyOpen {
		t.Fatalf("error code = %q, want %q", got, CodeWindowAlreadyOpen)
	}
	e := resp["error"].(map[string]any)
	details, ok := e["details"].(map[string]any)
	if !ok || details["deadline"] == nil {
		t.Fatalf("conflict error must carry current window details: %v", e)
	}
}

func TestHTTPClientCannotSetDeadline(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-3", "name": "HTTP Sterilizer 3"})

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-3/windows",
		map[string]any{"deadline": "2030-01-01T00:00:00.000Z"})
	if status != http.StatusBadRequest {
		t.Fatalf("register with client deadline status = %d, want 400 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeInvalidRequest {
		t.Fatalf("error code = %q, want %q", got, CodeInvalidRequest)
	}

	// 被拒绝的请求不得产生任何窗口。
	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-3/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	if resp["window"] != nil {
		t.Fatalf("rejected request must not create a window: %v", resp["window"])
	}
}

func TestHTTPConfirmWithoutOpenWindow(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-4", "name": "HTTP Sterilizer 4"})

	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-4/confirm", nil)
	if status != http.StatusConflict {
		t.Fatalf("confirm without window status = %d, want 409 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeNoOpenWindow {
		t.Fatalf("error code = %q, want %q", got, CodeNoOpenWindow)
	}

	status, resp = do(t, h, http.MethodPost, "/devices/NOPE/confirm", nil)
	if status != http.StatusNotFound {
		t.Fatalf("confirm on unknown device status = %d, want 404 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeDeviceNotFound {
		t.Fatalf("error code = %q, want %q", got, CodeDeviceNotFound)
	}
}

// policyOf 提取响应中的 policy 对象。
func policyOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	p, ok := resp["policy"].(map[string]any)
	if !ok {
		t.Fatalf("response has no policy object: %v", resp)
	}
	return p
}

// 策略全流程：默认 30 秒 → 更新为 10 秒 → 新窗口采用新时限 → 再次改策
// 不影响活动窗口 → 下一个窗口采用最新策略；状态查询始终返回当前策略与
// 窗口采用的时限快照。
func TestHTTPDevicePolicyFlow(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-P", "name": "HTTP Sterilizer P"})

	// 未配置：状态查询返回默认策略，window 为 null。
	status, resp := do(t, h, http.MethodGet, "/devices/STER-HTTP-P/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	p := policyOf(t, resp)
	if p["ttl_seconds"] != float64(30) || p["source"] != "default" {
		t.Fatalf("default policy = %v, want ttl_seconds=30 source=default", p)
	}
	if resp["window"] != nil {
		t.Fatalf("window = %v, want nil", resp["window"])
	}

	// 更新策略为 10 秒。
	status, resp = do(t, h, http.MethodPut, "/devices/STER-HTTP-P/policy",
		map[string]any{"ttl_seconds": 10})
	if status != http.StatusOK {
		t.Fatalf("update policy status = %d, want 200 (%v)", status, resp)
	}
	p = policyOf(t, resp)
	if p["ttl_seconds"] != float64(10) || p["source"] != "custom" || p["updated_at"] == nil {
		t.Fatalf("updated policy = %v, want ttl_seconds=10 source=custom updated_at set", p)
	}

	// 新窗口采用新时限。
	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-P/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("register window status = %d, want 201 (%v)", status, resp)
	}
	w := windowOf(t, resp)
	if w["ttl_seconds"] != float64(10) {
		t.Fatalf("window ttl_seconds = %v, want 10", w["ttl_seconds"])
	}
	deadline, _ := w["deadline"].(string)

	// 再次改策为 600 秒：活动窗口的截止时刻与时限快照不得改变。
	status, resp = do(t, h, http.MethodPut, "/devices/STER-HTTP-P/policy",
		map[string]any{"ttl_seconds": 600})
	if status != http.StatusOK {
		t.Fatalf("second policy update status = %d, want 200 (%v)", status, resp)
	}
	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-P/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	p = policyOf(t, resp)
	if p["ttl_seconds"] != float64(600) || p["source"] != "custom" {
		t.Fatalf("current policy = %v, want ttl_seconds=600 source=custom", p)
	}
	w = windowOf(t, resp)
	if w["ttl_seconds"] != float64(10) || w["deadline"] != deadline || w["state"] != store.StateOpen {
		t.Fatalf("active window changed after policy update: %v, want ttl_seconds=10 deadline=%q open",
			w, deadline)
	}

	// 活动窗口仍在 10 秒时限内，确认成功。
	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-P/confirm", nil)
	if status != http.StatusOK {
		t.Fatalf("confirm status = %d, want 200 (%v)", status, resp)
	}
	if got := windowOf(t, resp)["state"]; got != store.StateConfirmed {
		t.Fatalf("window state = %v, want confirmed", got)
	}

	// 下一个窗口采用最新策略 600 秒。
	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-P/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("register window status = %d, want 201 (%v)", status, resp)
	}
	if got := windowOf(t, resp)["ttl_seconds"]; got != float64(600) {
		t.Fatalf("window ttl_seconds = %v, want 600", got)
	}
}

// 策略参数校验：缺失、非整数、越界、未知字段都返回 400 INVALID_REQUEST；
// 未知设备返回 404 DEVICE_NOT_FOUND；失败的更新不留下半写入配置。
func TestHTTPDevicePolicyValidation(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-V", "name": "HTTP Sterilizer V"})

	badBodies := []struct {
		name string
		body any
	}{
		{"missing ttl_seconds", map[string]any{}},
		{"string value", map[string]any{"ttl_seconds": "30"}},
		{"float value", map[string]any{"ttl_seconds": 1.5}},
		{"bool value", map[string]any{"ttl_seconds": true}},
		{"null value", map[string]any{"ttl_seconds": nil}},
		{"below min", map[string]any{"ttl_seconds": 0}},
		{"negative", map[string]any{"ttl_seconds": -5}},
		{"above max", map[string]any{"ttl_seconds": 601}},
		{"unknown field", map[string]any{"ttl_seconds": 30, "deadline": "2030-01-01T00:00:00.000Z"}},
	}
	for _, tc := range badBodies {
		t.Run(tc.name, func(t *testing.T) {
			status, resp := do(t, h, http.MethodPut, "/devices/STER-HTTP-V/policy", tc.body)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%v)", status, resp)
			}
			if got := errCode(t, resp); got != CodeInvalidRequest {
				t.Fatalf("error code = %q, want %q", got, CodeInvalidRequest)
			}
			e := resp["error"].(map[string]any)
			details, ok := e["details"].(map[string]any)
			if !ok || details["field"] != "ttl_seconds" || details["min"] != float64(1) || details["max"] != float64(600) {
				t.Fatalf("error details = %v, want field/min/max", e)
			}
		})
	}

	// 边界值合法。
	for _, ttl := range []int{1, 600} {
		status, resp := do(t, h, http.MethodPut, "/devices/STER-HTTP-V/policy",
			map[string]any{"ttl_seconds": ttl})
		if status != http.StatusOK {
			t.Fatalf("boundary ttl %d status = %d, want 200 (%v)", ttl, status, resp)
		}
	}

	// 未知设备：404 DEVICE_NOT_FOUND。
	status, resp := do(t, h, http.MethodPut, "/devices/NOPE/policy",
		map[string]any{"ttl_seconds": 30})
	if status != http.StatusNotFound {
		t.Fatalf("unknown device status = %d, want 404 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeDeviceNotFound {
		t.Fatalf("error code = %q, want %q", got, CodeDeviceNotFound)
	}

	// 失败的更新不留半写入：先置为 45，再用越界值更新，配置保持 45。
	status, _ = do(t, h, http.MethodPut, "/devices/STER-HTTP-V/policy", map[string]any{"ttl_seconds": 45})
	if status != http.StatusOK {
		t.Fatalf("set policy 45 status = %d, want 200", status)
	}
	status, _ = do(t, h, http.MethodPut, "/devices/STER-HTTP-V/policy", map[string]any{"ttl_seconds": 0})
	if status != http.StatusBadRequest {
		t.Fatalf("out-of-range update status = %d, want 400", status)
	}
	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-V/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	if p := policyOf(t, resp); p["ttl_seconds"] != float64(45) || p["source"] != "custom" {
		t.Fatalf("policy after failed update = %v, want ttl_seconds=45 source=custom", p)
	}
}
