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
	"time"

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

// policyOf 提取状态响应中的 policy 对象。
func policyOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	p, ok := resp["policy"].(map[string]any)
	if !ok {
		t.Fatalf("response has no policy object: %v", resp)
	}
	return p
}

// 默认策略：未配置设备的状态返回默认 30 秒，登记的窗口快照 30 秒。
func TestHTTPDefaultPolicyStatus(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-P0", "name": "HTTP Sterilizer P0"})

	status, resp := do(t, h, http.MethodGet, "/devices/STER-HTTP-P0/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200 (%v)", status, resp)
	}
	p := policyOf(t, resp)
	if p["ttl_seconds"] != float64(store.DefaultWindowTTLSeconds) || p["source"] != "default" {
		t.Fatalf("policy = %v, want default %d seconds", p, store.DefaultWindowTTLSeconds)
	}

	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-P0/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("register window status = %d, want 201 (%v)", status, resp)
	}
	w := windowOf(t, resp)
	if w["ttl_seconds"] != float64(store.DefaultWindowTTLSeconds) {
		t.Fatalf("window ttl_seconds = %v, want %d", w["ttl_seconds"], store.DefaultWindowTTLSeconds)
	}
	opened, _ := time.Parse(time.RFC3339Nano, w["opened_at"].(string))
	deadline, _ := time.Parse(time.RFC3339Nano, w["deadline"].(string))
	if got := deadline.Sub(opened); got != store.DefaultWindowTTLSeconds*time.Second {
		t.Fatalf("deadline - opened_at = %v, want %ds", got, store.DefaultWindowTTLSeconds)
	}
}

// 策略更新：PUT 之后状态反映新策略，新窗口按新时限生成截止时刻并快照。
func TestHTTPPolicyUpdateFlow(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-P1", "name": "HTTP Sterilizer P1"})

	status, resp := do(t, h, http.MethodPut, "/devices/STER-HTTP-P1/policy", map[string]any{"ttl_seconds": 120})
	if status != http.StatusOK {
		t.Fatalf("update policy status = %d, want 200 (%v)", status, resp)
	}
	p := policyOf(t, resp)
	if p["ttl_seconds"] != float64(120) || p["source"] != "configured" || p["updated_at"] == nil {
		t.Fatalf("policy = %v, want configured 120 seconds with updated_at", p)
	}

	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-P1/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	if got := policyOf(t, resp)["ttl_seconds"]; got != float64(120) {
		t.Fatalf("status policy ttl_seconds = %v, want 120", got)
	}

	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-P1/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("register window status = %d, want 201 (%v)", status, resp)
	}
	w := windowOf(t, resp)
	if w["ttl_seconds"] != float64(120) {
		t.Fatalf("window ttl_seconds = %v, want 120", w["ttl_seconds"])
	}
	opened, _ := time.Parse(time.RFC3339Nano, w["opened_at"].(string))
	deadline, _ := time.Parse(time.RFC3339Nano, w["deadline"].(string))
	if got := deadline.Sub(opened); got != 120*time.Second {
		t.Fatalf("deadline - opened_at = %v, want 120s", got)
	}

	// 活动窗口不受再次改策影响：快照与截止时刻保持不变。
	status, resp = do(t, h, http.MethodPut, "/devices/STER-HTTP-P1/policy", map[string]any{"ttl_seconds": 5})
	if status != http.StatusOK {
		t.Fatalf("second update status = %d, want 200 (%v)", status, resp)
	}
	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-P1/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	if got := policyOf(t, resp)["ttl_seconds"]; got != float64(5) {
		t.Fatalf("status policy ttl_seconds = %v, want 5", got)
	}
	w = windowOf(t, resp)
	if w["ttl_seconds"] != float64(120) || w["deadline"] != deadline.Format("2006-01-02T15:04:05.000Z") {
		t.Fatalf("open window changed after policy update: %v", w)
	}

	// 确认仍按原截止时刻裁决。
	status, resp = do(t, h, http.MethodPost, "/devices/STER-HTTP-P1/confirm", nil)
	if status != http.StatusOK || windowOf(t, resp)["state"] != store.StateConfirmed {
		t.Fatalf("confirm status = %d state = %v, want 200 confirmed", status, resp)
	}
}

// 非法策略：缺失、非整数、越界都返回 400 INVALID_REQUEST；未知设备返回
// 404 DEVICE_NOT_FOUND；失败的更新不留下配置、不影响已有窗口。
func TestHTTPPolicyValidation(t *testing.T) {
	h := newTestHandler(t)
	do(t, h, http.MethodPost, "/devices", map[string]any{"device_id": "STER-HTTP-P2", "name": "HTTP Sterilizer P2"})
	status, resp := do(t, h, http.MethodPost, "/devices/STER-HTTP-P2/windows", nil)
	if status != http.StatusCreated {
		t.Fatalf("register window status = %d, want 201 (%v)", status, resp)
	}
	openDeadline := windowOf(t, resp)["deadline"]

	cases := []struct {
		name string
		body any
	}{
		{"missing field", map[string]any{}},
		{"float", map[string]any{"ttl_seconds": 1.5}},
		{"string", map[string]any{"ttl_seconds": "30"}},
		{"bool", map[string]any{"ttl_seconds": true}},
		{"null", map[string]any{"ttl_seconds": nil}},
		{"zero", map[string]any{"ttl_seconds": 0}},
		{"negative", map[string]any{"ttl_seconds": -5}},
		{"above max", map[string]any{"ttl_seconds": 601}},
		{"unknown field", map[string]any{"ttl_seconds": 30, "deadline": "2030-01-01T00:00:00.000Z"}},
	}
	for _, tc := range cases {
		status, resp := do(t, h, http.MethodPut, "/devices/STER-HTTP-P2/policy", tc.body)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400 (%v)", tc.name, status, resp)
		}
		if got := errCode(t, resp); got != CodeInvalidRequest {
			t.Fatalf("%s: error code = %q, want %q", tc.name, got, CodeInvalidRequest)
		}
	}

	// 非 JSON 请求体同样被拒绝。
	req := httptest.NewRequest(http.MethodPut, "/devices/STER-HTTP-P2/policy", bytes.NewBufferString("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("non-JSON body status = %d, want 400", rec.Code)
	}

	// 未知设备 → 404 DEVICE_NOT_FOUND。
	status, resp = do(t, h, http.MethodPut, "/devices/NOPE/policy", map[string]any{"ttl_seconds": 30})
	if status != http.StatusNotFound {
		t.Fatalf("unknown device status = %d, want 404 (%v)", status, resp)
	}
	if got := errCode(t, resp); got != CodeDeviceNotFound {
		t.Fatalf("error code = %q, want %q", got, CodeDeviceNotFound)
	}

	// 失败的更新不留下半写入配置，也不影响已有窗口。
	status, resp = do(t, h, http.MethodGet, "/devices/STER-HTTP-P2/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status query = %d, want 200", status)
	}
	p := policyOf(t, resp)
	if p["source"] != "default" || p["ttl_seconds"] != float64(store.DefaultWindowTTLSeconds) {
		t.Fatalf("policy = %v after rejected updates, want default %d", p, store.DefaultWindowTTLSeconds)
	}
	w := windowOf(t, resp)
	if w["state"] != store.StateOpen || w["deadline"] != openDeadline ||
		w["ttl_seconds"] != float64(store.DefaultWindowTTLSeconds) {
		t.Fatalf("window changed after rejected updates: %v", w)
	}
}
