// verify 是一次性验收客户端：对运行中的 API + worker 执行端到端验收，
// 覆盖及时确认、无人确认超时隔离、活动窗口冲突、客户端禁止指定截止时刻、
// 设备卸载时限策略（默认值、更新生效、快照不受改策影响、参数校验）以及
// 终态后重复确认，全部通过则以退出码 0 结束。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type window struct {
	ID          int64   `json:"id"`
	DeviceID    string  `json:"device_id"`
	State       string  `json:"state"`
	OpenedAt    string  `json:"opened_at"`
	Deadline    string  `json:"deadline"`
	ClosedAt    *string `json:"closed_at"`
	CloseReason *string `json:"close_reason"`
	TTLSeconds  int     `json:"ttl_seconds"`
}

type policy struct {
	DeviceID   string  `json:"device_id"`
	TTLSeconds int     `json:"ttl_seconds"`
	Source     string  `json:"source"`
	UpdatedAt  *string `json:"updated_at"`
}

type checker struct {
	base   string
	client *http.Client
	failed int
}

func (c *checker) check(name string, ok bool, detail string) {
	if ok {
		fmt.Printf("PASS  %s\n", name)
		return
	}
	c.failed++
	fmt.Printf("FAIL  %s: %s\n", name, detail)
}

func (c *checker) doJSON(method, path string, body any) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func decodeWindow(data []byte) (window, error) {
	var resp struct {
		Window *window `json:"window"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return window{}, err
	}
	if resp.Window == nil {
		return window{}, fmt.Errorf("response has no window field: %s", data)
	}
	return *resp.Window, nil
}

func decodeStatus(data []byte) (*window, *policy, error) {
	var resp struct {
		Window *window `json:"window"`
		Policy *policy `json:"policy"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, nil, err
	}
	if resp.Policy == nil {
		return nil, nil, fmt.Errorf("response has no policy field: %s", data)
	}
	return resp.Window, resp.Policy, nil
}

func decodePolicy(data []byte) (policy, error) {
	var resp struct {
		Policy *policy `json:"policy"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return policy{}, err
	}
	if resp.Policy == nil {
		return policy{}, fmt.Errorf("response has no policy field: %s", data)
	}
	return *resp.Policy, nil
}

func decodeErrCode(data []byte) string {
	var resp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return ""
	}
	return resp.Error.Code
}

func main() {
	base := strings.TrimRight(envOr("API_BASE_URL", "http://localhost:8080"), "/")
	c := &checker{base: base, client: &http.Client{Timeout: 5 * time.Second}}

	// 0. 等待 API 就绪（worker 无需直接探测，超时场景会间接验证它）。
	ready := false
	for i := 0; i < 60 && !ready; i++ {
		status, _, err := c.doJSON(http.MethodGet, "/health", nil)
		ready = err == nil && status == http.StatusOK
		if !ready {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !ready {
		fmt.Println("FAIL  api not healthy within 30s")
		fmt.Println("VERIFY: FAIL")
		os.Exit(1)
	}
	fmt.Println("PASS  api healthy")

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	devA := "VERIFY-A-" + runID
	devB := "VERIFY-B-" + runID
	devC := "VERIFY-C-" + runID
	devD := "VERIFY-D-" + runID

	// 场景 1：及时确认 → confirmed；未配置策略的设备得到默认 30 秒窗口。
	status, body, err := c.doJSON(http.MethodPost, "/devices",
		map[string]any{"device_id": devA, "name": "Verify Sterilizer A"})
	c.check("create device A", err == nil && status == http.StatusCreated,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devA+"/windows", map[string]any{})
	w1, werr := decodeWindow(body)
	c.check("register window A -> open", err == nil && status == http.StatusCreated && werr == nil && w1.State == "open",
		fmt.Sprintf("status=%d err=%v werr=%v body=%s", status, err, werr, body))

	opened, oerr := time.Parse(time.RFC3339Nano, w1.OpenedAt)
	deadline, derr := time.Parse(time.RFC3339Nano, w1.Deadline)
	c.check("deadline assigned by database as opened_at + 30s (default policy)",
		oerr == nil && derr == nil && deadline.Sub(opened) == 30*time.Second,
		fmt.Sprintf("opened_at=%q deadline=%q", w1.OpenedAt, w1.Deadline))
	c.check("window A snapshots default ttl_seconds=30", w1.TTLSeconds == 30,
		fmt.Sprintf("ttl_seconds=%d", w1.TTLSeconds))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devA+"/confirm", map[string]any{})
	cw, werr := decodeWindow(body)
	c.check("confirm within window -> confirmed",
		err == nil && status == http.StatusOK && werr == nil && cw.State == "confirmed",
		fmt.Sprintf("status=%d err=%v werr=%v body=%s", status, err, werr, body))
	c.check("close reason is door_open_confirmed",
		cw.CloseReason != nil && *cw.CloseReason == "door_open_confirmed",
		fmt.Sprintf("close_reason=%v", cw.CloseReason))
	c.check("closed_at set and strictly before deadline",
		cw.ClosedAt != nil && *cw.ClosedAt < cw.Deadline,
		fmt.Sprintf("closed_at=%v deadline=%q", cw.ClosedAt, cw.Deadline))

	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devA+"/status", nil)
	sw, sp, serr := decodeStatus(body)
	c.check("status query shows confirmed",
		err == nil && status == http.StatusOK && serr == nil && sw != nil && sw.State == "confirmed",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	c.check("status reports default policy ttl_seconds=30",
		serr == nil && sp.TTLSeconds == 30 && sp.Source == "default",
		fmt.Sprintf("policy=%+v", sp))

	// 场景 2：同一设备重复登记 → 409 WINDOW_ALREADY_OPEN。
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devB, "name": "Verify Sterilizer B"})
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devB+"/windows", map[string]any{})
	wb, werr := decodeWindow(body)
	c.check("register window B -> open", err == nil && status == http.StatusCreated && werr == nil && wb.State == "open",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devB+"/windows", map[string]any{})
	c.check("duplicate register -> 409 WINDOW_ALREADY_OPEN",
		err == nil && status == http.StatusConflict && decodeErrCode(body) == "WINDOW_ALREADY_OPEN",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 3：客户端不得指定截止时刻 → 400，且不产生任何窗口。
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devC, "name": "Verify Sterilizer C"})
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devC+"/windows",
		map[string]any{"deadline": "2030-01-01T00:00:00.000Z"})
	c.check("client-specified deadline rejected with 400 INVALID_REQUEST",
		err == nil && status == http.StatusBadRequest && decodeErrCode(body) == "INVALID_REQUEST",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devC+"/status", nil)
	nw, _, serr := decodeStatus(body)
	c.check("no window created for device C",
		err == nil && status == http.StatusOK && serr == nil && nw == nil,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 4：设备卸载时限策略。
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devD, "name": "Verify Sterilizer D"})

	// 4a. 参数校验：缺失、非整数、越界、未知字段 → 400 INVALID_REQUEST。
	badPolicies := []struct {
		name string
		body map[string]any
	}{
		{"missing ttl_seconds", map[string]any{}},
		{"string ttl_seconds", map[string]any{"ttl_seconds": "30"}},
		{"float ttl_seconds", map[string]any{"ttl_seconds": 1.5}},
		{"below min (0)", map[string]any{"ttl_seconds": 0}},
		{"above max (601)", map[string]any{"ttl_seconds": 601}},
		{"unknown field", map[string]any{"ttl_seconds": 30, "deadline": "2030-01-01T00:00:00.000Z"}},
	}
	for _, bp := range badPolicies {
		status, body, err = c.doJSON(http.MethodPut, "/devices/"+devD+"/policy", bp.body)
		c.check("invalid policy ("+bp.name+") -> 400 INVALID_REQUEST",
			err == nil && status == http.StatusBadRequest && decodeErrCode(body) == "INVALID_REQUEST",
			fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	}
	// 未知设备 → 404 DEVICE_NOT_FOUND。
	status, body, err = c.doJSON(http.MethodPut, "/devices/VERIFY-NOPE-"+runID+"/policy",
		map[string]any{"ttl_seconds": 30})
	c.check("policy update on unknown device -> 404 DEVICE_NOT_FOUND",
		err == nil && status == http.StatusNotFound && decodeErrCode(body) == "DEVICE_NOT_FOUND",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	// 失败的更新不留半写入：设备 D 仍是默认策略。
	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devD+"/status", nil)
	_, dp, serr := decodeStatus(body)
	c.check("rejected updates leave no half-written policy (still default 30s)",
		err == nil && status == http.StatusOK && serr == nil && dp.TTLSeconds == 30 && dp.Source == "default",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 4b. 更新策略为 10 秒 → 新窗口采用新时限并快照。
	status, body, err = c.doJSON(http.MethodPut, "/devices/"+devD+"/policy",
		map[string]any{"ttl_seconds": 10})
	p10, perr := decodePolicy(body)
	c.check("update policy to 10s -> 200 custom policy",
		err == nil && status == http.StatusOK && perr == nil &&
			p10.TTLSeconds == 10 && p10.Source == "custom" && p10.UpdatedAt != nil,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/windows", map[string]any{})
	wd1, werr := decodeWindow(body)
	openedD, oerrD := time.Parse(time.RFC3339Nano, wd1.OpenedAt)
	deadlineD, derrD := time.Parse(time.RFC3339Nano, wd1.Deadline)
	c.check("new window uses updated policy: ttl_seconds=10, deadline=opened_at+10s",
		err == nil && status == http.StatusCreated && werr == nil && wd1.TTLSeconds == 10 &&
			oerrD == nil && derrD == nil && deadlineD.Sub(openedD) == 10*time.Second,
		fmt.Sprintf("status=%d err=%v window=%+v", status, err, wd1))

	// 4c. 再次改策为 600 秒：活动窗口的截止时刻与快照不受影响。
	status, body, err = c.doJSON(http.MethodPut, "/devices/"+devD+"/policy",
		map[string]any{"ttl_seconds": 600})
	c.check("update policy to 600s -> 200", err == nil && status == http.StatusOK,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devD+"/status", nil)
	swD, spD, serr := decodeStatus(body)
	c.check("status explains deadline source: current policy 600s, active window still 10s",
		err == nil && status == http.StatusOK && serr == nil &&
			spD.TTLSeconds == 600 && spD.Source == "custom" &&
			swD != nil && swD.TTLSeconds == 10 && swD.Deadline == wd1.Deadline && swD.State == "open",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 4d. 活动窗口仍在 10 秒时限内，确认成功。
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/confirm", map[string]any{})
	cwd, werr := decodeWindow(body)
	c.check("confirm active window within its 10s snapshot -> confirmed",
		err == nil && status == http.StatusOK && werr == nil && cwd.State == "confirmed" && cwd.TTLSeconds == 10,
		fmt.Sprintf("status=%d err=%v window=%+v", status, err, cwd))

	// 4e. 下一个窗口采用最新策略 600 秒。
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/windows", map[string]any{})
	wd2, werr := decodeWindow(body)
	openedD2, oerrD2 := time.Parse(time.RFC3339Nano, wd2.OpenedAt)
	deadlineD2, derrD2 := time.Parse(time.RFC3339Nano, wd2.Deadline)
	c.check("next window uses latest policy: ttl_seconds=600, deadline=opened_at+600s",
		err == nil && status == http.StatusCreated && werr == nil && wd2.TTLSeconds == 600 &&
			oerrD2 == nil && derrD2 == nil && deadlineD2.Sub(openedD2) == 600*time.Second,
		fmt.Sprintf("status=%d err=%v window=%+v", status, err, wd2))
	// 清理：确认该窗口，避免遗留 open 窗口。
	c.doJSON(http.MethodPost, "/devices/"+devD+"/confirm", map[string]any{})

	// 场景 5：无人确认 → worker 在截止时刻后把窗口隔离为 quarantined。
	var finalB *window
	dead := time.Now().Add(45 * time.Second)
	for time.Now().Before(dead) {
		status, body, err = c.doJSON(http.MethodGet, "/devices/"+devB+"/status", nil)
		if err == nil && status == http.StatusOK {
			if w, _, serr := decodeStatus(body); serr == nil && w != nil && w.State == "quarantined" {
				finalB = w
				break
			}
		}
		time.Sleep(time.Second)
	}
	c.check("window B quarantined by worker after deadline", finalB != nil,
		"state did not become quarantined within 45s")
	if finalB != nil {
		c.check("close reason is scan_timeout",
			finalB.CloseReason != nil && *finalB.CloseReason == "scan_timeout",
			fmt.Sprintf("close_reason=%v", finalB.CloseReason))
		c.check("closed_at at or after deadline",
			finalB.ClosedAt != nil && *finalB.ClosedAt >= finalB.Deadline,
			fmt.Sprintf("closed_at=%v deadline=%q", finalB.ClosedAt, finalB.Deadline))
	}

	// 场景 6：终态之后再次确认 → 409 NO_OPEN_WINDOW。
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devB+"/confirm", map[string]any{})
	c.check("confirm after quarantine -> 409 NO_OPEN_WINDOW",
		err == nil && status == http.StatusConflict && decodeErrCode(body) == "NO_OPEN_WINDOW",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	if c.failed > 0 {
		fmt.Printf("VERIFY: FAIL (%d check(s) failed)\n", c.failed)
		os.Exit(1)
	}
	fmt.Println("VERIFY: PASS")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
