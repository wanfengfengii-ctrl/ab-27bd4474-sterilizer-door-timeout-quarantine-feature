// verify 是一次性验收客户端：对运行中的 API + worker 执行端到端验收，
// 覆盖及时确认、无人确认超时隔离、活动窗口冲突、客户端禁止指定截止时刻、
// 终态后重复确认，以及设备卸载时限策略（默认 30 秒、更新后新窗口生效、
// 活动窗口不受改策影响、非法策略值被拒且不留半写入配置），全部通过则以
// 退出码 0 结束。
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
	TTLSeconds  int     `json:"ttl_seconds"`
	ClosedAt    *string `json:"closed_at"`
	CloseReason *string `json:"close_reason"`
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

func decodeStatus(data []byte) (*window, error) {
	var resp struct {
		Window *window `json:"window"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp.Window, nil
}

// decodeStatusFull 同时取出状态响应中的当前策略与最近窗口。
func decodeStatusFull(data []byte) (*policy, *window, error) {
	var resp struct {
		Policy *policy `json:"policy"`
		Window *window `json:"window"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, nil, err
	}
	if resp.Policy == nil {
		return nil, nil, fmt.Errorf("response has no policy field: %s", data)
	}
	return resp.Policy, resp.Window, nil
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

	// 场景 1：及时确认 → confirmed。
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
	c.check("deadline assigned by database as opened_at + 30s",
		oerr == nil && derr == nil && deadline.Sub(opened) == 30*time.Second,
		fmt.Sprintf("opened_at=%q deadline=%q", w1.OpenedAt, w1.Deadline))
	c.check("default device window snapshots ttl_seconds=30", w1.TTLSeconds == 30,
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
	sw, serr := decodeStatus(body)
	c.check("status query shows confirmed",
		err == nil && status == http.StatusOK && serr == nil && sw != nil && sw.State == "confirmed",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

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
	nw, serr := decodeStatus(body)
	c.check("no window created for device C",
		err == nil && status == http.StatusOK && serr == nil && nw == nil,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 4：无人确认 → worker 在截止时刻后把窗口隔离为 quarantined。
	var finalB *window
	dead := time.Now().Add(45 * time.Second)
	for time.Now().Before(dead) {
		status, body, err = c.doJSON(http.MethodGet, "/devices/"+devB+"/status", nil)
		if err == nil && status == http.StatusOK {
			if w, serr := decodeStatus(body); serr == nil && w != nil && w.State == "quarantined" {
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

	// 场景 5：终态之后再次确认 → 409 NO_OPEN_WINDOW。
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devB+"/confirm", map[string]any{})
	c.check("confirm after quarantine -> 409 NO_OPEN_WINDOW",
		err == nil && status == http.StatusConflict && decodeErrCode(body) == "NO_OPEN_WINDOW",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 6：策略更新后新窗口使用新时限；状态查询同时暴露当前策略与窗口快照。
	devD := "VERIFY-D-" + runID
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devD, "name": "Verify Sterilizer D"})
	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devD+"/status", nil)
	pol, _, perr := decodeStatusFull(body)
	c.check("fresh device reports default policy 30s",
		err == nil && status == http.StatusOK && perr == nil && pol.TTLSeconds == 30 && pol.Source == "default",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPut, "/devices/"+devD+"/policy", map[string]any{"ttl_seconds": 10})
	pol2, perr2 := decodePolicy(body)
	c.check("update policy to 10s", err == nil && status == http.StatusOK && perr2 == nil &&
		pol2.TTLSeconds == 10 && pol2.Source == "configured" && pol2.UpdatedAt != nil,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/windows", map[string]any{})
	wd, werr := decodeWindow(body)
	openedD, oerrD := time.Parse(time.RFC3339Nano, wd.OpenedAt)
	deadlineD, derrD := time.Parse(time.RFC3339Nano, wd.Deadline)
	c.check("window after policy update uses new 10s limit",
		err == nil && status == http.StatusCreated && werr == nil && wd.TTLSeconds == 10 &&
			oerrD == nil && derrD == nil && deadlineD.Sub(openedD) == 10*time.Second,
		fmt.Sprintf("status=%d err=%v ttl=%d opened_at=%q deadline=%q", status, err, wd.TTLSeconds, wd.OpenedAt, wd.Deadline))

	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devD+"/status", nil)
	pol3, wdStatus, perr3 := decodeStatusFull(body)
	c.check("status exposes current policy and window snapshot",
		err == nil && status == http.StatusOK && perr3 == nil && pol3.TTLSeconds == 10 &&
			wdStatus != nil && wdStatus.ID == wd.ID && wdStatus.TTLSeconds == 10,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devD+"/confirm", map[string]any{})
	cwd, werr := decodeWindow(body)
	c.check("confirm within new 10s window -> confirmed",
		err == nil && status == http.StatusOK && werr == nil && cwd.State == "confirmed",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 7：活动窗口不受再次改策影响；下一个窗口才采用新策略。
	devE := "VERIFY-E-" + runID
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devE, "name": "Verify Sterilizer E"})
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devE+"/windows", map[string]any{})
	we, werr := decodeWindow(body)
	c.check("register window E with default 30s", err == nil && status == http.StatusCreated && werr == nil && we.TTLSeconds == 30,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPut, "/devices/"+devE+"/policy", map[string]any{"ttl_seconds": 600})
	c.check("update policy to 600s while window open", err == nil && status == http.StatusOK,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devE+"/status", nil)
	polE, weAfter, perrE := decodeStatusFull(body)
	c.check("open window keeps original deadline and snapshot after policy change",
		err == nil && status == http.StatusOK && perrE == nil && polE.TTLSeconds == 600 &&
			weAfter != nil && weAfter.ID == we.ID && weAfter.TTLSeconds == 30 &&
			weAfter.Deadline == we.Deadline && weAfter.State == "open",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devE+"/confirm", map[string]any{})
	cwe, werr := decodeWindow(body)
	c.check("confirm still judged against original deadline -> confirmed",
		err == nil && status == http.StatusOK && werr == nil && cwe.State == "confirmed" && cwe.TTLSeconds == 30,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devE+"/windows", map[string]any{})
	we2, werr := decodeWindow(body)
	openedE2, oerrE2 := time.Parse(time.RFC3339Nano, we2.OpenedAt)
	deadlineE2, derrE2 := time.Parse(time.RFC3339Nano, we2.Deadline)
	c.check("next window adopts updated 600s policy",
		err == nil && status == http.StatusCreated && werr == nil && we2.TTLSeconds == 600 &&
			oerrE2 == nil && derrE2 == nil && deadlineE2.Sub(openedE2) == 600*time.Second,
		fmt.Sprintf("status=%d err=%v ttl=%d body=%s", status, err, we2.TTLSeconds, body))
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devE+"/confirm", map[string]any{})
	c.check("confirm second window E -> confirmed",
		err == nil && status == http.StatusOK, fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	// 场景 8：非法策略值返回结构化 400，未知设备返回 404；失败的更新不留下
	// 半写入配置，也不影响已有窗口。
	devF := "VERIFY-F-" + runID
	c.doJSON(http.MethodPost, "/devices", map[string]any{"device_id": devF, "name": "Verify Sterilizer F"})
	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devF+"/windows", map[string]any{})
	wf, werr := decodeWindow(body)
	c.check("register window F", err == nil && status == http.StatusCreated && werr == nil,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	badBodies := []struct {
		name string
		body map[string]any
	}{
		{"missing ttl_seconds", map[string]any{}},
		{"float ttl_seconds", map[string]any{"ttl_seconds": 1.5}},
		{"string ttl_seconds", map[string]any{"ttl_seconds": "30"}},
		{"zero ttl_seconds", map[string]any{"ttl_seconds": 0}},
		{"negative ttl_seconds", map[string]any{"ttl_seconds": -5}},
		{"ttl_seconds above 600", map[string]any{"ttl_seconds": 601}},
	}
	for _, bad := range badBodies {
		status, body, err = c.doJSON(http.MethodPut, "/devices/"+devF+"/policy", bad.body)
		c.check("invalid policy rejected: "+bad.name,
			err == nil && status == http.StatusBadRequest && decodeErrCode(body) == "INVALID_REQUEST",
			fmt.Sprintf("status=%d err=%v body=%s", status, err, body))
	}

	status, body, err = c.doJSON(http.MethodPut, "/devices/VERIFY-NOPE-"+runID+"/policy", map[string]any{"ttl_seconds": 30})
	c.check("policy update on unknown device -> 404 DEVICE_NOT_FOUND",
		err == nil && status == http.StatusNotFound && decodeErrCode(body) == "DEVICE_NOT_FOUND",
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodGet, "/devices/"+devF+"/status", nil)
	polF, wfAfter, perrF := decodeStatusFull(body)
	c.check("rejected updates leave default policy and open window untouched",
		err == nil && status == http.StatusOK && perrF == nil &&
			polF.TTLSeconds == 30 && polF.Source == "default" &&
			wfAfter != nil && wfAfter.ID == wf.ID && wfAfter.State == "open" &&
			wfAfter.TTLSeconds == 30 && wfAfter.Deadline == wf.Deadline,
		fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

	status, body, err = c.doJSON(http.MethodPost, "/devices/"+devF+"/confirm", map[string]any{})
	c.check("confirm window F -> confirmed",
		err == nil && status == http.StatusOK, fmt.Sprintf("status=%d err=%v body=%s", status, err, body))

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
