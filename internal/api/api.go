// Package api 提供面向消毒供应中心设备集成的 HTTP 接口：
// 登记卸载窗口、接收开门确认、查询设备/窗口状态、更新设备卸载时限策略。
// 所有状态裁决都下沉到 store 层的 SQLite 原子语句，本层只做
// 参数校验、错误映射与 JSON 序列化。
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"cssd-unload/internal/store"
)

// 结构化错误码，随错误响应的 error.code 返回。
const (
	CodeInvalidRequest    = "INVALID_REQUEST"
	CodeDeviceNotFound    = "DEVICE_NOT_FOUND"
	CodeDeviceExists      = "DEVICE_ALREADY_EXISTS"
	CodeWindowAlreadyOpen = "WINDOW_ALREADY_OPEN"
	CodeNoOpenWindow      = "NO_OPEN_WINDOW"
	CodeInternal          = "INTERNAL_ERROR"
)

// Server 持有路由与存储依赖。
type Server struct {
	st     *store.Store
	router *gin.Engine
}

// NewServer 构造 HTTP 服务并注册全部路由。
func NewServer(st *store.Store) *Server {
	r := gin.New()
	r.Use(gin.Recovery())

	s := &Server{st: st, router: r}
	r.GET("/health", s.health)
	r.POST("/devices", s.createDevice)
	r.GET("/devices/:deviceID/status", s.deviceStatus)
	r.PUT("/devices/:deviceID/policy", s.updateDevicePolicy)
	r.POST("/devices/:deviceID/windows", s.registerWindow)
	r.POST("/devices/:deviceID/confirm", s.confirmWindow)
	return s
}

// Handler 返回根 http.Handler，便于挂载到 http.Server 或 httptest。
func (s *Server) Handler() http.Handler { return s.router }

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeError(c *gin.Context, status int, code, message string, details map[string]any) {
	c.JSON(status, gin.H{"error": errorBody{Code: code, Message: message, Details: details}})
}

func (s *Server) health(c *gin.Context) {
	if err := s.st.Ping(c.Request.Context()); err != nil {
		writeError(c, http.StatusServiceUnavailable, CodeInternal, "database unreachable", nil)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

type createDeviceRequest struct {
	DeviceID string `json:"device_id" binding:"required"`
	Name     string `json:"name" binding:"required"`
}

func (s *Server) createDevice(c *gin.Context) {
	var req createDeviceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			"request body must be a JSON object with non-empty device_id and name", nil)
		return
	}
	req.DeviceID = strings.TrimSpace(req.DeviceID)
	req.Name = strings.TrimSpace(req.Name)
	if req.DeviceID == "" || req.Name == "" || len(req.DeviceID) > 128 || len(req.Name) > 256 {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			"device_id and name must be non-empty (device_id <= 128 chars, name <= 256 chars)", nil)
		return
	}

	d, err := s.st.CreateDevice(c.Request.Context(), req.DeviceID, req.Name)
	switch {
	case errors.Is(err, store.ErrDeviceExists):
		writeError(c, http.StatusConflict, CodeDeviceExists,
			fmt.Sprintf("device %q already exists", req.DeviceID), nil)
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to create device", nil)
	default:
		c.JSON(http.StatusCreated, gin.H{"device": d})
	}
}

// rejectClientFields 解析请求体并拒绝任何客户端字段。登记与确认接口不接受
// 客户端参数：截止时刻、状态、时间戳一律由数据库生成，客户端指定即视为
// 非法请求。空请求体或空 JSON 对象 {} 均可接受。
func rejectClientFields(c *gin.Context) error {
	if c.Request.Body == nil {
		return nil
	}
	dec := json.NewDecoder(c.Request.Body)
	var payload map[string]any
	if err := dec.Decode(&payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("request body must be an empty JSON object: %v", err)
	}
	if len(payload) == 0 {
		return nil
	}
	return fmt.Errorf("unexpected field(s) %s: deadline, state and timestamps are always assigned by the database clock and cannot be set by clients",
		strings.Join(slices.Sorted(maps.Keys(payload)), ", "))
}

// parsePolicyPayload 解析并校验策略更新请求体：必须是仅含 ttl_seconds 字段
// 的 JSON 对象，且值为 [MinWindowTTLSeconds, MaxWindowTTLSeconds] 内的整数。
// 值缺失、非整数（含字符串、布尔、null、浮点）或越界都返回参数错误。
func parsePolicyPayload(c *gin.Context) (int, error) {
	rangeHint := fmt.Sprintf("an integer between %d and %d",
		store.MinWindowTTLSeconds, store.MaxWindowTTLSeconds)
	if c.Request.Body == nil {
		return 0, fmt.Errorf("request body must be a JSON object with ttl_seconds (%s)", rangeHint)
	}
	dec := json.NewDecoder(c.Request.Body)
	dec.UseNumber()
	var payload map[string]any
	if err := dec.Decode(&payload); err != nil {
		return 0, fmt.Errorf("request body must be a JSON object with ttl_seconds (%s)", rangeHint)
	}
	raw, ok := payload["ttl_seconds"]
	if !ok {
		return 0, fmt.Errorf("missing required field ttl_seconds (%s)", rangeHint)
	}
	if len(payload) != 1 {
		extra := make([]string, 0, len(payload)-1)
		for k := range payload {
			if k != "ttl_seconds" {
				extra = append(extra, k)
			}
		}
		slices.Sort(extra)
		return 0, fmt.Errorf("unexpected field(s) %s: only ttl_seconds is accepted", strings.Join(extra, ", "))
	}
	num, ok := raw.(json.Number)
	if !ok {
		return 0, fmt.Errorf("ttl_seconds must be %s, got %v", rangeHint, raw)
	}
	n, err := strconv.ParseInt(num.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ttl_seconds must be %s, got %v", rangeHint, num)
	}
	if n < store.MinWindowTTLSeconds || n > store.MaxWindowTTLSeconds {
		return 0, fmt.Errorf("ttl_seconds %d out of range: must be %s", n, rangeHint)
	}
	return int(n), nil
}

// updateDevicePolicy 处理 PUT /devices/{id}/policy：集成工程师提交 1–600 秒
// 的整数开门确认时限。更新是单条原子 UPSERT：失败不会留下半写入配置，
// 也不会改动设备已登记的窗口。策略值缺失、非整数或越界返回 400 结构化
// 参数错误；设备不存在返回 404。
func (s *Server) updateDevicePolicy(c *gin.Context) {
	deviceID := c.Param("deviceID")
	ttl, err := parsePolicyPayload(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest, err.Error(),
			map[string]any{
				"field": "ttl_seconds",
				"min":   store.MinWindowTTLSeconds,
				"max":   store.MaxWindowTTLSeconds,
			})
		return
	}

	p, err := s.st.SetDevicePolicy(c.Request.Context(), deviceID, ttl)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
	case errors.Is(err, store.ErrInvalidPolicyTTL):
		writeError(c, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("ttl_seconds must be an integer between %d and %d",
				store.MinWindowTTLSeconds, store.MaxWindowTTLSeconds),
			map[string]any{
				"field": "ttl_seconds",
				"min":   store.MinWindowTTLSeconds,
				"max":   store.MaxWindowTTLSeconds,
			})
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to update device policy", nil)
	default:
		c.JSON(http.StatusOK, gin.H{"policy": p})
	}
}

func (s *Server) registerWindow(c *gin.Context) {
	deviceID := c.Param("deviceID")
	if err := rejectClientFields(c); err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest, err.Error(), nil)
		return
	}

	w, err := s.st.RegisterWindow(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
	case errors.Is(err, store.ErrWindowAlreadyOpen):
		details := map[string]any{}
		if cur, lerr := s.st.LatestWindow(c.Request.Context(), deviceID); lerr == nil && cur.State == store.StateOpen {
			details["open_window_id"] = cur.ID
			details["deadline"] = cur.Deadline
		}
		writeError(c, http.StatusConflict, CodeWindowAlreadyOpen,
			fmt.Sprintf("device %q already has an open unload window", deviceID), details)
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to register unload window", nil)
	default:
		c.JSON(http.StatusCreated, gin.H{"window": w})
	}
}

func (s *Server) confirmWindow(c *gin.Context) {
	deviceID := c.Param("deviceID")
	if err := rejectClientFields(c); err != nil {
		writeError(c, http.StatusBadRequest, CodeInvalidRequest, err.Error(), nil)
		return
	}

	w, err := s.st.ConfirmWindow(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
	case errors.Is(err, store.ErrNoOpenWindow):
		details := map[string]any{}
		if cur, lerr := s.st.LatestWindow(c.Request.Context(), deviceID); lerr == nil {
			details["latest_window_id"] = cur.ID
			details["latest_state"] = cur.State
		}
		writeError(c, http.StatusConflict, CodeNoOpenWindow,
			fmt.Sprintf("device %q has no open unload window; it may already be confirmed or quarantined", deviceID),
			details)
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to confirm unload window", nil)
	default:
		c.JSON(http.StatusOK, gin.H{"window": w})
	}
}

func (s *Server) deviceStatus(c *gin.Context) {
	deviceID := c.Param("deviceID")
	d, err := s.st.GetDevice(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(c, http.StatusNotFound, CodeDeviceNotFound,
			fmt.Sprintf("device %q not found", deviceID), nil)
		return
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to query device", nil)
		return
	}

	// 当前生效策略与最近窗口采用的时限快照一并返回，调用方据此解释
	// 截止时刻的来源（策略的后续变化不回改已登记窗口）。
	p, err := s.st.GetDevicePolicy(c.Request.Context(), deviceID)
	if err != nil {
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to query device policy", nil)
		return
	}

	w, err := s.st.LatestWindow(c.Request.Context(), deviceID)
	switch {
	case errors.Is(err, store.ErrWindowNotFound):
		c.JSON(http.StatusOK, gin.H{"device": d, "policy": p, "window": nil})
	case err != nil:
		writeError(c, http.StatusInternalServerError, CodeInternal, "failed to query window", nil)
	default:
		c.JSON(http.StatusOK, gin.H{"device": d, "policy": p, "window": w})
	}
}
