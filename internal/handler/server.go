// Package handler 对应 Python 版 server.py 的公共设施（鉴权/rid/日志/落盘/
// health/models）+ routes/ 三协议路由。上游通道调度在 upstream 包，门禁规格在
// lanes 包，协议翻译在 adapters 包。
package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/dump"
	"opencode2api/internal/freelist"
	"opencode2api/internal/session"
	"opencode2api/internal/upstream"
)

// ChatPassthrough chat 线直通上游的请求字段白名单（其余客户端字段一律丢弃，
// 与 Python 版 routes/chat.py 一致）。注意：**没有 tools** —— chat 线的工具集
// 完全由各 lane 的 prepare_body 注入（客户端工具走 /responses 线保留）。
var ChatPassthrough = map[string]bool{
	"model": true, "messages": true, "temperature": true, "top_p": true,
	"max_tokens": true, "unit_choice": true, "stop": true, "user": true,
	"stream_options": true, "reasoning_effort": true,
}

// Server 聚合全部依赖；Handler 挂到 http.ServeMux。
type Server struct {
	Cfg  *config.Config
	Free *freelist.List
	Dump *dump.Writer
	Up   *upstream.Dispatcher
	Sess *session.Store
	Log  *slog.Logger

	reqSeq atomic.Int64
}

// ── 请求上下文 ───────────────────────────────────────────────────────────────

type reqInfo struct {
	rid     string
	t0      time.Time
	payload map[string]any
}

type reqInfoKey struct{}

func withReqInfo(ctx context.Context, info reqInfo) context.Context {
	return context.WithValue(ctx, reqInfoKey{}, info)
}

func reqInfoFrom(r *http.Request) reqInfo {
	info, _ := r.Context().Value(reqInfoKey{}).(reqInfo)
	return info
}

// nextRID 单调递增请求号（r%04d；进程内单调，dump 文件名带启动段防跨进程撞名）。
func (s *Server) nextRID() string {
	return fmt.Sprintf("r%04d", s.reqSeq.Add(1))
}

// Handler 组装路由。鉴权中间件包在最外层（/health 例外）。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/v1/models", s.models)
	mux.HandleFunc("/v1/chat/completions", s.chat)
	mux.HandleFunc("/v1/responses", s.responses)
	mux.HandleFunc("/v1/messages", s.anthropic)
	return s.auth(mux)
}

// auth 鉴权 + rid 分配 + in: 日志 + 请求落盘（对应 before_request _auth_and_rid）。
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := s.nextRID()
		t0 := time.Now()

		// 与 Python 版逐字对齐：整串比对 Authorization == "Bearer "+API_KEY。
		// （routes/anthropic.py 的 docstring 提到 x-api-key，但 before_request
		// 源码并没有放行它——以源码为准，不放宽鉴权面。）
		if s.Cfg.APIKey != "" && r.URL.Path != "/health" {
			if r.Header.Get("Authorization") != "Bearer "+s.Cfg.APIKey {
				writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
				return
			}
		}

		var payload map[string]any
		bodyLen := 0
		var rawBody []byte
		if r.Body != nil && r.Method == http.MethodPost {
			rawBody, _ = io.ReadAll(io.LimitReader(r.Body, 64<<20))
			bodyLen = len(rawBody)
			if len(rawBody) > 0 {
				_ = json.Unmarshal(rawBody, &payload)
			}
			r.Body = io.NopCloser(bytes.NewReader(rawBody))
		}
		s.logIn(rid, r, payload, bodyLen)

		// chat 与 messages 两条协议路径落 req 原文（可直接重放的纯 body，绝不落 header）
		switch r.URL.Path {
		case "/v1/chat/completions", "/v1/messages":
			s.Dump.Save(rid, "req", string(rawBody))
		}

		ctx := withReqInfo(r.Context(), reqInfo{rid: rid, t0: t0, payload: payload})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// logIn 出 in: 行（对应 Python before_request 的日志部分）。
func (s *Server) logIn(rid string, r *http.Request, payload map[string]any, bodyLen int) {
	ua := r.Header.Get("User-Agent")
	if ua == "" {
		ua = "-"
	}
	ua = strings.ReplaceAll(strings.ReplaceAll(ua, "\r", " "), "\n", " ")
	var msgs, tools int
	if l, ok := payload["messages"].([]any); ok {
		msgs = len(l)
	}
	if l, ok := payload["tools"].([]any); ok {
		tools = len(l)
	}
	s.Log.Info("in:",
		slog.String("rid", rid),
		slog.String("line", fmt.Sprintf("%s %s | model=%s msgs=%d tools=%d stream=%v | ua=%s | body=%dB",
			r.Method, r.URL.Path, str(payload["model"]), msgs, tools, payload["stream"], ua, bodyLen)),
	)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "service": "the unit2api", "route": "zen-direct",
		"free_models": s.Free.Count(),
	})
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	ids := s.Free.IDs() // 空时内部已 quick 拉一次
	now := time.Now().Unix()
	data := make([]any, 0, len(ids))
	for _, id := range ids {
		data = append(data, map[string]any{
			"id": id, "object": "model", "created": now, "owned_by": freelist.OwnedBy(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// ── 响应辅助 ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(marshalCompact(v))
}

// marshalCompact JSON 紧凑序列化，不转义 HTML 字符（中文/标签原样输出，
// 对齐 Python json.dumps(..., ensure_ascii=False)）。
func marshalCompact(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return []byte("null")
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func boolOf(v any) bool {
	b, _ := v.(bool)
	return b
}

// sseHeaders 流式响应头。
func sseHeaders(w http.ResponseWriter, contentType string) {
	if contentType == "" {
		contentType = "text/event-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
}

// sseLineFeed 把上游 SSE body 转成行流（iter.Seq[string]，"data:" 原始行）。
// SSE 的 data 行可能极长（整块 JSON），Scanner 上限放到 8MB。
// 上游 SSE 不带 charset，必须字节级收、显式 UTF-8 解码（sc.Text() 即按
// UTF-8 字节直接透传为 string，不做 Latin-1 猜测）。
func sseLineFeed(r io.Reader) iter.Seq[string] {
	return func(yield func(string) bool) {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
		for sc.Scan() {
			if !yield(sc.Text()) {
				return
			}
		}
	}
}

// ── 日志：request.log 2MB×3 轮转 + stdout（与 Python RotatingFileHandler 对齐）──

// NewLogger 建 slog logger：同时写 stdout 与 request.log（2MB 轮转，保 3 份）。
func NewLogger(stateDir string) *slog.Logger {
	path := filepath.Join(stateDir, "request.log")
	rw := &rotateWriter{path: path, maxBytes: 2 << 20, backupCount: 3}
	return slog.New(slog.NewTextHandler(io.MultiWriter(os.Stdout, rw), nil))
}

type rotateWriter struct {
	mu          sync.Mutex
	path        string
	maxBytes    int64
	backupCount int
	f           *os.File
	written     int64
}

func (rw *rotateWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.f == nil {
		if err := rw.open(); err != nil {
			return 0, err
		}
	}
	if rw.written+int64(len(p)) > rw.maxBytes {
		rw.rotate()
	}
	n, err := rw.f.Write(p)
	rw.written += int64(n)
	return n, err
}

func (rw *rotateWriter) open() error {
	f, err := os.OpenFile(rw.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	rw.f = f
	if info, err := f.Stat(); err == nil {
		rw.written = info.Size()
	}
	return nil
}

// rotate 关闭当前文件，.log.(n-1) → .log.n 链式改名，重开新文件。
func (rw *rotateWriter) rotate() {
	if rw.f != nil {
		rw.f.Close()
		rw.f = nil
	}
	for i := rw.backupCount - 1; i > 0; i-- {
		os.Rename(fmt.Sprintf("%s.%d", rw.path, i), fmt.Sprintf("%s.%d", rw.path, i+1))
	}
	os.Rename(rw.path, rw.path+".1")
	rw.written = 0
	_ = rw.open()
}
