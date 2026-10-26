// golden_test.go：httptest 假上游 + 完整依赖注入的端到端测试。
// 覆盖：鉴权、health/models、chat 三态（流式透传/非流式聚合/免费闸门）、
// 上游请求形状（门禁工具注入、stream 强制、指纹头、session 降序 TypeID）、
// 上游错误透传（429/403 冷却）、responses 透传、anthropic 翻译流/非流式。
package handler

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"opencode2api/internal/config"
	"opencode2api/internal/dump"
	"opencode2api/internal/freelist"
	"opencode2api/internal/lanes"
	"opencode2api/internal/session"
	"opencode2api/internal/upstream"
)

// ── 测试基建 ──────────────────────────────────────────────────────────────────

// recorder 假上游侧的请求捕获器（env 构造前就要可用，独立于 env 持锁）。
type recorder struct {
	mu  sync.Mutex
	cap captured
}

type captured struct {
	path string
	body map[string]any
	raw  []byte
	hdr  http.Header
}

// handlerFn 包装 respond：先记录再交给 respond。
func (rc *recorder) handlerFn(respond func(w http.ResponseWriter, r *http.Request, body map[string]any)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		rc.mu.Lock()
		rc.cap = captured{path: r.URL.Path, body: body, raw: raw, hdr: r.Header.Clone()}
		rc.mu.Unlock()
		respond(w, r, body)
	}
}

func (rc *recorder) last() captured {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.cap
}

// modelsRespond 假上游 /models 返回免费+付费各一；其余路径交给 respond。
func modelsRespond(respond func(w http.ResponseWriter, r *http.Request, body map[string]any)) func(w http.ResponseWriter, r *http.Request, body map[string]any) {
	return func(w http.ResponseWriter, r *http.Request, body map[string]any) {
		if strings.HasSuffix(r.URL.Path, "/models") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-free"},{"id":"claude-sonnet-4"}]}`))
			return
		}
		respond(w, r, body)
	}
}

// chatSSE 标准上游 Chat SSE：文本两段 + finish + usage + [DONE]。
const chatSSE = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"你好\"}}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: {\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":4,\"total_tokens\":15}}\n\n" +
	"data: [DONE]\n\n"

func sseRespond(body string) func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
	return func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(body))
	}
}

type testEnv struct {
	up   *httptest.Server
	h    http.Handler
	cfg  *config.Config
	free *freelist.List
}

func newEnv(t *testing.T, upstreamHandler http.HandlerFunc) *testEnv {
	t.Helper()
	env := &testEnv{}
	if upstreamHandler != nil {
		env.up = httptest.NewServer(upstreamHandler)
		t.Cleanup(env.up.Close)
	}
	stateDir := t.TempDir()
	env.cfg = &config.Config{
		StateDir:           stateDir,
		DumpOn:             false,
		DumpDir:            filepath.Join(stateDir, "dumps"),
		DumpMaxFiles:       10,
		CooldownS:          600,
		Cooldown429S:       60,
		Retry:              0,
		Concurrency:        2,
		Pool:               4,
		UpstreamTimeoutSec: 5,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := session.NewStore(filepath.Join(stateDir, "session_cache.json"))
	dw := dump.New(false, env.cfg.DumpDir, 10)

	// 两条通道都指向同一假上游（单上游即可验证调度与回落语义）
	var order []upstream.Lane
	if env.up != nil {
		for _, name := range []string{"zen", "inference"} {
			l := upstream.Lane{Name: name, Base: env.up.URL}
			if name == "zen" {
				l.Headers = func(ses string) map[string]string { return lanes.ZenBuildHeaders("Bearer public", ses) }
				l.PrepareBody = lanes.ZenPrepareBody
			} else {
				l.Headers = func(ses string) map[string]string { return lanes.BuildInferenceHeaders("Bearer public", ses) }
				l.PrepareBody = lanes.InferencePrepareBody
			}
			order = append(order, l)
		}
	}
	disp := upstream.New(order, upstream.Options{
		CooldownS: 600, Cooldown429S: 60, Retry: 0, Concurrency: 2, Pool: 4, Timeout: 5,
	}, store.For, logger)
	disp.Dump = dw.Save

	env.free = freelist.New(filepath.Join(stateDir, "free_cache.json"),
		func() freelist.Lane {
			l := disp.PickLaneForModels()
			return freelist.Lane{Name: l.Name, Base: l.Base, Headers: l.Headers}
		},
		func() string { return store.For("") })
	env.free.LoadCache()
	if env.up != nil {
		env.free.Fetch(true, false) // 从假上游 /models 拉清单
	}

	env.h = (&Server{Cfg: env.cfg, Free: env.free, Dump: dw, Up: disp, Sess: store, Log: logger}).Handler()
	return env
}

func postJSON(env *testEnv, path, apiKey string, payload any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	rec := httptest.NewRecorder()
	env.h.ServeHTTP(rec, req)
	return rec
}

func get(env *testEnv, path, apiKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	rec := httptest.NewRecorder()
	env.h.ServeHTTP(rec, req)
	return rec
}

// ── 鉴权 / health / models ────────────────────────────────────────────────────

func TestUnauthorized(t *testing.T) {
	env := newEnv(t, nil)
	env.cfg.APIKey = "secret"
	if rec := postJSON(env, "/v1/chat/completions", "wrong", map[string]any{}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	// /health 例外（对齐 Python before_request 的 path != "/health"）
	if rec := get(env, "/health", "wrong"); rec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", rec.Code)
	}
}

func TestHealth(t *testing.T) {
	env := newEnv(t, nil)
	rec := get(env, "/health", "")
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["ok"] != true || out["service"] != "the unit2api" || out["route"] != "zen-direct" {
		t.Fatalf("health body = %v", out)
	}
}

func TestModels(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(nil)))
	rec := get(env, "/v1/models", "")
	var out struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Data) != 1 || out.Data[0]["id"] != "gpt-free" {
		t.Fatalf("models = %v, want 仅 gpt-free（claude-* 是付费面）", out.Data)
	}
	if out.Data[0]["owned_by"] != "the unit-zen-free" {
		t.Fatalf("owned_by = %v", out.Data[0]["owned_by"])
	}
}

// ── chat 线 ───────────────────────────────────────────────────────────────────

func TestChatNonStream(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(chatSSE))))
	rec := postJSON(env, "/v1/chat/completions", "", map[string]any{
		"model":    "gpt-free",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["object"] != "chat.completion" || out["model"] != "gpt-free" {
		t.Fatalf("out = %v", out)
	}
	choices := out["choices"].([]any)
	c0 := choices[0].(map[string]any)
	msg := c0["message"].(map[string]any)
	if msg["content"] != "你好 world" {
		t.Fatalf("content = %v", msg["content"])
	}
	if c0["finish_reason"] != "stop" {
		t.Fatalf("finish_reason = %v", c0["finish_reason"])
	}
	usage := out["usage"].(map[string]any)
	if usage["prompt_tokens"] != float64(11) {
		t.Fatalf("usage = %v", usage)
	}
}

func TestChatStreamPassthrough(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(chatSSE))))
	rec := postJSON(env, "/v1/chat/completions", "", map[string]any{
		"model": "gpt-free", "stream": true,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "你好") || !strings.Contains(body, "[DONE]") {
		t.Fatalf("透传体缺关键帧: %q", body)
	}
}

func TestChatRejectsNonFree(t *testing.T) {
	env := newEnv(t, nil)
	rec := postJSON(env, "/v1/chat/completions", "", map[string]any{
		"model": "claude-sonnet-4", "messages": []any{},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if errObj := out["error"].(map[string]any); errObj["type"] != "invalid_request_error" {
		t.Fatalf("err = %v", errObj)
	}
}

// TestChatUpstreamShape 上游收到的请求形状：闸门工具注入、stream 强制、
// stream_options 覆写、指纹头全量、session 降序 TypeID 格式、四头同值。
func TestChatUpstreamShape(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(chatSSE))))
	rec := postJSON(env, "/v1/chat/completions", "", map[string]any{
		"model": "gpt-free",
		"messages": []any{
			map[string]any{"role": "system", "content": "sys"},
			map[string]any{"role": "user", "content": "seed-msg"},
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	capd := rc.last()
	if !strings.HasSuffix(capd.path, "/chat/completions") {
		t.Fatalf("upstream path = %s", capd.path)
	}
	if capd.body["stream"] != true {
		t.Fatalf("stream = %v, want true", capd.body["stream"])
	}
	so, _ := capd.body["stream_options"].(map[string]any)
	if so == nil || so["include_usage"] != true {
		t.Fatalf("stream_options = %v", capd.body["stream_options"])
	}
	// zen 通道闸门：客户端未带工具 → 注入 [bash, read]（嵌套 function 形）
	tools, _ := capd.body["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("tools = %v, want bash+read 两个 stub", tools)
	}
	t0 := tools[0].(map[string]any)
	if t0["function"].(map[string]any)["name"] != "bash" {
		t.Fatalf("tools[0] = %v, want bash", t0)
	}
	// 指纹头（zen legacy 形状）
	if got := capd.hdr.Get("User-Agent"); !strings.HasPrefix(got, "opencode/1.18.31 (") {
		t.Fatalf("UA = %q", got)
	}
	if got := capd.hdr.Get("Authorization"); got != "Bearer public" {
		t.Fatalf("Authorization = %q", got)
	}
	if capd.hdr.Get("x-opencode-request") == "" {
		t.Fatal("zen 通道必发 x-opencode-request")
	}
	ses := capd.hdr.Get("x-opencode-session")
	if !regexp.MustCompile(`^ses_[0-9a-f]{12}[0-9a-zA-Z]{14}$`).MatchString(ses) {
		t.Fatalf("session 形状非法: %q", ses)
	}
	// zen 通道 session 族三头同值（x-opencode-session-id 是 inference 通道的头，
	// zen 没有它——对齐 lanes/zen.py build_headers）
	for _, h := range []string{"x-session-id", "x-session-affinity"} {
		if capd.hdr.Get(h) != ses {
			t.Fatalf("%s = %q, session 四头必须同值", h, capd.hdr.Get(h))
		}
	}
}

// TestUpstream403Cooldown 403 FreeTierError：真实 403 透传且触发冷却；
// 单上游全冷却时透传缓存的真实 403（而不是合成 503）。
func TestUpstream403Cooldown(t *testing.T) {
	const ftBody = `{"error":{"message":"FreeTierError: flagged","type":"free_tier_error"}}`
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(ftBody))
	}))
	payload := map[string]any{
		"model": "gpt-free", "messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}
	for i := 0; i < 2; i++ {
		rec := postJSON(env, "/v1/chat/completions", "", payload)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("req %d: status = %d, want 403 透传", i, rec.Code)
		}
		if rec.Body.String() != ftBody {
			t.Fatalf("req %d: body = %q, want 真实 403 原样", i, rec.Body.String())
		}
	}
}

func TestUpstream429Passthrough(t *testing.T) {
	const rlBody = `{"error":{"message":"FreeUsageLimitError","type":"rate_limit"}}`
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(rlBody))
	}))
	rec := postJSON(env, "/v1/chat/completions", "", map[string]any{
		"model": "gpt-free", "messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if rec.Code != http.StatusTooManyRequests || rec.Body.String() != rlBody {
		t.Fatalf("status = %d body = %q, want 429 原样透传", rec.Code, rec.Body.String())
	}
}

// ── responses 线（muse-spark-* 原生透传）──────────────────────────────────────

const responsesSSE = "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_t1\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n" +
	"data: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_t1\",\"object\":\"response\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg_1\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"hi\"}]}]}}\n\n"

func TestResponsesPassthroughStream(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(responsesSSE))))
	rec := postJSON(env, "/v1/responses", "", map[string]any{
		"model": "gpt-free", "stream": true,
		"input": "seed-from-responses",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "response.completed") {
		t.Fatalf("透传体缺终止帧: %q", rec.Body.String())
	}
	// 请求形状：stream 强制 true、include_usage 补齐、flat 工具形状
	capd := rc.last()
	if !strings.HasSuffix(capd.path, "/responses") {
		t.Fatalf("upstream path = %s", capd.path)
	}
	if capd.body["stream"] != true {
		t.Fatalf("stream = %v", capd.body["stream"])
	}
	tools, _ := capd.body["tools"].([]any)
	if len(tools) == 0 {
		t.Fatalf("flat 线也要注入闸门工具: %v", capd.body["tools"])
	}
	t0 := tools[0].(map[string]any)
	if _, nested := t0["function"]; nested {
		t.Fatalf("flat 线工具不应有 function 包裹层: %v", t0)
	}
	if t0["name"] != "bash" {
		t.Fatalf("flat 工具[0] = %v", t0)
	}
}

func TestResponsesNonStreamAggregates(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(responsesSSE))))
	rec := postJSON(env, "/v1/responses", "", map[string]any{
		"model": "gpt-free", "input": "hi",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["status"] != "completed" || out["id"] != "resp_t1" {
		t.Fatalf("聚合结果 = %v", out)
	}
}

// ── anthropic 线（本地翻译，上游仍走 chat）─────────────────────────────────────

func TestAnthropicNonStream(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(chatSSE))))
	rec := postJSON(env, "/v1/messages", "", map[string]any{
		"model": "gpt-free", "stream": false, "max_tokens": 100,
		"system":   "be nice",
		"messages": []any{map[string]any{"role": "user", "content": "Hello"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["type"] != "message" || out["role"] != "assistant" {
		t.Fatalf("out = %v", out)
	}
	blocks := out["content"].([]any)
	b0 := blocks[0].(map[string]any)
	if b0["text"] != "你好 world" {
		t.Fatalf("text = %v", b0)
	}
	if out["stop_reason"] != "end_turn" {
		t.Fatalf("stop_reason = %v", out["stop_reason"])
	}
	usage := out["usage"].(map[string]any)
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(4) {
		t.Fatalf("usage = %v（chat 的 prompt/completion 必须透出为 input/output）", usage)
	}
	// 上游收到的是翻译后的 chat 体
	capd := rc.last()
	msgs := capd.body["messages"].([]any)
	if msgs[0].(map[string]any)["role"] != "system" {
		t.Fatalf("system 应翻译为首条 system: %v", msgs)
	}
	if msgs[1].(map[string]any)["content"] != "Hello" {
		t.Fatalf("user 翻译丢失: %v", msgs[1])
	}
	if capd.body["max_tokens"] != float64(100) {
		t.Fatalf("max_tokens = %v", capd.body["max_tokens"])
	}
}

func TestAnthropicStream(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(chatSSE))))
	rec := postJSON(env, "/v1/messages", "", map[string]any{
		"model": "gpt-free", "stream": true, "max_tokens": 100,
		"messages": []any{map[string]any{"role": "user", "content": "Hello"}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: message_start",
		"event: content_block_start",
		"event: content_block_delta", `"text_delta"`,
		"event: content_block_stop",
		`"stop_reason":"end_turn"`,
		`"input_tokens":11`,
		"event: message_stop",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("流缺帧 %q\nbody:\n%s", want, body)
		}
	}
}

func TestAnthropicUpstreamErrorShape(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"message":"FreeTierError","type":"free_tier_error"}}`))
	}))
	rec := postJSON(env, "/v1/messages", "", map[string]any{
		"model": "gpt-free", "messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["type"] != "error" {
		t.Fatalf("错误体必须是 Anthropic 形状: %v", out)
	}
	if errObj := out["error"].(map[string]any); errObj["type"] != "permission_error" {
		t.Fatalf("403 应映射 permission_error: %v", errObj)
	}
}

// ── session 指纹与种子 ─────────────────────────────────────────────────────────

// TestSessionDescending 同一种子稳定、跨种子不同、格式 ses_+12hex+14base62。
func TestSessionDescending(t *testing.T) {
	st := session.NewStore(filepath.Join(t.TempDir(), "session_cache.json"))
	a1, a2 := st.For("alpha"), st.For("alpha")
	if a1 != a2 {
		t.Fatalf("同种子必须稳定: %q != %q", a1, a2)
	}
	if b := st.For("beta"); b == a1 {
		t.Fatal("跨种子必须不同")
	}
	re := regexp.MustCompile(`^ses_([0-9a-f]{12})([0-9a-zA-Z]{14})$`)
	m := re.FindStringSubmatch(a1)
	if m == nil {
		t.Fatalf("形状非法: %q", a1)
	}
	// 降序编码 = ~(now_ms<<12+k) 低 48 位：不可能是全 0 / 全 f
	if m[1] == "ffffffffffff" || m[1] == "000000000000" {
		t.Fatalf("head 不是降序 TypeID 编码: %q", m[1])
	}
}

func TestSeedExtraction(t *testing.T) {
	seed := session.Seed(map[string]any{
		"messages": []any{
			map[string]any{"role": "system", "content": "sys"},
			map[string]any{"role": "user", "content": "first-user-msg"},
		},
	})
	if seed != "first-user-msg" {
		t.Fatalf("seed = %q", seed)
	}
	if s := session.Seed(map[string]any{"input": "resp-input"}); s != "resp-input" {
		t.Fatalf("responses 线 seed = %q", s)
	}
	if s := session.Seed(map[string]any{}); s != "" {
		t.Fatalf("空 payload seed = %q", s)
	}
}

// ── 免费清单闸门 ──────────────────────────────────────────────────────────────

// TestFreeGateFallback 清单拉不到时按命名规则兜底：-free 后缀与 big-pickle 放行。
func TestFreeGateFallback(t *testing.T) {
	env := newEnv(t, nil) // 无上游：清单恒空
	if !env.free.IsFree("mimo-v2.6-flash-free") {
		t.Fatal("命名规则兜底应放行 -free 模型")
	}
	if !env.free.IsFree("big-pickle") {
		t.Fatal("big-pickle 特例应放行")
	}
	if env.free.IsFree("gpt-5") {
		t.Fatal("付费 id 应被拒")
	}
}

// TestDumpReqChatOnlySimple chat 线落 req 原文、responses 线不落。
func TestDumpReqChatOnlySimple(t *testing.T) {
	rc := &recorder{}
	env := newEnv(t, rc.handlerFn(modelsRespond(sseRespond(chatSSE))))
	dir := filepath.Join(t.TempDir(), "dumps")
	env.cfg.DumpDir = dir
	// 重建一个带 Dump 的 env（newEnv 不导出内部依赖，直接换 Server 太绕，
	// 这里换一个独立小环境）
	stateDir := t.TempDir()
	cfg := *env.cfg
	cfg.StateDir = stateDir
	cfg.DumpDir = dir
	cfg.DumpOn = true
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := session.NewStore(filepath.Join(stateDir, "session_cache.json"))
	dw := dump.New(true, dir, 10)

	var order []upstream.Lane
	for _, name := range []string{"zen", "inference"} {
		l := upstream.Lane{Name: name, Base: env.up.URL}
		if name == "zen" {
			l.Headers = func(ses string) map[string]string { return lanes.ZenBuildHeaders("Bearer public", ses) }
			l.PrepareBody = lanes.ZenPrepareBody
		} else {
			l.Headers = func(ses string) map[string]string { return lanes.BuildInferenceHeaders("Bearer public", ses) }
			l.PrepareBody = lanes.InferencePrepareBody
		}
		order = append(order, l)
	}
	disp := upstream.New(order, upstream.Options{CooldownS: 600, Cooldown429S: 60, Retry: 0,
		Concurrency: 2, Pool: 4, Timeout: 5}, store.For, logger)
	disp.Dump = dw.Save
	free := freelist.New(filepath.Join(stateDir, "free_cache.json"),
		func() freelist.Lane { return freelist.Lane{Name: "zen", Base: env.up.URL, Headers: order[0].Headers} },
		func() string { return store.For("") })
	free.Fetch(true, false)

	h := (&Server{Cfg: &cfg, Free: free, Dump: dw, Up: disp, Sess: store, Log: logger}).Handler()
	payload := map[string]any{
		"model": "gpt-free", "messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}
	rawReq, _ := json.Marshal(payload)
	for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(rawReq))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	entries, err := filepath.Glob(filepath.Join(dir, "*.req.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("req dump 数 = %d, want 1（仅 chat 线落 req）: %v", len(entries), entries)
	}
	raw, err := os.ReadFile(entries[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "gpt-free") {
		t.Fatalf("req dump 内容异常: %s", raw)
	}
}
