// Package upstream 对应 Python 版 server.py 的「上游转发 + 通道选择与冷却调度」：
// 多通道直连（_post_upstream）、单通道发送含连接级重试（_post_lane）、
// 共享连接池（requests.Session）、并发闸（BoundedSemaphore）、
// 风控冷却（_gate / _freetier_body）。
//
// 2026-10-13 实测：chat 线 210 次失败全是 SSLEOFError，而同期纯 TLS 握手与
// 串行/并发 GET+POST 全部 0 失败。结论：是上游在坏窗口里「接受 TCP+TLS、读完
// 请求后不发响应直接 FIN」的偶发断连。两个动作各自解决一部分：
//
//	A. 共享连接池（keep-alive）：原先每请求新建连接都要重走握手，坏窗口里
//	   握手次数≈失败次数；池化复用长连接把暴露在坏窗口里的握手数降下来。
//	   ——Go 的 http.Transport 默认就池化，这里显式设 MaxIdleConnsPerHost。
//	B. 连接级失败重试 1 次：只对「尚未拿到任何响应」的连接类错误重试。
//	   SSE 字节一旦开始下发就绝不重试（客户端已收到半截流，重发会重复输出且
//	   重复计费）。
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Lane 一个上游通道（对应 lanes/*.py 暴露的 BASE / build_headers / prepare_body）。
type Lane struct {
	Name        string
	Base        string
	Headers     func(ses string) map[string]string
	PrepareBody func(payload map[string]any, flat bool) map[string]any
}

// Options 调度参数（对齐 server.py 的 env 常量）。
type Options struct {
	CooldownS    int // 403 FreeTierError → 该通道冷却秒数
	Cooldown429S int // 429 限流 → 短冷却秒数
	Retry        int // 连接级失败重试次数
	Concurrency  int // 并发闸
	Pool         int // 连接池大小
	Timeout      int // 拿响应头的超时秒（requests 的 timeout 语义）
}

// Dispatcher 多通道调度器。
type Dispatcher struct {
	laneOrder []string
	lanes     map[string]*Lane
	opts      Options
	log       *slog.Logger

	// Sess 会话种子 → 上游 session（session.Store.For）
	Sess func(seed string) string
	// Dump 上游错误体落盘（Python 在 403/404 与 >=400 分支落 resp）
	Dump func(rid, kind, data string)

	http *http.Client
	sema chan struct{}

	mu           sync.Mutex
	gate         map[string]time.Time // 通道 → 冷却截止时刻
	freetierBody map[string]string    // 通道 → 最后一次 FreeTierError 原始错误体
}

// ErrNoResponse 全部通道连接失败且底层错误不可归类时的兜底（对应 Python 抛
// requests.RequestException，路由层映射 502）。
var ErrNoResponse = errors.New("upstream connection failed")

func New(order []Lane, opts Options, sess func(seed string) string, log *slog.Logger) *Dispatcher {
	lanes := map[string]*Lane{}
	names := make([]string, 0, len(order))
	for i := range order {
		l := order[i]
		lanes[l.Name] = &l
		names = append(names, l.Name)
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Pool <= 0 {
		opts.Pool = 32
	}
	tr := &http.Transport{
		MaxIdleConns:          opts.Pool,
		MaxIdleConnsPerHost:   opts.Pool,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: time.Duration(opts.Timeout) * time.Second,
	}
	return &Dispatcher{
		laneOrder:    names,
		lanes:        lanes,
		opts:         opts,
		log:          log,
		Sess:         sess,
		Dump:         func(string, string, string) {},
		http:         &http.Client{Transport: tr},
		sema:         make(chan struct{}, opts.Concurrency),
		gate:         map[string]time.Time{},
		freetierBody: map[string]string{},
	}
}

// RemainingCooling 该通道剩余冷却秒数；0 = 未冷却。对应 _lane_cooling。
// （返回剩余而非绝对时刻，调用方直接可读。）
func (d *Dispatcher) RemainingCooling(lane string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	until := d.gate[lane]
	if now := time.Now(); now.Before(until) {
		return int(until.Sub(now).Seconds()) + 1
	}
	return 0
}

func (d *Dispatcher) mark(lane, rid, why string, seconds int) {
	if seconds <= 0 {
		seconds = d.opts.CooldownS
	}
	d.mu.Lock()
	d.gate[lane] = time.Now().Add(time.Duration(seconds) * time.Second)
	d.mu.Unlock()
	d.log.Warn("通道命中风控，冷却内不再请求",
		slog.String("rid", rid), slog.String("lane", lane),
		slog.String("why", why), slog.Int("cooldownS", seconds))
}

// PickLaneForModels 按通道顺序取首个未冷却通道；全冷却则用第一个。
// 对应 _fetch_free 里的 lane 选择（模型清单与推理同为匿名面）。
// 无通道配置时返回零值 Lane（Base==""，调用方按拉取失败处理；Python 侧
// _LANE_ORDER 有 `or ["zen","inference"]` 兜底不可能为空，这里仅防退化）。
func (d *Dispatcher) PickLaneForModels() Lane {
	if len(d.laneOrder) == 0 {
		return Lane{}
	}
	for _, name := range d.laneOrder {
		if d.RemainingCooling(name) == 0 {
			return *d.lanes[name]
		}
	}
	return *d.lanes[d.laneOrder[0]]
}

// Result 一次调度结果。Resp 非空即有响应可交付：status<400 时 Resp.Body 是尚未
// 消费的流（路由侧透传或本地聚合），status>=400 时 Body 是已物化的错误体文本
// （Python 的 r.text 语义：读一次进内存，路由可反复取用）。
type Result struct {
	Resp        *http.Response
	Body        string
	ContentType string // 交付给客户端的响应 content-type（对齐 Python headers.get）
	Lane        string
}

// Post 多通道直连：按 laneOrder 顺序逐通道尝试。对应 _post_upstream。
//
// 每通道的 body 准备由各自 lane 模块完成（自包含门禁规格），这里不掺和通道细节。
// 403 FreeTierError 触发该通道冷却并继续下一通道；所有通道都不可用（都在冷却 /
// 都 403 / 都断连）时分别以缓存的真实 403（无缓存才 503 合成）、最后一个真实 404、
// 或连接异常交付给调用方的既有错误分支。
func (d *Dispatcher) Post(ctx context.Context, path string, payload map[string]any, seed, rid string, flat bool) (*Result, error) {
	var candidate403 *Result
	var candidate404 *Result
	var netErr error

	for _, laneName := range d.laneOrder {
		if left := d.RemainingCooling(laneName); left > 0 {
			d.log.Info("通道冷却中，跳过",
				slog.String("rid", rid), slog.String("lane", laneName), slog.Int("leftS", left))
			continue
		}
		lane := d.lanes[laneName]
		resp, err := d.postLane(ctx, lane, path, payload, seed, rid, flat)
		if err != nil {
			netErr = err
			d.log.Error("通道连接失败",
				slog.String("rid", rid), slog.String("lane", laneName),
				slog.String("err", err.Error()))
			continue
		}

		status := resp.StatusCode
		switch {
		case status < 400:
			// 成功：body 一个字节都不许碰（SSE 一旦开始下发就绝不重试/读空），
			// 直接交付未消费的流；路由侧按 want_stream 决定透传还是本地聚合。
			if laneName != d.laneOrder[0] {
				d.log.Warn("首选通道不可用，回落通道成功",
					slog.String("rid", rid), slog.String("lane", laneName))
			}
			return &Result{Resp: resp, ContentType: ContentType(resp), Lane: laneName}, nil

		case status == 403 || status == 404:
			body := drainBody(resp)
			ct := ContentType(resp)
			d.log.Warn("通道上游异常状态",
				slog.String("rid", rid), slog.String("lane", laneName),
				slog.Int("status", status), slog.String("body", trunc(body, 400)))
			d.Dump(rid, "resp", body)
			if status == 403 && isFreeTier(body) {
				d.mark(laneName, rid, "FreeTierError", d.opts.CooldownS)
				d.mu.Lock()
				d.freetierBody[laneName] = body
				d.mu.Unlock()
				candidate403 = &Result{Resp: syntheticResponse(status, body, ct), Lane: laneName, Body: body, ContentType: ct}
				continue
			}
			if status == 404 {
				// 该通道没有这个端点（如 zen 通道可能只有 /chat/completions，
				// 没有 /responses）→ 不冷却，回落下一通道；全部 404 时交付最后一个
				candidate404 = &Result{Resp: syntheticResponse(status, body, ct), Lane: laneName, Body: body, ContentType: ct}
				continue
			}
			// 非 FreeTier 的 403（形状类问题）：不冷却，原样交付让人看见
			return &Result{Resp: syntheticResponse(status, body, ct), Lane: laneName, Body: body, ContentType: ct}, nil

		case status == 429:
			// 指纹已过、只是限流（FreeUsageLimitError）：短冷却防硬撞，429 原样
			// 透传——客户端据此等一等再试，比吃 503 语义更准。
			body := drainBody(resp)
			ct := ContentType(resp)
			d.log.Warn("通道上游 429 限流",
				slog.String("rid", rid), slog.String("lane", laneName), slog.String("body", trunc(body, 200)))
			d.mark(laneName, rid, "RateLimit", d.opts.Cooldown429S)
			return &Result{Resp: syntheticResponse(status, body, ct), Lane: laneName, Body: body, ContentType: ct}, nil

		default: // status >= 400
			body := drainBody(resp)
			ct := ContentType(resp)
			d.log.Warn("通道上游错误",
				slog.String("rid", rid), slog.String("lane", laneName),
				slog.Int("status", status), slog.String("body", trunc(body, 400)))
			d.Dump(rid, "resp", body)
			return &Result{Resp: syntheticResponse(status, body, ct), Lane: laneName, Body: body, ContentType: ct}, nil
		}
	}

	// 有真实 FreeTier 403 候选：交付它（Python: return resp403, resp403_lane）。
	if candidate403 != nil {
		return candidate403, nil
	}
	if candidate404 != nil {
		return candidate404, nil
	}
	if netErr != nil {
		return nil, netErr
	}

	// 全部通道都在冷却。
	left := 0
	for _, name := range d.laneOrder {
		if rem := d.RemainingCooling(name); rem > left {
			left = rem
		}
	}
	d.mu.Lock()
	var cached string
	for _, name := range d.laneOrder {
		if time.Now().Before(d.gate[name]) {
			if b := d.freetierBody[name]; b != "" {
				cached = b
				break
			}
		}
	}
	d.mu.Unlock()

	if cached != "" {
		// 冷却 ≠ 掩盖：把触发冷却的那份真实 403 原样交给客户端
		d.log.Warn("全部通道均在风控冷却，透传缓存的真实 403",
			slog.String("rid", rid), slog.Int("leftS", left))
		return &Result{Resp: syntheticResponse(403, cached, "application/json"),
			Lane: "cooldown", Body: cached, ContentType: "application/json"}, nil
	}
	d.log.Warn("全部通道均在风控冷却，本地快速失败",
		slog.String("rid", rid), slog.Int("leftS", left))
	msg := fmt.Sprintf(`{"error":{"message":"upstream free tier flagged (403), cooldown %ds left, retry later","type":"overloaded_error"}}`, left)
	return &Result{Resp: syntheticResponse(503, msg, "application/json"),
		Lane: "cooldown", Body: msg, ContentType: "application/json"}, nil
}

// postLane 单通道一次发送（含连接级重试）。对应 _post_lane。
func (d *Dispatcher) postLane(ctx context.Context, lane *Lane, path string, payload map[string]any, seed, rid string, flat bool) (*http.Response, error) {
	body := lane.PrepareBody(payload, flat)
	bodyJSON := marshalCompact(body)
	headers := lane.Headers(d.Sess(seed))
	url := lane.Base + path

	var lastErr error
	for attempt := 0; attempt <= d.opts.Retry; attempt++ {
		resp, err := d.doOnce(ctx, url, bodyJSON, headers)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if attempt >= d.opts.Retry || !isRetryable(err) {
			return nil, err
		}
		// 抖动一下再试：坏窗口通常成串，立刻重发很可能撞同一个坑。
		// 断开的那条连接已从池里剔除，重试自然用新连接。
		back := 0.4*float64(attempt+1) + rand.Float64()*0.3
		d.log.Warn("上游连接被切断，稍后重试",
			slog.String("rid", rid),
			slog.Float64("backoffS", back),
			slog.Int("attempt", attempt+1),
			slog.String("of", fmt.Sprint(d.opts.Retry)),
			slog.String("err", trunc(err.Error(), 120)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(back * float64(time.Second))):
		}
	}
	if lastErr == nil {
		lastErr = ErrNoResponse
	}
	return nil, lastErr
}

// doOnce 带并发闸地执行一次 POST。
//
// 超时语义对齐 requests 的 timeout=180：那是「连接+直到拿到响应头」的超时，
// 不是整个响应的上限——SSE 长流不能被掐断。所以用 Transport 的
// ResponseHeaderTimeout 实现，而不是 http.Client.Timeout（后者会连 body
// 读完一起限时）。请求 ctx 只跟随客户端断开。
func (d *Dispatcher) doOnce(ctx context.Context, url, bodyJSON string, headers map[string]string) (*http.Response, error) {
	d.sema <- struct{}{}
	defer func() { <-d.sema }()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return d.http.Do(req)
}

// isRetryable 只在「尚未拿到任何响应」的连接类错误时重试；其余一律放过。
// 对应 _is_retryable_upstream_error：ConnectTimeout 属于 ConnectionError（重试），
// ReadTimeout 不属于（不重试）——读超时说明请求很可能已被上游受理并可能已计费，
// 重发就是二次扣额度，宁可不试。Go 侧对应：拿到响应头之前的连接/DNS/TLS 层错误
// 重试；ResponseHeaderTimeout 触发的 net.Error.Timeout 不重试。
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false // 客户端主动断开，不重发
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false // 响应头超时：请求可能已受理，不重发
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	low := strings.ToLower(err.Error())
	for _, s := range []string{"connection reset", "connection refused", "broken pipe",
		"eof", "tls", "handshake", "no such host", "use of closed"} {
		if strings.Contains(low, s) {
			return true
		}
	}
	return false
}

// drainBody 读完错误响应 body 并关闭连接（对应 Python 的 r.text）。
func drainBody(resp *http.Response) string {
	if resp == nil || resp.Body == nil {
		return ""
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
	resp.Body.Close()
	return string(b)
}

// syntheticResponse 本地合成响应。路由层的错误分支只消费 status/body/content-type。
func syntheticResponse(status int, body, contentType string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{contentType}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// ContentType 响应 content-type（缺省 application/json，对齐 Python 的
// `headers.get("Content-Type", "application/json")`）。
func ContentType(resp *http.Response) string {
	if resp == nil {
		return "application/json"
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		return ct
	}
	return "application/json"
}

// isFreeTier 403 body 里是否是 FreeTierError 语义（大小写不敏感子串判定）。
func isFreeTier(body string) bool {
	low := strings.ToLower(body)
	return strings.Contains(low, "freetiererror") || strings.Contains(low, "free tier")
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// marshalCompact JSON 紧凑序列化，不转义 HTML 字符（对齐 requests 的 json= body，
// Python 侧 ensure_ascii=False 语义下的直接 UTF-8）。
func marshalCompact(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "null"
	}
	return strings.TrimRight(buf.String(), "\n")
}
