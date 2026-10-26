// Package freelist 对应 Python 版 server.py 的「free 模型清单」段：
// 活清单（10 分钟 TTL）+ 落盘上次可用清单 + 请求路径上的限时补拉。
//
// 为什么这么麻烦：2026-10-13 00:25 那波「拒绝非免费」的根因不是「模型变了」，
// 而是**清单拉不到 = 清单空 = 闸门把所有请求都判成非免费**。实测这条 GET 的
// 失败是**成串来的**（同一时刻 curl 也挂），一次失败要 5~6s 才返回，所以
// 「多试几次」在坏窗口里会把用户请求拖死十几秒。三条一起才成立：
//  1. 失败不清空旧清单（一次抖动不该丢掉已知可用的名单）
//  2. 清单落盘（重启不从空开始）—— 磁盘格式 {"ids":[...],"at":<unix>}
//  3. 请求路径上的补拉只发一次、超时很短，失败立刻走命名兜底 —— 重试交给后台线程
package freelist

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"opencode2api/internal/lanes"
)

const (
	freeTTL       = 600 * time.Second // 活清单有效期
	inlineTimeout = 4 * time.Second   // 请求路径上补拉的超时：宁可快速失败，不可拖死用户
	failBackoff   = 30 * time.Second  // 连续失败后这么久之内不再在请求路径上重试
	ownedBy       = "the unit-zen-free"
)

// Lane 清单拉取用的通道（基址 + 头生成）。
// 模型清单与推理同为匿名面，按通道顺序取首个未冷却通道拉取 ——
// 冷却状态归调度层管，所以这里收一个「挑当前可用通道」的回调。
type Lane struct {
	Name    string
	Base    string
	Headers func(ses string) map[string]string
}

// List 线程安全的免费清单。
type List struct {
	pickLane  func() Lane   // 当前应使用的通道（已按冷却挑好）
	sessionID func() string // 稳定 session（种子为空串的映射）
	cachePath string
	client    *http.Client

	mu        sync.Mutex
	ids       []string
	at        time.Time // 上次「联系上上游」的时刻（新鲜度按它记，不按清单非空记）
	failUntil time.Time
}

func New(cachePath string, pickLane func() Lane, sessionID func() string) *List {
	return &List{
		pickLane:  pickLane,
		sessionID: sessionID,
		cachePath: cachePath,
		client:    &http.Client{},
		ids:       []string{},
	}
}

// freeIDsFromNames 全清单里筛免费面。命名规则即免费面契约：
// 付费 id（claude-*、gpt-* 等）不会匹配。对应 server._free_ids_from_names。
func freeIDsFromNames(ids []string) []string {
	out := []string{}
	for _, id := range ids {
		if id != "" && lanes.FreeName(id) {
			out = append(out, id)
		}
	}
	return out
}

// LoadCache 读上次落盘的清单兜底（对应 _load_free_cache）。
// at 置零让它看起来过期：后台线程 60s 内就会重拉换新；请求路径上不会重拉
// —— IsFree 只在清单为空时才补拉，这里清单非空，直接用缓存不卡用户。
func (l *List) LoadCache() {
	raw, err := os.ReadFile(l.cachePath)
	if err != nil {
		return // 没有缓存或缓存坏了：正常，首次启动就是这样
	}
	var data struct {
		IDs []string `json:"ids"`
	}
	if json.Unmarshal(raw, &data) != nil {
		return
	}
	ids := freeIDsFromNames(data.IDs)
	if len(ids) == 0 {
		return
	}
	l.mu.Lock()
	l.ids, l.at = ids, time.Time{}
	l.mu.Unlock()
	slog.Info("[models] 用上次落盘清单兜底（后台 60s 内换新）", slog.Int("count", len(ids)))
}

func (l *List) saveCache(ids []string) {
	raw, err := json.Marshal(struct {
		IDs []string `json:"ids"`
		At  int64    `json:"at"`
	}{IDs: ids, At: time.Now().Unix()})
	if err != nil {
		return
	}
	tmp := l.cachePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		slog.Warn("[models] 清单落盘失败（不影响转发）", slog.String("err", err.Error()))
		return
	}
	if err := os.Rename(tmp, l.cachePath); err != nil {
		slog.Warn("[models] 清单落盘失败（不影响转发）", slog.String("err", err.Error()))
	}
}

// Fetch 从活清单拉 free 模型（清单会变，活清单优先）。对应 server._fetch_free。
//
// quick=true 给请求路径用：只试一次、超时 4s、失败进退避窗口。坏窗口里一次
// 失败要 5~6s 才返回，重试三轮会在用户请求上挂十几秒 —— 那比「暂时按命名兜底」
// 糟得多。真正的重试由后台线程做（它挂多久都不影响转发）。
//
// 新鲜度按**是否联系上上游**记（at），不按「清单非空」记：上游真把免费面清空时
// （全部转付费、或命名规则改了导致筛子筛空），空清单也是可信答案，得按 TTL 认
// 10 分钟 —— 否则每条请求都会重打一次上游，还会把日志刷成一片 free 模型 0 个。
func (l *List) Fetch(force, quick bool) {
	l.mu.Lock()
	if !force && time.Since(l.at) < freeTTL {
		l.mu.Unlock()
		return
	}
	if quick && time.Now().Before(l.failUntil) {
		l.mu.Unlock()
		return // 刚失败过：这个窗口别再让每个请求都去撞一次
	}
	l.mu.Unlock()

	attempts, timeout := 3, 20*time.Second
	if quick {
		attempts, timeout = 1, inlineTimeout
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		ids, contacted, err := l.fetchOnce(timeout)
		if err == nil && contacted {
			l.mu.Lock()
			l.ids, l.at = ids, time.Now()
			l.failUntil = time.Time{}
			l.mu.Unlock()
			if len(ids) > 0 {
				l.saveCache(ids)
			}
			slog.Info("[models] free 模型清单", slog.Int("count", len(ids)),
				slog.String("ids", strings.Join(head(ids, 12), ", ")))
			return
		}
		if err != nil {
			lastErr = err
		} else {
			// 非 200：fetchOnce 内部已告警，这里不重试（Python 版同语义 return）
			return
		}
		if attempt < attempts-1 {
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
	}
	l.mu.Lock()
	l.failUntil = time.Now().Add(failBackoff)
	kept := len(l.ids)
	l.mu.Unlock()
	// 带异常类型：这个分支把网络错误和代码错误（清单结构变了、解析错）吞在同一句
	// 「拉取失败」里，只打消息下次排查会认错方向。
	slog.Warn("[models] 拉取失败；保留旧清单",
		slog.Int("attempts", attempts),
		slog.String("err", describeErr(lastErr)),
		slog.Int("kept", kept))
}

// fetchOnce 单次 GET <lane.Base>/models。返回 (免费 id, 是否联系上上游, err)。
// 非 200 视为「联系上了但拿不到清单」→ contacted=true, err=nil（调用方直接返回，
// 对齐 Python 版 `status_code != 200 → return`）。
func (l *List) fetchOnce(timeout time.Duration) (ids []string, contacted bool, err error) {
	lane := l.pickLane()
	if lane.Base == "" {
		return nil, false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, lane.Base+"/models", nil)
	if err != nil {
		return nil, false, err
	}
	for k, v := range lane.Headers(l.sessionID()) {
		req.Header.Set(k, v)
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		slog.Warn("[models] 清单拉取非 200",
			slog.Int("status", resp.StatusCode), slog.String("lane", lane.Name))
		return nil, true, nil
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, false, err
		}
	}
	all := make([]string, 0, len(body.Data))
	for _, m := range body.Data {
		if m.ID != "" {
			all = append(all, m.ID)
		}
	}
	return freeIDsFromNames(all), true, nil
}

// IsFree 免费面判定（对应 server._model_free）。
// 拉不到、也没有可用缓存（首次启动正撞上坏窗口）时按免费面命名规则兜底 ——
// 付费 id 匹配不上这条规则，所以这不放宽「防误打订阅面扣费」的原意，
// 只是不再把合法 free 模型当成非免费全拒。
func (l *List) IsFree(mid string) bool {
	l.mu.Lock()
	empty := len(l.ids) == 0
	l.mu.Unlock()
	if empty {
		l.Fetch(false, true)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ids) > 0 {
		for _, id := range l.ids {
			if id == mid {
				return true
			}
		}
		return false
	}
	return lanes.FreeName(mid)
}

// IDs 当前免费清单（/v1/models 用）；空时先快速补拉一次。
func (l *List) IDs() []string {
	l.mu.Lock()
	empty := len(l.ids) == 0
	l.mu.Unlock()
	if empty {
		l.Fetch(false, true)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ids...)
}

// Count 当前清单长度（/health 用）。
func (l *List) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ids)
}

// OwnedBy /v1/models 的 owned_by 字段（与 Python 版字面一致）。
func OwnedBy() string { return ownedBy }

// RefreshLoop 后台刷新：每 60s 醒一次，清单还新鲜就立刻返回。
//
// 为什么不是睡 600：原来只在成功的那一轮之后才计时，一次失败要等满 10 分钟才再试，
// 坏窗口能连撞 2~3 轮 —— 用户这段时间全在吃 400。改成短周期轮询，失败后 60s 就有
// 下一次机会；成功时 freeTTL 仍保证 10 分钟才真打一次上游。
func (l *List) RefreshLoop(ctx context.Context) {
	t := time.NewTimer(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Fetch(false, false)
			t.Reset(60 * time.Second)
		}
	}
}

func head(vals []string, n int) []string {
	if len(vals) <= n {
		return vals
	}
	return vals[:n]
}

func describeErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
