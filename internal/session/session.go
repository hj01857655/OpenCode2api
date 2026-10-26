// Package session 对应 Python 版 server.py 的「闸门指纹」段：
// V2 session 降序 TypeID 生成 + 种子→会话映射（落盘 session_cache.json，3 天 TTL）。
//
// 上游 2026-10-11 起校验 session id 的降序时间戳语义（2026-10-15 消融定案）：
// 前 12 位 hex 必须是 ~(ts_ms<<12 + counter) 的低 48 位（对齐上游
// packages/schema/src/identifier.ts 的 descending TypeID），纯随机/升序即 403，
// counter 有上界 63（64 即拒）。同一会话值又必须稳定（prompt 缓存亲和），
// 所以用「种子→会话映射 + 落盘 + 3 天 TTL」，而不是逐请求生成。
//
// 磁盘格式与 Python 版字节兼容：{"<seed>": {"ses": "ses_...", "ts": <unix_ms>}, ...}
// （json.dump(_session_map, f, ensure_ascii=False)）。两个版本可交替读写。
package session

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	ttlMS     = int64(3 * 24 * 3600 * 1000) // _SESSION_TTL_MS
	maxEntry  = 256                         // _SESSION_MAX
	prefixHex = 12                          // 前 12 位 hex = 降序时间戳编码
	prefixB62 = 14                          // 后 14 位 base62 = 随机尾巴
)

const b62table = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Store 种子→session 映射（线程安全）。
type Store struct {
	path string

	mu      sync.Mutex
	m       map[string]entry
	lastMS  int64
	counter int64
	loaded  bool
}

type entry struct {
	Ses string `json:"ses"`
	Ts  int64  `json:"ts"`
}

func NewStore(path string) *Store {
	return &Store{path: path, m: map[string]entry{}}
}

// Load 读落盘映射（对应 _load_session_map）。文件缺失/损坏不是错误：
// 首次启动本就如此；单个条目形状不对（ses 非字符串 / ts 缺失）只跳过该条。
func (s *Store) Load() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loaded = true
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(raw, &data) != nil {
		return
	}
	for k, v := range data {
		var e entry
		if json.Unmarshal(v, &e) != nil {
			continue
		}
		if e.Ses == "" || e.Ts == 0 {
			continue
		}
		s.m[k] = e
	}
}

// save 全量落盘（对应 _save_session_map）。失败静默——映射丢了只是重建 session，
// 不该影响转发。
//
// 字节兼容：seed 里常含 HTML 尖括号（长上下文摘要），Go 默认会把它们转义成
// <，与 Python 的 ensure_ascii=False 输出不一致，所以关掉 EscapeHTML；
// 非 ASCII 字符两边都是直接写 UTF-8。
func (s *Store) save() {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s.m); err != nil {
		return
	}
	raw := bytes.TrimRight(buf.Bytes(), "\n")
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, s.path)
}

// newID ses_ + 26 位降序 TypeID 主体（前 12 hex 反转时间戳 + 14 base62 随机尾）。
//
// 每毫秒计数器：now_ms 变化则 counter 归 0 再自增，与 Python 版 _new_oc_session
// 同语义。上游 counter 上界 63 —— 同一毫秒内第 64 个即拒；本地生成频率远达不到。
func (s *Store) newID() string {
	nowMS := time.Now().UnixMilli()
	if nowMS != s.lastMS {
		s.lastMS = nowMS
		s.counter = 0
	}
	s.counter++
	// value = (~(now_ms * 0x1000 + counter)) & 0xFFFFFFFFFFFF
	value := ^(nowMS*0x1000 + s.counter) & 0xFFFFFFFFFFFF
	head := make([]byte, 12)
	const hexDigits = "0123456789abcdef"
	for i := 0; i < 6; i++ {
		b := byte((value >> (40 - 8*i)) & 0xFF)
		head[2*i] = hexDigits[b>>4]
		head[2*i+1] = hexDigits[b&0xF]
	}
	tail := make([]byte, prefixB62)
	rb := make([]byte, prefixB62)
	if _, err := rand.Read(rb); err != nil {
		for i := range tail {
			tail[i] = '0'
		}
	} else {
		for i := range tail {
			tail[i] = b62table[rb[i]%62]
		}
	}
	return "ses_" + string(head) + string(tail)
}

// For 会话种子 → 上游 session（稳定 + 降序编码 + 3 天 TTL 再生成）。
// 对应 _stable_session / _oc_session_for。
func (s *Store) For(seed string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.loaded {
		s.loaded = true
		// 延迟加载：构造后没显式 Load 的路径（测试）也要能吃到磁盘映射
		if raw, err := os.ReadFile(s.path); err == nil {
			var data map[string]json.RawMessage
			if json.Unmarshal(raw, &data) == nil {
				for k, v := range data {
					var e entry
					if json.Unmarshal(v, &e) == nil && e.Ses != "" && e.Ts != 0 {
						s.m[k] = e
					}
				}
			}
		}
	}
	nowMS := time.Now().UnixMilli()
	if e, ok := s.m[seed]; ok && nowMS-e.Ts < ttlMS {
		return e.Ses
	}
	if len(s.m) >= maxEntry {
		oldest, oldestTS := "", int64(0)
		for k, e := range s.m {
			if oldest == "" || e.Ts < oldestTS {
				oldest, oldestTS = k, e.Ts
			}
		}
		if oldest != "" {
			delete(s.m, oldest)
		}
	}
	s.m[seed] = entry{Ses: s.newID(), Ts: nowMS}
	s.save()
	return s.m[seed].Ses
}

// Seed 会话种子 = 首条用户消息（同对话稳定，跨请求不变）。对应 server._session_seed。
//
// chat 线读 messages；Responses 线读 input（字符串或分段列表）。
// 截断 256 字符（Python [:256] 是字符数，这里按 rune 数）。
func Seed(p map[string]any) string {
	for _, mi := range asList(p["messages"]) {
		m := asMap(mi)
		if m == nil || asString(m["role"]) != "user" {
			continue
		}
		c := m["content"]
		if parts, ok := c.([]any); ok {
			segs := make([]string, 0, len(parts))
			for _, pi := range parts {
				if pm := asMap(pi); pm != nil {
					segs = append(segs, asString(pm["text"]))
				}
			}
			c = strings.Join(segs, " ")
		}
		if s := asString(c); s != "" {
			return truncate(s, 256)
		}
	}
	if s, ok := p["input"].(string); ok && s != "" {
		return truncate(s, 256)
	}
	if list, ok := p["input"].([]any); ok {
		for _, xi := range list {
			x := asMap(xi)
			if x == nil || asString(x["type"]) != "message" {
				continue
			}
			c := x["content"]
			if parts, ok := c.([]any); ok {
				segs := make([]string, 0, len(parts))
				for _, pi := range parts {
					if pm := asMap(pi); pm != nil && asString(pm["type"]) == "input_text" {
						segs = append(segs, asString(pm["text"]))
					}
				}
				c = strings.Join(segs, " ")
			}
			if s := asString(c); s != "" {
				return truncate(s, 256)
			}
		}
	}
	return ""
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
