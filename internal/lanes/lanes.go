// Package lanes 对应 Python 版 lanes/：上游双通道适配，每通道一个自包含文件
// （门禁规格全在本通道文件里，上游改门禁只改对应通道）。
//
//	lanes/zen.py        legacy /zen/v1 —— v1.3.7 形状，仅需一个 read 工具（优先）
//	lanes/inference.py  /inference/v1 —— 2.x 指纹，bash+read 最少集（回落）
//
// 两个通道实现了同一个 Lane 接口（BASE / build_headers / prepare_body），
// server 层只按接口调度，不掺和通道细节。
package lanes

import (
	"crypto/rand"
	"regexp"
	"strings"
)

// Deprecated 上游已废弃模型（实测 410 ModelDeprecated）：清单里剔除，避免误调必失败。
// 对应 lanes/inference.py 的 DEPRECATED（server.py 的 `_DEPRECATED = inference.DEPRECATED`）。
var Deprecated = map[string]bool{"exo-free": true}

// FreeName 免费面命名契约：id 以 -free 结尾，或 big-pickle 特例。
// 对应 server._model_free 拉不到清单时的兜底判定（付费 id 匹配不上这条规则，
// 所以不放宽「防误打订阅面扣费」的原意）。注意兜底路径不看 DEPRECATED。
func FreeName(id string) bool {
	return strings.HasSuffix(id, "-free") || id == "big-pickle"
}

// FreeIDs 全清单里筛免费面。对应 server._free_ids_from_names：
// 命名规则 + 剔除上游已废弃模型（实测 410 ModelDeprecated，误调必失败）。
func FreeIDs(ids []string) []string {
	out := []string{}
	for _, id := range ids {
		if id != "" && FreeName(id) && !Deprecated[id] {
			out = append(out, id)
		}
	}
	return out
}

// Lane 单通道：基址 + 指纹头 + body 准备。
type Lane struct {
	Name         string
	Base         string
	Auth         string
	BuildHeaders func(ses string) map[string]string
	PrepareBody  func(payload map[string]any, flat bool) map[string]any
}

// ── 公共辅助 ──────────────────────────────────────────────────────────────────

const hexDigits = "0123456789abcdef"
const b62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// randHex 返回 n 个 hex 字符（crypto/rand，等价 Python random.choice 的取值域）。
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	out := make([]byte, n)
	for i := range b {
		out[i] = hexDigits[int(b[i])%16]
	}
	return string(out)
}

// randB62 返回 n 个 base62 字符（对齐 Python _B62 表：digits + ascii_letters）。
func randB62(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	out := make([]byte, n)
	for i := range b {
		out[i] = b62[int(b[i])%62]
	}
	return string(out)
}

// identReq 官方 ID 形态：req_ + 12位hex + 14位base62（对应 lanes/zen.py._ident）。
func identReq() string { return "req_" + randHex(12) + randB62(14) }

// toolName 取工具名：flat=扁平 Responses 形（无 function 包裹层），否则 Chat 形。
func toolName(t map[string]any, flat bool) string {
	if flat {
		s, _ := t["name"].(string)
		return s
	}
	if fn, ok := t["function"].(map[string]any); ok {
		s, _ := fn["name"].(string)
		return s
	}
	return ""
}

var sepRe = regexp.MustCompile(`[_\-.]`)

// normalizeName 门禁工具名归一化：小写 + 去掉 _ - . （对齐 zen._read_ok/_bash_ok）。
func normalizeName(n string) string {
	return sepRe.ReplaceAllString(strings.ToLower(n), "")
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// toolsOf 把 payload.tools 收进 []any 并列出已有工具名；tools 非法/缺失时 ok=false。
func toolsOf(payload map[string]any, flat bool) (tools []any, names []string, ok bool) {
	raw, exists := payload["tools"]
	if !exists {
		return nil, nil, false
	}
	list, isList := raw.([]any)
	if !isList || len(list) == 0 {
		return nil, nil, false
	}
	for _, ti := range list {
		tools = append(tools, ti)
		if m, ok := ti.(map[string]any); ok {
			names = append(names, toolName(m, flat))
		}
	}
	return tools, names, true
}
