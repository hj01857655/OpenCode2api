// lanes/inference.py 的 Go 版：/inference/v1 通道（自包含：本通道门禁的一切都在本文件）。
//
// 2026-10-11 上游迁移后的主通道；2026-10-18 起为回落通道（zen 优先）。
//
// 门禁规格以 docs/labs/2026-10-15-inference-v2-gate-ablation.md 消融定案为准：
//  1. session id 前 12 位 hex 必须是降序编码 ~(ts_ms<<12+k) 低 48 位
//     （对齐上游 packages/schema/src/identifier.ts）；纯随机/升序即 403 —— 见 session 包
//  2. x-opencode-session-id（v2.0.29 新头）必发，且 session 族 4 头中
//     至少再带 1 个（单发 1 头 / 缺新头均 403）→ 本函数四头全发同值
//  3. x-opencode-request 头不得出现（存在本身触发 403）——与 zen 通道相反
//  4. tools 必须含 bash+read（chat 线最少集，顺序无关；缺 read 即 403）
//  5. UA 必须形如 opencode/<ver>：second 段为 latest 或 ≥2.0.0 的 semver
//
// 与 zen 通道的分野：UA 形状 / x-opencode-request 禁发 / project 不校验（global）/
// 工具门禁 bash+read 最少集。
package lanes

// OCUserAgent UA 是门禁校验项（T/U 系列定案）：须 opencode/<ver≥2.0.0>（或 latest
// 通道）——低于下限 426、非法段 403。
const OCUserAgent = "opencode/latest/2.0.29/cli"

// BuildInferenceHeaders V2 指纹全量头（2026-10-15 T/U 系列定案）：
// Authorization 固定 `Bearer public`（免费层匿名面）；session 族四头全发同值
// （门禁：新头必需、族内 ≥2 头，全发最稳）；x-opencode-request 禁发。
// ses 由 session 包统一生成。
func BuildInferenceHeaders(auth, ses string) map[string]string {
	return map[string]string{
		"Content-Type":          "application/json",
		"Accept":                "application/json",
		"Authorization":         auth,
		"User-Agent":            OCUserAgent,
		"x-opencode-client":     "cli",
		"x-opencode-project":    "global", // 不参与校验，保留仅为形似客户端
		"x-opencode-session":    ses,
		"x-session-id":          ses,
		"x-session-affinity":    ses,
		"x-opencode-session-id": ses,
	}
}

// gateTool Chat 线（嵌套 function）形状的门禁工具。
// parameters 必须是真实 schema（含非空 properties；description 随意）。
func gateTool(name, desc, propKey string) map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        name,
			"description": desc,
			"parameters": map[string]any{
				"type":       "object",
				"properties": map[string]any{propKey: map[string]any{"type": "string"}},
			},
		},
	}
}

// gateToolFlat /responses 透传线（muse-spark-* 等 Responses 原生模型）：扁平工具格式。
func gateToolFlat(name, desc, propKey string) map[string]any {
	return map[string]any{
		"type":        "function",
		"name":        name,
		"description": desc,
		"parameters": map[string]any{
			"type":       "object",
			"properties": map[string]any{propKey: map[string]any{"type": "string"}},
		},
	}
}

// gateChat bash → read → glob → grep（GATE_TOOLS，chat 线）。
func gateChat() []map[string]any {
	return []map[string]any{
		gateTool("bash", "Execute a shell command", "command"),
		gateTool("read", "Read a file", "path"),
		gateTool("glob", "Find files by pattern", "pattern"),
		gateTool("grep", "Search file contents", "pattern"),
	}
}

// gateFlat bash → glob → grep → read（GATE_TOOLS_FLAT，/responses 线）。
// 注意与 Chat 线的顺序不同，是照搬 Python 版的既有形状，非笔误。
func gateFlat() []map[string]any {
	return []map[string]any{
		gateToolFlat("bash", "Execute a shell command", "command"),
		gateToolFlat("glob", "Find files by pattern", "pattern"),
		gateToolFlat("grep", "Search file contents", "pattern"),
		gateToolFlat("read", "Read a file", "path"),
	}
}

// InferencePrepareBody inference 化请求体：补齐 bash+read 最少集（客户端工具在前，缺失补后）。
//
// 返回新 map（不改调用方的原始 body）。flat=true 用于 /responses 透传线的
// 扁平工具格式（无 "function" 包裹层）。
//
// 与 zen 通道的注入差异：本通道按「四件套里缺哪个补哪个」（对齐 Python 的
// gate 遍历逻辑），且不看别名表 —— zen 通道才做 read/bash 的别名归一化。
func InferencePrepareBody(payload map[string]any, flat bool) map[string]any {
	body := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		body[k] = v
	}
	gate := gateChat()
	if flat {
		gate = gateFlat()
	}
	tools, names, ok := toolsOf(payload, flat)
	if !ok {
		out := make([]any, 0, len(gate))
		for _, t := range gate {
			out = append(out, t)
		}
		body["tools"] = out
		return body
	}
	gateNames := make([]string, 0, len(gate))
	for _, t := range gate {
		gateNames = append(gateNames, toolName(t, flat))
	}
	extra := []any{}
	for i, t := range gate {
		if !contains(names, gateNames[i]) {
			extra = append(extra, t)
		}
	}
	body["tools"] = append(append([]any{}, tools...), extra...)
	return body
}
