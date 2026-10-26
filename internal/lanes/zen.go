// lanes/zen.py 的 Go 版：legacy /zen/v1 通道（自包含：本通道门禁的一切都在本文件）。
//
// 上游改 zen 门禁时只改本文件；调度层通过 BuildHeaders()/PrepareBody()/Base
// 使用本通道，不含任何 zen 专属逻辑。
//
// 门禁规格（与 /inference/v1 的差异见 lanes/inference.go 头注）：
//  1. UA：v1.3.7 形 `opencode/1.18.31 (<os> <arch>; <rt>)`
//  2. x-opencode-request：必发（inference 通道禁发，存在即 403）
//  3. x-opencode-project：`prj_`+24hex 形状（值不校验）
//  4. 工具：门禁要 bash + read 两个 stub（description/schema 均不校验）；
//     客户端已带 read / bash 系别名则原样保留不注入
//  5. 鉴权 `Bearer public`；session 与 inference 同源校验（降序 TypeID，
//     由 session 包统一生成后传入，共用 session_cache.json）
package lanes

import "runtime"

// zenProjectID 固定 prj_+24hex；值不校验仅形状（对齐 Python 的写死值，
// 换成随机值没有实证收益，保持与已验证过的指纹一致）。
const zenProjectID = "prj_3e6c46914ea17a648ce6f769"

// legacyUA v1.3.7 UA 形状（版本取参考项目 legacyClientVersion 1.18.31）。
// Python 版用 platform.system()/machine()，这里用 GOOS/GOARCH —— 同一形似用途，
// UA 的 os/arch 段不在 zen 门禁校验项里。
func legacyUA() string {
	goos, arch := runtime.GOOS, runtime.GOARCH
	if goos == "" {
		goos = "windows"
	}
	if arch == "" {
		arch = "amd64"
	}
	return "opencode/1.18.31 (" + goos + " " + arch + "; bun/1.3.14)"
}

// readAliases read 系别名（对齐参考项目 isLegacyReadToolCandidate 候选表）：
// 归一化（小写、去 _-.）后精确匹配；read_file→readfile、view_file→viewfile。
var readAliases = []string{
	"readfile", "fileread", "readfiles", "readtext",
	"readtextfile", "readcontent", "viewfile",
}

// bashAliases bash 系别名：客户端已带这些工具名时不重复注入 stub。
var bashAliases = []string{
	"shell", "run", "execute", "exec", "command", "terminal",
	"runshell", "executeshell", "runcommand", "executecommand",
}

func readOK(names []string) bool {
	for _, n := range names {
		low := normalizeName(n)
		if low == "read" || contains(readAliases, low) {
			return true
		}
	}
	return false
}

func bashOK(names []string) bool {
	for _, n := range names {
		low := normalizeName(n)
		if low == "bash" || contains(bashAliases, low) {
			return true
		}
	}
	return false
}

// ZenBuildHeaders legacy 通道指纹头。ses 由 session 包统一生成（降序 TypeID，两通道同源）。
func ZenBuildHeaders(auth, ses string) map[string]string {
	return map[string]string{
		"Content-Type":       "application/json",
		"Accept":             "application/json",
		"Authorization":      auth,
		"User-Agent":         legacyUA(),
		"x-opencode-client":  "cli",
		"x-opencode-project": zenProjectID,
		"x-opencode-session": ses,
		"x-session-id":       ses,
		"x-session-affinity": ses,
		// 门禁要求必发（inference 通道相反：存在即 403），每请求一个新值
		"x-opencode-request": identReq(),
	}
}

// ZenPrepareBody zen 化请求体：确保工具集里有 bash + read（门禁硬要求），其余原样。
//
// 返回新 map（不改调用方的原始 body）。缺哪个补哪个，客户端自带的工具
// 保留在前（否则模型只剩假工具可调）。flat=true 用于 /responses 透传线
// 的扁平工具格式（无 "function" 包裹层）。
func ZenPrepareBody(payload map[string]any, flat bool) map[string]any {
	body := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		body[k] = v
	}
	readTool := zenReadTool
	bashTool := zenBashTool
	if flat {
		readTool = zenReadToolFlat
		bashTool = zenBashToolFlat
	}
	tools, names, ok := toolsOf(payload, flat)
	if !ok {
		body["tools"] = []any{bashTool, readTool}
		return body
	}
	extra := []any{}
	if !bashOK(names) {
		extra = append(extra, bashTool)
	}
	if !readOK(names) {
		extra = append(extra, readTool)
	}
	if len(extra) > 0 {
		body["tools"] = append(append([]any{}, tools...), extra...)
	}
	return body
}

// zenReadTool / zenBashTool：Chat 线（嵌套 function）与 flat 线（/responses）两种形状。
var (
	zenReadTool = map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "read",
			"description": "Read a file",
			"parameters": map[string]any{
				"type":       "object",
				"properties": map[string]any{"path": map[string]any{"type": "string"}},
			},
		},
	}
	zenReadToolFlat = map[string]any{
		"type":        "function",
		"name":        "read",
		"description": "Read a file",
		"parameters": map[string]any{
			"type":       "object",
			"properties": map[string]any{"path": map[string]any{"type": "string"}},
		},
	}
	zenBashTool = map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "bash",
			"description": "Execute a shell command",
			"parameters": map[string]any{
				"type":       "object",
				"properties": map[string]any{"command": map[string]any{"type": "string"}},
			},
		},
	}
	zenBashToolFlat = map[string]any{
		"type":        "function",
		"name":        "bash",
		"description": "Execute a shell command",
		"parameters": map[string]any{
			"type":       "object",
			"properties": map[string]any{"command": map[string]any{"type": "string"}},
		},
	}
)
