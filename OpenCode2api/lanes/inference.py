# -*- coding: utf-8 -*-
"""lanes/inference.py — /inference/v1 通道（自包含：本通道门禁的一切都在本文件）。

2026-10-11 上游迁移后的主通道；2026-10-18 起为回落通道（zen 优先）。上游改
inference 门禁时只改本文件；server.py 通过 build_headers()/prepare_body()/BASE
使用本通道，不含任何 inference 专属逻辑。

门禁规格以本仓库 docs/labs/2026-10-15-inference-v2-gate-ablation.md 消融定案
为准（与 Go 版 10-13 报告有 5 处出入，以本次实测为准）：
  1) session id 前 12 位 hex 必须是降序编码 ~(ts_ms<<12+k) 低 48 位
     （对齐上游 packages/schema/src/identifier.ts）；纯随机/升序即 403；
     counter k ∈ [1,63]（64 即拒）；时效容忍 ≥8 天；同会话值需稳定
  2) x-opencode-session-id（v2.0.29 新头）必发，且 session 族 4 头中
     至少再带 1 个（同值或异值均可；单发 1 头 / 缺新头均 403）
  3) x-opencode-request 头不得出现（存在本身触发 403）——与 zen 通道相反
  4) tools 必须含 bash+read（chat 线最少集，顺序无关；缺 read 即 403），
     parameters 为真实 schema（含非空 properties；description 随意）
  5) UA 必须形如 opencode/<ver>：second 段为 latest 或 ≥2.0.0 的 semver
     （1.0.0→426、非法段→403）；x-opencode-project / JSON 键序 / 连接方式
     实测不参与校验；免费面鉴权 Authorization: Bearer public

与 zen 通道（lanes/zen.py）的分野：
  UA 形状        2.x `opencode/latest/2.0.29/cli`  vs  v1.3.7 1.18.31 旧形
  x-opencode-request  禁发（存在即 403）           vs  必发
  x-opencode-project  不校验（global）             vs  prj_+24hex
  工具门禁       bash+read 最少集 + 真实 schema    vs  仅需一个 read

env：`INFERENCE_BASE` 覆盖本通道基址（默认 `https://opencode.ai/inference/v1`）。
"""
import os

NAME = "inference"

BASE = (os.environ.get("INFERENCE_BASE", "").strip()
        or ("https://" + "opencode" + ".ai" + "/inference/v1"))

# 免费层匿名面：只认字面 `Bearer public`；真实 sk- key 反而 401。
AUTH = os.environ.get("UPSTREAM_AUTH", "").strip() or "Bearer public"

# UA 是门禁校验项（T/U 系列定案）：须 opencode/<ver≥2.0.0>（或 latest 通道）
# ——低于下限 426、非法段 403。G 系列早期"不查 UA"系只测 opencode 系变体所致
OC_UA = "opencode/latest/2.0.29/cli"

# 上游已废弃模型（实测 410 ModelDeprecated）：剔除，避免误调必失败
DEPRECATED = {"exo-free"}

# 过闸门必需：units 声明 bash + read，且必须 OpenAI 线格式。
# 客户端自带工具时不能整包替换（否则模型只剩假工具可调，"发一下就停了"），
# 做法统一为：客户端工具在前，缺失的过闸门工具补在后面。
GATE_TOOLS_MORE = [
    {"type": "function",
     "function": {"name": "glob", "description": "Find files by pattern",
                  "parameters": {"type": "object", "properties": {"pattern": {"type": "string"}}}}},
    {"type": "function",
     "function": {"name": "grep", "description": "Search file contents",
                  "parameters": {"type": "object", "properties": {"pattern": {"type": "string"}}}}},
]
GATE_TOOLS = [
    {"type": "function",
     "function": {"name": "bash", "description": "Execute a shell command",
                  "parameters": {"type": "object", "properties": {"command": {"type": "string"}}}}},
    {"type": "function",
     "function": {"name": "read", "description": "Read a file",
                  "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}},
    *GATE_TOOLS_MORE,
]

# /responses 透传线（muse-spark-* 等 Responses 原生模型）：扁平工具格式
GATE_TOOLS_FLAT = [
    {"type": "function", "name": "bash", "description": "Execute a shell command",
     "parameters": {"type": "object", "properties": {"command": {"type": "string"}}}},
    {"type": "function", "name": "glob", "description": "Find files by pattern",
     "parameters": {"type": "object", "properties": {"pattern": {"type": "string"}}}},
    {"type": "function", "name": "grep", "description": "Search file contents",
     "parameters": {"type": "object", "properties": {"pattern": {"type": "string"}}}},
    {"type": "function", "name": "read", "description": "Read a file",
     "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}},
]


def build_headers(ses: str) -> dict:
    """V2 指纹全量头（2026-10-15 T/U 系列定案）：
    Authorization 固定 `Bearer public`（免费层匿名面）；x-opencode-session-id
    必发 + 四头全发同值（门禁：新头必需、族内 ≥2 头，值可不同，全发最稳）；
    UA 是校验项（OC_UA 注）；project 不参与校验（保留 global 仅为形似客户端）；
    x-opencode-request 禁发（存在本身触发 403）。ses 由 server 统一生成。"""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": AUTH,
        "User-Agent": OC_UA,
        "x-opencode-client": "cli",
        "x-opencode-project": "global",
        "x-opencode-session": ses,
        "x-session-id": ses,
        "x-session-affinity": ses,
        "x-opencode-session-id": ses,
    }


def prepare_body(payload, flat=False):
    """inference 化请求体：补齐 bash+read 最少集（客户端工具在前，缺失补后）。

    返回新 dict（不改调用方的原始 body）。flat=True 用于 /responses 透传线的
    扁平工具格式（无 "function" 包裹层）。
    """
    body = dict(payload)
    gate = GATE_TOOLS_FLAT if flat else GATE_TOOLS
    tools = body.get("tools")
    if not isinstance(tools, list) or not tools:
        body["tools"] = list(gate)
        return body
    if flat:
        names = {t.get("name") for t in tools if isinstance(t, dict)}
    else:
        names = {(t.get("function") or {}).get("name") for t in tools
                 if isinstance(t, dict)}
    body["tools"] = list(tools) + [t for t in gate
                                   if (t.get("name") if flat
                                       else (t.get("function") or {}).get("name"))
                                   not in names]
    return body
