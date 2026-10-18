# -*- coding: utf-8 -*-
"""lanes/zen.py — legacy /zen/v1 通道（自包含：本通道门禁的一切都在本文件）。

上游改 zen 门禁时只改本文件；server.py 通过 build_headers()/prepare_body()/BASE
使用本通道，不含任何 zen 专属逻辑。规格来源：jasonxu114514/opencode2api
076451f / 2319941 / a9470df 定案；本地探测实证（2026-10-18）：本指纹已过闸
——429 FreeUsageLimitError = 形状接受仅限流；403 FreeTierError = 形状/标记被拒。

门禁规格（与 /inference/v1 的差异见 lanes/inference.py 头注）：
  1) UA：v1.3.7 形 `opencode/1.18.31 (<os> <arch>; <rt>)`
  2) x-opencode-request：必发（inference 通道禁发，存在即 403）
  3) x-opencode-project：`prj_`+24hex 形状（值不校验）
  4) 工具：**只需一个名为 read 的工具**。探针实测 [read] 单独发即过，
     bash/shell/edit/glob/grep 单独发或无 read 均 403；description/schema
     均不校验。客户端已带 read 系别名（read_file / view_file / file_read …）
     则原样保留不注入。
  5) stream:true 必需；鉴权 `Bearer public`；session 与 inference 同源校验
     （降序 TypeID，由 server 统一生成后传入，共用 session_cache.json）。

env：`ZEN_BASE` 覆盖本通道基址（默认 `https://opencode.ai/zen/v1`）。
"""
import os
import platform
import random
import string

NAME = "zen"

BASE = (os.environ.get("ZEN_BASE", "").strip()
        or ("https://" + "opencode" + ".ai" + "/zen/v1"))

# 免费层匿名面：只认字面 `Bearer public`；真实 sk- key 反而 401。
AUTH = os.environ.get("UPSTREAM_AUTH", "").strip() or "Bearer public"

_HEX = string.digits + "abcdef"
_B62 = string.digits + string.ascii_letters


def _ident(prefix: str) -> str:
    """官方 ID 形态：req_ + 12位hex + 14位base62。"""
    return "%s_%s%s" % (prefix,
                        "".join(random.choice(_HEX) for _ in range(12)),
                        "".join(random.choice(_B62) for _ in range(14)))


def _legacy_ua() -> str:
    """v1.3.7 UA 形状（版本取参考项目 legacyClientVersion 1.18.31）。"""
    return "opencode/1.18.31 ({os} {arch}; {rt})".format(
        os=platform.system().lower() or "windows",
        arch=platform.machine().lower() or "amd64",
        rt="bun/1.3.14")


def build_headers(ses: str) -> dict:
    """legacy 通道指纹头。ses 由 server 统一生成（降序 TypeID，两通道同源）。"""
    prj = "prj_" + ("3e6c46914ea17a648ce6f769")  # 固定 prj_+24hex；值不校验仅形状
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": AUTH,
        "User-Agent": _legacy_ua(),
        "x-opencode-client": "cli",
        "x-opencode-project": prj,
        "x-opencode-session": ses,
        "x-session-id": ses,
        "x-session-affinity": ses,
        "x-opencode-request": _ident("req"),
    }


# ── read 工具桥（a9470df 门禁定案的唯一硬要求）───────────────────────────────
_ZEN_READ_TOOL = {"type": "function",
                  "function": {"name": "read", "description": "Read a file",
                               "parameters": {"type": "object",
                                              "properties": {"path": {"type": "string"}}}}}
_ZEN_READ_TOOL_FLAT = {"type": "function", "name": "read",
                       "description": "Read a file",
                       "parameters": {"type": "object",
                                      "properties": {"path": {"type": "string"}}}}

# read 系别名（对齐参考项目 isLegacyReadToolCandidate 候选表）：
# 归一化（小写、去 _-.）后精确匹配；read_file→readfile、view_file→viewfile。
_ZEN_READ_ALIASES = {"readfile", "fileread", "readfiles", "readtext",
                     "readtextfile", "readcontent", "viewfile"}


def _read_ok(names):
    for n in names:
        low = str(n or "").lower()
        for ch in ("_", "-", "."):
            low = low.replace(ch, "")
        if low == "read" or low in _ZEN_READ_ALIASES:
            return True
    return False


def prepare_body(payload, flat=False):
    """zen 化请求体：确保工具集里有 read（门禁唯一硬要求），其余原样。

    返回新 dict（不改调用方的原始 body）。客户端没有工具时注入单个 read
    stub；有 read 系别名时完全不动。flat=True 用于 /responses 透传线的
    扁平工具格式（无 "function" 包裹层）。
    """
    body = dict(payload)
    tool = _ZEN_READ_TOOL_FLAT if flat else _ZEN_READ_TOOL
    tools = body.get("tools")
    if not isinstance(tools, list) or not tools:
        body["tools"] = [tool]
        return body
    if flat:
        names = [t.get("name") for t in tools if isinstance(t, dict)]
    else:
        names = [(t.get("function") or {}).get("name") for t in tools
                 if isinstance(t, dict)]
    if not _read_ok(names):
        body["tools"] = list(tools) + [tool]
    return body
