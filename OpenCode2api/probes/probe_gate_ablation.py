# -*- coding: utf-8 -*-
"""免费层闸门消融探针（2026-10-15，/inference/v1 V2 门禁）。

基线 B0 = server.py 当前工作请求形状（Bearer public 匿名面）；
每个探针只动一个变量，钉住门禁的真实校验面。自包含，不影响服务。

用法：python probe_gate_ablation.py
"""
import json
import random
import string
import time

import requests
import urllib3

urllib3.disable_warnings()

BASE = "https://opencode.ai/inference/v1"
UA = "opencode/latest/2.0.29/cli"
UA_OLD = "opencode/2.0.26 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"
B62 = string.digits + string.ascii_letters
MASK = 0xFFFFFFFFFFFF


def ses(desc=True, ts_ms=None, rand_head=False):
    """ses_ + 12hex + 14base62；默认降序时间戳编码（对齐上游 TypeID）。"""
    ts = ts_ms if ts_ms is not None else int(time.time() * 1000)
    if rand_head:
        head = "".join(random.choice("0123456789abcdef") for _ in range(6))
    else:
        value = ts * 0x1000 + 1
        if desc:
            value = (~value) & MASK
        value &= MASK
        head = "".join(format((value >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


TOOLS_CHAT = [{"type": "function",
               "function": {"name": n, "description": "x",
                            "parameters": {"type": "object", "properties": {}}}}
              for n in ("bash", "glob", "grep", "read")]
TOOLS_FLAT = [{"type": "function", "name": n, "description": "x",
               "parameters": {"type": "object", "properties": {}}}
              for n in ("bash", "glob", "grep", "read")]


def headers(s, ua=UA, project="global", four=True, oc_request=False):
    h = {"Content-Type": "application/json", "Accept": "application/json",
         "User-Agent": ua, "Authorization": "Bearer public",
         "x-opencode-client": "cli"}
    if project is not None:
        h["x-opencode-project"] = project
    for k in (["x-opencode-session", "x-session-id", "x-session-affinity",
               "x-opencode-session-id"] if four else ["x-opencode-session"]):
        h[k] = s
    if oc_request:
        h["x-opencode-request"] = "msg_" + "".join(
            random.choice(B62) for _ in range(26))
    return h


def body_chat(**over):
    b = {"model": "nemotron-3.5-lightning-free", "stream": True,
         "max_tokens": 32,
         "messages": [{"role": "user", "content": "pong"}],
         "tools": TOOLS_CHAT,
         "stream_options": {"include_usage": True}}
    for k, v in over.items():
        if v is None:
            b.pop(k, None)
        else:
            b[k] = v
    return b


def post(path, h, b, tries=3):
    """POST 并归类响应：SSE 首帧 type / 错误 type / 原文截断。"""
    for _ in range(tries):
        try:
            r = requests.post(BASE + path, data=json.dumps(b), headers=h,
                              timeout=45, stream=True)
            first = next(r.iter_content(400), b"").decode("utf-8", "replace")
            r.close()
            payload = first.split("\n\n")[0]
            payload = payload[5:].strip() if payload.startswith("data:") else payload
            try:
                j = json.loads(payload)
            except Exception:
                return r.status_code, first[:60].replace("\n", " ")
            if isinstance(j, dict) and "error" in j:
                e = j["error"]
                et = e.get("type") if isinstance(e, dict) else e
                return r.status_code, "ERR:%s" % str(et)[:44]
            if isinstance(j, dict):
                return r.status_code, str(j.get("type", j.get("id", "?")))[:44]
            return r.status_code, first[:44]
        except requests.RequestException:
            time.sleep(1.2)
    return None, "EXC"


def main():
    print("=== /inference/v1 闸门消融（基线=当前工作形状, Bearer public）===")
    rows = []

    def probe(tag, path="/chat/completions", h=None, b=None):
        st, info = post(path, h or headers(ses()), b or body_chat())
        rows.append((tag, st, info))
        print("%-32s -> %-4s %s" % (tag, st, info), flush=True)
        time.sleep(1.0)

    # 基线
    probe("B0 baseline(desc fresh)")
    # ── session 编码面 ──
    probe("A1 head=random(不编码)", h=headers(ses(rand_head=True)))
    probe("A2 ascending(不取反)", h=headers(ses(desc=False)))
    probe("A3 ts=-8d", h=headers(ses(ts_ms=int(time.time() * 1000) - 8 * 86400_000)))
    probe("A4 ts=-6d", h=headers(ses(ts_ms=int(time.time() * 1000) - 6 * 86400_000)))
    probe("A5 仅旧头x-opencode-session", h=headers(ses(), four=False))
    # ── 头面 ──
    probe("B1 +x-opencode-request", h=headers(ses(), oc_request=True))
    probe("B2 project=40hex", h=headers(ses(), project="a" * 40))
    probe("B3 旧式UA(2.0.26后缀)", h=headers(ses(), ua=UA_OLD))
    probe("B4 UA=1.19.0(低于下限)", h=headers(ses(), ua="opencode/latest/1.19.0/cli"))
    # ── body 面 ──
    probe("C1 无tools", b=body_chat(tools=None))
    probe("C2 PascalCase工具名", b=body_chat(tools=[
        {"type": "function", "function": {"name": n, "description": "x",
                                          "parameters": {"type": "object",
                                                         "properties": {}}}}
        for n in ("Bash", "Read")]))
    probe("C3 stream:false", b=body_chat(stream=False))
    probe("C4 无stream_options", b=body_chat(stream_options=None))
    # ── 协议路由面 ──
    probe("D1 /responses muse-spark", path="/responses",
          b={"model": "muse-spark-1.2-contributor-free", "stream": True,
             "max_output_tokens": 32, "input": "pong",
             "stream_options": {"include_usage": True}, "tools": TOOLS_FLAT})
    probe("D2 付费模型(public key)", b=body_chat(model="claude-sonnet-4"))

    print("\n=== 汇总 ===")
    for tag, st, info in rows:
        print("%-32s -> %-4s %s" % (tag, st, info))


if __name__ == "__main__":
    main()
