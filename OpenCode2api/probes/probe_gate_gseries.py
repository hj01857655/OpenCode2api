# -*- coding: utf-8 -*-
"""G 系列：真实 200 字节 + k=1 fresh session 为基线，钉死头部面 + counter 边界。

F 系列已实锤：降序编码必需、counter 参与校验、工具 parameters 必须真实、
description/键序/stream_options 宽松。本系列把早前被坏工具定义污染的
头部结论重测干净，并粗框 counter 上界。
"""
import json
import random
import string
import time

import requests
import urllib3

urllib3.disable_warnings()

URL = "https://opencode.ai/inference/v1/chat/completions"
URL_RESP = "https://opencode.ai/inference/v1/responses"
B62 = string.digits + string.ascii_letters
MASK = 0xFFFFFFFFFFFF

HDRS = json.load(open("dump_upstream_headers.json", encoding="utf-8"))
BODY0 = open("dump_upstream_body.json", "rb").read()
AUTO = {"Host", "Content-Length", "Accept-Encoding", "Connection",
        "Transfer-Encoding"}
H0 = {k: v for k, v in HDRS.items() if k not in AUTO}
SES_KEYS = ("x-opencode-session", "x-session-id", "x-session-affinity",
            "x-opencode-session-id")


def fresh_ses(k=1):
    ts = int(time.time() * 1000)
    value = (~(ts * 0x1000 + k)) & MASK
    head = "".join(format((value >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


def h_variant(**over):
    """over: ua=, project=None(删), drop_other_ses=True, add_request=True,
    auth=, no_accept=True"""
    h = dict(H0)
    for k in SES_KEYS:
        h[k] = fresh_ses(1)
    if "ua" in over:
        h["User-Agent"] = over["ua"]
    if "project" in over:
        if over["project"] is None:
            h.pop("x-opencode-project", None)
        else:
            h["x-opencode-project"] = over["project"]
    if over.get("drop_other_ses"):
        for k in SES_KEYS[1:]:
            h.pop(k, None)
    if over.get("add_request"):
        h["x-opencode-request"] = "msg_" + "".join(
            random.choice(B62) for _ in range(26))
    if "auth" in over:
        h["Authorization"] = over["auth"]
    if over.get("no_accept"):
        h.pop("Accept", None)
    return h


def post(tag, url, body_bytes, headers, tries=2):
    for _ in range(tries):
        try:
            r = requests.post(url, data=body_bytes, headers=headers,
                              timeout=45, stream=True)
            first = next(r.iter_content(200), b"").decode("utf-8", "replace")
            r.close()
            payload = first.split("\n\n")[0]
            payload = payload[5:].strip() if payload.startswith("data:") else payload
            try:
                j = json.loads(payload)
                if isinstance(j, dict) and "error" in j:
                    e = j["error"]
                    info = "ERR:%s" % str(e.get("type") if isinstance(e, dict)
                                          else e)[:34]
                else:
                    info = str(j.get("type", j.get("id", "?")))[:34]
            except Exception:
                info = first[:34].replace("\n", " ")
            print("%-34s -> %s %s" % (tag, r.status_code, info), flush=True)
            time.sleep(1.5)
            return r.status_code
        except requests.RequestException:
            time.sleep(1.2)
    print("%-34s -> EXC" % tag, flush=True)
    return None


def main():
    print("base UA:", H0["User-Agent"])

    # ── 头部面（全部 k=1 fresh，body 原字节）──
    post("G0 基线(k1)", URL, BODY0, h_variant())
    post("G1 旧式UA", URL, BODY0,
         h_variant(ua="opencode/2.0.26 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"))
    post("G2 UA=1.19.0(低于下限)", URL, BODY0,
         h_variant(ua="opencode/latest/1.19.0/cli"))
    post("G3 UA=opencode/9.9.9", URL, BODY0,
         h_variant(ua="opencode/latest/9.9.9/cli"))
    post("G4 project=40hex", URL, BODY0, h_variant(project="a" * 40))
    post("G5 无project头", URL, BODY0, h_variant(project=None))
    post("G6 +x-opencode-request", URL, BODY0, h_variant(add_request=True))
    post("G7 仅旧头单发", URL, BODY0, h_variant(drop_other_ses=True))
    post("G8 无Accept头", URL, BODY0, h_variant(no_accept=True))

    # ── counter 边界粗框 ──
    for k in (0x100, 0x800):
        h = dict(H0)
        for sk in SES_KEYS:
            h[sk] = fresh_ses(k)
        post("G9 k=%d" % k, URL, BODY0, h)

    # ── /responses 线（真实 flat 工具参数）──
    flat = [
        {"type": "function", "name": "bash", "description": "Execute a shell command",
         "parameters": {"type": "object", "properties": {"command": {"type": "string"}}}},
        {"type": "function", "name": "read", "description": "Read a file",
         "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}},
    ]
    b = {"model": "muse-spark-1.2-contributor-free", "stream": True,
         "max_output_tokens": 32, "input": "pong", "tools": flat}
    post("G10 /responses 真实flat参数", URL_RESP, json.dumps(b).encode(), h_variant())

    # ── /responses 线空参数（复现污染源）──
    flat_bad = [{"type": "function", "name": n, "description": "x",
                 "parameters": {"type": "object", "properties": {}}}
                for n in ("bash", "read")]
    b2 = dict(b, tools=flat_bad)
    post("G11 /responses 空参数", URL_RESP, json.dumps(b2).encode(), h_variant())

    print("\n判读：")
    print("  G1/G3 通 → UA 不参与；G2 拒 → 只查版本下限")
    print("  G4 拒 → project 必须 global；G6 拒 → request 头禁发")
    print("  G7 通 → 四头非必需（单头即可）；G10 通 G11 拒 → 工具参数检查跨线一致")


if __name__ == "__main__":
    main()
