# -*- coding: utf-8 -*-
"""字节级差分：真实 200 字节为基线，一次只改一处，定位闸门校验的字节属性。

前置：probe_spy_replay.py 已产出 dump_upstream_headers.json / dump_upstream_body.json
（真实 200 请求）。回放已证明传输层无关（一次性连接也 200）。
"""
import json
import random
import string
import time

import requests
import urllib3

urllib3.disable_warnings()

B62 = string.digits + string.ascii_letters
MASK = 0xFFFFFFFFFFFF
URL = "https://opencode.ai/inference/v1/chat/completions"

HDRS = json.load(open("dump_upstream_headers.json", encoding="utf-8"))
BODY0 = open("dump_upstream_body.json", "rb").read()
AUTO = {"Host", "Content-Length", "Accept-Encoding", "Connection",
        "Transfer-Encoding"}
H0 = {k: v for k, v in HDRS.items() if k not in AUTO}
SESION_KEYS = ("x-opencode-session", "x-session-id", "x-session-affinity",
               "x-opencode-session-id")
SES0 = H0[SESION_KEYS[0]]


def fresh_ses(counter=1, ts_ms=None):
    ts = ts_ms if ts_ms is not None else int(time.time() * 1000)
    value = (~(ts * 0x1000 + counter)) & MASK
    head = "".join(format((value >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


def post(tag, body_bytes, headers, tries=2):
    for _ in range(tries):
        try:
            r = requests.post(URL, data=body_bytes, headers=headers,
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
                                          else e)[:36]
                else:
                    info = str(j.get("type", j.get("id", "?")))[:36]
            except Exception:
                info = first[:36].replace("\n", " ")
            print("%-36s -> %s %s" % (tag, r.status_code, info), flush=True)
            time.sleep(1.5)
            return r.status_code
        except requests.RequestException:
            time.sleep(1.2)
    print("%-36s -> EXC" % tag, flush=True)
    return None


def main():
    b0 = json.loads(BODY0)
    print("base session:", SES0)
    print("base body order:", list(b0.keys()))

    # R0 原字节回放（基线自检）
    post("R0 原字节", BODY0, H0)

    # R1 session -> 全新 counter=1
    h = dict(H0)
    for k in SESION_KEYS:
        h[k] = fresh_ses(counter=1)
    post("R1 session=新铸counter1", BODY0, h)

    # R2 session -> 全新 counter=1281（模仿真实请求的 counter 位）
    h = dict(H0)
    for k in SESION_KEYS:
        h[k] = fresh_ses(counter=0x501)
    post("R2 session=新铸counter1281", BODY0, h)

    # R3 session -> 真实那枚但时间戳回拨 1 小时（同 counter）
    old = json.loads(BODY0)  # no-op，仅占位
    head_val = (~int(SES0[4:16], 16)) & MASK
    ts0 = head_val >> 12
    h = dict(H0)
    for k in SESION_KEYS:
        h[k] = fresh_ses(counter=0x501, ts_ms=ts0 - 3600_000)
    post("R3 session=同counter回拨1h", BODY0, h)

    # R4 键序打乱（非字母序）
    order = ["model", "stream", "max_tokens", "messages", "tools",
             "stream_options"]
    b = {k: b0[k] for k in order}
    post("R4 键序=构造序(非字母)", json.dumps(b).encode(), H0)

    # R5 tools 顺序换（bash,glob,grep,read）
    tools = b0["tools"]
    if len(tools) >= 4:
        t = list(tools)
        t[1], t[2] = t[2], t[1]      # read,glob -> glob,read
        b = dict(b0)
        b["tools"] = t
        post("R5 tools顺序互换", json.dumps(b).encode(), H0)

    # R6 消息内容换
    b = dict(b0)
    b["messages"] = [{"role": "user", "content": "probe xyz"}]
    post("R6 消息内容换", json.dumps(b).encode(), H0)

    # R7 message 键序换（role 在前）
    b = dict(b0)
    b["messages"] = [{"role": "user", "content": b0["messages"][0]["content"]}]
    post("R7 message键序role在前", json.dumps(b).encode(), H0)

    print("\n判读：")
    print("  R1/R2 拒  → session 的 counter/身份位参与校验（非纯时间戳编码）")
    print("  R3 拒     → session 时效敏感（1h 也拒）")
    print("  R4 拒     → 闸门做字节级键序检查（罕见但实锤）")
    print("  R5/R6/R7 拒 → tools 顺序/内容参与")


if __name__ == "__main__":
    main()
