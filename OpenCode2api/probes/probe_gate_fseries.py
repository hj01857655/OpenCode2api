# -*- coding: utf-8 -*-
"""F 系列二分：以"真实 200 字节 + 全新 k=1 session"为干净基线，一次动一处。

R 系列结论：fresh k=1 session + 真实 body = 200；k=1281 = 403；
旧探针（自拼 body，工具 description="x"/空 properties）全 403。
本轮钉死：body 里到底是哪个属性被查 + counter 容许边界。
"""
import json
import random
import string
import time

import requests
import urllib3

urllib3.disable_warnings()

URL = "https://opencode.ai/inference/v1/chat/completions"
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
    """降序 TypeID：解码值 C = ts<<12 + k，编码 E = ~C。"""
    ts = int(time.time() * 1000)
    value = (~(ts * 0x1000 + k)) & MASK
    head = "".join(format((value >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


def rand_ses():
    head = "".join(random.choice("0123456789abcdef") for _ in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


def with_session(ses_v):
    h = dict(H0)
    for k in SES_KEYS:
        h[k] = ses_v
    return h


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
                                          else e)[:34]
                else:
                    info = str(j.get("type", j.get("id", "?")))[:34]
            except Exception:
                info = first[:34].replace("\n", " ")
            print("%-30s -> %s %s" % (tag, r.status_code, info), flush=True)
            time.sleep(1.5)
            return r.status_code
        except requests.RequestException:
            time.sleep(1.2)
    print("%-30s -> EXC" % tag, flush=True)
    return None


def main():
    b0 = json.loads(BODY0)

    def dump_body(b):
        # 与真实请求同款序列化风格（默认分隔符），键序=传入 dict 序
        return json.dumps(b).encode("utf-8")

    # 基线自检
    post("F0 真实字节+k1", BODY0, with_session(fresh_ses(1)))

    # ── counter 边界 ──
    post("F1 k=2", BODY0, with_session(fresh_ses(2)))
    post("F2 k=0xFFE(4094)", BODY0, with_session(fresh_ses(0xFFE)))
    post("F3 随机head(不编码)", BODY0, with_session(rand_ses()))

    # ── body 属性二分（全部 k=1，唯一变量=body）──
    def tools_clone():
        return json.loads(json.dumps(b0["tools"]))

    # 只改 description
    b = dict(b0)
    tools = tools_clone()
    for t in tools:
        t["function"]["description"] = "x"
    b["tools"] = tools
    post("F4 description→x(params真)", dump_body(b), with_session(fresh_ses(1)))

    # 只改 parameters
    b = dict(b0)
    tools = tools_clone()
    for t in tools:
        t["function"]["parameters"] = {"type": "object", "properties": {}}
    b["tools"] = tools
    post("F5 properties→空(desc真)", dump_body(b), with_session(fresh_ses(1)))

    # 两者都改（≈旧探针的工具）
    b = dict(b0)
    tools = tools_clone()
    for t in tools:
        t["function"]["description"] = "x"
        t["function"]["parameters"] = {"type": "object", "properties": {}}
    b["tools"] = tools
    post("F6 desc x + 空properties", dump_body(b), with_session(fresh_ses(1)))

    # 去掉 tools
    b = dict(b0)
    b.pop("tools")
    post("F7 无tools", dump_body(b), with_session(fresh_ses(1)))

    # 去掉 stream_options
    b = dict(b0)
    b.pop("stream_options")
    post("F8 无stream_options", dump_body(b), with_session(fresh_ses(1)))

    # 反向：旧探针 body + 真实工具定义
    my_body = {"model": "nemotron-3.5-lightning-free", "stream": True,
               "max_tokens": 32,
               "messages": [{"role": "user", "content": "pong"}],
               "tools": json.loads(json.dumps(b0["tools"])),
               "stream_options": {"include_usage": True}}
    post("F9 旧body+真实工具定义", dump_body(my_body), with_session(fresh_ses(1)))

    # 键序（fresh session 下重验 R4）
    order = ["model", "stream", "max_tokens", "messages", "tools",
             "stream_options"]
    b = {k: b0[k] for k in order}
    post("F10 键序=构造序(k1)", dump_body(b), with_session(fresh_ses(1)))

    print("\n判读：")
    print("  F1/F2 拒 → counter 有上界；F1 通 → 只拒大 k（防农场）")
    print("  F4 拒 F5 通 → 查 description；F5 拒 F4 通 → 查 parameters")
    print("  F6 拒 + F9 通 → 工具定义是旧探针全灭的唯一原因")


if __name__ == "__main__":
    main()
