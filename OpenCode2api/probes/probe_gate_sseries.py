# -*- coding: utf-8 -*-
"""S 系列：从已知 200 基线（真实字节+fresh session+全头）逐步剥头/换体，
钉死 session 头数量规则与 client/project 必要性。

历史数据矛盾点：
  - 单头（1 个）→ 403；"两两"（实为 4 头 2+2 值）→ 200；三头+1 旧值 → 200
  - "≥2 头同值" 与 "4 头必须齐全" 从未被区分（两两测试被旧值污染）
本轮：S0 基线复验 → S1 去 client+project → S2 再剥到只剩 2 个 session 头
→ S3 UA=x → S5 换最小 1 工具体 → S7 复现 H6（判确定性 vs IP 风控）。
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
ALL_SK = ("x-opencode-session", "x-session-id", "x-session-affinity",
          "x-opencode-session-id")


def ses(k=1):
    ts = int(time.time() * 1000)
    v = (~(ts * 0x1000 + k)) & MASK
    head = "".join(format((v >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


def post(tag, h, body=BODY0, tries=3):
    for _ in range(tries):
        try:
            r = requests.post(URL, data=body, headers=h, timeout=40, stream=True)
            next(r.iter_content(40), b"")
            r.close()
            print("%-46s -> %s" % (tag, r.status_code), flush=True)
            time.sleep(1.2)
            return r.status_code
        except requests.RequestException:
            time.sleep(1.5)
    print("%-46s -> EXC(3x)" % tag, flush=True)
    return None


def main():
    # S0 基线复验（真实字节 + 全头 + fresh 同值 session）
    h = dict(H0)
    s = ses()
    for k in ALL_SK:
        h[k] = s
    post("S0 基线复验(全头4session)", h)

    # S1: 去 client + project（4 session 头保留）
    h = dict(H0)
    s = ses()
    for k in ALL_SK:
        h[k] = s
    h.pop("x-opencode-client")
    h.pop("x-opencode-project")
    post("S1 S0-去client-去project", h)

    # S2: S1 基础上只留 2 个 session 头
    h = dict(H0)
    s = ses()
    h["x-opencode-session"] = s
    h["x-session-id"] = s
    h.pop("x-session-affinity")
    h.pop("x-opencode-session-id")
    h.pop("x-opencode-client")
    h.pop("x-opencode-project")
    post("S2 S1-只留2个session头", h)

    # S3: S2 但保留 4 个 session 头（区分"头数量"与"client/project"）
    h = dict(H0)
    s = ses()
    for k in ALL_SK:
        h[k] = s
    h.pop("x-opencode-client")
    h.pop("x-opencode-project")
    h["User-Agent"] = "x"
    post("S3 4session头+UA=x(无client/project)", h)

    # S4: 全头 + 只留 2 个 session 头 + UA=x（与 S3 对照：头数量单独变量）
    h = dict(H0)
    s = ses()
    h["x-opencode-session"] = s
    h["x-session-id"] = s
    h.pop("x-session-affinity")
    h.pop("x-opencode-session-id")
    h["User-Agent"] = "x"
    post("S4 2session头+UA=x(有client/project)", h)

    # S5: H6 精确复现（2 头同值 + 无 client/project + UA=x + 1 工具体）
    s = ses()
    h = {"Content-Type": "application/json", "Authorization": "Bearer public",
         "User-Agent": "x", "x-opencode-session": s, "x-session-id": s}
    b = {"model": "nemotron-3.5-lightning-free", "stream": True,
         "max_tokens": 48,
         "messages": [{"role": "user", "content": "hi"}],
         "tools": [{"type": "function", "function": {"name": "bash",
                    "description": "x",
                    "parameters": {"type": "object",
                                   "properties": {"command": {"type": "string"}}}}}]}
    post("S5 H6复现(2头+UA=x+1工具)", h, json.dumps(b).encode("utf-8"))

    # S6: 全头 + 最小 1 工具体（隔离"体"因素）
    h = dict(H0)
    s = ses()
    for k in ALL_SK:
        h[k] = s
    post("S6 全头+最小1工具体", h, json.dumps(b).encode("utf-8"))

    print("""
判读：
  S1 拒 → client/project 至少必需其一；S1 通 → 纯 session 头问题
  S2 拒 + S3 通 → session 头必须 4 个齐（"≥2 同值"结论作废）
  S5 拒 + S6 通 → 1 工具体本身合规，问题全在头
  S5 拒 + S6 拒 → 门禁在 19:xx 后又变了（时间敏感，需重抓基线）""")


if __name__ == "__main__":
    main()
