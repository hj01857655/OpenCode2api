# -*- coding: utf-8 -*-
"""T 系列：修正 S 系列证伪的三条结论，钉死最终规格。

待钉死：
  1) session 头：S2(2头)403 / 4头(含混值)200 / 单头403 → "全部4个必须在场"还是">=3"？
     另测 4 头全不同值（真实客户端 session-id 与 affinity 本就是两个值）
  2) UA：S3(UA=x)403 vs S1(真UA)200，而 G1/G2/G3(opencode 系变体)全 200
     → 匹配模式是"opencode 前缀"还是"非空"还是"含 opencode"？
  3) 工具：S6(仅bash)403 vs F9(bash/glob/grep/read)200 vs G10(/responses线 bash+read)200
     → chat 线最少工具集是 {bash,read} 还是全四件？
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

POOL = requests.Session()   # 连接池化只为压 SSL EOF 瞬态；已证与门禁无关


def ses(k=1):
    ts = int(time.time() * 1000)
    v = (~(ts * 0x1000 + k)) & MASK
    head = "".join(format((v >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


def hh(*drops, **setv):
    """全头基线：4 session 头同值 fresh；可删头、可覆写值。"""
    h = dict(H0)
    s = ses()
    for k in ALL_SK:
        h[k] = s
    for d in drops:
        h.pop(d, None)
    h.update(setv)
    return h


def post(tag, h, body=BODY0, tries=4):
    for _ in range(tries):
        try:
            r = POOL.post(URL, data=body, headers=h, timeout=40, stream=True)
            next(r.iter_content(40), b"")
            r.close()
            print("%-52s -> %s" % (tag, r.status_code), flush=True)
            time.sleep(1.2)
            return r.status_code
        except requests.RequestException:
            time.sleep(1.5)
    print("%-52s -> EXC(4x)" % tag, flush=True)
    return None


def tools_spec(names):
    """按名字生成嵌套格式工具，description=x，parameters 真实单字段。"""
    fields = {"bash": "command", "read": "path",
              "glob": "pattern", "grep": "pattern"}
    return [{"type": "function", "function": {"name": n, "description": "x",
                                              "parameters": {"type": "object",
                                                             "properties": {fields[n]: {"type": "string"}}}}}
            for n in names]


def body_tools(names):
    b = json.loads(BODY0)
    b["tools"] = tools_spec(names)
    return json.dumps(b).encode("utf-8")


def main():
    # ── 1) session 头数量 ──
    post("T0 基线复验(4头同值)", hh())
    post("T1 恰3头(去session-id-new)", hh("x-opencode-session-id"))
    post("T2 恰3头(去x-opencode-session)", hh("x-opencode-session"))
    h = hh()
    for k in ALL_SK:                       # 4 头全不同值
        h[k] = ses()
    post("T3 4头全不同值", h)
    # 混值对（复证 2+2）：
    h = hh()
    s2 = ses()
    h["x-session-affinity"] = s2
    h["x-opencode-session-id"] = s2
    post("T4 4头2+2两对值", h)

    # ── 2) UA 模式 ──
    post("T5 UA=opencode(无斜杠)", hh(**{"User-Agent": "opencode"}))
    post("T6 UA=opencode/9.9.9(极简)", hh(**{"User-Agent": "opencode/9.9.9"}))
    post("T7 UA=Mozilla/5.0", hh(**{"User-Agent": "Mozilla/5.0 (Windows NT)"}))
    post("T8 UA=xopencode/1.0(含非开头)", hh(**{"User-Agent": "xopencode/1.0"}))

    # ── 3) chat 线工具最少集 ──
    post("T9 工具=bash+read(真实参数)", hh(), body_tools(["bash", "read"]))
    post("T10 工具=全四件(真实参数)", hh(), body_tools(["bash", "glob", "grep", "read"]))
    post("T11 工具=bash+glob(无read)", hh(), body_tools(["bash", "glob"]))
    post("T12 工具=read+bash(顺序反)", hh(),
         body_tools(["read", "bash"]))

    # 收尾基线（门禁漂移自检）
    post("T13 收尾基线复验", hh())

    print("""
判读：
  T1/T2 拒 → session 头必须 4 个齐；通 → >=3 即可
  T3 通 → 头值自由（不限对数）；T3 拒 → 值须 <=2 个不同
  T5/T6 通 + T7 拒 → UA 须 opencode* 前缀；T7 通 → 任意非空
  T9 通 + T11 拒 → chat 线最少 {bash,read}；T10 拒 → 必须全四件""")

if __name__ == "__main__":
    main()
