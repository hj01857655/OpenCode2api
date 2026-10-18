# -*- coding: utf-8 -*-
"""抓取 server.py 真实路由发出的上游请求字节（spy Session.send），
再用真字节做回放判别：同进程池化 vs 一次性连接。

产出：
  dump_upstream_headers.json / dump_upstream_body.json —— 上游请求真相
  回放结果 —— 分离"内容差异"与"传输层差异"
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import urllib3
from dotenv import dotenv_values

urllib3.disable_warnings()

import server  # noqa: E402

KEY = (dotenv_values(".env").get("API_KEY") or "").strip()
client = server.app.test_client()

captured = {}
orig_send = server._upstream_session.send


def spy_send(prepared, **kw):
    captured["url"] = prepared.url
    captured["headers"] = dict(prepared.headers)
    body = prepared.body
    if isinstance(body, str):
        body = body.encode("utf-8")
    captured["body"] = body or b""
    return orig_send(prepared, **kw)


# ── 第一发：完整真实路由（真实上游），抓字节 ──
server._upstream_session.send = spy_send
try:
    r = client.post(
        "/v1/chat/completions",
        json={"model": "nemotron-3.5-lightning-free", "stream": False,
              "max_tokens": 32,
              "messages": [{"role": "user", "content": "pong"}]},
        headers={"Authorization": "Bearer " + KEY})
finally:
    server._upstream_session.send = orig_send

print("route status:", r.status_code, r.data[:120].decode("utf-8", "replace"))
print("upstream url:", captured.get("url"))
json.dump(captured.get("headers", {}), open("dump_upstream_headers.json", "w",
                                            encoding="utf-8"),
          ensure_ascii=False, indent=1)
open("dump_upstream_body.json", "wb").write(captured.get("body", b""))
print("--- upstream headers ---")
for k, v in captured.get("headers", {}).items():
    if k.lower() in ("authorization",):
        print("%s: %s…" % (k, str(v)[:14]))
    else:
        print("%s: %s" % (k, v))
print("--- upstream body head ---")
print(captured.get("body", b"")[:400].decode("utf-8", "replace"))

if r.status_code != 200:
    print("\n路由本身也 403 —— 闸门已翻状态，内容判别无意义，先等冷却再测。")
    raise SystemExit(0)

# ── 第二步：真字节回放 ──
AUTO = {"host", "content-length", "accept-encoding", "connection",
        "transfer-encoding"}
h2 = {k: v for k, v in captured["headers"].items() if k.lower() not in AUTO}

first = None
try:
    s = requests.Session()
    ra = s.post(captured["url"], data=captured["body"], headers=h2,
                stream=True, timeout=45)
    first = next(ra.iter_content(200), b"")
    print("replay pooled   ->", ra.status_code, first[:80])
    ra.close()
except requests.RequestException as e:
    print("replay pooled   -> EXC", type(e).__name__)
try:
    rb = requests.post(captured["url"], data=captured["body"], headers=h2,
                       stream=True, timeout=45)
    first = next(rb.iter_content(200), b"")
    print("replay fresh    ->", rb.status_code, first[:80])
    rb.close()
except requests.RequestException as e:
    print("replay fresh    -> EXC", type(e).__name__)
