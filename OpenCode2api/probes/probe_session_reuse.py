# -*- coding: utf-8 -*-
"""判别实验：闸门到底认什么——session 身份历史 vs 连接复用 vs 鉴权。

背景：probe_gate_ablation.py 全 403，但运行中服务同形状 200。
服务与单发探针的两个结构差异：
  1) session：服务用 session_cache.json 的长命缓存 session（反复使用），
     探针每请求铸全新 session
  2) 连接：服务用 requests.Session 连接池（keep-alive 复用 TLS），
     探针 requests.post 一次性新连接
本脚本 2×2 分离 {缓存,全新} × {真实key,Bearer public}，全部走共享
Session 连接池；另加一组"缓存 session + 一次性连接"对照连接因素。
"""
import json
import glob
import random
import string
import time

import requests
import urllib3

urllib3.disable_warnings()

BASE = "https://opencode.ai/inference/v1"
UA = "opencode/latest/2.0.29/cli"
B62 = string.digits + string.ascii_letters
MASK = 0xFFFFFFFFFFFF


def load_key():
    for p in glob.glob("*.json"):
        try:
            d = json.load(open(p, encoding="utf-8"))
            if isinstance(d, dict) and d.get("zen_key"):
                return d["zen_key"]
        except Exception:
            pass
    return None


def load_cached_sessions():
    try:
        d = json.load(open("session_cache.json", encoding="utf-8"))
        return [v["ses"] for v in d.values() if isinstance(v, dict) and v.get("ses")]
    except Exception:
        return []


def fresh_ses():
    now_ms = int(time.time() * 1000)
    value = (~(now_ms * 0x1000 + 1)) & MASK
    head = "".join(format((value >> (40 - 8 * i)) & 0xFF, "02x") for i in range(6))
    return "ses_" + head + "".join(random.choice(B62) for _ in range(14))


TOOLS_CHAT = [{"type": "function",
               "function": {"name": n, "description": "x",
                            "parameters": {"type": "object", "properties": {}}}}
              for n in ("bash", "glob", "grep", "read")]


def headers(ses_v, auth):
    h = {"Content-Type": "application/json", "Accept": "application/json",
         "User-Agent": UA, "Authorization": auth, "x-opencode-client": "cli",
         "x-opencode-project": "global"}
    for k in ("x-opencode-session", "x-session-id", "x-session-affinity",
              "x-opencode-session-id"):
        h[k] = ses_v
    return h


def body():
    return {"model": "nemotron-3.5-lightning-free", "stream": True,
            "max_tokens": 32,
            "messages": [{"role": "user", "content": "probe %d" % random.randint(0, 9)}],
            "tools": TOOLS_CHAT,
            "stream_options": {"include_usage": True}}


def post(sess, ses_v, auth, tries=3):
    """sess=None 表示一次性连接；否则用共享 Session（连接复用）。"""
    for _ in range(tries):
        try:
            sender = sess or requests
            r = sender.post(BASE + "/chat/completions", json=body(),
                            headers=headers(ses_v, auth), timeout=45, stream=True)
            first = next(r.iter_content(300), b"").decode("utf-8", "replace")
            r.close()
            payload = first.split("\n\n")[0]
            payload = payload[5:].strip() if payload.startswith("data:") else payload
            try:
                j = json.loads(payload)
            except Exception:
                return r.status_code, first[:50].replace("\n", " ")
            if isinstance(j, dict) and "error" in j:
                e = j["error"]
                return r.status_code, "ERR:%s" % str(
                    e.get("type") if isinstance(e, dict) else e)[:40]
            return r.status_code, str(j.get("type", j.get("id", "?")))[:40]
        except requests.RequestException:
            time.sleep(1.2)
    return None, "EXC"


def main():
    key = load_key()
    cached = load_cached_sessions()
    assert key and cached, "need credentials.json zen_key + session_cache.json"
    print("cached sessions: %d, key: %s…" % (len(cached), key[:8]))

    rows = []

    def probe(tag, ses_v, auth, reuse=True, session_obj=None):
        st, info = post(session_obj, ses_v, auth)
        rows.append((tag, st, info))
        print("%-40s -> %-4s %s" % (tag, st, info), flush=True)
        time.sleep(1.5)

    warm = cached[0]
    # 共享连接池（模拟服务）
    pool = requests.Session()
    probe("X1 缓存session+真实key+池化", warm, "Bearer " + key, session_obj=pool)
    probe("X2 缓存session+public+池化", warm, "Bearer public", session_obj=pool)
    probe("X3 全新session+真实key+池化", fresh_ses(), "Bearer " + key, session_obj=pool)
    probe("X4 全新session+public+池化", fresh_ses(), "Bearer public", session_obj=pool)
    # 一次性连接（模拟消融探针）
    probe("X5 缓存session+真实key+一次性连接", warm, "Bearer " + key, session_obj=None)
    probe("X6 缓存session+public+一次性连接", warm, "Bearer public", session_obj=None)
    # 同 session 第二发（会话历史累积效应）
    probe("X7 复跑X1(同session第2发)", warm, "Bearer " + key, session_obj=pool)
    probe("X8 复跑X2(同session第2发)", warm, "Bearer public", session_obj=pool)

    print("\n=== 判读 ===")
    print("X1/X2 通而 X3/X4 拒      → session 必须是'已登记'身份（历史/年龄）")
    print("X1 通 X2 拒              → 真实 key 必需")
    print("X5/X6 拒而 X1/X2 通      → 连接复用（keep-alive）参与指纹")
    for tag, st, info in rows:
        print("%-40s -> %-4s %s" % (tag, st, info))


if __name__ == "__main__":
    main()
