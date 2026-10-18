# -*- coding: utf-8 -*-
"""/v1/messages 回归（Flask test_client + mock 上游，不连真实上游）。

锁定 anthropic_bridge 的协议翻译与路由行为：
  1. 请求侧 anthropic_to_chat：system / tool_use / tool_result / tools / tool_choice
  2. 响应侧 chat_json_to_anthropic：文本 + tool_use + stop_reason + usage
  3. 流式 chat_sse_to_anthropic：事件序列与帧形状
  4. 非流式聚合 aggregate_chat_sse：tool_calls + usage 收齐
  5. 路由：流式 / 非流式 / 上游错误 / 闸门工具 / 非免费模型 / x-api-key 鉴权
"""
import json

import server
import anthropic_bridge as ab


class FakeUpstream:
    def __init__(self, raw=b"", status=200, content_type="text/event-stream"):
        self._raw = raw
        self.status_code = status
        self.headers = {"Content-Type": content_type}

    @property
    def text(self):
        return self._raw.decode("utf-8", "replace")

    def iter_lines(self, decode_unicode=False):
        for block in self._raw.split(b"\n\n"):
            yield block


def sse(*objs):
    """拼一串 Chat SSE：每个 obj 一个 data 帧，末尾 [DONE]。"""
    parts = []
    for o in objs:
        parts.append("data: " + json.dumps(o, ensure_ascii=False))
    parts.append("data: [DONE]")
    return ("\n\n".join(parts) + "\n\n").encode("utf-8")


CHAT_TEXT = sse(
    {"choices": [{"index": 0, "delta": {"role": "assistant", "content": "Hello"},
                  "finish_reason": None}]},
    {"choices": [{"index": 0, "delta": {"content": " world"}, "finish_reason": None}]},
    {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
     "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}},
)

CHAT_TOOL = sse(
    {"choices": [{"index": 0, "delta": {"role": "assistant", "content": None,
                  "tool_calls": [{"index": 0, "id": "call_abc", "type": "function",
                                  "function": {"name": "bash", "arguments": "{\"comm"}}]},
                  "finish_reason": None}]},
    {"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0,
                  "function": {"arguments": "and\":\"ls\"}"}}]},
                  "finish_reason": None}]},
    {"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}],
     "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
)

captured = {}


def install_fake(raw=CHAT_TEXT, status=200):
    server.API_KEY = ""
    server._model_free = lambda m: True
    server._dump_save = lambda *a, **k: None

    def fake_post(path, payload, seed, rid, timeout=180, flat=False):
        captured["path"] = path
        captured["payload"] = payload
        return FakeUpstream(raw, status=status), "zen"

    server._post_upstream = fake_post


def parse_ant_frames(data):
    """把 Anthropic SSE 字节流拆成 [(event_type, data_dict), ...]。"""
    out = []
    for block in data.split(b"\n\n"):
        if not block:
            continue
        lines = block.split(b"\n")
        et = None
        d = None
        for ln in lines:
            if ln.startswith(b"event: "):
                et = ln[7:].decode("utf-8")
            elif ln.startswith(b"data: "):
                d = json.loads(ln[6:].decode("utf-8"))
        if et:
            out.append((et, d))
    return out


# ============================================================
# 1) 请求侧
# ============================================================
def test_request():
    body = {
        "model": "muse-spark-1.2-free",
        "system": "你是助手",
        "messages": [
            {"role": "user", "content": "列出文件"},
            {"role": "assistant", "content": [
                {"type": "text", "text": "好的"},
                {"type": "tool_use", "id": "toolu_1", "name": "bash",
                 "input": {"command": "ls"}},
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1",
                 "content": "a.txt\nb.txt"},
            ]},
        ],
        "tools": [{"name": "bash", "description": "run",
                   "input_schema": {"type": "object",
                                    "properties": {"command": {"type": "string"}}}}],
        "tool_choice": {"type": "any"},
        "max_tokens": 1024,
        "stop_sequences": ["END"],
        "temperature": 0.5,
    }
    chat = ab.anthropic_to_chat(body)
    assert chat["model"] == "muse-spark-1.2-free"
    msgs = chat["messages"]
    assert msgs[0] == {"role": "system", "content": "你是助手"}
    assert msgs[1] == {"role": "user", "content": "列出文件"}
    # assistant: text + tool_calls
    assert msgs[2]["role"] == "assistant"
    assert msgs[2]["content"] == "好的"
    assert msgs[2]["tool_calls"][0]["function"]["name"] == "bash"
    assert json.loads(msgs[2]["tool_calls"][0]["function"]["arguments"]) == {"command": "ls"}
    # tool_result -> role=tool
    assert msgs[3] == {"role": "tool", "tool_call_id": "toolu_1",
                       "content": "a.txt\nb.txt"}
    # tools
    assert chat["tools"][0]["function"]["parameters"]["properties"]["command"]["type"] == "string"
    assert chat["tool_choice"] == "required"
    assert chat["max_tokens"] == 1024
    assert chat["stop"] == ["END"]
    assert chat["temperature"] == 0.5


# ============================================================
# 2) 非流式响应
# ============================================================
def test_response_json():
    chat = {"choices": [{"index": 0, "message": {
        "role": "assistant", "content": "hi",
        "tool_calls": [{"id": "call_1", "type": "function",
                        "function": {"name": "bash", "arguments": '{"command":"ls"}'}}]},
        "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12}}
    out = ab.chat_json_to_anthropic(chat, "m")
    assert out["type"] == "message" and out["role"] == "assistant"
    assert out["content"][0] == {"type": "text", "text": "hi"}
    tu = out["content"][1]
    assert tu["type"] == "tool_use" and tu["name"] == "bash"
    assert tu["input"] == {"command": "ls"}
    assert out["stop_reason"] == "tool_use"
    assert out["usage"]["input_tokens"] == 8 and out["usage"]["output_tokens"] == 4


# ============================================================
# 3) 流式响应
# ============================================================
def test_response_stream():
    frames = list(ab.chat_sse_to_anthropic(
        FakeUpstream(CHAT_TEXT).iter_lines(), "m"))
    types = [f.split(b"\n")[0][7:].decode() for f in frames]
    assert types[0] == "message_start"
    assert "content_block_start" in types
    assert "content_block_delta" in types
    assert types[-1] == "message_stop"
    assert types[-2] == "message_delta"
    # 文本拼回
    data = b"".join(frames)
    txt = "".join(d["delta"]["text"] for _, d in parse_ant_frames(data)
                  if d and d.get("type") == "content_block_delta"
                  and d.get("delta", {}).get("type") == "text_delta")
    assert txt == "Hello world", txt
    # stop_reason
    md = [d for _, d in parse_ant_frames(data)
          if d and d.get("type") == "message_delta"][0]
    assert md["delta"]["stop_reason"] == "end_turn"


# ============================================================
# 4) 非流式聚合
# ============================================================
def test_aggregate():
    chat = ab.aggregate_chat_sse(FakeUpstream(CHAT_TOOL).iter_lines(), "m")
    msg = chat["choices"][0]["message"]
    assert msg["tool_calls"][0]["function"]["name"] == "bash"
    assert json.loads(msg["tool_calls"][0]["function"]["arguments"]) == {"command": "ls"}
    assert chat["choices"][0]["finish_reason"] == "tool_calls"
    assert chat["usage"]["completion_tokens"] == 5


# ============================================================
# 5) 路由
# ============================================================
def test_route():
    client = server.app.test_client()
    req = {"model": "muse-spark-1.2-free", "max_tokens": 64,
           "messages": [{"role": "user", "content": "hi"}]}

    # 流式
    install_fake(CHAT_TEXT)
    r = client.post("/v1/messages", json=dict(req, stream=True))
    assert r.status_code == 200, (r.status_code, r.data[:300])
    assert b"event: message_start" in r.data and b"event: message_stop" in r.data
    assert captured["path"] == "/chat/completions"
    assert captured["payload"]["stream"] is True
    # 闸门工具注入已下沉 lane：断言 lane_zen 对该 payload 的准备结果
    zen_body = server.lane_zen.prepare_body(captured["payload"])
    names = [t["function"]["name"] for t in zen_body["tools"]]
    assert "read" in names, names

    # 非流式文本
    install_fake(CHAT_TEXT)
    r = client.post("/v1/messages", json=dict(req, stream=False))
    assert r.status_code == 200, (r.status_code, r.data[:300])
    out = r.get_json()
    assert out["type"] == "message"
    assert out["content"][0] == {"type": "text", "text": "Hello world"}
    assert out["stop_reason"] == "end_turn"

    # 非流式 tool_use
    install_fake(CHAT_TOOL)
    r = client.post("/v1/messages", json=dict(req, stream=False))
    out = r.get_json()
    tu = [b for b in out["content"] if b["type"] == "tool_use"]
    assert tu and tu[0]["input"] == {"command": "ls"}, out
    assert out["stop_reason"] == "tool_use"

    # 上游 400 -> Anthropic 错误体
    install_fake(b'{"error":{"message":"upstream says no"}}', status=400)
    r = client.post("/v1/messages", json=req)
    assert r.status_code == 400
    assert r.get_json()["type"] == "error"
    assert r.get_json()["error"]["type"] == "invalid_request_error"

    # 非免费模型 -> 400
    install_fake(CHAT_TEXT)
    server._model_free = lambda m: m == "muse-spark-1.2-free"
    r = client.post("/v1/messages", json=dict(req, model="claude-x"))
    assert r.status_code == 400 and r.get_json()["type"] == "error"

    # Bearer 鉴权（Claude Code 走 ANTHROPIC_AUTH_TOKEN -> Authorization: Bearer）
    install_fake(CHAT_TEXT)
    server.API_KEY = "secret"
    r = client.post("/v1/messages", json=req,
                    headers={"Authorization": "Bearer secret"})
    assert r.status_code == 200, (r.status_code, r.data[:300])
    r = client.post("/v1/messages", json=req)
    assert r.status_code == 401
    server.API_KEY = ""


def main():
    test_request()
    test_response_json()
    test_response_stream()
    test_aggregate()
    test_route()
    print("ALL PASS")


if __name__ == "__main__":
    main()
