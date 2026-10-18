# -*- coding: utf-8 -*-
"""/v1/responses 透传回归（Flask test_client + mock 上游，不连真实上游）。

OpenCode 上游原生支持 Responses 协议，此路由是原样透传（非 bridge 转换），
本测试锁定透传行为不被后续改动破坏：
  1. 流式：字节级原样透传
  2. 非流式：聚合 SSE 取终止帧的 response 对象
  3. 上游 4xx：状态码与正文透传
  4. 免费清单闸门工具注入：无工具注入四件套，有工具客户端在前
"""
import json

import server


class FakeUpstream:
    def __init__(self, raw=b"", status=200, content_type="text/event-stream"):
        self._raw = raw
        self.status_code = status
        self.headers = {"Content-Type": content_type}

    @property
    def content(self):
        return self._raw

    @property
    def text(self):
        return self._raw.decode("utf-8", "replace")

    def iter_content(self, chunk_size=4096):
        yield self._raw

    def iter_lines(self, decode_unicode=False):
        for block in self._raw.split(b"\n\n"):
            yield block


RESPONSE = {
    "id": "resp_1", "object": "response", "created_at": 1, "model": "muse-spark-x",
    "status": "completed",
    "output": [{"type": "message", "id": "msg_1", "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "Hello!", "annotations": []}]}],
    "usage": {"input_tokens": 5, "output_tokens": 3, "total_tokens": 8},
}

RESPONSES_SSE = (
    'data: {"type":"response.created","response":{"id":"resp_1","object":"response",'
    '"created_at":1,"model":"muse-spark-x","status":"in_progress","output":[]}}\n\n'
    'data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,'
    '"content_index":0,"delta":"Hello!"}\n\n'
    'data: ' + json.dumps({"type": "response.completed", "response": RESPONSE},
                          ensure_ascii=False) + "\n\n"
).encode("utf-8")

captured = {}


def install_fake(mode="ok"):
    server.API_KEY = ""
    server._model_free = lambda m: True
    server._dump_save = lambda *a, **k: None

    def fake_post(path, payload, seed, rid, timeout=180, flat=False):
        captured["path"] = path
        captured["payload"] = payload
        captured["flat"] = flat
        if mode == "error":
            return FakeUpstream(b'{"message":"upstream says no"}', status=400,
                                content_type="application/json"), "zen"
        return FakeUpstream(RESPONSES_SSE), "zen"

    server._post_upstream = fake_post


def main():
    client = server.app.test_client()
    req = {"model": "muse-spark-x", "stream": True,
           "input": [{"type": "message", "role": "user", "content": "hi"}]}

    # ---------- 1) 流式：字节级透传 ----------
    install_fake("ok")
    r = client.post("/v1/responses", json=req)
    assert r.status_code == 200, (r.status_code, r.data[:300])
    assert r.data == RESPONSES_SSE, "流式必须字节级原样透传"
    assert captured["path"] == "/responses"
    assert captured["payload"]["stream"] is True
    assert captured["flat"] is True
    # 闸门工具注入已下沉 lane（flat=True 扁平格式）：断言 lane_inference 的准备结果
    inf_body = server.lane_inference.prepare_body(captured["payload"], flat=True)
    names = [t["name"] for t in inf_body["tools"]]
    assert names[:4] == ["bash", "glob", "grep", "read"], names

    # 客户端带了自己的工具 → 客户端在前、补齐缺失件（同走 lane 断言）
    r = client.post("/v1/responses",
                    json=dict(req, tools=[{"type": "function", "name": "my_tool",
                                           "description": "d",
                                           "parameters": {"type": "object"}}]))
    inf_body = server.lane_inference.prepare_body(captured["payload"], flat=True)
    names = [t["name"] for t in inf_body["tools"]]
    assert names[0] == "my_tool" and set(names) == {"my_tool", "bash", "glob", "grep", "read"}, names

    # ---------- 2) 非流式：聚合终止帧 ----------
    install_fake("ok")
    r = client.post("/v1/responses", json=dict(req, stream=False))
    assert r.status_code == 200, (r.status_code, r.data[:300])
    out = r.get_json()
    assert out == RESPONSE, "非流式应原样返回终止帧的 response 对象"

    # ---------- 3) 上游 400 透传 ----------
    install_fake("error")
    r = client.post("/v1/responses", json=req)
    assert r.status_code == 400, (r.status_code, r.data[:300])
    assert "upstream says no" in r.get_data(as_text=True)

    print("ALL PASS")


if __name__ == "__main__":
    main()
