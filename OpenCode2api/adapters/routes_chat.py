# -*- coding: utf-8 -*-
"""routes_chat.py — OpenAI Chat Completions 协议适配（/v1/chat/completions）。

流式：上游 SSE 字节级透传（上游只收流式请求，stream 由本层强制置 true）；
非流式：本地聚合 SSE 成 Chat JSON（content / reasoning_content / tool_calls /
usage 全量还原）。闸门工具注入已下沉各 lane（adapters/lane_*.py），本层只管
协议形状，不掺和通道门禁。
"""
import json
import time
import uuid

import requests
from flask import Response, g, jsonify

# 直通上游的请求字段白名单（其余客户端字段一律丢弃）
PASSTHROUGH = ("model", "messages", "temperature", "top_p", "max_tokens",
               "unit_choice", "stop", "user", "stream_options", "reasoning_effort")


def register(app, core):
    """挂载 chat 路由。core = server 模块：调度（_post_upstream）、清单
    （_model_free）、会话（_session_seed）、落盘（_dump_save）、日志——
    经模块属性动态查找，测试对 server._post_upstream 等的替换因此生效。"""
    logger = core.logger

    @app.route("/v1/chat/completions", methods=["POST"])
    def chat():
        p = g.payload
        model = p.get("model") or ""
        if model and not core._model_free(model):
            logger.warning("[%s] 拒绝非免费模型: %s", g.rid, model)
            return jsonify(error={"message": "model %s 不在免费清单（the unit2api 只转发 free 模型）" % model,
                                  "type": "invalid_request_error"}), 400

        payload = {k: v for k, v in p.items() if k in PASSTHROUGH}
        payload["model"] = model
        payload["stream"] = True            # 上游只收流式；非流式本地聚合
        payload["stream_options"] = {"include_usage": True}
        # 闸门工具注入已下沉到各 lane 文件（lane_*.prepare_body，随通道而异）：
        # 路由只管协议透传，不再掺和通道门禁细节。
        want_stream = bool(p.get("stream"))

        # ── 上游：按 UPSTREAM_LANES 顺序逐通道尝试（默认 zen 优先、inference 回落）──
        # 连接被切断时补试一次，理由与边界见 server._post_lane。
        try:
            upstream, lane = core._post_upstream("/chat/completions", payload,
                                                 core._session_seed(p), g.rid)
        except requests.RequestException as e:
            # 必须在这里返回：upstream 不存在，继续往下走 upstream.status_code
            # 会抛 AttributeError，把网络故障伪装成 500。
            logger.error("[%s] 直连失败: %s: %s", g.rid, type(e).__name__, e)
            return jsonify(error={"message": "upstream error: %s" % e,
                                  "type": "upstream_error"}), 502

        if upstream.status_code >= 400:
            body = upstream.text[:400]
            logger.warning("[%s] 上游错误 HTTP %s (lane=%s): %s",
                           g.rid, upstream.status_code, lane, body)
            core._dump_save(g.rid, "resp", upstream.text)
            return Response(upstream.text, status=upstream.status_code,
                            content_type=upstream.headers.get("Content-Type", "application/json"))

        if want_stream:
            # 生成器是惰性的，迭代时请求上下文已结束，不能再碰 flask.g
            # （Working outside of application context → done 日志丢失/500）。
            rid, t0 = g.rid, g.t0

            def gen():
                n = 0
                ttft = None
                try:
                    for chunk in upstream.iter_content(chunk_size=4096):
                        if chunk:
                            if ttft is None:
                                ttft = time.time() - t0
                            n += len(chunk)
                            yield chunk
                finally:
                    # 必须记字节数与首帧耗时：只记耗时时"200 但空流"和"正常返回"无法区分
                    logger.info("[%s] done: stream %dB %.1fs ttft=%s", rid, n,
                                time.time() - t0,
                                "%.1fs" % ttft if ttft else "-")
            return Response(gen(), status=200, content_type="text/event-stream")

        # 非流式：本地聚合 SSE
        content, reasoning = [], []
        finish = None
        tool_calls = {}          # index -> {id,name,arguments}（流式按 index 分片）
        usage = None
        for raw in upstream.iter_lines(decode_unicode=False):
            # 上游 SSE 不带 charset，requests 的 unicode 模式会按 Latin-1 猜，
            # 中文全毁；必须字节级收，显式 UTF-8 解码。
            line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
            if not line or not line.startswith("data:"):
                continue
            if line.strip() == "data: [DONE]":
                break
            try:
                delta = json.loads(line[5:].strip())
            except Exception:
                continue
            if isinstance(delta.get("usage"), dict):
                usage = delta["usage"]
            for ch in (delta.get("choices") or []):
                d = ch.get("delta") or {}
                if d.get("content"):
                    content.append(d["content"])
                if d.get("reasoning_content"):
                    reasoning.append(d["reasoning_content"])
                for tc in d.get("tool_calls") or []:
                    slot = tool_calls.setdefault(tc.get("index", 0),
                                                 {"id": "", "name": "", "arguments": ""})
                    fn = tc.get("function") or {}
                    slot["id"] = tc.get("id") or slot["id"]
                    slot["name"] += fn.get("name") or ""
                    slot["arguments"] += fn.get("arguments") or ""
                if ch.get("finish_reason"):
                    finish = ch["finish_reason"]
        text = "".join(content)
        message = {"role": "assistant", "content": text}
        if reasoning:
            message["reasoning_content"] = "".join(reasoning)
        if tool_calls:
            message["tool_calls"] = [
                {"id": v["id"] or "call_%d" % i, "type": "function",
                 "function": {"name": v["name"], "arguments": v["arguments"]}}
                for i, v in sorted(tool_calls.items())]
            if not text:
                message["content"] = None
            if finish == "stop":
                finish = "tool_calls"
        out = {"id": "chatcmpl-" + uuid.uuid4().hex[:24], "object": "chat.completion",
               "created": int(time.time()), "model": model,
               "choices": [{"index": 0, "message": message,
                            "finish_reason": finish or "stop"}],
               "usage": usage or {"prompt_tokens": 0, "completion_tokens": 0,
                                  "total_tokens": 0}}
        body = json.dumps(out, ensure_ascii=False)
        logger.info("[%s] done: status=200 %dB %.1fs", g.rid, len(body), time.time() - g.t0)
        core._dump_save(g.rid, "resp", body)
        return Response(body, status=200, content_type="application/json")
