# -*- coding: utf-8 -*-
"""routes/messages.py — Anthropic Messages 协议路由（/v1/messages）。

上游 /messages 是付费凭证面（免费模型 403、付费 401），免费层走不了直通；
本层经 adapters/anthropic.py 在本地把 Anthropic ⇄ Chat 双向翻译，
上游照旧走 chat 线（同一套双通道调度 + 闸门工具）。鉴权同 chat，另接受
x-api-key（由 server 的 before_request 统一放行，见 core.API_KEY 段）。
"""
import json
import time

import requests
from flask import Response, g, jsonify

from adapters import anthropic


def register(app, core):
    """挂载 messages 路由。core = server 模块（动态查找，见 routes/chat.py）。"""
    logger = core.logger

    @app.route("/v1/messages", methods=["POST"])
    def messages_route():
        p = g.payload
        model = p.get("model") or ""
        if model and not core._model_free(model):
            return jsonify(anthropic.error_body(
                "invalid_request_error",
                "model %s 不在免费清单（the unit2api 只转发 free 模型）" % model)), 400

        try:
            chat_payload = anthropic.anthropic_to_chat(p)
        except Exception as e:
            return jsonify(anthropic.error_body(
                "invalid_request_error", "请求翻译失败: %s" % e)), 400
        chat_payload["model"] = model
        chat_payload["stream"] = True            # 上游只收流式
        chat_payload["stream_options"] = {"include_usage": True}
        # 闸门工具注入已下沉到各 lane 文件
        want_stream = bool(p.get("stream"))

        try:
            upstream, lane = core._post_upstream("/chat/completions", chat_payload,
                                                 core._session_seed(chat_payload), g.rid)
        except requests.RequestException as e:
            logger.error("[%s] messages 上游连接失败: %s: %s", g.rid, type(e).__name__, e)
            return jsonify(anthropic.error_body("api_error", str(e))), 502

        if upstream.status_code >= 400:
            body = upstream.text[:400]
            logger.warning("[%s] messages 上游错误 HTTP %s (lane=%s): %s",
                           g.rid, upstream.status_code, lane, body)
            core._dump_save(g.rid, "resp", upstream.text)
            err = anthropic.error_body(
                anthropic.err_type_for_http(upstream.status_code), body)
            return Response(json.dumps(err, ensure_ascii=False),
                            status=upstream.status_code,
                            content_type="application/json")

        if want_stream:
            rid, t0 = g.rid, g.t0

            def gen():
                n = 0
                ttft = None
                try:
                    for frame in anthropic.chat_sse_to_anthropic(
                            upstream.iter_lines(decode_unicode=False), model):
                        if ttft is None:
                            ttft = time.time() - t0
                        n += len(frame)
                        yield frame
                finally:
                    logger.info("[%s] done: messages %dB %.1fs ttft=%s", rid, n,
                                time.time() - t0,
                                "%.1fs" % ttft if ttft else "-")
            return Response(gen(), status=200, content_type="text/event-stream")

        # 非流式：聚合 Chat SSE -> Chat JSON -> Anthropic Message
        chat_json = anthropic.aggregate_chat_sse(
            upstream.iter_lines(decode_unicode=False), model)
        uerr = chat_json.pop("_upstream_error", None)
        if uerr is not None:
            err = anthropic.error_body("api_error", uerr[1])
            return Response(json.dumps(err, ensure_ascii=False),
                            status=502, content_type="application/json")
        result = anthropic.chat_json_to_anthropic(chat_json, model)
        body = json.dumps(result, ensure_ascii=False)
        logger.info("[%s] done: messages-aggr %dB %.1fs", g.rid, len(body),
                    time.time() - g.t0)
        return Response(body, status=200, content_type="application/json")
