# -*- coding: utf-8 -*-
"""routes/responses.py — OpenAI Responses 协议路由（/v1/responses）。

muse-spark-* 等模型的上游原生说 Responses 协议，本层字节级透传（请求侧仅补
闸门要件：stream:true、include_usage；闸门工具注入下沉各 lane，flat=True 走
扁平工具格式）；非流式聚合终止帧（completed / incomplete / failed 三态）。
"""
import json
import time

import requests
from flask import Response, g, jsonify


def register(app, core):
    """挂载 responses 路由。core = server 模块（动态查找，见 routes/chat.py）。"""
    logger = core.logger

    @app.route("/v1/responses", methods=["POST"])
    def responses_route():
        p = g.payload
        model = p.get("model") or ""
        if model and not core._model_free(model):
            return jsonify(error={"message": "model %s 不在免费清单" % model,
                                  "type": "invalid_request_error"}), 400

        payload = dict(p)
        payload["model"] = model
        # 过闸门必需：stream:true（实测 stream:false 即 403）；stream_options
        # 非必需（2026-10-15 F8/G10 定案），补 include_usage 仅对齐官方客户端形
        payload["stream"] = True
        if isinstance(payload.get("stream_options"), dict):
            payload["stream_options"]["include_usage"] = True
        else:
            payload["stream_options"] = {"include_usage": True}
        # 闸门工具注入已下沉到各 lane 文件（flat=True 走扁平工具格式）
        want_stream = bool(p.get("stream"))

        try:
            upstream, lane = core._post_upstream("/responses", payload,
                                                 core._session_seed(p), g.rid,
                                                 flat=True)
        except requests.RequestException as e:
            logger.error("[%s] 上游连接失败: %s: %s", g.rid, type(e).__name__, e)
            return jsonify(error={"message": str(e), "type": "upstream_error"}), 502

        if upstream.status_code >= 400:
            logger.warning("[%s] 上游错误 HTTP %s (lane=%s): %s", g.rid,
                           upstream.status_code, lane, upstream.text[:400])
            core._dump_save(g.rid, "resp", upstream.text)
            return Response(upstream.text, status=upstream.status_code,
                            content_type=upstream.headers.get("Content-Type", "application/json"))

        if want_stream:
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
                    logger.info("[%s] done: responses %dB %.1fs ttft=%s", rid, n,
                                time.time() - t0,
                                "%.1fs" % ttft if ttft else "-")
            return Response(gen(), status=200,
                            content_type=upstream.headers.get("Content-Type", "text/event-stream"))

        # 非流式：聚合 Responses SSE。终止帧可能是 completed（正常）或
        # incomplete（max_output_tokens 被推理耗尽即截断，muse-spark 等推理模型
        # 小额度必现）或 failed；三者都是合法的 Responses 终态，原样返回。
        done = None
        for raw in upstream.iter_lines(decode_unicode=False):
            line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
            if not line.startswith("data:"):
                continue
            try:
                evt = json.loads(line[5:].strip())
            except Exception:
                continue
            t = evt.get("type")
            if t in ("response.completed", "response.incomplete", "response.failed") \
                    and isinstance(evt.get("response"), dict):
                done = evt["response"]
                if t != "response.incomplete":
                    break
        if done is None:
            return jsonify(error={"message": "上游未返回终止帧（completed/incomplete/failed）",
                                  "type": "upstream_error"}), 502
        body = json.dumps(done, ensure_ascii=False)
        logger.info("[%s] done: responses-aggr %dB %.1fs", g.rid, len(body), time.time() - g.t0)
        return Response(body, status=200, content_type="application/json")
