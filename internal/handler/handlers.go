// Package handler 的三个协议路由，对应 Python 版 routes/chat.py、
// routes/responses.py、routes/anthropic.py。
package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"opencode2api/internal/adapters"
	"opencode2api/internal/session"
	"opencode2api/internal/upstream"
)

// freeGate 免费清单门：非免费模型 400。返回 true = 放行。
func (s *Server) freeGate(model string) bool {
	return model == "" || s.Free.IsFree(model)
}

// openAIError OpenAI 风格错误体。
func openAIError(message, typ string) map[string]any {
	return map[string]any{"error": map[string]any{"message": message, "type": typ}}
}

// ── /v1/chat/completions（routes/chat.py）─────────────────────────────────────

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	info := reqInfoFrom(r)
	p := info.payload
	model := str(p["model"])

	if !s.freeGate(model) {
		s.Log.Warn("拒绝非免费模型", slog.String("rid", info.rid), slog.String("model", model))
		writeJSON(w, http.StatusBadRequest, openAIError(
			fmt.Sprintf("model %s 不在免费清单（the unit2api 只转发 free 模型）", model),
			"invalid_request_error"))
		return
	}

	payload := map[string]any{}
	for k, v := range p {
		if ChatPassthrough[k] {
			payload[k] = v
		}
	}
	payload["model"] = model
	payload["stream"] = true // 上游只收流式；非流式本地聚合
	// stream_options 覆写为 {"include_usage":true}（照搬 Python，客户端传的值被丢）
	payload["stream_options"] = map[string]any{"include_usage": true}
	wantStream := boolOf(p["stream"])

	res, err := s.Up.Post(r.Context(), "/chat/completions", payload,
		session.Seed(p), info.rid, false)
	if err != nil {
		// 必须在这里返回：上游响应不存在，继续往下走会把网络故障伪装成 500
		s.Log.Error("直连失败", slog.String("rid", info.rid), slog.String("err", err.Error()))
		writeJSON(w, http.StatusBadGateway, openAIError(
			"upstream error: "+err.Error(), "upstream_error"))
		return
	}

	if res.Resp.StatusCode >= 400 {
		body := errBody(res)
		s.Log.Warn("上游错误", slog.String("rid", info.rid),
			slog.Int("status", res.Resp.StatusCode), slog.String("lane", res.Lane),
			slog.String("body", truncStr(body, 400)))
		writeRaw(w, res.Resp.StatusCode, upstream.ContentType(res.Resp), []byte(body))
		return
	}
	defer res.Resp.Body.Close()

	if wantStream {
		s.passthroughStream(w, r, info, res.Resp.Body, "stream", "text/event-stream")
		return
	}

	// 非流式：本地聚合 SSE 成 Chat JSON
	out := aggregateChatSSE(sseLineFeed(res.Resp.Body), model)
	body := marshalCompact(out)
	s.Log.Info("done:", slog.String("rid", info.rid),
		slog.String("line", fmt.Sprintf("status=200 %dB %.1fs", len(body), elapsed(info.t0))))
	s.Dump.Save(info.rid, "resp", string(body))
	writeRaw(w, http.StatusOK, "application/json", body)
}

// aggregateChatSSE Chat SSE 行流 -> chat.completion JSON（routes/chat.py 的简易聚合）。
// 与 adapters.AggregateChatSSE 是两个实现：本函数额外收 reasoning_content，且
// 有 tool_calls 时把 finish=stop 改成 tool_calls —— 照搬 Python 版语义。
func aggregateChatSSE(lines iter.Seq[string], model string) map[string]any {
	var content, reasoning []string
	finish := ""
	type callSlot struct{ id, name, arguments string }
	toolCalls := map[int]*callSlot{}
	var usage map[string]any

	for line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payloadStr := strings.TrimSpace(line[len("data:"):])
		if payloadStr == "" || payloadStr == "[DONE]" {
			continue
		}
		var delta map[string]any
		if json.Unmarshal([]byte(payloadStr), &delta) != nil || delta == nil {
			continue
		}
		if u, ok := delta["usage"].(map[string]any); ok {
			usage = u
		}
		choices, _ := delta["choices"].([]any)
		for _, ci := range choices {
			ch, _ := ci.(map[string]any)
			if ch == nil {
				continue
			}
			d, _ := ch["delta"].(map[string]any)
			if d == nil {
				d = map[string]any{}
			}
			if ct := str(d["content"]); ct != "" {
				content = append(content, ct)
			}
			if rc := str(d["reasoning_content"]); rc != "" {
				reasoning = append(reasoning, rc)
			}
			tcs, _ := d["tool_calls"].([]any)
			for _, tci := range tcs {
				tc, _ := tci.(map[string]any)
				if tc == nil {
					continue
				}
				idx := 0
				if f, ok := tc["index"].(float64); ok {
					idx = int(f)
				}
				slot, ok := toolCalls[idx]
				if !ok {
					slot = &callSlot{}
					toolCalls[idx] = slot
				}
				if id := str(tc["id"]); id != "" {
					slot.id = id
				}
				fn, _ := tc["function"].(map[string]any)
				if fn != nil {
					slot.name += str(fn["name"])
					slot.arguments += str(fn["arguments"])
				}
			}
			if fr := str(ch["finish_reason"]); fr != "" {
				finish = fr
			}
		}
	}

	text := strings.Join(content, "")
	message := map[string]any{"role": "assistant", "content": text}
	if len(reasoning) > 0 {
		message["reasoning_content"] = strings.Join(reasoning, "")
	}
	if len(toolCalls) > 0 {
		keys := make([]int, 0, len(toolCalls))
		for k := range toolCalls {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		tcs := make([]any, 0, len(keys))
		for _, i := range keys {
			v := toolCalls[i]
			id := v.id
			if id == "" {
				id = fmt.Sprintf("call_%d", i)
			}
			tcs = append(tcs, map[string]any{
				"id": id, "type": "function",
				"function": map[string]any{"name": v.name, "arguments": v.arguments},
			})
		}
		message["tool_calls"] = tcs
		if text == "" {
			message["content"] = nil
		}
		if finish == "stop" {
			finish = "tool_calls"
		}
	}
	if finish == "" {
		finish = "stop"
	}
	out := map[string]any{
		"id":      "chatcmpl-" + adapters.HexID(24),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{map[string]any{
			"index": 0, "message": message, "finish_reason": finish,
		}},
	}
	if usage != nil {
		out["usage"] = usage
	} else {
		out["usage"] = map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
	}
	return out
}

// ── /v1/responses（routes/responses.py）───────────────────────────────────────

func (s *Server) responses(w http.ResponseWriter, r *http.Request) {
	info := reqInfoFrom(r)
	p := info.payload
	model := str(p["model"])

	if !s.freeGate(model) {
		writeJSON(w, http.StatusBadRequest, openAIError(
			fmt.Sprintf("model %s 不在免费清单", model), "invalid_request_error"))
		return
	}

	// muse-spark-* 等模型的上游原生说 Responses 协议，本层透传；请求侧仅补
	// 闸门要件：stream:true、include_usage。
	payload := map[string]any{}
	for k, v := range p {
		payload[k] = v
	}
	payload["model"] = model
	// 过闸门必需：stream:true（实测 stream:false 即 403）；stream_options
	// 非必需（2026-10-15 F8/G10 定案），补 include_usage 仅对齐官方客户端形
	payload["stream"] = true
	if so, ok := payload["stream_options"].(map[string]any); ok {
		so["include_usage"] = true
	} else {
		payload["stream_options"] = map[string]any{"include_usage": true}
	}
	wantStream := boolOf(p["stream"])

	res, err := s.Up.Post(r.Context(), "/responses", payload,
		session.Seed(p), info.rid, true)
	if err != nil {
		s.Log.Error("上游连接失败", slog.String("rid", info.rid), slog.String("err", err.Error()))
		writeJSON(w, http.StatusBadGateway, openAIError(err.Error(), "upstream_error"))
		return
	}

	if res.Resp.StatusCode >= 400 {
		body := errBody(res)
		s.Log.Warn("上游错误", slog.String("rid", info.rid),
			slog.Int("status", res.Resp.StatusCode), slog.String("lane", res.Lane),
			slog.String("body", truncStr(body, 400)))
		writeRaw(w, res.Resp.StatusCode, upstream.ContentType(res.Resp), []byte(body))
		return
	}
	defer res.Resp.Body.Close()

	if wantStream {
		// 流式：字节级原样透传，content-type 用上游的（对齐 Python）
		s.passthroughStream(w, r, info, res.Resp.Body, "responses", upstream.ContentType(res.Resp))
		return
	}

	// 非流式：聚合 Responses SSE。终止帧可能是 completed（正常）、incomplete
	// （max_output_tokens 被推理耗尽即截断，muse-spark 等推理模型小额度必现）
	// 或 failed；三者都是合法的 Responses 终态，原样返回。
	var done map[string]any
	for line := range sseLineFeed(res.Resp.Body) {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var evt map[string]any
		if json.Unmarshal([]byte(strings.TrimSpace(line[len("data:"):])), &evt) != nil || evt == nil {
			continue
		}
		t := str(evt["type"])
		if (t == "response.completed" || t == "response.incomplete" || t == "response.failed") &&
			evt["response"] != nil {
			if ro, ok := evt["response"].(map[string]any); ok {
				done = ro
				if t != "response.incomplete" {
					break
				}
			}
		}
	}
	if done == nil {
		writeJSON(w, http.StatusBadGateway, openAIError(
			"上游未返回终止帧（completed/incomplete/failed）", "upstream_error"))
		return
	}
	body := marshalCompact(done)
	s.Log.Info("done:", slog.String("rid", info.rid),
		slog.String("line", fmt.Sprintf("responses-aggr %dB %.1fs", len(body), elapsed(info.t0))))
	writeRaw(w, http.StatusOK, "application/json", body)
}

// ── /v1/messages（routes/anthropic.py，经 adapters 翻译）──────────────────────

func (s *Server) anthropic(w http.ResponseWriter, r *http.Request) {
	info := reqInfoFrom(r)
	p := info.payload
	model := str(p["model"])

	if !s.freeGate(model) {
		writeJSON(w, http.StatusBadRequest, adapters.AnthropicErrorBody(
			"invalid_request_error",
			fmt.Sprintf("model %s 不在免费清单（the unit2api 只转发 free 模型）", model)))
		return
	}

	chatPayload := adapters.AnthropicToChat(p)
	chatPayload["model"] = model
	chatPayload["stream"] = true // 上游只收流式
	chatPayload["stream_options"] = map[string]any{"include_usage": true}
	wantStream := boolOf(p["stream"])

	// 种子从翻译后的 payload 取（首条 user 消息在 messages 里），对齐 Python：
	// routes/anthropic.py 传的是 core._session_seed(chat_payload)
	res, err := s.Up.Post(r.Context(), "/chat/completions", chatPayload,
		session.Seed(chatPayload), info.rid, false)
	if err != nil {
		s.Log.Error("messages 上游连接失败", slog.String("rid", info.rid), slog.String("err", err.Error()))
		writeJSON(w, http.StatusBadGateway, adapters.AnthropicErrorBody("api_error", err.Error()))
		return
	}

	if res.Resp.StatusCode >= 400 {
		body := truncStr(errBody(res), 400)
		s.Log.Warn("messages 上游错误", slog.String("rid", info.rid),
			slog.Int("status", res.Resp.StatusCode), slog.String("lane", res.Lane),
			slog.String("body", body))
		writeJSON(w, res.Resp.StatusCode, adapters.AnthropicErrorBody(
			adapters.AnthropicErrTypeForHTTP(res.Resp.StatusCode), body))
		return
	}
	defer res.Resp.Body.Close()

	if wantStream {
		s.adapterStream(w, r, info,
			adapters.ChatSSEToAnthropic(sseLineFeed(res.Resp.Body), model), "messages")
		return
	}

	// 非流式：聚合 Chat SSE -> Chat JSON -> Anthropic Message
	chatJSON := adapters.AggregateChatSSE(sseLineFeed(res.Resp.Body), model)
	if uerr, ok := chatJSON["_upstream_error"].([]string); ok && len(uerr) >= 2 {
		writeJSON(w, http.StatusBadGateway, adapters.AnthropicErrorBody("api_error", uerr[1]))
		return
	}
	body := marshalCompact(adapters.ChatJSONToAnthropic(chatJSON, model))
	s.Log.Info("done:", slog.String("rid", info.rid),
		slog.String("line", fmt.Sprintf("messages-aggr %dB %.1fs", len(body), elapsed(info.t0))))
	writeRaw(w, http.StatusOK, "application/json", body)
}

// ── 流式转发 ─────────────────────────────────────────────────────────────────

// passthroughStream 上游 SSE 字节级透传（chat / responses 线）。
// 必须记字节数与首帧耗时：只记耗时时「200 但空流」和「正常返回」无法区分。
func (s *Server) passthroughStream(w http.ResponseWriter, r *http.Request, info reqInfo,
	upstreamBody io.Reader, kind, contentType string,
) {
	sseHeaders(w, contentType)
	flusher, _ := w.(http.Flusher)
	total := 0
	ttft := "-"
	for {
		if err := r.Context().Err(); err != nil {
			s.logDone(info, kind, total, ttft, true)
			return
		}
		chunk := make([]byte, 4096)
		n, rerr := upstreamBody.Read(chunk)
		if n > 0 {
			if ttft == "-" {
				ttft = fmt.Sprintf("%.1fs", elapsed(info.t0))
			}
			total += n
			if _, werr := w.Write(chunk[:n]); werr != nil {
				s.logDone(info, kind, total, ttft, true)
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			s.logDone(info, kind, total, ttft, false)
			return
		}
	}
}

// adapterStream 翻译后的帧流转发（messages 线）。
func (s *Server) adapterStream(w http.ResponseWriter, r *http.Request, info reqInfo,
	frames iter.Seq[[]byte], kind string,
) {
	sseHeaders(w, "text/event-stream")
	flusher, _ := w.(http.Flusher)
	total := 0
	ttft := "-"
	for frame := range frames {
		if err := r.Context().Err(); err != nil {
			s.logDone(info, kind, total, ttft, true)
			return
		}
		if ttft == "-" {
			ttft = fmt.Sprintf("%.1fs", elapsed(info.t0))
		}
		if _, werr := w.Write(frame); werr != nil {
			s.logDone(info, kind, total, ttft, true)
			return
		}
		total += len(frame)
		if flusher != nil {
			flusher.Flush()
		}
	}
	s.logDone(info, kind, total, ttft, false)
}

func (s *Server) logDone(info reqInfo, kind string, total int, ttft string, clientGone bool) {
	line := fmt.Sprintf("%s %dB %.1fs ttft=%s", kind, total, elapsed(info.t0), ttft)
	if clientGone {
		s.Log.Warn("client 断开", slog.String("rid", info.rid), slog.String("line", line))
	} else {
		s.Log.Info("done:", slog.String("rid", info.rid), slog.String("line", line))
	}
}

// errBody 错误分支的响应体：合成响应直接取 Body，真实上游响应读完一次并缓存。
func errBody(res *upstream.Result) string {
	if res.Body != "" {
		return res.Body
	}
	b, _ := io.ReadAll(io.LimitReader(res.Resp.Body, 512<<10))
	res.Resp.Body.Close()
	res.Body = string(b)
	return res.Body
}

func writeRaw(w http.ResponseWriter, status int, contentType string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func truncStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func elapsed(t0 time.Time) float64 { return time.Since(t0).Seconds() }
