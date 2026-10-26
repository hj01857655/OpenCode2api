# OpenCode2api (Go)

OpenCode 转 OpenAI / Anthropic 兼容 API 的 Go 网关实现。

## 构建

```bash
go build -o opencode2api .
```

## 运行

| 环境变量 | 说明 |
|---|---|
| `API_KEY` | 客户端鉴权密钥 |
| `OPENCODE2API_STATE_DIR` | 状态目录 |
| `ZEN_BASE` / `INFERENCE_BASE` | 上游地址覆盖 |
| `UPSTREAM_AUTH` | 上游鉴权头，默认 `Bearer public` |
| `DUMP` / `DUMP_DIR` / `DUMP_MAX_FILES` | 请求转储开关与目录 |

## 接口

- `GET /health` — 健康检查
- `GET /models`、`GET /v1/models` — 模型列表
- `POST /v1/chat/completions` — OpenAI Chat 兼容
- `POST /v1/messages` — Anthropic Messages 兼容
- `POST /v1/responses` — OpenAI Responses 兼容
