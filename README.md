# OpenCode2api (Go)

![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white) ![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux-lightgrey) ![Type](https://img.shields.io/badge/type-API%20Gateway-blue)

OpenCode（the unit，zen 路由）→ OpenAI / Anthropic 兼容 API 的 Go 网关，纯 stdlib 零第三方依赖。只转发免费面模型（`gpt-free` 等），`claude-*` 等付费 id 被免费闸门直接 400。

## 构建 / 运行

```bash
go build -o opencode2api .
./opencode2api          # 默认 127.0.0.1:8390
```

## 凭据 / 免费面

免费层是**匿名面**：上游只认字面 `Bearer public`（真实 sk- key 反而 401），所以本网关没有凭据概念，不需要登录。健康信号有两处，面板上直接可见：

- **上游通道冷却**：403（FreeTierError）→ 该通道冷却 600s；429 限流 → 短冷却 60s，到期自动恢复；
- **免费清单新鲜度**：活清单 TTL 10 分钟，后台线程每 60s 自查；拉取失败保留旧清单并落盘兜底（`free_cache.json`），重启不从空开始。

状态文件（`session_cache.json` / `free_cache.json` / `request.log` / `dumps/`）全部落在状态目录（exe 所在目录，或 `OPENCODE2API_STATE_DIR`）。

## 接口

| 端点 | 说明 |
|---|---|
| `GET /health` | 健康检查（route=zen-direct、free 模型数） |
| `GET /v1/models` | 模型列表（当前免费清单） |
| `POST /v1/chat/completions` | OpenAI Chat 兼容（上游只收流式，非流式本地聚合） |
| `POST /v1/messages` | Anthropic Messages 兼容（Claude Code 直连） |
| `POST /v1/responses` | OpenAI Responses 兼容（muse-spark 等原生 Responses 模型透传） |
| `GET /admin` | Web 管理面板（内置，开箱即用）：服务状态、上游通道冷却、免费清单与手动重拉。状态目录放一份自定义 `panel.html` 可覆盖内置版 |

`/admin/api/*` 面板后端：`overview`（概览）/ `refresh-models`（强制重拉免费清单）。`API_KEY` 非空时与转发面同一把钥匙（Bearer）。

## 配置（环境变量 > `.env` > 默认值）

| 变量 | 默认 | 说明 |
|---|---|---|
| `API_KEY` | 空 | 非空 = 入口鉴权（整串比对 `Bearer <key>`，/health 与面板页例外） |
| `PORT` | `8390` | 监听端口（固定 127.0.0.1，仅回环） |
| `OPENCODE2API_STATE_DIR` | exe 所在目录 | 状态目录落点 |
| `ZEN_BASE` / `INFERENCE_BASE` | 内置值 | 上游地址覆盖（按通道命名，值必须和通道对上，错配上游一律 403） |
| `UPSTREAM_AUTH` | `Bearer public` | 上游鉴权头覆盖 |
| `UPSTREAM_LANES` | `zen,inference` | 通道顺序（zen 优先、inference 回落），未知名字忽略 |
| `UPSTREAM_COOLDOWN` / `UPSTREAM_COOLDOWN_429` | `600` / `60` | 403 长冷却 / 429 短冷却秒数 |
| `UPSTREAM_RETRY` / `UPSTREAM_CONCURRENCY` / `UPSTREAM_POOL` / `UPSTREAM_TIMEOUT` | `1` / `4` / `32` / `180` | 连接级重试 / 并发闸 / 连接池 / 上游超时秒 |
| `DUMP` / `DUMP_DIR` / `DUMP_MAX_FILES` | 开 / `dumps` / 200 | 上游请求 / 响应落盘（排障用） |

## Docker 部署

```bash
# 配置环境变量后启动
cp .env.example .env   # 按需填写
docker compose up -d
```

镜像同时发布到 GHCR：`ghcr.io/hj01857655/opencode2api`（跟随 v* 标签发布，含 latest，linux/amd64 + linux/arm64）。

二进制发版：推送 `v*` 标签后 Actions 自动构建 linux / windows / darwin 的 amd64 + arm64 六平台产物并发布 Release。
