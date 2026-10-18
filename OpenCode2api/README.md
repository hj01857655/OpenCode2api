# OpenCode2api

> OpenCode 免费模型 → OpenAI / Anthropic 兼容 API 网关

把 OpenCode 免费层（`/inference/v1`）暴露为三协议兼容接口，供任意客户端调用。只转发 free 模型，付费面一律拒绝。

## 功能特性

- **三协议兼容**：OpenAI Chat Completions · OpenAI Responses · Anthropic Messages
- **免费模型透传**：自动拉取活清单，筛 free 模型（id 以 `-free` 结尾或 `big-pickle`），每 10 分钟刷新
- **匿名接入**：免费层为匿名面，上游固定 `Bearer public`，无需任何凭证
- **流式 / 非流式双模式**

## 快速开始

```bash
pip install -r requirements.txt
python server.py
```

```
Base URL : http://127.0.0.1:8390/v1
API Key  : .env 的 API_KEY
Model    : /v1/models 返回的 id（当前免费清单见下方「免费模型清单」）
```

## 免费模型清单

| 模型 id | 备注（本地约定） |
| --- | --- |
| `big-pickle` | 免费面特例：id 不以 `-free` 结尾 |
| `cat-jam-free` | |
| `mimo-v2.6-flash-free` | |
| `muse-spark-1.2-contributor-free` | 推理模型，走 `/v1/responses` 原生透传；`max_output_tokens` 给小会 `incomplete` 截断 |
| `muse-spark-1.3-contributor-free` | 同上 |
| `nemotron-3.5-super-vl-free` | |
| `nemotron-3.5-lightning-free` | |
| `jev-1.13-free` | |
| `longcat-2.5-preview-free` | |
| `deepseek-v4.1-flash-free` | |
| `space-bunny-free` | |
| `glm-5.3-flash-free` | 推理模型，先吐 `reasoning_content`；`max_tokens` 给小会 `finish_reason=length` 且正文为空 |
| `ll-alpha-free` | |

## API 接口

| 端点 | 方法 | 说明 |
|---|---|---|
| `/v1/chat/completions` | POST | OpenAI Chat Completions（流式/非流式） |
| `/v1/responses` | POST | OpenAI Responses API（**原生透传**，muse-spark-\* 等 Responses 原生模型直连上游） |
| `/v1/messages` | POST | Anthropic Messages API（翻译层 ⇄ Chat） |
| `/v1/models` | GET | 可用模型列表（仅 free 模型） |
| `/health` | GET | 存活探针 |

### 协议翻译架构

```
客户端                proxy 处理                      上游
─────────            ─────────────                  ─────────────
Chat Completions ──→  直通                       ──→  /zen/v1（优先）→ /inference/v1（回落） /chat/completions
Responses        ──→  原生透传（仅补闸门要件：     ──→  /responses
                      stream:true、gate 工具、
                      会话头；模型字段原样）
Anthropic        ──→  anthropic_bridge（双向翻译）──→  /chat/completions
```

### 目录结构（routes/ 协议路由 + lanes/ 通道 + adapters/ 翻译桥）

```
OpenCode2api/
├── server.py                 # bootstrap：常量、session 指纹、双通道调度与冷却、
│                             #   模型清单、鉴权/rid 中间件、app.run（只做调度）
├── lanes/                    # 通道层：每通道一个自包含文件（门禁规格全在本文件）
│   ├── zen.py                #   zen 通道（指纹+read 桥）
│   └── inference.py          #   inference 通道（V2 指纹+闸门工具）
├── routes/                   # 协议路由层：每协议一个文件
│   ├── chat.py               #   /v1/chat/completions（流式透传 + 非流式聚合）
│   ├── responses.py          #   /v1/responses（muse-spark 原生字节透传）
│   └── anthropic.py          #   /v1/messages（经 adapters/anthropic.py 翻译）
├── adapters/                 # 协议翻译层：⇄ Chat 双向翻译
│   ├── anthropic.py          #   Anthropic ⇄ Chat
│   └── responses.py          #   Responses ⇄ Chat（本仓当前未挂载，六仓同步留档）
├── tests/                    # 离线路由测试（打桩 _post_upstream，不起真实服务）
├── probes/                   # 门禁消融探针（历史证据，一锤子买卖脚本）
└── docs/                     # doc.html（反代自检页）+ labs/（门禁定案文档）
```

上游改协议形状 → 改对应 `routes/*.py`；改通道门禁 → 改对应 `lanes/*.py`；
都不用动 `server.py`。路由经 `register(app, core)` 挂载，`core` 即 server 模块
（动态属性查找），测试对 `server._post_upstream` 等的打桩对路由层同样生效。

OpenCode 免费层的 muse-spark-\* 系上游原生说 Responses 协议，`/v1/responses` 不走翻译、字节级透传（与 Chat 通道分路）；`cache_control` 与 Anthropic 入站 `thinking` 配置在翻译层丢弃，Responses 透传路径协议字段原样直达上游。

### 双通道（2026-10-18 起）

上游免费层有两个匿名入口，`_post_upstream` 按 **zen 优先、inference 回落**逐请求选择：

| | legacy `/zen/v1` | `/inference/v1`（2026-10-15 起） |
|---|---|---|
| UA 形状 | `opencode/1.18.31 (<os> <arch>; <rt>)`（v1.3.7 形） | `opencode/latest/2.0.29/cli`（2.x 形） |
| `x-opencode-request` | **必发** | 禁发（存在即 403） |
| `x-opencode-project` | `prj_`+24hex | 裸 40hex / global（不校验） |
| 工具门禁 | **仅需一个名为 `read` 的工具**（别名 read_file/view_file 等自动匹配，缺失注入 deprecated stub） | bash+read 最少集 + 真实 schema |
| 风控响应 | 403 FreeTierError → 冷却 600s；429 FreeUsageLimitError → 冷却 60s | 同左 |

配套防再犯：403 FreeTierError 后**按通道**冷却（默认 600s，`UPSTREAM_COOLDOWN` 可调），冷却期内请求跳过该通道；两通道均在冷却时本地 503 快速失败。429 限流走 60s 短冷却（`UPSTREAM_COOLDOWN_429`）并原样透传。上游并发闸 `UPSTREAM_CONCURRENCY`（默认 4）限制同时在途请求数——2026-10-18 01:11 的 1MB 级 10 连发即触发 IP 级滥用标记，三模型全 403 秒拒。

## 配置

| 变量 | 默认 | 说明 |
|---|---|---|
| `API_KEY` | — | 本地鉴权 Bearer token |
| `PORT` | `8390` | 监听端口 |
| `ZEN_BASE` | `https://opencode.ai/zen/v1` | zen 通道基址（2026-10-18 起优先通道） |
| `INFERENCE_BASE` | `https://opencode.ai/inference/v1` | inference 通道基址（回落通道） |
| `UPSTREAM_AUTH` | `Bearer public` | 上游鉴权（免费层匿名，两通道同） |
| `UPSTREAM_COOLDOWN` | `600` | 403 FreeTierError 后该通道冷却秒数 |
| `UPSTREAM_COOLDOWN_429` | `60` | 429 限流后该通道冷却秒数 |
| `UPSTREAM_CONCURRENCY` | `4` | 上游并发闸（同时在途请求上限） |

## 免费层指纹

上游对 free 模型有身份闸门，要件缺一即 `403 FreeTierError`。两通道规格不同（详见「双通道」一节），以 `/inference/v1` 为例：

| 要件 | 实现 |
|---|---|
| `User-Agent: opencode/latest/2.0.29/cli` | `OC_UA` 常量（zen 通道为 1.18.31 旧形，见 `_zen_headers()`） |
| 会话四头同值，降序时间戳格式 `ses_`+12hex+14base62 | `_stable_session()` 种子→会话映射 + 落盘 + 3 天 TTL（两通道共用） |
| `x-opencode-client: cli`、`x-opencode-project: global` | `_upstream_headers()`（zen 通道为 `prj_`+24hex + 必发 `x-opencode-request`） |
| body 声明四件工具 `bash/glob/grep/read` | `GATE_TOOLS`（zen 通道仅需一个 `read`，见 `_zenize_tools_chat()`） |
| `stream: true` + `stream_options.include_usage: true` | `chat()` 构造 |
| `Authorization: Bearer public` | `UPSTREAM_AUTH` |

## 已知限制

- **配额**：free 面有 per-IP 限流，密集测试触发 `429`/`403`，冷却后自恢复
- **模型会变**：以活清单为准，见上方「免费模型清单」；`exo-free` 已废弃（410）
- **muse-spark-\* 走 Responses**：推理模型，原生 Responses 透传；`max_output_tokens` 给太小触发 `incomplete` 截断
- **cache_control / Anthropic thinking 配置丢弃**：Anthropic 翻译层丢弃（Chat 侧本就无对应项）；Responses 透传路径不受影响
- **绝不读 OpenCode 本体凭证**（`~/.local/share/OpenCode/auth.json`）；dumps 只落 body 不落 header

