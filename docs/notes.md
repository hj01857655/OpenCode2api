# OpenCode2api 说明

- 免费层为匿名面：上游只认字面 `Bearer public`，无凭据落盘需求（config 无 credentials 概念）。
- 状态目录：session_cache.json / free_cache.json / request.log / dumps/（均已 gitignore）。
- 上游 free 模型 9 个（big-pickle、beam-free、flint-40b-free、mimo-v2.6-flash-free、nemotron-3.5-super-vl-free、nemotron-3.5-lightning-free、jev-1.13-free、deepseek-v4.1-flash-free、glm-5.3-flash-free）。
- 429 FreeUsageLimitError = 匿名免费额度耗尽（上游限制，非代码问题）。
