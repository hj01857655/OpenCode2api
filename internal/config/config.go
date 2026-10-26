// Package config 装配 OpenCode2api 的运行配置：.env 加载 + 上游常量 + 状态目录。
//
// 状态目录规则与 Python 版对齐（Python: dirname(abspath(__file__))）：
// session_cache.json / free_cache.json / request.log / dumps/ 全部落在状态目录。
// 部署时把 exe 放进原 OpenCode2api 目录（或设 OPENCODE2API_STATE_DIR 指向它），
// 即可与 Python 版交替读写同一套状态文件。
//
// 免费层是匿名面（上游只认字面 `Bearer public`），所以本包没有 credentials
// 相关配置——Python 版的 credentials.json 也不参与任何上游请求。
package config

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	DefaultZenBase       = "https://" + "opencode" + ".ai" + "/zen/v1"
	DefaultInferenceBase = "https://" + "opencode" + ".ai" + "/inference/v1"
	ListenHost           = "127.0.0.1"
	DefaultPort          = 8390
)

// LaneCfg 单通道配置（基址 + 鉴权常量，对应 lanes/*.py 的 BASE / AUTH）。
type LaneCfg struct {
	Name string
	Base string
	Auth string
}

type Config struct {
	APIKey             string
	Port               int
	StateDir           string // session_cache.json / free_cache.json / request.log / dumps/ 所在
	DumpOn             bool
	DumpDir            string
	DumpMaxFiles       int
	Lanes              []LaneCfg // 通道顺序（UPSTREAM_LANES），调度与清单拉取共用
	CooldownS          int       // 403 FreeTierError 冷却
	Cooldown429S       int       // 429 限流短冷却
	Retry              int       // 连接级失败重试次数
	Concurrency        int       // 上游并发闸（默认 4，10-18 被风控标记后增）
	Pool               int       // 连接池大小（对齐 Python 版 requests adapter）
	UpstreamTimeoutSec int       // 上游请求超时（Python 默认 180）
}

// Lane 按名查通道配置。
func (c *Config) Lane(name string) LaneCfg {
	for _, l := range c.Lanes {
		if l.Name == name {
			return l
		}
	}
	return LaneCfg{}
}

// StateFilePath 返回状态目录下的文件路径。
func (c *Config) StateFilePath(name string) string { return filepath.Join(c.StateDir, name) }

// Load 读取配置。优先级：环境变量 > .env > 默认值。
// .env 从状态目录和工作目录两处尝试（与 python-dotenv 的 load_dotenv 语义近似：
// 不覆盖已有环境变量，两处都有的键先到先得）。
func Load() *Config {
	c := &Config{
		Port:               DefaultPort,
		DumpOn:             true,
		DumpMaxFiles:       200,
		CooldownS:          600,
		Cooldown429S:       60,
		Retry:              1,
		Concurrency:        4,
		Pool:               32,
		UpstreamTimeoutSec: 180,
	}

	// 状态目录：env 显式指定 > exe 所在目录
	c.StateDir = os.Getenv("OPENCODE2API_STATE_DIR")
	if c.StateDir == "" {
		if exe, err := os.Executable(); err == nil {
			c.StateDir = filepath.Dir(exe)
		} else {
			wd, _ := os.Getwd()
			c.StateDir = wd
		}
	}

	// .env：状态目录优先，其次工作目录
	for _, dir := range []string{c.StateDir, mustWd()} {
		loadDotEnv(filepath.Join(dir, ".env"))
	}

	c.APIKey = strings.TrimSpace(os.Getenv("API_KEY"))
	if p, err := strconv.Atoi(strings.TrimSpace(envOr("PORT", "8390"))); err == nil && p > 0 {
		c.Port = p
	}
	if v := strings.TrimSpace(os.Getenv("DUMP")); v == "0" {
		c.DumpOn = false
	}
	if v := strings.TrimSpace(os.Getenv("DUMP_MAX_FILES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.DumpMaxFiles = n
		}
	}
	c.DumpDir = strings.TrimSpace(os.Getenv("DUMP_DIR"))
	if c.DumpDir == "" {
		c.DumpDir = filepath.Join(c.StateDir, "dumps")
	}

	c.CooldownS = envInt("UPSTREAM_COOLDOWN", 600)
	c.Cooldown429S = envInt("UPSTREAM_COOLDOWN_429", 60)
	c.Retry = envInt("UPSTREAM_RETRY", 1)
	c.Concurrency = envInt("UPSTREAM_CONCURRENCY", 4)
	c.Pool = envInt("UPSTREAM_POOL", 32)
	c.UpstreamTimeoutSec = envInt("UPSTREAM_TIMEOUT", 180)

	// 通道注册表（对齐 server._LANE_MODS / lanes/*.py 的模块级常量）：
	// UPSTREAM_AUTH 为空 = 免费层匿名面字面 `Bearer public`（真实 sk- key 反而 401）。
	zen := LaneCfg{Name: "zen",
		Base: firstNonEmpty(strings.TrimSpace(os.Getenv("ZEN_BASE")), DefaultZenBase),
		Auth: firstNonEmpty(strings.TrimSpace(os.Getenv("UPSTREAM_AUTH")), "Bearer public")}
	inf := LaneCfg{Name: "inference",
		Base: firstNonEmpty(strings.TrimSpace(os.Getenv("INFERENCE_BASE")), DefaultInferenceBase),
		Auth: firstNonEmpty(strings.TrimSpace(os.Getenv("UPSTREAM_AUTH")), "Bearer public")}

	// UPSTREAM_LANES 逗号分隔（默认 zen 优先、inference 回落）；未知名字忽略，
	// 全部无效时回默认双通道（与 Python 版 `or ["zen","inference"]` 一致）。
	seen := map[string]bool{}
	for _, s := range strings.Split(envOr("UPSTREAM_LANES", "zen,inference"), ",") {
		s = strings.TrimSpace(s)
		if (s != "zen" && s != "inference") || seen[s] {
			continue
		}
		seen[s] = true
		if s == "zen" {
			c.Lanes = append(c.Lanes, zen)
		} else {
			c.Lanes = append(c.Lanes, inf)
		}
	}
	if len(c.Lanes) == 0 {
		c.Lanes = []LaneCfg{zen, inf}
	}
	return c
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key))); err == nil {
		return v
	}
	return def
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func mustWd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// loadDotEnv 极简 .env 解析：KEY=VALUE 行，# 注释，不展开变量，不覆盖已有 env。
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if k != "" && os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
