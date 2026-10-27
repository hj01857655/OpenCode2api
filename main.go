// OpenCode2api-go 入口：装配配置、日志、session 指纹、免费清单与双通道调度。
//
// 状态文件（session_cache.json / free_cache.json / request.log / dumps/）全部
// 落在状态目录，与 Python 版字节兼容、可交替读写。
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"opencode2api/internal/config"
	"opencode2api/internal/dump"
	"opencode2api/internal/freelist"
	"opencode2api/internal/handler"
	"opencode2api/internal/lanes"
	"opencode2api/internal/session"
	"opencode2api/internal/upstream"
)

func main() {
	cfg := config.Load()
	logger := handler.NewLogger(cfg.StateDir)
	dw := dump.New(cfg.DumpOn, cfg.DumpDir, cfg.DumpMaxFiles)

	// 基址错配告警（对齐 server.py：历史上踩过两次，ZEN_BASE / INFERENCE_BASE
	// 按通道命名，值必须和通道对上，否则门禁形状和通道不符，上游一律 403）。
	if strings.Contains(cfg.Lane("zen").Base, "/inference") {
		logger.Warn("ZEN_BASE 指向 /inference/v1：两通道将打到同一处，zen 通道应设 " + config.DefaultZenBase)
	}
	if strings.Contains(cfg.Lane("inference").Base, "/zen/") {
		logger.Warn("INFERENCE_BASE 指向 /zen/v1：inference 通道应设 " + config.DefaultInferenceBase)
	}

	// session 指纹：两通道同源校验，共用一份落盘映射
	sessStore := session.NewStore(cfg.StateFilePath("session_cache.json"))
	sessStore.Load()

	// 双通道注册表：顺序即 cfg.Lanes（UPSTREAM_LANES），lane 门禁规格全在
	// lanes 包各自文件里，这里只接接口。
	var order []upstream.Lane
	for _, lc := range cfg.Lanes {
		auth := lc.Auth
		l := upstream.Lane{Name: lc.Name, Base: lc.Base}
		if lc.Name == "zen" {
			l.Headers = func(ses string) map[string]string { return lanes.ZenBuildHeaders(auth, ses) }
			l.PrepareBody = lanes.ZenPrepareBody
		} else {
			l.Headers = func(ses string) map[string]string { return lanes.BuildInferenceHeaders(auth, ses) }
			l.PrepareBody = lanes.InferencePrepareBody
		}
		order = append(order, l)
	}
	disp := upstream.New(order, upstream.Options{
		CooldownS:    cfg.CooldownS,
		Cooldown429S: cfg.Cooldown429S,
		Retry:        cfg.Retry,
		Concurrency:  cfg.Concurrency,
		Pool:         cfg.Pool,
		Timeout:      cfg.UpstreamTimeoutSec,
	}, sessStore.For, logger)
	disp.Dump = dw.Save

	// 免费清单：落盘上次可用清单兜底，冷启动不从空开始（对应 _load_free_cache）
	freeList := freelist.New(cfg.StateFilePath("free_cache.json"),
		func() freelist.Lane {
			l := disp.PickLaneForModels() // 模型清单与推理同为匿名面，挑首个未冷却通道
			return freelist.Lane{Name: l.Name, Base: l.Base, Headers: l.Headers}
		},
		func() string { return sessStore.For("") }) // 对齐 _stable_session("")
	freeList.LoadCache()
	freeList.Fetch(false, true) // quick：启动只试一次、4s 封顶，补拉交给后台线程
	go freeList.RefreshLoop(context.Background())

	srv := &handler.Server{
		Cfg:  cfg,
		Free: freeList,
		Dump: dw,
		Up:   disp,
		Sess: sessStore,
		Log:  logger,
	}

	schemeHost := fmt.Sprintf("%s:%d", config.ListenHost, cfg.Port)
	fmt.Printf("OpenCode2api (the unit2api) 启动: http://%s\n", schemeHost)
	fmt.Printf("OpenAI Base URL:    http://%s/v1\n", schemeHost)
	fmt.Printf("Anthropic Base URL: http://%s\n", schemeHost)
	fmt.Printf("面板:               http://%s/admin\n", schemeHost)
	fmt.Printf("通道: %s\n", laneNames(cfg))
	fmt.Printf("free 模型: %d 个\n", freeList.Count())
	if cfg.APIKey != "" {
		fmt.Println("本地鉴权: 已开启（API_KEY，/health 例外）")
	} else {
		fmt.Println("本地鉴权: 未开启")
	}

	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", config.ListenHost, cfg.Port), srv.Handler()); err != nil {
		log.Fatal(err)
	}
}

func laneNames(cfg *config.Config) string {
	out := ""
	for i, l := range cfg.Lanes {
		if i > 0 {
			out += ","
		}
		out += l.Name
	}
	return out
}
