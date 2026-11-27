package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/database"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inbox"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxhttp"
	"github.com/Ad-Quanta/moyoung-im-mautrix-whatsapp/internal/inboxservice"
	"github.com/rs/zerolog"
	"go.mau.fi/util/exzerolog"
)

// 版本信息由构建工具写入，二进制名仍可用于现有镜像构建脚本。
var Tag, Commit, BuildTime = "unknown", "unknown", "unknown"

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "WhatsApp Inbox 配置文件")
	flag.StringVar(&configPath, "c", "config.yaml", "WhatsApp Inbox 配置文件")
	version := flag.Bool("version", false, "显示版本")
	flag.Parse()
	if *version {
		fmt.Printf("WhatsApp Inbox %s (%s, %s)\n", Tag, Commit, BuildTime)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runInbox(ctx, configPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runInbox(ctx context.Context, path string) error {
	cfg, err := loadInboxConfig(path)
	if err != nil {
		return fmt.Errorf("读取 WhatsApp Inbox 配置: %w", err)
	}
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	if len(cfg.Logging.Writers) > 0 {
		configured, logErr := cfg.Logging.Compile()
		if logErr != nil {
			return fmt.Errorf("初始化日志: %w", logErr)
		}
		log = *configured
	}
	exzerolog.SetupDefaults(&log)
	db, err := database.Open(ctx, path, cfg.Database, log)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = database.MigrateInbox(ctx, db); err != nil {
		return err
	}
	cfg.Network.applyDeviceProfile()
	prepareWhatsAppVersion(ctx, db, log)
	manager := inboxservice.NewManager(ctx, db, "", log, cfg.Network.sessionConfig(), inbox.NewPushClient(cfg.Network.Inbox.PushURL, cfg.Network.Inbox.PushSecret))
	defer manager.Stop()
	if err = manager.Start(ctx); err != nil {
		return fmt.Errorf("启动 WhatsApp Inbox: %w", err)
	}
	provision := inboxhttp.New(manager, cfg.httpConfig())
	defer provision.Close()
	addr := cfg.Listen
	server := &http.Server{
		Addr: addr, Handler: inboxHandler(provision.Handler()), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("监听 WhatsApp Inbox: %w", err)
	}
	result := make(chan error, 1)
	go func() { result <- server.Serve(listener) }()
	log.Info().Str("listen", addr).Msg("WhatsApp Inbox HTTP is ready")
	select {
	case err = <-result:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		provision.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("停止 WhatsApp Inbox HTTP: %w", err)
		}
	}
	return nil
}

// inboxHandler 只挂载 Inbox 业务路由和健康检查。保留 Router 已使用的路径前缀。
func inboxHandler(provision http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.Handle("/_matrix/provision/", provision)
	return mux
}
