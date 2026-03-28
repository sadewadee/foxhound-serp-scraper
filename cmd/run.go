//go:build playwright

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/sadewadee/serp-scraper/internal/api"
	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/monitor"
	"github.com/sadewadee/serp-scraper/internal/pipeline"
	"github.com/sadewadee/serp-scraper/internal/telegram"
)

// RunPipeline starts the API server and scraping pipeline.
// API server is the main process — stays alive even when pipeline is idle.
// Pipeline stages run in background goroutines.
func RunPipeline(cfg *config.Config, stageName string, workers int) error {
	if workers > 0 {
		switch stageName {
		case "serp":
			cfg.SERP.Workers = workers
			cfg.SERP.Concurrency = workers
		case "website":
			cfg.Website.Workers = workers
		case "contact":
			cfg.Contact.Workers = workers
		case "enrich":
			cfg.Website.Workers = workers
			cfg.Contact.Workers = workers
		default:
			cfg.Contact.Workers = workers
		}
	}

	// Connect to database.
	database, err := db.Open(cfg)
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	defer database.Close()

	if err := db.Migrate(database); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	// Connect to Redis.
	dd, err := dedup.New(&cfg.Redis)
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	defer dd.Close()

	// Graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Start Prometheus metrics server.
	if cfg.Monitor.Enabled {
		go func() {
			addr := fmt.Sprintf(":%d", cfg.Monitor.Port)
			http.Handle("/metrics", monitor.Handler())
			slog.Info("monitor: metrics server starting", "addr", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				slog.Warn("monitor: metrics server error", "error", err)
			}
		}()
	}

	// Start pipeline stages in background (skip for "none" — API only mode).
	if stageName != "none" {
		orch := pipeline.New(cfg, database, dd)
		go func() {
			var pipeErr error
			switch stageName {
			case "", "all":
				pipeErr = orch.RunAll(ctx)
			case "enrich":
				pipeErr = orch.RunEnrich(ctx)
			default:
				pipeErr = orch.RunStage(ctx, stageName)
			}
			if pipeErr != nil {
				slog.Error("pipeline error", "error", pipeErr)
			}
			slog.Info("pipeline stages finished — API server still running")
		}()
	} else {
		slog.Info("stage=none: API-only mode, no pipeline stages")
	}

	// Start REST API server (blocking — keeps process alive).
	apiAddr := cfg.API.Addr
	if apiAddr == "" {
		apiAddr = ":8080"
	}

	authCfg := api.AuthConfig{Secret: cfg.API.Secret}
	for _, u := range cfg.API.Users {
		authCfg.Users = append(authCfg.Users, api.User{
			Username: u.Username,
			Password: u.Password,
			APIKey:   u.APIKey,
			Role:     api.Role(u.Role),
		})
	}

	dokployCfg := api.DokployConfig{
		URL:    cfg.Dokploy.URL,
		APIKey: cfg.Dokploy.APIKey,
	}

	apiServer := api.NewServer(database, dd.Client(), authCfg, dokployCfg)

	slog.Info("api: server starting", "addr", apiAddr)
	go func() {
		if err := apiServer.Start(apiAddr); err != nil && err.Error() != "http: Server closed" {
			slog.Error("api: server error", "error", err)
		}
	}()

	// Start Telegram bot (optional — only if token is set).
	if cfg.Telegram.BotToken != "" {
		tgBot := telegram.New(cfg.Telegram.BotToken, database, dd.Client(), cfg.Telegram.AllowedChatIDs)
		go tgBot.Run(ctx)
	}

	// Block until shutdown signal.
	<-ctx.Done()
	slog.Info("shutting down API server")
	apiServer.Shutdown(context.Background())

	return nil
}
