//go:build playwright

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/monitor"
	"github.com/sadewadee/serp-scraper/internal/pipeline"
)

// RunPipeline starts the scraping pipeline.
func RunPipeline(cfg *config.Config, stageName string, workers int) error {
	// Override worker count if specified.
	if workers > 0 {
		switch stageName {
		case "serp":
			cfg.SERP.Workers = workers
		case "website":
			cfg.Website.Workers = workers
		case "contact":
			cfg.Contact.Workers = workers
		default:
			// For "all", apply to contact stage (most scalable).
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

	// Start Prometheus metrics server if enabled.
	if cfg.Monitor.Enabled {
		go func() {
			addr := fmt.Sprintf(":%d", cfg.Monitor.Port)
			http.Handle("/metrics", monitor.Handler())
			slog.Info("monitor: starting metrics server", "addr", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				slog.Warn("monitor: metrics server error", "error", err)
			}
		}()
	}

	// Create and run orchestrator.
	orch := pipeline.New(cfg, database, dd)

	ctx := context.Background()
	if stageName == "" || stageName == "all" {
		return orch.RunAll(ctx)
	}
	return orch.RunStage(ctx, stageName)
}
