//go:build playwright

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sadewadee/serp-scraper/internal/api"
	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/monitor"
	"github.com/sadewadee/serp-scraper/internal/pipeline"
	"github.com/sadewadee/serp-scraper/internal/query"
	"github.com/sadewadee/serp-scraper/internal/reconciler"
	"github.com/sadewadee/serp-scraper/internal/telegram"
	"github.com/sadewadee/serp-scraper/internal/validate"
)

// RunPipeline starts the API server and scraping pipeline.
// API server is the main process — stays alive even when pipeline is idle.
// Pipeline stages run in background goroutines.
// Workers write directly to DB; triggers handle normalization.
func RunPipeline(cfg *config.Config, stageName string, workers int) error {
	if workers > 0 {
		switch stageName {
		case "serp":
			cfg.SERP.Concurrency = workers
		default:
			cfg.Enrich.Concurrency = workers
		}
	}

	// Connect to database.
	database, err := db.Open(cfg)
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	defer database.Close()

	// Migrate is serialized across containers by a session advisory lock, but the
	// catalog DDL (CREATE OR REPLACE FUNCTION / trigger binding) can still hit a
	// transient "tuple concurrently updated" (XX000) during a fleet boot when
	// other sessions touch the same pg_catalog tuples (autovacuum, overlapping
	// boots, orphaned sessions). It's safe to retry — the whole migration is
	// idempotent (IF NOT EXISTS / version-gated / CONCURRENTLY log-and-continue).
	// Without this the manager would exit on the conflict and crash-loop with the
	// API never starting (the 2026-06-01/02 incident).
	for attempt := 1; ; attempt++ {
		err := db.Migrate(database)
		if err == nil {
			break
		}
		if attempt < 8 && strings.Contains(err.Error(), "tuple concurrently updated") {
			slog.Warn("run: migrate transient catalog conflict — retrying",
				"attempt", attempt, "error", err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
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

	// Start email backfill validator — validates existing unvalidated emails via Mordibouncer.
	// Only runs in manager (stage=none) to avoid competing with workers.
	if stageName == "none" {
		if validator := validate.NewMordibouncer(&cfg.Mordibouncer); validator != nil {
			go validate.BackfillValidation(ctx, database, validator)
		}
		// Start project-level reconciler — manages the full pipeline from manager.
		projReconciler := reconciler.New(database, dd.Client())
		go projReconciler.Run(ctx)

		// One-time, background backfill of business_listings.completeness_score so
		// the reenrich eligibility query filters on the indexed column. Manager-only,
		// non-blocking (does not delay boot); version-gated → no-op after first run.
		go db.BackfillCompletenessScore(ctx, database)

		// One-time, background cleanup that flags off_niche for schema.org
		// content/junk + niche-less generic @type categories (Article, FAQPage,
		// WPHeader, Organization-without-niche, …). Backup-first, batched,
		// version-gated → no-op after first run. Manager-only, non-blocking.
		go db.BackfillSchemaTypeDenylist(ctx, database)

		// Keep the category_stats matview fresh so /api/v2/results/categories reads
		// precomputed counts instead of the live aggregation that blew the 5s API
		// budget at scale. Manager-only, non-blocking (seeds on boot, refreshes on
		// a ticker).
		go db.RefreshCategoryStatsLoop(ctx, database)

		// Geo lineage (v4 schema direction): seed the countries + geo_cities
		// reference tables, then run the one-time geo backfills IN ORDER —
		// queries first (city token in text → ISO-2 country + city), then
		// listing inheritance from the source query (geo_source =
		// 'query_inference'), then the legacy full-name → ISO-2 country_code
		// map. Sequential by design: inheritance reads queries.country.
		// Manager-only, background, version-gated → no-op after first run.
		go func() {
			geoCities := query.GeoCityRows()
			db.SeedCountries(ctx, database, query.CountryRows())
			db.SeedGeoCities(ctx, database, geoCities)
			db.BackfillQueryGeo(ctx, database, geoCities)
			db.BackfillListingGeoInherit(ctx, database)
			db.BackfillListingCountryCode(ctx, database)
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
			case "reenrich":
				pipeErr = orch.RunReenrich(ctx)
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
			APIKey:   u.APIKey,
			Role:     api.Role(u.Role),
		})
	}

	apiServer := api.NewServer(database, dd.Client(), authCfg)

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
