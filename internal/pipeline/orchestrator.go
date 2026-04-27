//go:build playwright

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/stage"
)

// Orchestrator manages all pipeline stages.
type Orchestrator struct {
	cfg   *config.Config
	db    *sql.DB
	dedup *dedup.Store

	serp     *stage.SERPStage
	enrich   *stage.EnrichStage
	reenrich *stage.ReenrichStage
}

// New creates a pipeline orchestrator.
func New(cfg *config.Config, database *sql.DB, dd *dedup.Store) *Orchestrator {
	return &Orchestrator{
		cfg:      cfg,
		db:       database,
		dedup:    dd,
		serp:     stage.NewSERPStage(cfg, database, dd),
		enrich:   stage.NewEnrichStage(cfg, database, dd),
		reenrich: stage.NewReenrichStage(cfg, database, dd),
	}
}

// RunAll starts all stages concurrently.
func (o *Orchestrator) RunAll(ctx context.Context) error {
	slog.Info("pipeline: starting all stages")

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// SERP discovery.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.serp.Run(ctx); err != nil {
			errCh <- fmt.Errorf("serp stage: %w", err)
		}
	}()

	// Enrich (contact extraction + contact page discovery).
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.enrich.Run(ctx); err != nil {
			errCh <- fmt.Errorf("enrich stage: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("pipeline errors: %v", errs)
	}

	slog.Info("pipeline: all stages completed")
	return nil
}

// RunEnrich starts contact enrichment only (no serp).
func (o *Orchestrator) RunEnrich(ctx context.Context) error {
	slog.Info("pipeline: starting enrich stage")
	return o.enrich.Run(ctx)
}

// RunReenrich starts the autonomous reenrich worker.
// Targets business_listings rows with re_enriched_at IS NULL and low completeness score.
// Manual trigger: UPDATE business_listings SET re_enriched_at = NULL WHERE domain IN (...)
func (o *Orchestrator) RunReenrich(ctx context.Context) error {
	slog.Info("pipeline: starting reenrich stage")
	return o.reenrich.Run(ctx)
}

// RunStage starts a specific stage.
func (o *Orchestrator) RunStage(ctx context.Context, stageName string) error {
	switch stageName {
	case "serp":
		return o.serp.Run(ctx)
	case "enrich":
		return o.enrich.Run(ctx)
	case "reenrich":
		return o.reenrich.Run(ctx)
	default:
		return fmt.Errorf("unknown stage: %s", stageName)
	}
}

// SERP returns the SERP stage for metrics access.
func (o *Orchestrator) SERP() *stage.SERPStage { return o.serp }

// Enrich returns the Enrich stage for metrics access.
func (o *Orchestrator) Enrich() *stage.EnrichStage { return o.enrich }
