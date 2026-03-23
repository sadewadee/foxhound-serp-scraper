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

	serp    *stage.SERPStage
	website *stage.WebsiteStage
	contact *stage.ContactStage
}

// New creates a pipeline orchestrator.
func New(cfg *config.Config, database *sql.DB, dd *dedup.Store) *Orchestrator {
	return &Orchestrator{
		cfg:     cfg,
		db:      database,
		dedup:   dd,
		serp:    stage.NewSERPStage(cfg, database, dd),
		website: stage.NewWebsiteStage(cfg, database, dd),
		contact: stage.NewContactStage(cfg, database, dd),
	}
}

// RunAll starts all stages concurrently.
func (o *Orchestrator) RunAll(ctx context.Context) error {
	slog.Info("pipeline: starting all stages")

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// Stage 2: SERP discovery.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.serp.Run(ctx); err != nil {
			errCh <- fmt.Errorf("serp stage: %w", err)
		}
	}()

	// Stage 3: Website discovery.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.website.Run(ctx); err != nil {
			errCh <- fmt.Errorf("website stage: %w", err)
		}
	}()

	// Stage 4: Contact enrichment.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.contact.Run(ctx); err != nil {
			errCh <- fmt.Errorf("contact stage: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	// Collect errors.
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

// RunEnrich starts website discovery + contact enrichment concurrently (no serp).
func (o *Orchestrator) RunEnrich(ctx context.Context) error {
	slog.Info("pipeline: starting enrich stages (website + contact)")

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.website.Run(ctx); err != nil {
			errCh <- fmt.Errorf("website stage: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := o.contact.Run(ctx); err != nil {
			errCh <- fmt.Errorf("contact stage: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("enrich pipeline errors: %v", errs)
	}

	slog.Info("pipeline: enrich stages completed")
	return nil
}

// RunStage starts a specific stage.
func (o *Orchestrator) RunStage(ctx context.Context, stageName string) error {
	switch stageName {
	case "serp":
		return o.serp.Run(ctx)
	case "website":
		return o.website.Run(ctx)
	case "contact":
		return o.contact.Run(ctx)
	default:
		return fmt.Errorf("unknown stage: %s", stageName)
	}
}

// SERP returns the SERP stage for metrics access.
func (o *Orchestrator) SERP() *stage.SERPStage { return o.serp }

// Website returns the Website stage for metrics access.
func (o *Orchestrator) Website() *stage.WebsiteStage { return o.website }

// Contact returns the Contact stage for metrics access.
func (o *Orchestrator) Contact() *stage.ContactStage { return o.contact }
