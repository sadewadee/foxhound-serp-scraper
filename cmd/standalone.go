//go:build playwright

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/standalone"
)

// RunStandalone runs the full pipeline in-memory for a single query.
// No PG/Redis required — results go directly to CSV.
func RunStandalone(cfg *config.Config, query, output string, workers int) error {
	if query == "" {
		return fmt.Errorf("standalone: -q is required")
	}

	if workers > 0 {
		cfg.Contact.Workers = workers
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nshutting down gracefully...")
		cancel()
	}()

	runner := standalone.New(cfg, query, output)
	return runner.Run(ctx)
}
