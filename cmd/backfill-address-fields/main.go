//go:build playwright

// Command backfill-address-fields sweeps business_listings rows whose country
// and/or city columns are NULL but whose address column contains parseable
// data, and fills them via scraper.ParseAddressFallback. Pure offline — no
// network calls.
//
// Use case: sprint 2 added a per-row offline pre-pass inside the reenrich
// worker, which fixes rows organically as workers claim them. But rows
// already above the reenrich score threshold (sufficiently complete that
// they never get claimed) stay stuck with NULL geo. This binary sweeps
// them in one pass.
//
// Usage:
//
//	./backfill-address-fields                                 # dry-run, full table
//	./backfill-address-fields -dry-run=false                  # live, full table
//	./backfill-address-fields -batch 500 -max 20 -sleep-ms 200
//	./backfill-address-fields -config path/to/config.yaml -dry-run=false
//
// Safe to run alongside live reenrich workers — UPDATE uses COALESCE so it
// will never overwrite a non-NULL existing column.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/scraper"
)

type backfillRow struct {
	ID      int64
	Address string
	Country sql.NullString
	City    sql.NullString
}

type backfillUpdate struct {
	ID      int64
	Country sql.NullString
	City    sql.NullString
}

type backfiller struct {
	db     *sql.DB
	dryRun bool

	// running totals
	totalScanned int64
	totalUpdated int64
	totalCountry int64
	totalCity    int64
}

func main() {
	dryRun := flag.Bool("dry-run", true, "preview without writing (default true — explicit -dry-run=false to mutate)")
	batchSize := flag.Int("batch", 1000, "rows per scan + update batch")
	maxBatches := flag.Int("max", 0, "max batches (0 = unlimited)")
	sleepMs := flag.Int("sleep-ms", 100, "sleep between batches (ms)")
	configPath := flag.String("config", "config.yaml", "config file path")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("backfill: config load failed", "error", err, "path", *configPath)
		os.Exit(1)
	}

	database, err := db.Open(cfg)
	if err != nil {
		slog.Error("backfill: db open failed", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("backfill: signal received, finishing current batch then stopping", "signal", sig)
		cancel()
	}()

	b := &backfiller{db: database, dryRun: *dryRun}
	mode := "DRY-RUN"
	if !*dryRun {
		mode = "LIVE"
	}
	slog.Info("backfill: starting",
		"mode", mode,
		"batch_size", *batchSize,
		"max_batches", *maxBatches,
		"sleep_ms", *sleepMs)

	b.run(ctx, *batchSize, *maxBatches, time.Duration(*sleepMs)*time.Millisecond)
}

func (b *backfiller) run(ctx context.Context, batchSize, maxBatches int, sleep time.Duration) {
	var lastID int64

	for batch := 0; ; batch++ {
		if ctx.Err() != nil {
			slog.Info("backfill: cancelled by signal")
			break
		}
		if maxBatches > 0 && batch >= maxBatches {
			slog.Info("backfill: max-batches limit reached", "batch", batch)
			break
		}

		rows, err := b.fetchBatch(ctx, lastID, batchSize)
		if err != nil {
			slog.Error("backfill: fetch batch failed", "batch", batch, "error", err)
			return
		}
		if len(rows) == 0 {
			slog.Info("backfill: no more candidate rows", "scanned_total", b.totalScanned)
			break
		}

		var updates []backfillUpdate
		for _, r := range rows {
			b.totalScanned++
			lastID = r.ID

			u := computeUpdate(r)
			if u.Country.Valid || u.City.Valid {
				updates = append(updates, u)
				if u.Country.Valid {
					b.totalCountry++
				}
				if u.City.Valid {
					b.totalCity++
				}
			}
		}

		if b.dryRun {
			slog.Info("backfill: dry-run batch",
				"batch", batch,
				"scanned", len(rows),
				"would_update", len(updates),
				"running_country", b.totalCountry,
				"running_city", b.totalCity)
		} else {
			n, err := b.bulkUpdate(ctx, updates)
			if err != nil {
				slog.Warn("backfill: bulk update failed — skipping batch", "batch", batch, "error", err)
			} else {
				b.totalUpdated += n
				slog.Info("backfill: live batch applied",
					"batch", batch,
					"scanned", len(rows),
					"updated", n,
					"running_total", b.totalUpdated)
			}
		}

		if sleep > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(sleep):
			}
		}
	}

	mode := "dry-run"
	if !b.dryRun {
		mode = "live"
	}
	slog.Info("backfill: complete",
		"mode", mode,
		"scanned", b.totalScanned,
		"updated", b.totalUpdated,
		"country_filled", b.totalCountry,
		"city_filled", b.totalCity)
}

// fetchBatch returns the next page of candidate rows in ID order. ID-based
// pagination (lastID > prev) avoids the seek-position drift that OFFSET
// suffers on a growing table.
func (b *backfiller) fetchBatch(ctx context.Context, lastID int64, limit int) ([]backfillRow, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	const q = `
		SELECT id, address, country, city
		FROM business_listings
		WHERE id > $1
		  AND address IS NOT NULL AND address != ''
		  AND (country IS NULL OR country = '' OR city IS NULL OR city = '')
		ORDER BY id
		LIMIT $2
	`
	dbRows, err := b.db.QueryContext(queryCtx, q, lastID, limit)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	var out []backfillRow
	for dbRows.Next() {
		var r backfillRow
		if err := dbRows.Scan(&r.ID, &r.Address, &r.Country, &r.City); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, dbRows.Err()
}

// bulkUpdate applies up to len(updates) rows in a single VALUES-based UPDATE
// statement. COALESCE preserves any non-NULL existing column value so the
// migration is idempotent and concurrent-write safe with the reenrich worker.
// 30s statement_timeout caps lock duration on the 478K-row table.
func (b *backfiller) bulkUpdate(ctx context.Context, updates []backfillUpdate) (int64, error) {
	if len(updates) == 0 {
		return 0, nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	tx, err := b.db.BeginTx(queryCtx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(queryCtx, `SET LOCAL statement_timeout = '30000'`); err != nil {
		return 0, err
	}

	// Build VALUES list. Use 3 params per row: id, country, city.
	var sb strings.Builder
	sb.WriteString(`
		UPDATE business_listings bl
		SET country = COALESCE(bl.country, v.new_country),
		    city    = COALESCE(bl.city,    v.new_city),
		    updated_at = NOW()
		FROM (VALUES `)

	args := make([]any, 0, len(updates)*3)
	for i, u := range updates {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i*3 + 1
		fmt.Fprintf(&sb, "($%d::bigint, $%d::text, $%d::text)", base, base+1, base+2)
		args = append(args, u.ID, u.Country, u.City)
	}
	sb.WriteString(`) AS v(id, new_country, new_city)
		WHERE bl.id = v.id
		  AND (bl.country IS NULL OR bl.country = '' OR bl.city IS NULL OR bl.city = '')
	`)

	res, err := tx.ExecContext(queryCtx, sb.String(), args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// computeUpdate is the pure decision: given a row, what should be written?
// Extracted as a free function so it can be unit-tested without a DB.
func computeUpdate(r backfillRow) backfillUpdate {
	parsedCountry, parsedCity := scraper.ParseAddressFallback(r.Address)
	u := backfillUpdate{ID: r.ID}
	if (!r.Country.Valid || r.Country.String == "") && parsedCountry != "" {
		u.Country = sql.NullString{String: parsedCountry, Valid: true}
	}
	if (!r.City.Valid || r.City.String == "") && parsedCity != "" {
		u.City = sql.NullString{String: parsedCity, Valid: true}
	}
	return u
}
