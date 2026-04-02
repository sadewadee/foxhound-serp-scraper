package feeder

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	EnrichBufferKey    = "enrich:buffer"
	EnrichBufferMaxLen = 200
)

// EnrichBufferItem is the JSON payload pushed to enrich:buffer.
type EnrichBufferItem struct {
	ID            string `json:"id"`
	URL           string `json:"url"`
	URLHash       string `json:"url_hash"`
	Domain        string `json:"domain"`
	ParentQueryID int64  `json:"parent_query_id"`
	Source        string `json:"source"`
}

// EnrichFeeder polls enrichment_jobs from DB and fills the enrich:buffer Redis LIST.
type EnrichFeeder struct {
	db    *sql.DB
	redis *redis.Client
}

// NewEnrichFeeder creates a new enrich feeder.
func NewEnrichFeeder(db *sql.DB, redisClient *redis.Client) *EnrichFeeder {
	return &EnrichFeeder{db: db, redis: redisClient}
}

// Run starts the enrich feeder loop. Blocks until ctx is cancelled.
func (f *EnrichFeeder) Run(ctx context.Context) {
	slog.Info("enrich-feeder: starting")

	// Start stale-pick reconciler in background.
	go f.reconcileStale(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check buffer depth.
		bufLen, err := f.redis.LLen(ctx, EnrichBufferKey).Result()
		if err != nil {
			slog.Warn("enrich-feeder: LLEN failed", "error", err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}
		if bufLen >= EnrichBufferMaxLen {
			sleepCtx(ctx, 1*time.Second)
			continue
		}

		// Atomically claim unclaimed jobs from DB.
		rows, err := f.db.QueryContext(ctx, `
			UPDATE enrichment_jobs SET picked_at = NOW()
			WHERE id IN (
				SELECT id FROM enrichment_jobs
				WHERE status = 'pending' AND picked_at IS NULL
				ORDER BY created_at
				LIMIT 50
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id, url, url_hash, domain, COALESCE(parent_query_id, 0), COALESCE(source, 'serp_result')
		`)
		if err != nil {
			slog.Warn("enrich-feeder: query failed", "error", err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}

		var items []EnrichBufferItem
		for rows.Next() {
			var item EnrichBufferItem
			if err := rows.Scan(&item.ID, &item.URL, &item.URLHash, &item.Domain, &item.ParentQueryID, &item.Source); err != nil {
				continue
			}
			items = append(items, item)
		}
		rows.Close()

		if len(items) == 0 {
			sleepCtx(ctx, 2*time.Second)
			continue
		}

		// Push to buffer.
		for _, item := range items {
			data, _ := json.Marshal(item)
			f.redis.RPush(ctx, EnrichBufferKey, string(data))
		}

		slog.Debug("enrich-feeder: pushed to buffer", "count", len(items))
	}
}

// reconcileStale resets picked_at for jobs that were picked but never processed.
func (f *EnrichFeeder) reconcileStale(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		res, err := f.db.ExecContext(ctx, `
			UPDATE enrichment_jobs SET picked_at = NULL
			WHERE status = 'pending' AND picked_at IS NOT NULL AND picked_at < NOW() - INTERVAL '5 minutes'
		`)
		if err == nil {
			if n, _ := res.RowsAffected(); n > 0 {
				slog.Info("enrich-feeder: reset stale picked jobs", "count", n)
			}
		}
	}
}
