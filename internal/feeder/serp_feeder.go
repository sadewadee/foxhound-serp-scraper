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
	SERPBufferKey    = "serp:buffer"
	SERPBufferMaxLen = 200
)

// SERPBufferItem is the JSON payload pushed to serp:buffer.
type SERPBufferItem struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	QueryID int64  `json:"query_id"`
	PageNum int    `json:"page_num"`
	Engine  string `json:"engine"`
}

// SERPFeeder polls serp_jobs from DB and fills the serp:buffer Redis LIST.
type SERPFeeder struct {
	db      *sql.DB
	redis   *redis.Client
	engines []string
}

// NewSERPFeeder creates a new SERP feeder. engines is the list of engine names
// to round-robin claim from serp_jobs — must match SERP_ENGINES config so we
// don't pick up jobs for disabled engines (e.g. legacy Google rows).
func NewSERPFeeder(db *sql.DB, redisClient *redis.Client, engines []string) *SERPFeeder {
	if len(engines) == 0 {
		engines = []string{"google", "bing", "duckduckgo"}
	}
	return &SERPFeeder{db: db, redis: redisClient, engines: engines}
}

// Run starts the SERP feeder loop. Blocks until ctx is cancelled.
func (f *SERPFeeder) Run(ctx context.Context) {
	slog.Info("serp-feeder: starting", "engines", f.engines)

	// Start stale-pick reconciler in background.
	go f.reconcileStale(ctx)

	engineIdx := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check buffer depth.
		bufLen, err := f.redis.LLen(ctx, SERPBufferKey).Result()
		if err != nil {
			slog.Warn("serp-feeder: LLEN failed", "error", err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}
		if bufLen >= SERPBufferMaxLen {
			sleepCtx(ctx, 1*time.Second)
			continue
		}

		// Round-robin: pick from one engine at a time.
		engine := f.engines[engineIdx%len(f.engines)]
		engineIdx++

		items := f.claimJobs(ctx, engine)
		if len(items) == 0 {
			// If all engines return 0 in a row, sleep briefly.
			if engineIdx%len(f.engines) == 0 {
				sleepCtx(ctx, 2*time.Second)
			}
			continue
		}

		// Push to buffer.
		for _, item := range items {
			data, _ := json.Marshal(item)
			f.redis.RPush(ctx, SERPBufferKey, string(data))
		}

		slog.Info("serp-feeder: pushed to buffer", "engine", engine, "count", len(items))
	}
}

// claimJobs atomically claims up to 20 unclaimed jobs for a specific engine.
func (f *SERPFeeder) claimJobs(ctx context.Context, engine string) []SERPBufferItem {
	rows, err := f.db.QueryContext(ctx, `
		UPDATE serp_jobs SET status = 'processing', locked_at = NOW(), picked_at = NOW()
		WHERE id IN (
			SELECT id FROM serp_jobs
			WHERE status = 'new'
			  AND engine = $1
			  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
			ORDER BY priority DESC, created_at
			LIMIT 20
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, search_url, parent_job_id, page_num, COALESCE(engine, 'google')
	`, engine)
	if err != nil {
		slog.Warn("serp-feeder: query failed", "engine", engine, "error", err)
		return nil
	}
	defer rows.Close()

	var items []SERPBufferItem
	for rows.Next() {
		var item SERPBufferItem
		if err := rows.Scan(&item.ID, &item.URL, &item.QueryID, &item.PageNum, &item.Engine); err != nil {
			continue
		}
		items = append(items, item)
	}
	return items
}

// reconcileStale resets picked_at for jobs that were picked but never processed.
func (f *SERPFeeder) reconcileStale(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		res, err := f.db.ExecContext(ctx, `
			UPDATE serp_jobs SET picked_at = NULL
			WHERE status = 'new' AND picked_at IS NOT NULL AND picked_at < NOW() - INTERVAL '5 minutes'
		`)
		if err == nil {
			if n, _ := res.RowsAffected(); n > 0 {
				slog.Info("serp-feeder: reset stale picked jobs", "count", n)
			}
		}
	}
}

// sleepCtx sleeps for the given duration or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
