// Package reconciler provides a unified project-level reconciler that runs
// in the manager container. It monitors all pipeline stages, heals zombie jobs,
// manages throughput balance, and provides project health metrics.
//
// The per-stage reconcilers (in serp.go / enrich.go) handle fast, local recovery.
// This reconciler handles cross-stage coordination and project-level decisions.
package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Snapshot is a point-in-time view of the entire pipeline.
type Snapshot struct {
	Taken time.Time

	// Queries
	QueriesPending    int
	QueriesProcessing int
	QueriesCompleted  int
	QueriesError      int

	// SERP jobs by status
	SerpNew        int
	SerpProcessing int
	SerpCompleted  int
	SerpFailed     int

	// Enrich jobs by status
	EnrichPending    int
	EnrichProcessing int
	EnrichCompleted  int
	EnrichFailed     int
	EnrichDead       int

	// Websites
	WebsitesPending int
	WebsitesTotal   int

	// Redis queues
	QueueQueries int64
	QueueSerp    int64
	QueueEnrich  int64

	// Redis memory
	RedisUsedMB float64

	// Contacts found
	EmailsTotal int
	PhonesTotal int

	// Throughput (per hour, computed from prev snapshot)
	SerpPerHour   int
	EnrichPerHour int
	EmailsPerHour int
}

// ProjectReconciler coordinates the full pipeline from the manager container.
type ProjectReconciler struct {
	db    *sql.DB
	redis *redis.Client

	mu       sync.RWMutex
	snapshot Snapshot
	prev     Snapshot
}

// New creates a ProjectReconciler.
func New(db *sql.DB, redisClient *redis.Client) *ProjectReconciler {
	return &ProjectReconciler{db: db, redis: redisClient}
}

// Snapshot returns the latest pipeline snapshot (thread-safe).
func (r *ProjectReconciler) GetSnapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshot
}

// Run starts the reconciler loop. Blocks until ctx is cancelled.
func (r *ProjectReconciler) Run(ctx context.Context) {
	slog.Info("project-reconciler: starting", "interval", "60s")

	// Take initial snapshot.
	r.tick(ctx)

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("project-reconciler: stopping")
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *ProjectReconciler) tick(ctx context.Context) {
	snap := r.collectSnapshot(ctx)

	// Compute throughput from previous snapshot.
	r.mu.RLock()
	prev := r.snapshot
	r.mu.RUnlock()

	if !prev.Taken.IsZero() {
		elapsed := snap.Taken.Sub(prev.Taken).Hours()
		if elapsed > 0 {
			snap.SerpPerHour = int(float64(snap.SerpCompleted-prev.SerpCompleted) / elapsed)
			snap.EnrichPerHour = int(float64(snap.EnrichCompleted-prev.EnrichCompleted) / elapsed)
			snap.EmailsPerHour = int(float64(snap.EmailsTotal-prev.EmailsTotal) / elapsed)
		}
	}

	// Store snapshot.
	r.mu.Lock()
	r.prev = prev
	r.snapshot = snap
	r.mu.Unlock()

	// Run healing actions.
	r.healZombieQueries(ctx, snap)
	r.healStaleSerp(ctx, snap)
	r.healEnrichBacklog(ctx, snap)

	// Log health summary.
	slog.Info("project-reconciler: tick",
		"queries", fmt.Sprintf("%d pending / %d processing / %d done", snap.QueriesPending, snap.QueriesProcessing, snap.QueriesCompleted),
		"serp", fmt.Sprintf("%d new / %d done / %d failed", snap.SerpNew, snap.SerpCompleted, snap.SerpFailed),
		"enrich", fmt.Sprintf("%d pending / %d done / %d dead", snap.EnrichPending, snap.EnrichCompleted, snap.EnrichDead),
		"queues", fmt.Sprintf("q:%d s:%d e:%d", snap.QueueQueries, snap.QueueSerp, snap.QueueEnrich),
		"emails", snap.EmailsTotal,
		"redis_mb", fmt.Sprintf("%.0f", snap.RedisUsedMB),
		"rate", fmt.Sprintf("serp:%d/h enrich:%d/h emails:%d/h", snap.SerpPerHour, snap.EnrichPerHour, snap.EmailsPerHour),
	)
}

// collectSnapshot gathers metrics from DB and Redis.
// All queries use indexed columns and small LIMITs where needed.
func (r *ProjectReconciler) collectSnapshot(ctx context.Context) Snapshot {
	snap := Snapshot{Taken: time.Now()}

	// Queries — use index on status column.
	r.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'error')
		FROM queries
	`).Scan(&snap.QueriesPending, &snap.QueriesProcessing, &snap.QueriesCompleted, &snap.QueriesError)

	// SERP jobs — use idx_serp_jobs_status.
	r.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'new'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed')
		FROM serp_jobs
	`).Scan(&snap.SerpNew, &snap.SerpProcessing, &snap.SerpCompleted, &snap.SerpFailed)

	// Enrich jobs — use idx_enrich_status_domain.
	r.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'dead')
		FROM enrich_jobs
	`).Scan(&snap.EnrichPending, &snap.EnrichProcessing, &snap.EnrichCompleted, &snap.EnrichFailed, &snap.EnrichDead)

	// Websites pending.
	r.db.QueryRow(`SELECT COUNT(*) FROM websites WHERE status = 'pending'`).Scan(&snap.WebsitesPending)
	r.db.QueryRow(`SELECT COUNT(*) FROM websites`).Scan(&snap.WebsitesTotal)

	// Contacts.
	r.db.QueryRow(`
		SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'
	`).Scan(&snap.EmailsTotal)
	r.db.QueryRow(`
		SELECT COUNT(DISTINCT p) FROM enrich_jobs, unnest(phones) AS p WHERE status = 'completed'
	`).Scan(&snap.PhonesTotal)

	// Redis queues.
	snap.QueueQueries, _ = r.redis.ZCard(ctx, "serp:queue:queries").Result()
	snap.QueueSerp, _ = r.redis.ZCard(ctx, "serp:queue:serp").Result()
	snap.QueueEnrich, _ = r.redis.ZCard(ctx, "serp:queue:enrich").Result()

	// Redis memory.
	info, _ := r.redis.Info(ctx, "memory").Result()
	fmt.Sscanf(extractRedisField(info, "used_memory_human"), "%fM", &snap.RedisUsedMB)

	return snap
}

// healZombieQueries resets processing queries that have no active serp_jobs.
// This is the project-level version — the per-stage reconciler also does this
// but with smaller batches. The project reconciler handles the bulk.
func (r *ProjectReconciler) healZombieQueries(ctx context.Context, snap Snapshot) {
	if snap.QueriesProcessing < 100 {
		return // not enough to worry about
	}

	// Only heal if serp queue has capacity.
	if snap.QueueSerp > 5000 {
		return
	}

	limit := 1000
	if snap.QueueSerp > 2000 {
		limit = 200
	}

	res, err := r.db.Exec(fmt.Sprintf(`
		UPDATE queries SET status = 'pending', updated_at = NOW()
		WHERE id IN (
			SELECT q.id FROM queries q
			WHERE q.status = 'processing'
			  AND q.updated_at < NOW() - INTERVAL '10 minutes'
			  AND NOT EXISTS (
				SELECT 1 FROM serp_jobs s
				WHERE s.parent_job_id = q.id AND s.status IN ('new', 'processing')
			  )
			LIMIT %d
		)
	`, limit))
	if err == nil {
		if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("project-reconciler: healed zombie queries", "count", n)
			// Push healed queries to Redis so queryFeeder picks them up.
			r.requeuePendingQueries(ctx, int(n))
		}
	}
}

// healStaleSerp resets failed serp_jobs that deserve another round.
// Unlike per-stage reconciler which only handles retry-eligible (status=new, attempt>0),
// this handles permanently-failed jobs that should get fresh attempts.
func (r *ProjectReconciler) healStaleSerp(ctx context.Context, snap Snapshot) {
	if snap.SerpFailed < 100 {
		return
	}
	if snap.QueueSerp > 5000 {
		return
	}

	limit := 500
	if snap.QueueSerp > 2000 {
		limit = 100
	}

	// Reset old failed jobs (>1 hour old) for fresh retry.
	// Only those that haven't been retried recently.
	res, err := r.db.Exec(fmt.Sprintf(`
		UPDATE serp_jobs SET status = 'new', attempt_count = 0, error_msg = '',
			locked_by = NULL, next_attempt_at = NULL, updated_at = NOW()
		WHERE id IN (
			SELECT id FROM serp_jobs
			WHERE status = 'failed'
			  AND updated_at < NOW() - INTERVAL '1 hour'
			LIMIT %d
		)
	`, limit))
	if err == nil {
		if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("project-reconciler: resurrected failed serp_jobs", "count", n)
		}
	}
}

// healEnrichBacklog ensures the enrich Redis queue stays fed from DB
// without exceeding the 50K cap.
func (r *ProjectReconciler) healEnrichBacklog(ctx context.Context, snap Snapshot) {
	// If enrich queue is low and there are pending websites not yet in enrich_jobs,
	// the per-stage reconciler handles this. But if enrich queue is empty and
	// there are pending enrich_jobs in DB, we need to push them.
	if snap.QueueEnrich > 10000 || snap.EnrichPending == 0 {
		return
	}

	// Feed pending enrich_jobs into Redis queue (up to cap).
	feedLimit := 50000 - int(snap.QueueEnrich)
	if feedLimit > 5000 {
		feedLimit = 5000 // max 5K per tick to avoid Redis pressure
	}
	if feedLimit <= 0 {
		return
	}

	rows, err := r.db.Query(`
		SELECT id, url, url_hash, parent_job_id FROM enrich_jobs
		WHERE status = 'pending' AND locked_by IS NULL
		ORDER BY created_at
		LIMIT $1
	`, feedLimit)
	if err != nil {
		return
	}
	defer rows.Close()

	type enrichJob struct {
		ID        string         `json:"id"`
		URL       string         `json:"url"`
		Method    string         `json:"method"`
		Priority  int            `json:"priority"`
		CreatedAt time.Time      `json:"created_at"`
		Meta      map[string]any `json:"meta"`
	}

	fed := 0
	pipe := r.redis.Pipeline()
	for rows.Next() {
		var id, pageURL, urlHash string
		var queryID int64
		if err := rows.Scan(&id, &pageURL, &urlHash, &queryID); err == nil {
			short := urlHash
			if len(short) > 8 {
				short = short[:8]
			}
			job := enrichJob{
				ID:        fmt.Sprintf("web-%d-%s", queryID, short),
				URL:       pageURL,
				Method:    "GET",
				Priority:  3,
				CreatedAt: time.Now(),
				Meta:      map[string]any{"query_id": queryID},
			}
			data, _ := json.Marshal(job)
			score := float64(time.Now().UnixMicro())
			pipe.ZAdd(ctx, "serp:queue:enrich", redis.Z{Score: score, Member: string(data)})
			fed++
		}
	}
	if fed > 0 {
		pipe.Exec(ctx)
		slog.Info("project-reconciler: fed enrich queue from DB", "count", fed)
	}
}

// queryMsg matches the format expected by queryFeeder.
type queryMsg struct {
	ID   int64  `json:"id"`
	Text string `json:"text"`
}

// requeuePendingQueries pushes recently-healed pending queries to Redis.
func (r *ProjectReconciler) requeuePendingQueries(ctx context.Context, limit int) {
	rows, err := r.db.Query(`
		SELECT id, text FROM queries
		WHERE status = 'pending'
		ORDER BY updated_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return
	}
	defer rows.Close()

	pipe := r.redis.Pipeline()
	for rows.Next() {
		var q queryMsg
		if err := rows.Scan(&q.ID, &q.Text); err == nil {
			data, _ := json.Marshal(q)
			pipe.ZAdd(ctx, "serp:queue:queries", redis.Z{Score: float64(q.ID), Member: string(data)})
		}
	}
	pipe.Exec(ctx)
}

// extractRedisField extracts a field value from Redis INFO output.
func extractRedisField(info, field string) string {
	for i := 0; i < len(info); i++ {
		if i+len(field) < len(info) && info[i:i+len(field)] == field {
			// Find value after ':'
			j := i + len(field)
			if j < len(info) && info[j] == ':' {
				j++
				end := j
				for end < len(info) && info[end] != '\r' && info[end] != '\n' {
					end++
				}
				return info[j:end]
			}
		}
	}
	return ""
}
