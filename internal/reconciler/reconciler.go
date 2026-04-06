// Package reconciler provides a unified project-level reconciler that runs
// in the manager container. It monitors all pipeline stages, heals zombie jobs,
// manages throughput balance, and provides project health metrics.
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
	SerpDead       int

	// Enrich jobs by status
	EnrichPending    int
	EnrichProcessing int
	EnrichCompleted  int
	EnrichFailed     int
	EnrichDead       int

	// Redis queues
	QueueQueries   int64
	QueueSerpBuf   int64
	QueueEnrichBuf int64

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

// GetSnapshot returns the latest pipeline snapshot (thread-safe).
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

	// Store snapshot.
	r.mu.Lock()
	r.prev = r.snapshot
	r.snapshot = snap
	r.mu.Unlock()

	// Run healing actions.
	r.healZombieQueries(ctx, snap)
	r.healStaleSerp(ctx, snap)
	r.markStaleWorkers()

	// Log health summary.
	slog.Info("project-reconciler: tick",
		"queries", fmt.Sprintf("%d pending / %d processing / %d done", snap.QueriesPending, snap.QueriesProcessing, snap.QueriesCompleted),
		"serp", fmt.Sprintf("%d new / %d done / %d failed / %d dead", snap.SerpNew, snap.SerpCompleted, snap.SerpFailed, snap.SerpDead),
		"enrich", fmt.Sprintf("%d pending / %d done / %d dead", snap.EnrichPending, snap.EnrichCompleted, snap.EnrichDead),
		"queues", fmt.Sprintf("q:%d sb:%d eb:%d", snap.QueueQueries, snap.QueueSerpBuf, snap.QueueEnrichBuf),
		"emails", snap.EmailsTotal,
		"redis_mb", fmt.Sprintf("%.0f", snap.RedisUsedMB),
		"rate", fmt.Sprintf("serp:%d/h enrich:%d/h emails:%d/h", snap.SerpPerHour, snap.EnrichPerHour, snap.EmailsPerHour),
	)
}

// collectSnapshot gathers metrics from DB and Redis.
func (r *ProjectReconciler) collectSnapshot(ctx context.Context) Snapshot {
	snap := Snapshot{Taken: time.Now()}

	// Queries.
	r.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'error')
		FROM queries
	`).Scan(&snap.QueriesPending, &snap.QueriesProcessing, &snap.QueriesCompleted, &snap.QueriesError)

	// SERP jobs.
	r.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'new'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'dead')
		FROM serp_jobs
	`).Scan(&snap.SerpNew, &snap.SerpProcessing, &snap.SerpCompleted, &snap.SerpFailed, &snap.SerpDead)

	// Enrichment jobs.
	r.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'dead')
		FROM enrichment_jobs
	`).Scan(&snap.EnrichPending, &snap.EnrichProcessing, &snap.EnrichCompleted, &snap.EnrichFailed, &snap.EnrichDead)

	// Contacts from normalized tables.
	r.db.QueryRow(`SELECT COUNT(*) FROM emails`).Scan(&snap.EmailsTotal)
	r.db.QueryRow(`SELECT COUNT(*) FROM business_listings WHERE phone IS NOT NULL AND phone != ''`).Scan(&snap.PhonesTotal)

	// Throughput rates from DB timestamps (5-min window * 12 = hourly projection).
	r.db.QueryRow(`SELECT COUNT(*) * 12 FROM emails WHERE created_at > NOW() - INTERVAL '5 minutes'`).Scan(&snap.EmailsPerHour)
	r.db.QueryRow(`SELECT COUNT(*) * 12 FROM serp_results WHERE created_at > NOW() - INTERVAL '5 minutes'`).Scan(&snap.SerpPerHour)
	r.db.QueryRow(`SELECT COUNT(*) * 12 FROM enrichment_jobs WHERE completed_at > NOW() - INTERVAL '5 minutes' AND status='completed'`).Scan(&snap.EnrichPerHour)

	// Redis queues.
	snap.QueueQueries, _ = r.redis.ZCard(ctx, "serp:queue:queries").Result()
	snap.QueueSerpBuf, _ = r.redis.LLen(ctx, "serp:buffer").Result()
	snap.QueueEnrichBuf, _ = r.redis.LLen(ctx, "enrich:buffer").Result()

	// Redis memory.
	info, _ := r.redis.Info(ctx, "memory").Result()
	fmt.Sscanf(extractRedisField(info, "used_memory_human"), "%fM", &snap.RedisUsedMB)

	return snap
}

// healZombieQueries resets processing queries that have no active serp_jobs.
func (r *ProjectReconciler) healZombieQueries(ctx context.Context, snap Snapshot) {
	if snap.QueriesProcessing < 100 {
		return
	}

	limit := 1000

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
			r.requeuePendingQueries(ctx, int(n))
		}
	}
}

// healStaleSerp marks exhausted failed serp_jobs as dead, then resets
// remaining failed jobs that still have retries left.
func (r *ProjectReconciler) healStaleSerp(ctx context.Context, snap Snapshot) {
	if snap.SerpFailed < 100 {
		return
	}

	// First: permanently retire jobs that have been retried too many times.
	// This prevents the infinite retry loop where attempt_count was reset to 0.
	deadRes, deadErr := r.db.Exec(`
		UPDATE serp_jobs SET status = 'dead', updated_at = NOW()
		WHERE status = 'failed'
		  AND attempt_count >= 10
		  AND updated_at < NOW() - INTERVAL '1 hour'
	`)
	if deadErr == nil {
		if n, _ := deadRes.RowsAffected(); n > 0 {
			slog.Info("project-reconciler: retired exhausted serp_jobs to dead", "count", n)
		}
	}

	// Then: resurrect jobs that still have retries left.
	// Increment attempt_count (never reset to 0) so they eventually reach the cap.
	limit := 500
	res, err := r.db.Exec(fmt.Sprintf(`
		UPDATE serp_jobs SET status = 'new', attempt_count = attempt_count + 1, error_msg = '',
			locked_by = NULL, next_attempt_at = NULL, picked_at = NULL, updated_at = NOW()
		WHERE id IN (
			SELECT id FROM serp_jobs
			WHERE status = 'failed'
			  AND attempt_count < 10
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

// markStaleWorkers flags workers whose heartbeat is older than 2 minutes as dead.
func (r *ProjectReconciler) markStaleWorkers() {
	res, err := r.db.Exec(`UPDATE workers SET status = 'dead' WHERE status != 'dead' AND last_heartbeat < NOW() - INTERVAL '2 minutes'`)
	if err == nil {
		if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("project-reconciler: marked stale workers as dead", "count", n)
		}
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
