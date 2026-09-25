package stage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/lib/pq"
)

// ReconcileResult holds the counts of queries affected by a reconciliation pass.
type ReconcileResult struct {
	Completed int
	Failed    int
	Requeued  int
	Active    int
}

// ParseRetiredEngines splits a SERP_RETIRED_ENGINES value ("google,bing")
// into a normalized name list. Untagged pure logic, shared by the stage and
// tested without a database.
func ParseRetiredEngines(csv string) []string {
	var out []string
	for _, name := range strings.Split(csv, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

// ReconcileProcessingQueries resolves a batch of up to 500 'processing' queries:
//   - Marks jobs for globally retired engines as 'dead'. A host's SERP_ENGINES
//     only says what that host can process, so engines that are merely absent
//     from this host's set (served by another host against the same tables)
//     are never touched here.
//   - Marks queries 'completed' / 'failed' only when all jobs of every
//     non-retired engine are terminal (an engine active on one host counts as
//     active everywhere — reconciles run on all hosts).
//   - Marks queries 'pending' (requeued) if they have 0 serp_jobs (true zombies).
//   - Touches updated_at on active queries so the queue scans forward.
func ReconcileProcessingQueries(ctx context.Context, db *sql.DB, retiredEngines []string) (ReconcileResult, error) {
	if len(retiredEngines) == 0 {
		retiredEngines = []string{"google"}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("serp: reconcile tx begin: %w", err)
	}
	defer tx.Rollback()

	// 5-second statement timeout per Operational Invariant #2
	if _, err := tx.ExecContext(ctx, `SET LOCAL statement_timeout = '5000'`); err != nil {
		_ = err
	}

	// 1. Select bounded batch of processing queries ordered by updated_at ASC.
	// Uses idx_queries_processing_updated.
	//
	// FOR UPDATE SKIP LOCKED (same idiom as the feeder's job claim) is what keeps
	// the hachibi and kurawa serp containers from fighting over the same rows:
	// every reconcile runs in every container on both hosts, and without it both
	// pick the same "500 oldest processing" batch and then contend on the
	// trailing UPDATEs, which showed up in prod as
	// `serp: advance active queries: canceling statement due to statement
	// timeout (57014)`. With the lock held until COMMIT, a concurrent caller
	// skips the locked rows and picks the next 500 instead, so the two hosts
	// work on disjoint batches and still both make progress.
	//
	// A transaction-scoped advisory lock was the alternative, but it serializes
	// the pass: the loser returns having done nothing, wasting a host's tick.
	// SKIP LOCKED gets the same contention-free behavior while keeping both
	// hosts useful.
	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM queries
		WHERE status = 'processing'
		ORDER BY updated_at ASC
		LIMIT 500
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("serp: query processing batch: %w", err)
	}
	defer rows.Close()

	var queryIDs []int64
	for rows.Next() {
		var qid int64
		if err := rows.Scan(&qid); err == nil {
			queryIDs = append(queryIDs, qid)
		}
	}
	rows.Close()

	if len(queryIDs) == 0 {
		return ReconcileResult{}, nil
	}

	pqQueryIDs := pq.Array(queryIDs)
	pqRetired := pq.Array(retiredEngines)

	// 2. Retire only globally-retired engines. Jobs of engines that simply run
	// on another host (bing on kurawa, searxng on hachibi) must never be
	// touched by a host that cannot itself run them.
	if deadRes, err := tx.ExecContext(ctx, `
		UPDATE serp_jobs SET status = 'dead', error_msg = 'engine retired globally (SERP_RETIRED_ENGINES)', updated_at = NOW()
		WHERE parent_job_id = ANY($1)
		  AND engine = ANY($2)
		  AND status IN ('new', 'processing')
	`, pqQueryIDs, pqRetired); err == nil {
		if n, _ := deadRes.RowsAffected(); n > 0 {
			slog.Info("serp: reconciler retired retired-engine jobs to dead", "count", n)
		}
	}

	// 3. Summarize serp_jobs for this batch of queries.
	type jobStats struct {
		totalJobs    int
		activeJobs   int
		totalResults int
	}
	stats := make(map[int64]*jobStats, len(queryIDs))

	jobRows, err := tx.QueryContext(ctx, `
		SELECT
			s.parent_job_id,
			COUNT(s.id) AS total_jobs,
			COUNT(s.id) FILTER (WHERE s.status IN ('new', 'processing') AND NOT (s.engine = ANY($2))) AS active_jobs,
			COALESCE(SUM(s.result_count), 0) AS total_results
		FROM serp_jobs s
		WHERE s.parent_job_id = ANY($1)
		GROUP BY s.parent_job_id
	`, pqQueryIDs, pqRetired)
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("serp: query job stats: %w", err)
	}
	defer jobRows.Close()

	for jobRows.Next() {
		var parentID int64
		var total, active, results int
		if err := jobRows.Scan(&parentID, &total, &active, &results); err == nil {
			stats[parentID] = &jobStats{
				totalJobs:    total,
				activeJobs:   active,
				totalResults: results,
			}
		}
	}
	jobRows.Close()

	var completedIDs []int64
	var completedResults []int
	var failedIDs []int64
	var zombieIDs []int64
	var activeIDs []int64

	for _, id := range queryIDs {
		st := stats[id]
		if st == nil || st.totalJobs == 0 {
			zombieIDs = append(zombieIDs, id)
		} else if st.activeJobs > 0 {
			activeIDs = append(activeIDs, id)
		} else if st.totalResults > 0 {
			completedIDs = append(completedIDs, id)
			completedResults = append(completedResults, st.totalResults)
		} else {
			failedIDs = append(failedIDs, id)
		}
	}

	// 4. Batch updates.
	if len(zombieIDs) > 0 {
		if res, err := tx.ExecContext(ctx, `
			UPDATE queries SET status = 'pending', updated_at = NOW()
			WHERE id = ANY($1)
		`, pq.Array(zombieIDs)); err != nil {
			return ReconcileResult{}, fmt.Errorf("serp: requeue zombies: %w", err)
		} else if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("serp: reconciler requeued zombie queries", "count", n)
		}
	}

	if len(completedIDs) > 0 {
		if res, err := tx.ExecContext(ctx, `
			UPDATE queries SET
				status = 'completed',
				result_count = v.results,
				error_msg = NULL,
				updated_at = NOW()
			FROM (
				SELECT UNNEST($1::bigint[]) AS id, UNNEST($2::int[]) AS results
			) v
			WHERE queries.id = v.id
		`, pq.Array(completedIDs), pq.Array(completedResults)); err != nil {
			return ReconcileResult{}, fmt.Errorf("serp: complete queries: %w", err)
		} else if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("serp: reconciler marked queries completed", "count", n)
		}
	}

	if len(failedIDs) > 0 {
		if res, err := tx.ExecContext(ctx, `
			UPDATE queries SET
				status = 'failed',
				result_count = 0,
				error_msg = 'all serp jobs failed/dead with 0 results',
				updated_at = NOW()
			WHERE id = ANY($1)
		`, pq.Array(failedIDs)); err != nil {
			return ReconcileResult{}, fmt.Errorf("serp: fail queries: %w", err)
		} else if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("serp: reconciler marked queries failed", "count", n)
		}
	}

	if len(activeIDs) > 0 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE queries SET updated_at = NOW()
			WHERE id = ANY($1)
		`, pq.Array(activeIDs)); err != nil {
			return ReconcileResult{}, fmt.Errorf("serp: advance active queries: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return ReconcileResult{}, fmt.Errorf("serp: commit reconcile: %w", err)
	}

	return ReconcileResult{
		Completed: len(completedIDs),
		Failed:    len(failedIDs),
		Requeued:  len(zombieIDs),
		Active:    len(activeIDs),
	}, nil
}
