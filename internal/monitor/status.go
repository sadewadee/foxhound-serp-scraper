package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// Status holds pipeline status information.
type Status struct {
	Queries TableStatus   `json:"queries"`
	Seeds   TableStatus   `json:"seeds"`
	Enrich  TableStatus   `json:"enrich"`
	Queues  []QueueStatus `json:"queues"`
}

// TableStatus shows counts per status for a table.
type TableStatus struct {
	Total    int            `json:"total"`
	ByStatus map[string]int `json:"by_status"`
}

// QueueStatus shows the depth of a Redis queue.
type QueueStatus struct {
	Name  string `json:"name"`
	Depth int64  `json:"depth"`
}

// GetStatus collects current pipeline status from PG and Redis.
func GetStatus(db *sql.DB, redisClient *redis.Client) (*Status, error) {
	ctx := context.Background()
	s := &Status{}

	// Query counts by status.
	s.Queries = getTableStatus(db, "queries")
	s.Seeds = getTableStatus(db, "serp_jobs")
	s.Enrich = getTableStatus(db, "enrichment_jobs")

	// Queue depths (Redis LIST buffers).
	buffers := []string{"serp:queue:queries", "serp:buffer", "enrich:buffer"}
	for _, q := range buffers {
		var depth int64
		if q == "serp:queue:queries" {
			depth, _ = redisClient.ZCard(ctx, q).Result()
		} else {
			depth, _ = redisClient.LLen(ctx, q).Result()
		}
		s.Queues = append(s.Queues, QueueStatus{Name: q, Depth: depth})
	}

	return s, nil
}

func getTableStatus(db *sql.DB, table string) TableStatus {
	ts := TableStatus{ByStatus: make(map[string]int)}

	// All COUNT(*) queries run inside a short-lived read-only transaction whose
	// statement_timeout is capped at 5 s (Invariant #2: no unbounded COUNT on
	// large tables such as serp_jobs/enrichment_jobs which exceed 1 M rows).
	// On any error we return the zero/partial value gracefully rather than
	// blocking the status endpoint.
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		slog.Warn("getTableStatus: could not begin tx", "table", table, "err", err)
		return ts
	}
	defer tx.Rollback() //nolint:errcheck // read-only; rollback is always safe

	if _, err = tx.ExecContext(ctx, "SET LOCAL statement_timeout = '5000'"); err != nil {
		slog.Warn("getTableStatus: could not set statement_timeout", "table", table, "err", err)
		return ts
	}

	// Total count — bounded by the 5 s timeout set above.
	row := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if err = row.Scan(&ts.Total); err != nil {
		slog.Warn("getTableStatus: total count failed", "table", table, "err", err)
		return ts
	}

	// Count by status — also bounded by the same timeout.
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT status, COUNT(*) FROM %s GROUP BY status", table))
	if err != nil {
		slog.Warn("getTableStatus: by-status count failed", "table", table, "err", err)
		return ts
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if rows.Scan(&status, &count) == nil {
			ts.ByStatus[status] = count
		}
	}
	return ts
}
