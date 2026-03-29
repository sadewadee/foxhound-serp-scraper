package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
)

// QueueKey is the Redis sorted-set queue for pending queries.
const QueueKey = "serp:queue:queries"

// Repository handles PostgreSQL CRUD for the queries table.
type Repository struct {
	db    *sql.DB
	redis *redis.Client
}

// NewRepository creates a query repository (DB only, no Redis queue push).
func NewRepository(database *sql.DB) *Repository {
	return &Repository{db: database}
}

// NewRepositoryWithRedis creates a query repository with Redis queue support.
func NewRepositoryWithRedis(database *sql.DB, redisClient *redis.Client) *Repository {
	return &Repository{db: database, redis: redisClient}
}

// InsertBatch bulk-inserts queries, skipping duplicates by text_hash.
// If Redis is configured, newly inserted queries are pushed to the query queue.
// Returns the number of new queries inserted.
func (r *Repository) InsertBatch(queries []string) (int, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("query: begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO queries (text, text_hash, status, created_at, updated_at)
		VALUES ($1, $2, 'pending', NOW(), NOW())
		ON CONFLICT (text_hash) DO NOTHING
		RETURNING id, text
	`)
	if err != nil {
		return 0, fmt.Errorf("query: prepare insert: %w", err)
	}
	defer stmt.Close()

	type inserted struct {
		ID   int64
		Text string
	}
	var newQueries []inserted

	for _, q := range queries {
		hash := dedup.HashQuery(q)
		var id int64
		var text string
		err := stmt.QueryRow(q, hash).Scan(&id, &text)
		if err == sql.ErrNoRows {
			continue // duplicate, skipped
		}
		if err != nil {
			return len(newQueries), fmt.Errorf("query: insert %q: %w", q, err)
		}
		newQueries = append(newQueries, inserted{ID: id, Text: text})
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("query: commit: %w", err)
	}

	// Push newly inserted queries to Redis queue for workers to consume.
	if r.redis != nil && len(newQueries) > 0 {
		ctx := context.Background()
		for _, q := range newQueries {
			r.pushQueryToQueue(ctx, q.ID, q.Text)
		}
	}

	return len(newQueries), nil
}

// pushQueryToQueue enqueues a query onto the Redis sorted-set queue.
func (r *Repository) pushQueryToQueue(ctx context.Context, queryID int64, text string) {
	data, _ := json.Marshal(map[string]any{
		"id":   queryID,
		"text": text,
	})
	score := float64(time.Now().UnixMicro())
	r.redis.ZAdd(ctx, QueueKey, redis.Z{Score: score, Member: string(data)})
}

// GetPending returns all queries with status='pending'.
func (r *Repository) GetPending(limit int) ([]db.Query, error) {
	rows, err := r.db.Query(`
		SELECT id, text, text_hash, status, result_count,
		       COALESCE(error_msg,''), created_at, updated_at
		FROM queries
		WHERE status = 'pending'
		ORDER BY id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query: get pending: %w", err)
	}
	defer rows.Close()

	var result []db.Query
	for rows.Next() {
		var q db.Query
		if err := rows.Scan(&q.ID, &q.Text, &q.TextHash,
			&q.Status, &q.ResultCount, &q.ErrorMsg, &q.CreatedAt, &q.UpdatedAt); err != nil {
			return nil, fmt.Errorf("query: scan: %w", err)
		}
		result = append(result, q)
	}
	return result, rows.Err()
}

// UpdateStatus sets a query's status and optional error message.
func (r *Repository) UpdateStatus(id int64, status string, resultCount int, errMsg string) error {
	_, err := r.db.Exec(`
		UPDATE queries SET status = $1, result_count = $2, error_msg = $3, updated_at = NOW()
		WHERE id = $4
	`, status, resultCount, errMsg, id)
	if err != nil {
		return fmt.Errorf("query: update status %d: %w", id, err)
	}
	return nil
}

// RequeueProcessing resets processing queries back to pending (for resume on restart).
func (r *Repository) RequeueProcessing() (int, error) {
	res, err := r.db.Exec(`
		UPDATE queries SET status = 'pending', updated_at = NOW()
		WHERE status = 'processing'
	`)
	if err != nil {
		return 0, fmt.Errorf("query: requeue processing: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// CountByStatus returns a map of status → count.
func (r *Repository) CountByStatus() (map[string]int, error) {
	rows, err := r.db.Query(`SELECT status, COUNT(*) FROM queries GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("query: count by status: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, rows.Err()
}

// Total returns the total number of queries.
func (r *Repository) Total() (int, error) {
	var n int
	err := r.db.QueryRow(`SELECT COUNT(*) FROM queries`).Scan(&n)
	return n, err
}

// DeleteByIDs removes queries by their IDs and returns the count deleted.
func (r *Repository) DeleteByIDs(ids []int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf("DELETE FROM queries WHERE id IN (%s)", strings.Join(placeholders, ","))
	res, err := r.db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("query: delete by ids: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// RetryErrors resets all error-status queries back to pending and pushes to Redis queue.
func (r *Repository) RetryErrors() (int, error) {
	rows, err := r.db.Query(`
		UPDATE queries SET status = 'pending', error_msg = '', updated_at = NOW()
		WHERE status = 'error'
		RETURNING id, text
	`)
	if err != nil {
		return 0, fmt.Errorf("query: retry errors: %w", err)
	}
	defer rows.Close()

	ctx := context.Background()
	count := 0
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			continue
		}
		if r.redis != nil {
			r.pushQueryToQueue(ctx, id, text)
		}
		count++
	}
	return count, rows.Err()
}

// RequeuePendingToRedis pushes all pending queries to the Redis queue.
// Called at startup to ensure workers can pick up queries after a restart.
func (r *Repository) RequeuePendingToRedis() (int, error) {
	if r.redis == nil {
		return 0, nil
	}
	rows, err := r.db.Query(`SELECT id, text FROM queries WHERE status = 'pending' ORDER BY id ASC`)
	if err != nil {
		return 0, fmt.Errorf("query: requeue pending to redis: %w", err)
	}
	defer rows.Close()

	ctx := context.Background()
	count := 0
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			continue
		}
		r.pushQueryToQueue(ctx, id, text)
		count++
	}
	return count, rows.Err()
}

// MarkProcessing atomically marks a pending query as processing and returns it.
func (r *Repository) MarkProcessing() (*db.Query, error) {
	var q db.Query
	err := r.db.QueryRow(`
		UPDATE queries SET status = 'processing', updated_at = NOW()
		WHERE id = (
			SELECT id FROM queries WHERE status = 'pending' ORDER BY id ASC LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, text, text_hash, status, result_count,
		          COALESCE(error_msg,''), created_at, updated_at
	`).Scan(&q.ID, &q.Text, &q.TextHash,
		&q.Status, &q.ResultCount, &q.ErrorMsg, &q.CreatedAt, &q.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query: mark processing: %w", err)
	}
	return &q, nil
}

