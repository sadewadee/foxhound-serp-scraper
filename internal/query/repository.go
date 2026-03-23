package query

import (
	"database/sql"
	"fmt"

	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
)

// Repository handles PostgreSQL CRUD for the queries table.
type Repository struct {
	db *sql.DB
}

// NewRepository creates a query repository.
func NewRepository(database *sql.DB) *Repository {
	return &Repository{db: database}
}

// InsertBatch bulk-inserts queries, skipping duplicates by text_hash.
// Returns the number of new queries inserted.
func (r *Repository) InsertBatch(queries []string, templateID string) (int, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("query: begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO queries (text, text_hash, template_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, 'pending', NOW(), NOW())
		ON CONFLICT (text_hash) DO NOTHING
	`)
	if err != nil {
		return 0, fmt.Errorf("query: prepare insert: %w", err)
	}
	defer stmt.Close()

	inserted := 0
	for _, q := range queries {
		hash := dedup.HashQuery(q)
		res, err := stmt.Exec(q, hash, templateID)
		if err != nil {
			return inserted, fmt.Errorf("query: insert %q: %w", q, err)
		}
		n, _ := res.RowsAffected()
		inserted += int(n)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("query: commit: %w", err)
	}
	return inserted, nil
}

// GetPending returns all queries with status='pending'.
func (r *Repository) GetPending(limit int) ([]db.Query, error) {
	rows, err := r.db.Query(`
		SELECT id, text, text_hash, COALESCE(template_id,''), status, result_count,
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
		if err := rows.Scan(&q.ID, &q.Text, &q.TextHash, &q.TemplateID,
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

// MarkProcessing atomically marks a pending query as processing and returns it.
func (r *Repository) MarkProcessing() (*db.Query, error) {
	var q db.Query
	err := r.db.QueryRow(`
		UPDATE queries SET status = 'processing', updated_at = NOW()
		WHERE id = (
			SELECT id FROM queries WHERE status = 'pending' ORDER BY id ASC LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, text, text_hash, COALESCE(template_id,''), status, result_count,
		          COALESCE(error_msg,''), created_at, updated_at
	`).Scan(&q.ID, &q.Text, &q.TextHash, &q.TemplateID,
		&q.Status, &q.ResultCount, &q.ErrorMsg, &q.CreatedAt, &q.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query: mark processing: %w", err)
	}
	return &q, nil
}

