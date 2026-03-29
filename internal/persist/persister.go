//go:build playwright

package persist

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// Redis result queue keys — workers push here, persister drains to DB.
const (
	KeyResultURLs   = "serp:results:urls"
	KeyResultSERP   = "serp:results:serp"
	KeyResultEnrich = "serp:results:enrich"
)

// URLResult is pushed by SERP tab workers when they discover URLs.
type URLResult struct {
	URL       string `json:"url"`
	URLHash   string `json:"url_hash"`
	Domain    string `json:"domain"`
	QueryID   int64  `json:"query_id"`
	SerpJobID string `json:"serp_job_id"`
}

// SERPResult is pushed by SERP tab workers when a SERP page is done.
type SERPResult struct {
	JobID        string `json:"job_id"`
	QueryID      int64  `json:"query_id"`
	ResultCount  int    `json:"result_count"`
	Status       string `json:"status"` // "completed", "failed", "new_job", "parse_failed", "requeue"
	ErrorMsg     string `json:"error_msg,omitempty"`
	AttemptCount int    `json:"attempt_count,omitempty"`
	BackoffSec   int    `json:"backoff_sec,omitempty"`
}

// EnrichResult is pushed by enrich workers when a page is processed.
type EnrichResult struct {
	URLHash          string            `json:"url_hash"`
	Status           string            `json:"status"` // "completed", "failed", "dead", "contact_page", "directory_listing"
	Emails           []string          `json:"emails,omitempty"`
	Phones           []string          `json:"phones,omitempty"`
	SocialLinks      map[string]string `json:"social_links,omitempty"`
	ContactName      string            `json:"contact_name,omitempty"`
	BusinessName     string            `json:"business_name,omitempty"`
	BusinessCategory string            `json:"business_category,omitempty"`
	Description      string            `json:"description,omitempty"`
	Website          string            `json:"website,omitempty"`
	Address          string            `json:"address,omitempty"`
	Location         string            `json:"location,omitempty"`
	OpeningHours     string            `json:"opening_hours,omitempty"`
	Rating           string            `json:"rating,omitempty"`
	PageTitle        string            `json:"page_title,omitempty"`
	MXValid          *bool             `json:"mx_valid,omitempty"`
	ErrorMsg         string            `json:"error_msg,omitempty"`
	AttemptIncr      int               `json:"attempt_incr,omitempty"`
	IsPermanent      bool              `json:"is_permanent,omitempty"`
}

// Persister drains Redis result queues and batch-persists to PostgreSQL.
// Runs in every container — operations are idempotent via ON CONFLICT / WHERE.
type Persister struct {
	db        *sql.DB
	redis     *redis.Client
	interval  time.Duration
	batchSize int64
}

// New creates a Persister.
func New(db *sql.DB, redisClient *redis.Client, intervalMs, batchSize int) *Persister {
	if intervalMs <= 0 {
		intervalMs = 5000
	}
	if batchSize <= 0 {
		batchSize = 500
	}
	return &Persister{
		db:        db,
		redis:     redisClient,
		interval:  time.Duration(intervalMs) * time.Millisecond,
		batchSize: int64(batchSize),
	}
}

// Run starts the persist loop. Blocks until ctx is cancelled.
func (p *Persister) Run(ctx context.Context) {
	slog.Info("persister: starting", "interval", p.interval, "batch_size", p.batchSize)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final drain before exit.
			slog.Info("persister: final drain before shutdown")
			p.flush(context.Background())
			return
		case <-ticker.C:
			p.flush(ctx)
		}
	}
}

func (p *Persister) flush(ctx context.Context) {
	p.flushURLResults(ctx)
	p.flushSERPResults(ctx)
	p.flushEnrichResults(ctx)
}

// requeueItems pushes items back to Redis list on tx failure to prevent data loss.
func (p *Persister) requeueItems(ctx context.Context, key string, items []string) {
	for _, item := range items {
		if err := p.redis.RPush(ctx, key, item).Err(); err != nil {
			slog.Error("persister: CRITICAL re-queue failed, data may be lost", "key", key, "error", err)
		}
	}
}

// flushURLResults drains serp:results:urls → INSERT websites + enrich_jobs.
func (p *Persister) flushURLResults(ctx context.Context) {
	for {
		items, err := p.redis.LPopCount(ctx, KeyResultURLs, int(p.batchSize)).Result()
		if err != nil || len(items) == 0 {
			return
		}

		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			slog.Warn("persister: url tx begin failed, re-queuing", "error", err)
			p.requeueItems(ctx, KeyResultURLs, items)
			return
		}

		count := 0
		for _, raw := range items {
			var r URLResult
			if err := json.Unmarshal([]byte(raw), &r); err != nil {
				slog.Warn("persister: invalid url result", "error", err)
				continue
			}

			if _, err := tx.ExecContext(ctx, `
				INSERT INTO websites (domain, url, url_hash, source_query_id, source_serp_id, page_type, status)
				VALUES ($1, $2, $3, $4, $5, 'serp_result', 'pending')
				ON CONFLICT (url_hash) DO NOTHING
			`, r.Domain, r.URL, r.URLHash, r.QueryID, r.SerpJobID); err != nil {
				slog.Warn("persister: website insert failed", "url_hash", r.URLHash, "error", err)
			}

			if _, err := tx.ExecContext(ctx, `
				INSERT INTO enrich_jobs (parent_job_id, domain, url, url_hash, status)
				VALUES ($1, $2, $3, $4, 'pending')
				ON CONFLICT (url_hash) DO NOTHING
			`, r.QueryID, r.Domain, r.URL, r.URLHash); err != nil {
				slog.Warn("persister: enrich_job insert failed", "url_hash", r.URLHash, "error", err)
			}

			count++
		}

		if err := tx.Commit(); err != nil {
			slog.Warn("persister: url tx commit failed, re-queuing", "error", err, "count", len(items))
			tx.Rollback()
			p.requeueItems(ctx, KeyResultURLs, items)
		} else if count > 0 {
			slog.Info("persister: flushed URL results", "count", count)
		}

		if len(items) < int(p.batchSize) {
			return
		}
	}
}

// flushSERPResults drains serp:results:serp → UPDATE/INSERT serp_jobs.
func (p *Persister) flushSERPResults(ctx context.Context) {
	for {
		items, err := p.redis.LPopCount(ctx, KeyResultSERP, int(p.batchSize)).Result()
		if err != nil || len(items) == 0 {
			return
		}

		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			slog.Warn("persister: serp tx begin failed, re-queuing", "error", err)
			p.requeueItems(ctx, KeyResultSERP, items)
			return
		}

		count := 0
		for _, raw := range items {
			var r SERPResult
			if err := json.Unmarshal([]byte(raw), &r); err != nil {
				slog.Warn("persister: invalid serp result", "error", err)
				continue
			}

			var execErr error
			switch r.Status {
			case "completed":
				_, execErr = tx.ExecContext(ctx, `
					UPDATE serp_jobs SET status = 'completed', result_count = $1, updated_at = NOW()
					WHERE id = $2
				`, r.ResultCount, r.JobID)

			case "failed":
				if r.BackoffSec > 0 {
					_, execErr = tx.ExecContext(ctx, `
						UPDATE serp_jobs SET status = 'new', attempt_count = $1,
							next_attempt_at = NOW() + interval '1 second' * $2,
							error_msg = $3, updated_at = NOW()
						WHERE id = $4
					`, r.AttemptCount, r.BackoffSec, r.ErrorMsg, r.JobID)
				} else {
					_, execErr = tx.ExecContext(ctx, `
						UPDATE serp_jobs SET status = 'failed', attempt_count = $1, error_msg = $2, updated_at = NOW()
						WHERE id = $3
					`, r.AttemptCount, r.ErrorMsg, r.JobID)
				}

			case "new_job":
				// Query feeder creating new serp_jobs.
				// ErrorMsg carries searchURL, ResultCount carries pageNum.
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, status)
					VALUES ($1, $2, $3, $4, 'new')
					ON CONFLICT (id) DO NOTHING
				`, r.JobID, r.QueryID, r.ErrorMsg, r.ResultCount)

			case "parse_failed":
				_, execErr = tx.ExecContext(ctx, `
					UPDATE serp_jobs SET status = 'failed', error_msg = $1, updated_at = NOW()
					WHERE id = $2
				`, r.ErrorMsg, r.JobID)

			case "requeue":
				_, execErr = tx.ExecContext(ctx, `
					UPDATE serp_jobs SET status = 'new', locked_by = NULL, updated_at = NOW()
					WHERE id = $1
				`, r.JobID)
			}

			if execErr != nil {
				slog.Warn("persister: serp exec failed", "job_id", r.JobID, "status", r.Status, "error", execErr)
			}
			count++
		}

		if err := tx.Commit(); err != nil {
			slog.Warn("persister: serp tx commit failed, re-queuing", "error", err, "count", len(items))
			tx.Rollback()
			p.requeueItems(ctx, KeyResultSERP, items)
		} else if count > 0 {
			slog.Info("persister: flushed SERP results", "count", count)
		}

		if len(items) < int(p.batchSize) {
			return
		}
	}
}

// flushEnrichResults drains serp:results:enrich → UPDATE/INSERT enrich_jobs.
func (p *Persister) flushEnrichResults(ctx context.Context) {
	for {
		items, err := p.redis.LPopCount(ctx, KeyResultEnrich, int(p.batchSize)).Result()
		if err != nil || len(items) == 0 {
			return
		}

		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			slog.Warn("persister: enrich tx begin failed, re-queuing", "error", err)
			p.requeueItems(ctx, KeyResultEnrich, items)
			return
		}

		count := 0
		for _, raw := range items {
			var r EnrichResult
			if err := json.Unmarshal([]byte(raw), &r); err != nil {
				slog.Warn("persister: invalid enrich result", "error", err)
				continue
			}

			socialJSON, _ := json.Marshal(r.SocialLinks)
			if r.SocialLinks == nil {
				socialJSON = []byte("{}")
			}

			var execErr error
			switch r.Status {
			case "completed":
				_, execErr = tx.ExecContext(ctx, `
					UPDATE enrich_jobs SET
						contact_name = $1,
						business_name = $2,
						business_category = $3,
						description = $4,
						website = $5,
						emails = $6,
						phones = $7,
						social_links = $8,
						address = $9,
						location = $10,
						opening_hours = $11,
						rating = $12,
						page_title = $13,
						mx_valid = $14,
						status = 'completed',
						completed_at = NOW(),
						updated_at = NOW()
					WHERE url_hash = $15
				`, r.ContactName, r.BusinessName, r.BusinessCategory, r.Description, r.Website,
					pq.Array(r.Emails), pq.Array(r.Phones), socialJSON,
					r.Address, r.Location, r.OpeningHours, r.Rating, r.PageTitle,
					r.MXValid, r.URLHash)

			case "failed", "dead":
				if r.IsPermanent {
					_, execErr = tx.ExecContext(ctx, `
						UPDATE enrich_jobs SET
							attempt_count = attempt_count + $1,
							error_msg = $2,
							status = 'dead',
							updated_at = NOW()
						WHERE url_hash = $3
					`, r.AttemptIncr, r.ErrorMsg, r.URLHash)
				} else {
					_, execErr = tx.ExecContext(ctx, `
						UPDATE enrich_jobs SET
							attempt_count = attempt_count + $1,
							error_msg = $2,
							status = CASE WHEN attempt_count + $1 >= max_attempts THEN 'dead' ELSE 'failed' END,
							next_attempt_at = CASE WHEN attempt_count + $1 >= max_attempts THEN NULL
								ELSE NOW() + interval '1 second' * 30 * power(2, attempt_count) END,
							updated_at = NOW()
						WHERE url_hash = $3
					`, r.AttemptIncr, r.ErrorMsg, r.URLHash)
				}

			case "contact_page":
				// New contact page: Website=domain, Address=url, AttemptIncr=queryID.
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO websites (domain, url, url_hash, source_query_id, page_type, status)
					VALUES ($1, $2, $3, $4, 'contact', 'pending')
					ON CONFLICT (url_hash) DO NOTHING
				`, r.Website, r.Address, r.URLHash, r.AttemptIncr)
				if execErr != nil {
					slog.Warn("persister: contact page website insert failed", "url_hash", r.URLHash, "error", execErr)
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO enrich_jobs (parent_job_id, domain, url, url_hash, status)
					VALUES ($1, $2, $3, $4, 'pending')
					ON CONFLICT (url_hash) DO NOTHING
				`, r.AttemptIncr, r.Website, r.Address, r.URLHash)

			case "directory_listing":
				// New directory listing: Website=domain, Address=url, AttemptIncr=queryID.
				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO enrich_jobs (parent_job_id, domain, url, url_hash, status)
					VALUES ($1, $2, $3, $4, 'pending')
					ON CONFLICT (url_hash) DO NOTHING
				`, r.AttemptIncr, r.Website, r.Address, r.URLHash)
			}

			if execErr != nil {
				slog.Warn("persister: enrich exec failed", "url_hash", r.URLHash, "status", r.Status, "error", execErr)
			}
			count++
		}

		if err := tx.Commit(); err != nil {
			slog.Warn("persister: enrich tx commit failed, re-queuing", "error", err, "count", len(items))
			tx.Rollback()
			p.requeueItems(ctx, KeyResultEnrich, items)
		} else if count > 0 {
			slog.Info("persister: flushed enrich results", "count", count)
		}

		if len(items) < int(p.batchSize) {
			return
		}
	}
}

// QueueLen returns the total length of all result queues (for monitoring).
func QueueLen(ctx context.Context, redisClient *redis.Client) (int64, error) {
	var total int64
	for _, key := range []string{KeyResultURLs, KeyResultSERP, KeyResultEnrich} {
		n, err := redisClient.LLen(ctx, key).Result()
		if err != nil {
			return 0, fmt.Errorf("persister: llen %s: %w", key, err)
		}
		total += n
	}
	return total, nil
}
