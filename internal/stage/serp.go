//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/behavior"
	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/query"
	"github.com/sadewadee/serp-scraper/internal/scraper"
)

// SERPStage runs Stage 2: SERP discovery workers.
type SERPStage struct {
	cfg       *config.Config
	db        *sql.DB
	dedup     *dedup.Store
	queryRepo *query.Repository
	timing    *behavior.Timing

	// Metrics.
	queriesProcessed atomic.Int64
	urlsFound        atomic.Int64
}

// NewSERPStage creates a new SERP discovery stage.
func NewSERPStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *SERPStage {
	return &SERPStage{
		cfg:       cfg,
		db:        database,
		dedup:     dd,
		queryRepo: query.NewRepository(database),
		timing:    behavior.NewTiming(behavior.CarefulProfile().Timing),
	}
}

// Run starts SERP discovery workers. Blocks until ctx is cancelled or work is done.
func (s *SERPStage) Run(ctx context.Context) error {
	// Requeue any queries stuck in 'processing' from previous run.
	if n, err := s.queryRepo.RequeueProcessing(); err != nil {
		slog.Warn("serp: requeue failed", "error", err)
	} else if n > 0 {
		slog.Info("serp: requeued processing queries", "count", n)
	}

	numWorkers := s.cfg.SERP.Workers
	slog.Info("serp: starting workers", "count", numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.worker(ctx, workerID)
		}(w)
	}

	wg.Wait()
	slog.Info("serp: all workers done",
		"queries", s.queriesProcessed.Load(),
		"urls", s.urlsFound.Load())
	return nil
}

func (s *SERPStage) worker(ctx context.Context, workerID int) {
	workerIDStr := fmt.Sprintf("serp-worker-%d", workerID)
	slog.Info("serp: worker starting", "worker", workerID)

	// Each worker gets its own browser instance with persistent session + page pooling.
	browser, err := scraper.NewSERPBrowser(s.cfg)
	if err != nil {
		slog.Error("serp: worker browser init failed", "worker", workerID, "error", err)
		return
	}
	defer browser.Close()

	// Each worker tracks its own browser lifecycle independently — each has its
	// own browser process so counters must not be shared across workers.
	lifecycle := scraper.NewBrowserLifecycle(s.cfg, scraper.NewSERPBrowser, fmt.Sprintf("serp-worker-%d", workerID))

	for {
		if ctx.Err() != nil {
			return
		}

		// Get next pending query from PG (atomic claim).
		q, err := s.queryRepo.MarkProcessing()
		if err != nil {
			slog.Error("serp: mark processing failed", "worker", workerID, "error", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if q == nil {
			// No more pending queries — check periodically.
			slog.Debug("serp: no pending queries, waiting", "worker", workerID)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}

		slog.Info("serp: processing query", "worker", workerID, "query", q.Text, "id", q.ID)
		resultCount := s.processQuery(ctx, &browser, q, workerIDStr, lifecycle)

		status := "completed"
		errMsg := ""
		if err := s.queryRepo.UpdateStatus(q.ID, status, resultCount, errMsg); err != nil {
			slog.Error("serp: update status failed", "error", err)
		}

		s.queriesProcessed.Add(1)

		// Delay between queries.
		delay := s.timing.SearchDelay()
		slog.Debug("serp: delay between queries", "delay", delay, "worker", workerID)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (s *SERPStage) processQuery(ctx context.Context, browserPtr **fetch.CamoufoxFetcher, q *db.Query, workerID string, lifecycle *scraper.BrowserLifecycle) int {
	totalURLs := 0
	queueKey := "serp:queue:websites"

	// Step 1: Generate all serp_jobs upfront for pages 0 to PagesPerQuery-1.
	for pageNum := 0; pageNum < s.cfg.SERP.PagesPerQuery; pageNum++ {
		serpURL := scraper.BuildSERPURL(q.Text, pageNum, s.cfg.SERP.ResultsPerPage)
		jobID := fmt.Sprintf("serp-%d-p%d", q.ID, pageNum)

		_, err := s.db.Exec(`
			INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, status)
			VALUES ($1, $2, $3, $4, 'new')
			ON CONFLICT (id) DO NOTHING
		`, jobID, q.ID, serpURL, pageNum)
		if err != nil {
			slog.Error("serp: insert serp_job failed", "job", jobID, "error", err)
		}
	}

	// Step 2: Process serp_jobs with retry loop.
	for {
		if ctx.Err() != nil {
			break
		}

		// Get next unprocessed job for this query.
		var job db.SERPJob
		err := s.db.QueryRow(`
			SELECT id, search_url, page_num, attempt_count, max_attempts
			FROM serp_jobs
			WHERE parent_job_id = $1 AND status = 'new'
			ORDER BY page_num ASC
			LIMIT 1
		`, q.ID).Scan(&job.ID, &job.SearchURL, &job.PageNum, &job.AttemptCount, &job.MaxAttempts)
		if err == sql.ErrNoRows {
			break // All jobs for this query are done (completed, failed, or still processing).
		}
		if err != nil {
			slog.Error("serp: query serp_jobs failed", "query_id", q.ID, "error", err)
			break
		}

		// Lock it.
		s.db.Exec(`
			UPDATE serp_jobs SET status = 'processing', locked_by = $1, locked_at = NOW(), updated_at = NOW()
			WHERE id = $2
		`, workerID, job.ID)

		browser := *browserPtr

		// Fetch SERP.
		slog.Debug("serp: fetching page", "query", q.Text, "page", job.PageNum, "url", job.SearchURL)
		body, fetchErr := scraper.FetchSERP(ctx, browser, job.SearchURL, job.ID)

		// Check for captcha even on successful fetch.
		if fetchErr == nil && scraper.IsCaptchaPage(body) {
			fetchErr = fmt.Errorf("captcha detected")
		}

		if fetchErr != nil {
			newAttempt := job.AttemptCount + 1
			slog.Warn("serp: fetch failed", "job", job.ID, "attempt", newAttempt, "error", fetchErr)

			if newAttempt >= job.MaxAttempts {
				s.db.Exec(`
					UPDATE serp_jobs SET status = 'failed', attempt_count = $1, error_msg = $2, updated_at = NOW()
					WHERE id = $3
				`, newAttempt, fetchErr.Error(), job.ID)
			} else {
				// Backoff: 30s, 60s, 120s, ...
				backoffSeconds := 30 * (1 << (newAttempt - 1))
				s.db.Exec(`
					UPDATE serp_jobs SET status = 'new', attempt_count = $1,
						next_attempt_at = NOW() + interval '1 second' * $2,
						error_msg = $3, updated_at = NOW()
					WHERE id = $4
				`, newAttempt, backoffSeconds, fetchErr.Error(), job.ID)

				// Restart browser for fresh fingerprint on captcha/block.
				// Use lifecycle.Restart so temp dirs are cleaned and orphan
				// processes are reaped (counts as a lifecycle-triggered restart).
				slog.Warn("serp: captcha/block detected, restarting browser", "job", job.ID, "attempt", newAttempt)
				newBrowser, restartErr := lifecycle.Restart(*browserPtr)
				if restartErr != nil {
					slog.Error("serp: browser restart failed", "error", restartErr)
					return totalURLs
				}
				*browserPtr = newBrowser

				// Wait for backoff.
				backoff := time.Duration(backoffSeconds) * time.Second
				select {
				case <-ctx.Done():
					return totalURLs
				case <-time.After(backoff):
				}
			}
			// Count this fetch attempt for lifecycle tracking.
			if lifecycle.IncrementAndCheck() {
				slog.Info("serp: page reuse limit reached, restarting browser", "worker", workerID)
				newBrowser, restartErr := lifecycle.Restart(*browserPtr)
				if restartErr != nil {
					slog.Error("serp: lifecycle restart failed", "error", restartErr)
					return totalURLs
				}
				*browserPtr = newBrowser
			}
			continue
		}

		// Parse results.
		urls, parseErr := scraper.ParseSERPResults(body)
		if parseErr != nil {
			slog.Warn("serp: parse failed", "job", job.ID, "error", parseErr)
			s.db.Exec(`
				UPDATE serp_jobs SET status = 'failed', error_msg = $1, updated_at = NOW()
				WHERE id = $2
			`, parseErr.Error(), job.ID)
			continue
		}

		// Insert websites (no URL dedup — every unique URL must be enriched).
		inserted := 0
		for _, u := range urls {
			urlHash := dedup.HashURL(u)

			domain := dedup.ExtractDomain(u)
			if domain == "" {
				continue
			}

			// Insert website — source_serp_id is now TEXT (serp_jobs.id).
			_, insertErr := s.db.Exec(`
				INSERT INTO websites (domain, url, url_hash, source_query_id, source_serp_id, page_type, status)
				VALUES ($1, $2, $3, $4, $5, 'serp_result', 'pending')
				ON CONFLICT (url_hash) DO NOTHING
			`, domain, u, urlHash, q.ID, job.ID)
			if insertErr != nil {
				slog.Warn("serp: insert website failed", "url", u, "error", insertErr)
				continue
			}

			// Push to Redis queue for website stage.
			if err := pushToQueue(ctx, s.dedup.Client(), queueKey, u, q.ID, job.ID); err != nil {
				slog.Warn("serp: push to queue failed", "url", u, "error", err)
			}

			inserted++
		}

		totalURLs += inserted
		s.urlsFound.Add(int64(inserted))

		// Mark completed.
		s.db.Exec(`
			UPDATE serp_jobs SET status = 'completed', result_count = $1, updated_at = NOW()
			WHERE id = $2
		`, len(urls), job.ID)

		slog.Info("serp: page done",
			"query", q.Text, "page", job.PageNum,
			"found", len(urls), "new", inserted)

		// Count this successful fetch for lifecycle tracking.
		if lifecycle.IncrementAndCheck() {
			slog.Info("serp: page reuse limit reached, restarting browser", "worker", workerID)
			newBrowser, restartErr := lifecycle.Restart(*browserPtr)
			if restartErr != nil {
				slog.Error("serp: lifecycle restart failed", "error", restartErr)
				return totalURLs
			}
			*browserPtr = newBrowser
		}

		// Delay between pages.
		if job.PageNum < s.cfg.SERP.PagesPerQuery-1 {
			delay := s.timing.PaginationDelay()
			select {
			case <-ctx.Done():
				return totalURLs
			case <-time.After(delay):
			}
		}
	}

	return totalURLs
}

// pushToQueue pushes a URL job to a Redis sorted set queue.
func pushToQueue(ctx context.Context, client *redis.Client, queueKey, urlStr string, queryID int64, serpID string) error {
	job := &foxhound.Job{
		ID:        fmt.Sprintf("web-%d-%s", queryID, dedup.HashURL(urlStr)[:8]),
		URL:       urlStr,
		Method:    "GET",
		Priority:  foxhound.PriorityNormal,
		CreatedAt: time.Now(),
		Meta: map[string]any{
			"query_id": queryID,
			"serp_id":  serpID,
		},
	}

	micros := job.CreatedAt.UnixMicro()
	score := -(float64(job.Priority) * 1_000_000_000) + float64(micros)

	data := fmt.Sprintf(`{"id":"%s","url":"%s","method":"GET","priority":%d,"meta":{"query_id":%d,"serp_id":"%s"}}`,
		job.ID, urlStr, job.Priority, queryID, serpID)

	return client.ZAdd(ctx, queueKey, redis.Z{
		Score:  score,
		Member: data,
	}).Err()
}

// QueriesProcessed returns the count of processed queries.
func (s *SERPStage) QueriesProcessed() int64 {
	return s.queriesProcessed.Load()
}

// URLsFound returns the count of discovered URLs.
func (s *SERPStage) URLsFound() int64 {
	return s.urlsFound.Load()
}
