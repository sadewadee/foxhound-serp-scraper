//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
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

// SERPStage runs Stage 2: SERP discovery with a shared browser pool.
// One browser process hosts N tabs (pool). N goroutines pop jobs from
// a Redis sorted-set queue, each acquiring a tab independently. A query
// feeder goroutine converts pending queries into per-page serp_jobs and
// pushes them to the queue. A reconciler resets stuck jobs every 60 s.
type SERPStage struct {
	cfg       *config.Config
	db        *sql.DB
	redis     *redis.Client
	dedup     *dedup.Store
	queryRepo *query.Repository
	timing    *behavior.Timing
	lifecycle *scraper.BrowserLifecycle

	// Shared browser — protected by mutex for rotation.
	browser   *fetch.CamoufoxFetcher
	browserMu sync.Mutex

	// Metrics.
	queriesProcessed atomic.Int64
	urlsFound        atomic.Int64
	pagesProcessed   atomic.Int64
}

// NewSERPStage creates a new SERP discovery stage.
func NewSERPStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *SERPStage {
	s := &SERPStage{
		cfg:       cfg,
		db:        database,
		redis:     dd.Client(),
		dedup:     dd,
		queryRepo: query.NewRepository(database),
		timing:    behavior.NewTiming(behavior.CarefulProfile().Timing),
	}
	s.lifecycle = scraper.NewBrowserLifecycle(cfg, scraper.NewSERPBrowser, "serp")
	return s
}

// Run starts SERP discovery. Blocks until ctx is cancelled.
func (s *SERPStage) Run(ctx context.Context) error {
	// Requeue any queries stuck in 'processing' from previous run.
	if n, err := s.queryRepo.RequeueProcessing(); err != nil {
		slog.Warn("serp: requeue failed", "error", err)
	} else if n > 0 {
		slog.Info("serp: requeued processing queries", "count", n)
	}
	s.requeueStuckJobs()

	concurrency := s.cfg.SERP.Concurrency
	browser, err := scraper.NewSERPBrowserWithPool(s.cfg, concurrency)
	if err != nil {
		return fmt.Errorf("serp: browser init failed: %w", err)
	}
	s.browserMu.Lock()
	s.browser = browser
	s.browserMu.Unlock()

	defer func() {
		s.browserMu.Lock()
		if s.browser != nil {
			s.browser.Close()
		}
		s.browserMu.Unlock()
	}()

	slog.Info("serp: starting", "concurrency", concurrency)

	// Start healthcheck heartbeat.
	go touchHealthFile(ctx, "/tmp/worker-healthy")

	// Start reconciler.
	go s.reconciler(ctx)

	// Start query feeder — watches for pending queries and generates serp_jobs.
	go s.queryFeeder(ctx)

	// Start N concurrent tab workers.
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(tabID int) {
			defer wg.Done()
			s.tabWorker(ctx, tabID)
		}(i)
	}

	wg.Wait()
	slog.Info("serp: all tab workers done",
		"queries", s.queriesProcessed.Load(),
		"urls", s.urlsFound.Load(),
		"pages", s.pagesProcessed.Load())
	return nil
}

// queryFeeder watches for pending queries, generates per-page serp_jobs, and
// pushes each job onto the Redis sorted-set queue for tab workers to consume.
func (s *SERPStage) queryFeeder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		q, err := s.queryRepo.MarkProcessing()
		if err != nil {
			slog.Error("serp: mark processing failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		if q == nil {
			// No pending queries — wait before polling again.
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}

		slog.Info("serp: generating jobs for query", "query", q.Text, "id", q.ID)

		for page := 0; page < s.cfg.SERP.PagesPerQuery; page++ {
			jobID := fmt.Sprintf("serp-%d-p%d", q.ID, page)
			serpURL := scraper.BuildSERPURL(q.Text, page, s.cfg.SERP.ResultsPerPage)

			s.db.Exec(`
				INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, status)
				VALUES ($1, $2, $3, $4, 'new')
				ON CONFLICT (id) DO NOTHING
			`, jobID, q.ID, serpURL, page)

			s.pushSerpJob(ctx, jobID, serpURL, q.ID, page)
		}
	}
}

// pushSerpJob enqueues a serp_job onto the Redis sorted-set queue.
// Score is the current time in microseconds — FIFO within the queue.
func (s *SERPStage) pushSerpJob(ctx context.Context, jobID, serpURL string, queryID int64, pageNum int) {
	data := fmt.Sprintf(`{"id":"%s","url":"%s","query_id":%d,"page_num":%d}`,
		jobID, serpURL, queryID, pageNum)
	score := float64(time.Now().UnixMicro())
	if err := s.redis.ZAdd(ctx, "serp:queue:serp", redis.Z{Score: score, Member: data}).Err(); err != nil {
		slog.Warn("serp: push to queue failed", "job", jobID, "error", err)
	}
}

// tabWorker is one of N goroutines that pops jobs from the Redis queue and
// fetches SERP pages. All workers share the single pooled browser.
func (s *SERPStage) tabWorker(ctx context.Context, tabID int) {
	workerID := fmt.Sprintf("serp-tab-%d", tabID)
	slog.Info("serp: tab worker starting", "tab", tabID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Pop the highest-priority (lowest score = oldest) job from the queue.
		results, err := s.redis.ZPopMin(ctx, "serp:queue:serp", 1).Result()
		if err != nil || len(results) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		// Parse job payload.
		var job struct {
			ID      string `json:"id"`
			URL     string `json:"url"`
			QueryID int64  `json:"query_id"`
			PageNum int    `json:"page_num"`
		}
		if err := json.Unmarshal([]byte(results[0].Member.(string)), &job); err != nil {
			slog.Warn("serp: invalid job in queue", "raw", results[0].Member, "error", err)
			continue
		}

		// Load attempt metadata from DB.
		var attemptCount, maxAttempts int
		err = s.db.QueryRow(`
			SELECT attempt_count, max_attempts FROM serp_jobs WHERE id = $1
		`, job.ID).Scan(&attemptCount, &maxAttempts)
		if err != nil {
			slog.Warn("serp: job not found in DB", "job", job.ID)
			continue
		}

		// Lock the job so the reconciler skips it.
		s.db.Exec(`
			UPDATE serp_jobs SET status = 'processing', locked_by = $1, locked_at = NOW(), updated_at = NOW()
			WHERE id = $2
		`, workerID, job.ID)

		// Snapshot the browser reference under lock.
		s.browserMu.Lock()
		browser := s.browser
		s.browserMu.Unlock()

		if browser == nil {
			slog.Warn("serp: browser is nil, re-queuing job", "tab", tabID, "job", job.ID)
			s.pushSerpJob(ctx, job.ID, job.URL, job.QueryID, job.PageNum)
			s.db.Exec(`UPDATE serp_jobs SET status = 'new', locked_by = NULL, updated_at = NOW() WHERE id = $1`, job.ID)
			time.Sleep(5 * time.Second)
			continue
		}

		// Fetch SERP page.
		body, fetchErr := scraper.FetchSERP(ctx, browser, job.URL, job.ID)

		// Treat captcha pages as a fetch error.
		if fetchErr == nil && scraper.IsCaptchaPage(body) {
			fetchErr = fmt.Errorf("captcha detected")
		}

		if fetchErr != nil {
			newAttempt := attemptCount + 1
			slog.Warn("serp: fetch failed", "job", job.ID, "attempt", newAttempt, "tab", tabID, "error", fetchErr)

			if newAttempt >= maxAttempts {
				s.db.Exec(`
					UPDATE serp_jobs SET status = 'failed', attempt_count = $1, error_msg = $2, updated_at = NOW()
					WHERE id = $3
				`, newAttempt, fetchErr.Error(), job.ID)
			} else {
				backoffSec := 30 * (1 << (newAttempt - 1)) // 30 s, 60 s, 120 s, …
				s.db.Exec(`
					UPDATE serp_jobs SET status = 'new', attempt_count = $1,
						next_attempt_at = NOW() + interval '1 second' * $2,
						error_msg = $3, updated_at = NOW()
					WHERE id = $4
				`, newAttempt, backoffSec, fetchErr.Error(), job.ID)
				// Re-queue after the backoff window so the reconciler picks it up,
				// but also schedule it here to avoid relying solely on the reconciler.
				go func(id, url string, qID int64, pNum, delay int) {
					time.Sleep(time.Duration(delay) * time.Second)
					s.pushSerpJob(ctx, id, url, qID, pNum)
				}(job.ID, job.URL, job.QueryID, job.PageNum, backoffSec)
			}

			// Rotate browser after captcha/block regardless of lifecycle counter.
			if s.lifecycle.IncrementAndCheck() {
				s.restartBrowser()
			}
			continue
		}

		// Parse SERP results.
		urls, parseErr := scraper.ParseSERPResults(body)
		if parseErr != nil {
			slog.Warn("serp: parse failed", "job", job.ID, "error", parseErr)
			s.db.Exec(`
				UPDATE serp_jobs SET status = 'failed', error_msg = $1, updated_at = NOW()
				WHERE id = $2
			`, parseErr.Error(), job.ID)
			continue
		}

		// Insert websites and push to website queue.
		inserted := 0
		for _, u := range urls {
			urlHash := dedup.HashURL(u)
			domain := dedup.ExtractDomain(u)
			if domain == "" {
				continue
			}
			_, insertErr := s.db.Exec(`
				INSERT INTO websites (domain, url, url_hash, source_query_id, source_serp_id, page_type, status)
				VALUES ($1, $2, $3, $4, $5, 'serp_result', 'pending')
				ON CONFLICT (url_hash) DO NOTHING
			`, domain, u, urlHash, job.QueryID, job.ID)
			if insertErr != nil {
				slog.Warn("serp: insert website failed", "url", u, "error", insertErr)
				continue
			}
			if err := pushToQueue(ctx, s.redis, "serp:queue:websites", u, job.QueryID, job.ID); err != nil {
				slog.Warn("serp: push website to queue failed", "url", u, "error", err)
			}
			inserted++
		}

		s.urlsFound.Add(int64(inserted))
		s.pagesProcessed.Add(1)

		s.db.Exec(`
			UPDATE serp_jobs SET status = 'completed', result_count = $1, updated_at = NOW()
			WHERE id = $2
		`, len(urls), job.ID)

		slog.Info("serp: page done", "job", job.ID, "found", len(urls), "new", inserted, "tab", tabID)

		// Check lifecycle — rotate browser when page-reuse limit is reached.
		if s.lifecycle.IncrementAndCheck() {
			slog.Info("serp: page reuse limit reached, rotating browser", "tab", tabID)
			s.restartBrowser()
		}

		// Small inter-page delay to avoid hammering Google from the same browser.
		delay := s.timing.PaginationDelay()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// restartBrowser performs a thread-safe browser rotation. While the new browser
// is being created, s.browser is set to nil so tab workers re-queue their jobs
// rather than fetching with a dead browser.
func (s *SERPStage) restartBrowser() {
	s.browserMu.Lock()
	defer s.browserMu.Unlock()

	old := s.browser
	s.browser = nil // tabs will detect nil and wait

	newBrowser, err := s.lifecycle.Restart(old)
	if err != nil {
		slog.Error("serp: browser restart failed, attempting fresh create", "error", err)
		newBrowser, err = scraper.NewSERPBrowserWithPool(s.cfg, s.cfg.SERP.Concurrency)
		if err != nil {
			slog.Error("serp: fresh browser create failed", "error", err)
			return
		}
	}
	s.browser = newBrowser
	slog.Info("serp: browser rotated successfully")
}

// reconciler runs every 60 s to:
//  1. Reset stuck processing jobs (locked >5 min) and re-queue them.
//  2. Re-queue new jobs whose next_attempt_at has elapsed.
//  3. Mark queries completed when all their serp_jobs are done.
func (s *SERPStage) reconciler(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// 1. Reset stuck processing jobs (>5 min without progress).
		res, err := s.db.Exec(`
			UPDATE serp_jobs SET status = 'new', locked_by = NULL, locked_at = NULL, updated_at = NOW()
			WHERE status = 'processing' AND locked_at < NOW() - INTERVAL '5 minutes'
		`)
		if err == nil {
			if n, _ := res.RowsAffected(); n > 0 {
				slog.Info("serp: reconciler reset stuck jobs", "count", n)
				// Re-queue the jobs that were just unblocked.
				rows, _ := s.db.Query(`
					SELECT id, search_url, parent_job_id, page_num FROM serp_jobs
					WHERE status = 'new' AND locked_by IS NULL
					  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
					LIMIT 100
				`)
				if rows != nil {
					for rows.Next() {
						var id, url string
						var queryID int64
						var pageNum int
						if err := rows.Scan(&id, &url, &queryID, &pageNum); err == nil {
							s.pushSerpJob(ctx, id, url, queryID, pageNum)
						}
					}
					rows.Close()
				}
			}
		}

		// 2. Re-queue new retry-eligible jobs.
		retryRows, _ := s.db.Query(`
			SELECT id, search_url, parent_job_id, page_num FROM serp_jobs
			WHERE status = 'new' AND attempt_count > 0
			  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
			LIMIT 100
		`)
		if retryRows != nil {
			requeued := 0
			for retryRows.Next() {
				var id, url string
				var queryID int64
				var pageNum int
				if err := retryRows.Scan(&id, &url, &queryID, &pageNum); err == nil {
					s.pushSerpJob(ctx, id, url, queryID, pageNum)
					requeued++
				}
			}
			retryRows.Close()
			if requeued > 0 {
				slog.Info("serp: reconciler re-queued retry jobs", "count", requeued)
			}
		}

		// 3. Mark queries completed when all their serp_jobs are done.
		s.db.Exec(`
			UPDATE queries SET status = 'completed', updated_at = NOW()
			WHERE status = 'processing'
			AND NOT EXISTS (
				SELECT 1 FROM serp_jobs
				WHERE parent_job_id = queries.id
				  AND status IN ('new', 'processing')
			)
		`)
	}
}

// requeueStuckJobs is called at startup to reset any jobs left in 'processing'
// state by the previous process run.
func (s *SERPStage) requeueStuckJobs() {
	res, err := s.db.Exec(`
		UPDATE serp_jobs SET status = 'new', locked_by = NULL, locked_at = NULL, updated_at = NOW()
		WHERE status = 'processing'
	`)
	if err != nil {
		slog.Warn("serp: requeueStuckJobs failed", "error", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		slog.Info("serp: requeued stuck serp_jobs from previous run", "count", n)
	}
}

// pushToQueue pushes a URL job to a Redis sorted set queue used by website/enrich workers.
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

// PagesProcessed returns the count of SERP pages fetched.
func (s *SERPStage) PagesProcessed() int64 {
	return s.pagesProcessed.Load()
}

// Ensure db import is used (models.go SERPJob type is referenced indirectly via DB queries).
var _ = db.SERPJob{}
