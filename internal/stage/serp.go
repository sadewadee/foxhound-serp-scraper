//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/behavior"
	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/persist"
	"github.com/sadewadee/serp-scraper/internal/query"
	"github.com/sadewadee/serp-scraper/internal/scraper"
)

// SERPStage runs Stage 2: SERP discovery with a shared browser pool.
// One browser process hosts N tabs (pool). N goroutines pop jobs from
// a Redis sorted-set queue, each acquiring a tab independently. A query
// feeder goroutine converts pending queries into per-page serp_jobs and
// pushes them to the queue. A reconciler resets stuck jobs every 60 s.
//
// Hot path is zero-DB: all claims via Redis SETNX, results pushed to
// Redis lists, persister drains to DB in background batches.
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
		queryRepo: query.NewRepositoryWithRedis(database, dd.Client()),
		timing:    behavior.NewTiming(behavior.CarefulProfile().Timing),
	}
	s.lifecycle = scraper.NewBrowserLifecycle(cfg, scraper.NewSERPBrowser, "serp")
	return s
}

// Run starts SERP discovery. Blocks until ctx is cancelled.
func (s *SERPStage) Run(ctx context.Context) error {
	// Requeue stuck processing queries back to pending.
	if n, err := s.queryRepo.RequeueProcessing(); err != nil {
		slog.Warn("serp: requeue failed", "error", err)
	} else if n > 0 {
		slog.Info("serp: requeued processing queries", "count", n)
	}
	// Push all pending queries to Redis queue (recovery after restart).
	if n, err := s.queryRepo.RequeuePendingToRedis(); err != nil {
		slog.Warn("serp: push pending queries to redis failed", "error", err)
	} else if n > 0 {
		slog.Info("serp: pushed pending queries to redis", "count", n)
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

// queryFeeder pops queries from the Redis query queue, generates per-page
// serp_jobs, and pushes each job onto the Redis SERP queue for tab workers.
// Claims via Redis SETNX — no DB in hot path.
func (s *SERPStage) queryFeeder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Pop from Redis query queue.
		results, err := s.redis.ZPopMin(ctx, query.QueueKey, 1).Result()
		if err != nil || len(results) == 0 {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		var qMsg struct {
			ID   int64  `json:"id"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(results[0].Member.(string)), &qMsg); err != nil {
			slog.Warn("serp: invalid query in queue", "error", err)
			continue
		}

		// Atomic claim via DB — queryFeeder is a singleton goroutine (not hot path).
		// DB claim is fine here: only 1 query/sec throughput, prevents duplicates.
		res, claimErr := s.db.Exec(`
			UPDATE queries SET status = 'processing', updated_at = NOW()
			WHERE id = $1 AND status = 'pending'
		`, qMsg.ID)
		if claimErr != nil {
			continue
		}
		if n, _ := res.RowsAffected(); n == 0 {
			continue
		}

		slog.Info("serp: generating jobs for query", "query", qMsg.Text, "id", qMsg.ID)

		for page := 0; page < s.cfg.SERP.PagesPerQuery; page++ {
			jobID := fmt.Sprintf("serp-%d-p%d", qMsg.ID, page)
			serpURL := scraper.BuildSERPURL(qMsg.Text, page, s.cfg.SERP.ResultsPerPage)

			// INSERT directly — queryFeeder is not hot path (1 query/sec).
			s.db.Exec(`
				INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, status)
				VALUES ($1, $2, $3, $4, 'new')
				ON CONFLICT (id) DO NOTHING
			`, jobID, qMsg.ID, serpURL, page)

			s.pushSerpJob(ctx, jobID, serpURL, qMsg.ID, page)
		}
	}
}

// pushSerpJob enqueues a serp_job onto the Redis sorted-set queue.
// Score is the current time in microseconds — FIFO within the queue.
func (s *SERPStage) pushSerpJob(ctx context.Context, jobID, serpURL string, queryID int64, pageNum int) {
	payload := struct {
		ID      string `json:"id"`
		URL     string `json:"url"`
		QueryID int64  `json:"query_id"`
		PageNum int    `json:"page_num"`
	}{jobID, serpURL, queryID, pageNum}
	data, _ := json.Marshal(payload)
	score := float64(time.Now().UnixMicro())
	if err := s.redis.ZAdd(ctx, "serp:queue:serp", redis.Z{Score: score, Member: string(data)}).Err(); err != nil {
		slog.Warn("serp: push to queue failed", "job", jobID, "error", err)
	}
}

// tabWorker is one of N goroutines that pops jobs from the Redis queue and
// fetches SERP pages. All workers share the single pooled browser.
// Zero DB calls in hot path — claims via Redis SETNX, results via Redis lists.
func (s *SERPStage) tabWorker(ctx context.Context, tabID int) {
	workerID := fmt.Sprintf("serp-%s-%d", shortHostname(), tabID)
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

		// Redis SETNX claim — only one worker wins. If job is already claimed
		// (e.g. duplicate in Redis queue from reconciler), skip.
		ok, claimErr := s.redis.SetNX(ctx, "serp:lock:"+job.ID, workerID, 5*time.Minute).Result()
		if claimErr != nil || !ok {
			continue
		}

		// Snapshot the browser reference under lock.
		s.browserMu.Lock()
		browser := s.browser
		s.browserMu.Unlock()

		if browser == nil {
			slog.Warn("serp: browser is nil, re-queuing job", "tab", tabID, "job", job.ID)
			s.pushSerpJob(ctx, job.ID, job.URL, job.QueryID, job.PageNum)
			s.redis.Del(ctx, "serp:lock:"+job.ID) // release lock

			// Push requeue status to persister.
			result := persist.SERPResult{JobID: job.ID, Status: "requeue"}
			data, _ := json.Marshal(result)
			s.redis.RPush(ctx, persist.KeyResultSERP, string(data))

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
			// We don't know attempt_count without DB — use Redis counter.
			attemptKey := "serp:attempt:" + job.ID
			newAttempt, _ := s.redis.Incr(ctx, attemptKey).Result()
			s.redis.Expire(ctx, attemptKey, 1*time.Hour)

			slog.Warn("serp: fetch failed", "job", job.ID, "attempt", newAttempt, "tab", tabID, "error", fetchErr)

			maxAttempts := int64(5) // default
			if newAttempt >= maxAttempts {
				result := persist.SERPResult{
					JobID:        job.ID,
					Status:       "failed",
					ErrorMsg:     fetchErr.Error(),
					AttemptCount: int(newAttempt),
				}
				data, _ := json.Marshal(result)
				s.redis.RPush(ctx, persist.KeyResultSERP, string(data))
			} else {
				backoffSec := 30 * (1 << (newAttempt - 1))
				result := persist.SERPResult{
					JobID:        job.ID,
					Status:       "failed",
					ErrorMsg:     fetchErr.Error(),
					AttemptCount: int(newAttempt),
					BackoffSec:   int(backoffSec),
				}
				data, _ := json.Marshal(result)
				s.redis.RPush(ctx, persist.KeyResultSERP, string(data))
			}

			s.redis.Del(ctx, "serp:lock:"+job.ID) // release lock

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
			result := persist.SERPResult{
				JobID:    job.ID,
				Status:   "parse_failed",
				ErrorMsg: parseErr.Error(),
			}
			data, _ := json.Marshal(result)
			s.redis.RPush(ctx, persist.KeyResultSERP, string(data))
			s.redis.Del(ctx, "serp:lock:"+job.ID)
			continue
		}

		// Push URL results to Redis list (persister will INSERT to DB).
		inserted := 0
		for _, u := range urls {
			urlHash := dedup.HashURL(u)
			domain := dedup.ExtractDomain(u)
			if domain == "" {
				continue
			}

			// Push URL result to persister queue.
			urlResult := persist.URLResult{
				URL:       u,
				URLHash:   urlHash,
				Domain:    domain,
				QueryID:   job.QueryID,
				SerpJobID: job.ID,
			}
			data, _ := json.Marshal(urlResult)
			s.redis.RPush(ctx, persist.KeyResultURLs, string(data))

			// Push to enrich queue immediately (workers can start enriching
			// even before persister creates the DB record).
			if err := pushToQueue(ctx, s.redis, "serp:queue:enrich", u, job.QueryID, job.ID); err != nil {
				slog.Warn("serp: push to enrich queue failed", "url", u, "error", err)
			}
			inserted++
		}

		s.urlsFound.Add(int64(inserted))
		s.pagesProcessed.Add(1)

		// Push SERP completion to persister queue.
		result := persist.SERPResult{
			JobID:       job.ID,
			QueryID:     job.QueryID,
			ResultCount: len(urls),
			Status:      "completed",
		}
		data, _ := json.Marshal(result)
		s.redis.RPush(ctx, persist.KeyResultSERP, string(data))

		// Release lock.
		s.redis.Del(ctx, "serp:lock:"+job.ID)

		slog.Info("serp: page done", "job", job.ID, "found", len(urls), "new", inserted, "tab", tabID)

		// Check lifecycle — rotate browser when page-reuse limit is reached.
		if s.lifecycle.IncrementAndCheck() {
			slog.Info("serp: page reuse limit reached, rotating browser", "tab", tabID)
			s.restartBrowser()
		}

		// Inter-page delay — use configured SERPDelayMs or timing profile.
		var delay time.Duration
		if s.cfg.SERP.SERPDelayMs > 0 {
			delay = time.Duration(s.cfg.SERP.SERPDelayMs) * time.Millisecond
		} else {
			delay = s.timing.PaginationDelay()
		}
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
//
// NOTE: Reconciler still uses DB for recovery (not hot path).
// Workers never hit these queries during normal operation.
func (s *SERPStage) reconciler(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Fetch.ReconcilerIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// 1. Reset stuck processing jobs (>5 min without progress).
		stuckRows, err := s.db.Query(`
			UPDATE serp_jobs SET status = 'new', locked_by = NULL, locked_at = NULL, updated_at = NOW()
			WHERE status = 'processing' AND locked_at < NOW() - INTERVAL '5 minutes'
			RETURNING id, search_url, parent_job_id, page_num
		`)
		if err == nil {
			n := 0
			for stuckRows.Next() {
				var id, url string
				var queryID int64
				var pageNum int
				if err := stuckRows.Scan(&id, &url, &queryID, &pageNum); err == nil {
					s.redis.Del(ctx, "serp:lock:"+id) // clear stale lock
					s.pushSerpJob(ctx, id, url, queryID, pageNum)
					n++
				}
			}
			stuckRows.Close()
			if n > 0 {
				slog.Info("serp: reconciler reset+requeued stuck jobs", "count", n)
			}
		}

		// 2. Re-queue retry-eligible jobs whose backoff has elapsed.
		retryRows, _ := s.db.Query(`
			UPDATE serp_jobs SET updated_at = NOW()
			WHERE status = 'new' AND attempt_count > 0
			  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
			RETURNING id, search_url, parent_job_id, page_num
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

		// 3. Mark queries completed.
		completedRes, _ := s.db.Exec(`
			UPDATE queries SET
				status = 'completed',
				result_count = COALESCE((
					SELECT SUM(result_count) FROM serp_jobs WHERE parent_job_id = queries.id
				), 0),
				updated_at = NOW()
			WHERE status = 'processing'
			AND id IN (
				SELECT DISTINCT parent_job_id FROM serp_jobs
				WHERE status IN ('completed', 'failed')
				  AND updated_at > NOW() - INTERVAL '2 minutes'
			)
			AND NOT EXISTS (
				SELECT 1 FROM serp_jobs
				WHERE parent_job_id = queries.id
				  AND status IN ('new', 'processing')
			)
		`)
		if completedRes != nil {
			if n, _ := completedRes.RowsAffected(); n > 0 {
				slog.Info("serp: reconciler marked queries completed", "count", n)
				// Clean up query locks for completed queries.
			}
		}

		// 4. Requeue queries stuck in 'processing' with no active serp_jobs.
		stuckQRes, stuckQErr := s.db.Exec(`
			UPDATE queries SET status = 'pending', updated_at = NOW()
			WHERE id IN (
				SELECT q.id FROM queries q
				LEFT JOIN serp_jobs s ON s.parent_job_id = q.id
				WHERE q.status = 'processing'
				  AND q.updated_at < NOW() - INTERVAL '10 minutes'
				GROUP BY q.id
				HAVING COUNT(s.id) FILTER (WHERE s.status IN ('new', 'processing')) = 0
				LIMIT 100
			)
		`)
		if stuckQErr == nil {
			if n, _ := stuckQRes.RowsAffected(); n > 0 {
				slog.Info("serp: reconciler requeued stuck queries", "count", n)
				s.requeuePendingQueriesToRedis(ctx)
			}
		}

		// 5. Auto-expand completed queries.
		s.expandCompletedQueries()
	}
}

// queryExpanders are suffixes appended to a base keyword to generate variants.
var queryExpanders = []string{
	"near me",
	"reviews",
	"classes",
	"best rated",
	"\"@gmail.com\"",
	"\"@yahoo.com\"",
	"email",
	"contact",
	"instagram",
}

// expandCompletedQueries takes completed queries that yielded results and spawns
// variant queries by appending expander suffixes.
func (s *SERPStage) expandCompletedQueries() {
	rows, err := s.db.Query(`
		SELECT id, text FROM queries
		WHERE status = 'completed'
		  AND result_count > 0
		  AND expanded_at IS NULL
		ORDER BY id ASC
		LIMIT 50
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	expanded := 0
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			continue
		}

		for _, suffix := range queryExpanders {
			variant := text + " " + suffix
			inserted, insertErr := s.queryRepo.InsertBatch([]string{variant})
			if insertErr != nil {
				continue
			}
			if inserted > 0 {
				expanded++
			}
		}

		s.db.Exec(`UPDATE queries SET expanded_at = NOW() WHERE id = $1`, id)
	}

	if expanded > 0 {
		slog.Info("serp: auto-expanded completed queries into variants", "new_queries", expanded)
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
	} else if n, _ := res.RowsAffected(); n > 0 {
		slog.Info("serp: requeued stuck serp_jobs from previous run", "count", n)
	}

	qRes, qErr := s.db.Exec(`
		UPDATE queries SET status = 'pending', updated_at = NOW()
		WHERE status = 'processing'
	`)
	if qErr != nil {
		slog.Warn("serp: requeue stuck queries failed", "error", qErr)
	} else if n, _ := qRes.RowsAffected(); n > 0 {
		slog.Info("serp: requeued stuck queries from previous run", "count", n)
	}
}

// requeuePendingQueriesToRedis pushes all pending queries to the Redis query queue.
func (s *SERPStage) requeuePendingQueriesToRedis(ctx context.Context) {
	rows, err := s.db.Query(`SELECT id, text FROM queries WHERE status = 'pending' LIMIT 500`)
	if err != nil {
		return
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			continue
		}
		payload := struct {
			ID   int64  `json:"id"`
			Text string `json:"text"`
		}{id, text}
		data, _ := json.Marshal(payload)
		s.redis.ZAdd(ctx, query.QueueKey, redis.Z{Score: float64(id), Member: string(data)})
		n++
	}
	if n > 0 {
		slog.Info("serp: pushed pending queries to redis", "count", n)
	}
}

// pushToQueue pushes a URL job to a Redis sorted set queue used by enrich workers.
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

	data, _ := json.Marshal(job)

	return client.ZAdd(ctx, queueKey, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
}

// shortHostname returns the first 12 chars of the hostname.
func shortHostname() string {
	host, _ := os.Hostname()
	if len(host) > 12 {
		host = host[:12]
	}
	return host
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

