//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
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
	engines   []scraper.SearchEngine // enabled search engines

	// Shared browser — protected by mutex for rotation.
	browser   *fetch.CamoufoxFetcher
	browserMu sync.Mutex

	// Direct browser (no proxy) — fallback when proxy is blocked.
	// Lazy-initialized on first proxy failure. Protected by browserMu.
	directBrowser *fetch.CamoufoxFetcher

	// Beta: circuit breaker wraps browser fetch (nil when disabled).
	circuitBreaker foxhound.Middleware

	// Beta: session fatigue for human-like timing (nil when disabled).
	fatigue *behavior.SessionFatigue

	// Metrics.
	queriesProcessed atomic.Int64
	urlsFound        atomic.Int64
	pagesProcessed   atomic.Int64

	// Beta metrics (circuit breaker + fatigue + direct fallback).
	betaCBSkipped    atomic.Int64 // requests skipped by open circuit
	betaCBPassed     atomic.Int64 // requests allowed through circuit
	betaCBTripped    atomic.Int64 // times circuit opened
	betaDirectUsed   atomic.Int64 // requests served by direct (no-proxy) browser
	betaDirectOK     atomic.Int64 // successful direct fetches
	betaFatigueSum   atomic.Int64 // cumulative fatigue-adjusted delay (ms) for averaging
	betaFatigueN   atomic.Int64 // number of fatigue-adjusted delays
}

// NewSERPStage creates a new SERP discovery stage.
func NewSERPStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *SERPStage {
	engines := scraper.EnabledEngines(cfg.SERP.Engines)
	engineNames := make([]string, len(engines))
	for i, e := range engines {
		engineNames[i] = e.Name()
	}
	slog.Info("serp: engines enabled", "engines", engineNames)

	s := &SERPStage{
		cfg:            cfg,
		db:             database,
		redis:          dd.Client(),
		dedup:          dd,
		queryRepo:      query.NewRepositoryWithRedis(database, dd.Client()),
		timing:         behavior.NewTiming(behavior.CarefulProfile().Timing),
		engines:        engines,
		circuitBreaker: scraper.NewCircuitBreaker(cfg),
		fatigue:        scraper.NewSessionFatigue(cfg),
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
		if s.directBrowser != nil {
			s.directBrowser.Close()
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

		// Backpressure: if too many serp jobs queued, wait before generating more.
		depth, _ := s.redis.ZCard(ctx, "serp:queue:serp").Result()
		if depth > 10000 {
			slog.Info("serp: backpressure — queue depth too high, waiting", "depth", depth)
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
				// Re-queue the query for later processing.
				data, _ := json.Marshal(qMsg)
				s.redis.ZAdd(ctx, query.QueueKey, redis.Z{Score: float64(qMsg.ID), Member: string(data)})
				continue
			}
		}

		// Look up query country for geo-locale (empty = default US/en).
		var country string
		s.db.QueryRow(`SELECT COALESCE(country, '') FROM queries WHERE id = $1`, qMsg.ID).Scan(&country)
		locale := scraper.GetLocale(country)

		slog.Info("serp: generating jobs for query", "query", qMsg.Text, "id", qMsg.ID, "engines", len(s.engines), "country", country)

		// Generate jobs for ALL enabled engines.
		for _, eng := range s.engines {
			maxPages := eng.MaxPages()
			// Override max pages from config if set.
			switch eng.Name() {
			case "google":
				if s.cfg.SERP.GoogleMaxPages > 0 {
					maxPages = s.cfg.SERP.GoogleMaxPages
				}
			case "bing":
				if s.cfg.SERP.BingMaxPages > 0 {
					maxPages = s.cfg.SERP.BingMaxPages
				}
			case "duckduckgo":
				if s.cfg.SERP.DDGMaxPages > 0 {
					maxPages = s.cfg.SERP.DDGMaxPages
				}
			}

			// Get locale params for this engine.
			var gl, hl string
			switch eng.Name() {
			case "google":
				gl, hl = locale.GoogleGL, locale.GoogleHL
			case "bing":
				gl, hl = locale.BingCC, locale.BingLang
			case "duckduckgo":
				gl, hl = locale.DDGKL, "" // DDG uses single kl param
			}

			for page := 0; page < maxPages; page++ {
				jobID := fmt.Sprintf("%s-%d-p%d", eng.Name(), qMsg.ID, page)
				serpURL := eng.BuildURL(qMsg.Text, page, s.cfg.SERP.ResultsPerPage, gl, hl)

				s.db.Exec(`
					INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, engine, status)
					VALUES ($1, $2, $3, $4, $5, 'new')
					ON CONFLICT (id) DO UPDATE SET
						status = 'new', attempt_count = 0, error_msg = '', locked_by = NULL,
						next_attempt_at = NULL, updated_at = NOW()
					WHERE serp_jobs.status = 'failed'
				`, jobID, qMsg.ID, serpURL, page, eng.Name())

				s.pushSerpJob(ctx, jobID, serpURL, qMsg.ID, page)
			}
		}
	}
}

// pushSerpJob enqueues a serp_job onto the Redis sorted-set queue.
// Score is the current time in microseconds — FIFO within the queue.
func (s *SERPStage) pushSerpJob(ctx context.Context, jobID, serpURL string, queryID int64, pageNum int) {
	// Extract engine name from jobID prefix (e.g. "google-123-p0" → "google").
	engine := "google"
	if idx := strings.Index(jobID, "-"); idx > 0 {
		engine = jobID[:idx]
	}
	payload := struct {
		ID      string `json:"id"`
		URL     string `json:"url"`
		QueryID int64  `json:"query_id"`
		PageNum int    `json:"page_num"`
		Engine  string `json:"engine"`
	}{jobID, serpURL, queryID, pageNum, engine}
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

		// Parse job payload (now includes engine field).
		var job struct {
			ID      string `json:"id"`
			URL     string `json:"url"`
			QueryID int64  `json:"query_id"`
			PageNum int    `json:"page_num"`
			Engine  string `json:"engine"`
		}
		if err := json.Unmarshal([]byte(results[0].Member.(string)), &job); err != nil {
			slog.Warn("serp: invalid job in queue", "raw", results[0].Member, "error", err)
			continue
		}

		// Default to google for backward compat with old queue items.
		if job.Engine == "" {
			job.Engine = "google"
		}

		// Resolve engine implementation.
		eng := scraper.GetEngine(job.Engine)
		if eng == nil {
			slog.Warn("serp: unknown engine, skipping", "engine", job.Engine, "job", job.ID)
			continue
		}

		// Redis SETNX claim.
		ok, claimErr := s.redis.SetNX(ctx, "serp:lock:"+job.ID, workerID, 5*time.Minute).Result()
		if claimErr != nil || !ok {
			continue
		}

		// Fetch SERP page — route based on engine type.
		var body []byte
		var fetchErr error

		if eng.NeedsBrowser() {
			// Beta: apply session fatigue to inter-request delay.
			if s.fatigue != nil {
				base := s.timing.Delay()
				adjusted := s.fatigue.AdjustDelay(base)
				s.betaFatigueSum.Add(adjusted.Milliseconds())
				s.betaFatigueN.Add(1)
				time.Sleep(adjusted)
			}

			s.browserMu.Lock()
			browser := s.browser
			s.browserMu.Unlock()

			if browser == nil {
				slog.Warn("serp: browser is nil, re-queuing job", "tab", tabID, "job", job.ID)
				s.pushSerpJob(ctx, job.ID, job.URL, job.QueryID, job.PageNum)
				s.redis.Del(ctx, "serp:lock:"+job.ID)
				result := persist.SERPResult{JobID: job.ID, Status: "requeue"}
				data, _ := json.Marshal(result)
				s.redis.RPush(ctx, persist.KeyResultSERP, string(data))
				time.Sleep(5 * time.Second)
				continue
			}

			// Beta: circuit breaker — when proxy is blocked, fallback to direct browser.
			if s.circuitBreaker != nil {
				cbFetcher := s.circuitBreaker.Wrap(browser)
				probeResp, probeErr := cbFetcher.Fetch(ctx, &foxhound.Job{
					ID: job.ID, URL: job.URL, Method: "GET", FetchMode: foxhound.FetchBrowser,
				})
				if probeErr == nil && probeResp != nil && probeResp.StatusCode == 503 {
					// Circuit is open (proxy blocked) — try direct browser.
					s.betaCBSkipped.Add(1)
					directBody, directErr := s.fetchDirect(ctx, job.ID, job.URL, eng)
					if directErr == nil && directBody != nil {
						body = directBody
					} else {
						fetchErr = fmt.Errorf("proxy: circuit open, direct: %v", directErr)
					}
				} else if probeErr == nil && probeResp != nil {
					s.betaCBPassed.Add(1)
					body = probeResp.Body
					if eng.IsCaptchaPage(body) {
						s.betaCBTripped.Add(1)
					}
				} else {
					s.betaCBTripped.Add(1)
					fetchErr = probeErr
				}
			}

			// Normal fetch (when circuit breaker is disabled or didn't handle it).
			if body == nil && fetchErr == nil {
				steps := eng.FetchSteps()
				if len(steps) > 0 {
					body, fetchErr = scraper.FetchSERPWithEngine(ctx, browser, job.URL, job.ID, steps)
				} else {
					body, fetchErr = scraper.FetchSERP(ctx, browser, job.URL, job.ID)
				}
			}
		} else {
			// DDG path — stealth HTTP (plain HTML endpoint).
			stealth := scraper.NewStealth(s.cfg)
			body, fetchErr = scraper.FetchSERPStealth(ctx, stealth, job.URL, job.ID)
			stealth.Close()
		}

		// Treat captcha pages as a fetch error (engine-specific detection).
		if fetchErr == nil && eng.IsCaptchaPage(body) {
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
				shift := newAttempt - 1
				if shift < 0 {
					shift = 0
				}
				backoffSec := 30 * (1 << shift)
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

			// Rotate browser after captcha/block (only if engine uses browser).
			if eng.NeedsBrowser() && s.lifecycle.IncrementAndCheck() {
				s.restartBrowser()
			}
			continue
		}

		// Parse SERP results using engine-specific parser.
		urls, parseErr := eng.ParseResults(body)
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
		// Check enrich queue depth for backpressure — skip enrich push if over cap.
		// URLs still go to websites table via persister; enrich reconciler will pick them up later.
		enrichDepth, _ := s.redis.ZCard(ctx, "serp:queue:enrich").Result()
		skipEnrich := enrichDepth > 50000

		inserted := 0
		for _, u := range urls {
			urlHash := dedup.HashURL(u)
			domain := dedup.ExtractDomain(u)
			if domain == "" {
				continue
			}

			// Push URL result to persister queue (always — creates DB record).
			urlResult := persist.URLResult{
				URL:       u,
				URLHash:   urlHash,
				Domain:    domain,
				QueryID:   job.QueryID,
				SerpJobID: job.ID,
			}
			data, _ := json.Marshal(urlResult)
			s.redis.RPush(ctx, persist.KeyResultURLs, string(data))

			// Push to enrich queue only if below cap.
			// When over cap, URLs wait in websites table (status=pending).
			// Enrich reconciler will feed them when queue drains.
			if !skipEnrich {
				if err := pushToQueue(ctx, s.redis, "serp:queue:enrich", u, job.QueryID, job.ID); err != nil {
					slog.Warn("serp: push to enrich queue failed", "url", u, "error", err)
				}
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

		slog.Info("serp: page done", "job", job.ID, "engine", job.Engine, "found", len(urls), "new", inserted, "tab", tabID)

		// Check lifecycle — rotate browser when page-reuse limit is reached (browser engines only).
		if eng.NeedsBrowser() && s.lifecycle.IncrementAndCheck() {
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

// fetchDirect tries fetching a SERP page via the direct (no-proxy) browser.
// Lazy-initializes the direct browser on first call. Only 1 tab to protect server IP.
// Returns nil body + error if direct also fails.
func (s *SERPStage) fetchDirect(ctx context.Context, jobID, jobURL string, eng scraper.SearchEngine) ([]byte, error) {
	s.browserMu.Lock()
	if s.directBrowser == nil {
		db, err := scraper.NewSERPBrowserDirect(s.cfg)
		if err != nil {
			s.browserMu.Unlock()
			return nil, fmt.Errorf("direct browser init: %w", err)
		}
		s.directBrowser = db
	}
	db := s.directBrowser
	s.browserMu.Unlock()

	s.betaDirectUsed.Add(1)

	steps := eng.FetchSteps()
	var body []byte
	var err error
	if len(steps) > 0 {
		body, err = scraper.FetchSERPWithEngine(ctx, db, jobURL, "direct-"+jobID, steps)
	} else {
		body, err = scraper.FetchSERP(ctx, db, jobURL, "direct-"+jobID)
	}

	if err != nil {
		return nil, err
	}

	// Check if direct also got captcha.
	if eng.IsCaptchaPage(body) {
		return nil, fmt.Errorf("captcha on direct")
	}

	s.betaDirectOK.Add(1)
	slog.Info("serp: direct fetch succeeded", "job", jobID, "engine", eng.Name())
	return body, nil
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
			WHERE id IN (
				SELECT id FROM serp_jobs
				WHERE status = 'processing' AND locked_at < NOW() - INTERVAL '5 minutes'
				LIMIT 200
			)
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
		// Skip if serp queue is already deep — no point adding more to a full queue.
		serpQueueDepth, _ := s.redis.ZCard(ctx, "serp:queue:serp").Result()
		retryLimit := 500
		if serpQueueDepth > 10000 {
			retryLimit = 0
		} else if serpQueueDepth > 5000 {
			retryLimit = 50
		}
		var retryRows *sql.Rows
		if retryLimit > 0 {
			retryRows, _ = s.db.Query(fmt.Sprintf(`
				UPDATE serp_jobs SET updated_at = NOW()
				WHERE id IN (
					SELECT id FROM serp_jobs
					WHERE status = 'new' AND attempt_count > 0
					  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
					LIMIT %d
				)
				RETURNING id, search_url, parent_job_id, page_num
			`, retryLimit))
		}
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
		// Uses recently-updated serp_jobs as candidates to avoid full table scan.
		completedRes, _ := s.db.Exec(`
			UPDATE queries SET
				status = 'completed',
				result_count = sub.total_results,
				updated_at = NOW()
			FROM (
				SELECT s.parent_job_id, SUM(s.result_count) AS total_results
				FROM serp_jobs s
				WHERE s.parent_job_id IN (
					SELECT DISTINCT parent_job_id FROM serp_jobs
					WHERE status IN ('completed', 'failed')
					  AND updated_at > NOW() - INTERVAL '2 minutes'
					LIMIT 200
				)
				GROUP BY s.parent_job_id
				HAVING COUNT(*) FILTER (WHERE s.status IN ('new', 'processing')) = 0
			) sub
			WHERE queries.id = sub.parent_job_id
			  AND queries.status = 'processing'
		`)
		if completedRes != nil {
			if n, _ := completedRes.RowsAffected(); n > 0 {
				slog.Info("serp: reconciler marked queries completed", "count", n)
				// Clean up query locks for completed queries.
			}
		}

		// 4. Requeue queries stuck in 'processing' with no active serp_jobs.
		// These are zombies — all serp_jobs finished (completed or failed) but query never transitioned.
		serpDepth, _ := s.redis.ZCard(ctx, "serp:queue:serp").Result()
		requeueLimit := 500
		if serpDepth > 5000 {
			requeueLimit = 0 // backpressure: don't add more if serp queue is busy
		} else if serpDepth > 2000 {
			requeueLimit = 50
		}
		if requeueLimit > 0 {
			stuckQRes, stuckQErr := s.db.Exec(fmt.Sprintf(`
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
			`, requeueLimit))
			if stuckQErr == nil {
				if n, _ := stuckQRes.RowsAffected(); n > 0 {
					slog.Info("serp: reconciler requeued stuck queries", "count", n)
					s.requeuePendingQueriesToRedis(ctx)
				}
			}
		}

		// 5. Auto-expand completed queries.
		s.expandCompletedQueries()

		// 6. Beta metrics report (only when beta features are active).
		if s.circuitBreaker != nil || s.fatigue != nil {
			cbSkipped := s.betaCBSkipped.Load()
			cbPassed := s.betaCBPassed.Load()
			cbTripped := s.betaCBTripped.Load()
			fatigueN := s.betaFatigueN.Load()
			avgFatigueMs := int64(0)
			if fatigueN > 0 {
				avgFatigueMs = s.betaFatigueSum.Load() / fatigueN
			}

			// Compute circuit breaker skip rate.
			cbTotal := cbSkipped + cbPassed
			cbSkipPct := 0.0
			if cbTotal > 0 {
				cbSkipPct = float64(cbSkipped) / float64(cbTotal) * 100
			}

			directUsed := s.betaDirectUsed.Load()
			directOK := s.betaDirectOK.Load()
			directSuccPct := 0.0
			if directUsed > 0 {
				directSuccPct = float64(directOK) / float64(directUsed) * 100
			}

			slog.Info("beta-metrics: serp",
				"cb_skipped", cbSkipped,
				"cb_passed", cbPassed,
				"cb_tripped", cbTripped,
				"cb_skip_pct", fmt.Sprintf("%.1f%%", cbSkipPct),
				"direct_used", directUsed,
				"direct_ok", directOK,
				"direct_succ_pct", fmt.Sprintf("%.1f%%", directSuccPct),
				"fatigue_avg_ms", avgFatigueMs,
				"fatigue_samples", fatigueN,
			)
		}
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
	// Use subquery with LIMIT to avoid full-table scan timeout on large tables.
	res, err := s.db.Exec(`
		UPDATE serp_jobs SET status = 'new', locked_by = NULL, locked_at = NULL, updated_at = NOW()
		WHERE id IN (SELECT id FROM serp_jobs WHERE status = 'processing' LIMIT 500)
	`)
	if err != nil {
		slog.Warn("serp: requeueStuckJobs failed", "error", err)
	} else if n, _ := res.RowsAffected(); n > 0 {
		slog.Info("serp: requeued stuck serp_jobs from previous run", "count", n)
	}

	qRes, qErr := s.db.Exec(`
		UPDATE queries SET status = 'pending', updated_at = NOW()
		WHERE id IN (SELECT id FROM queries WHERE status = 'processing' LIMIT 500)
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

