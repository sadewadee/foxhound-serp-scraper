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
	"github.com/sadewadee/serp-scraper/internal/feeder"
	"github.com/sadewadee/serp-scraper/internal/query"
	"github.com/sadewadee/serp-scraper/internal/scraper"
)

// SERPStage runs SERP discovery with a shared browser pool.
// Workers pop from serp:buffer (fed by SERPFeeder) and write results
// directly to DB (serp_results + serp_jobs UPDATE).
type SERPStage struct {
	cfg       *config.Config
	db        *sql.DB
	redis     *redis.Client
	dedup     *dedup.Store
	queryRepo *query.Repository
	timing    *behavior.Timing
	lifecycle *scraper.BrowserLifecycle
	engines   []scraper.SearchEngine

	browser       *fetch.CamoufoxFetcher
	browserMu     sync.Mutex
	directBrowser *fetch.CamoufoxFetcher

	circuitBreaker foxhound.Middleware
	fatigue        *behavior.SessionFatigue

	queriesProcessed atomic.Int64
	urlsFound        atomic.Int64
	pagesProcessed   atomic.Int64

	// 429 cooldown: when consecutive 429s exceed threshold, all tabs back off.
	consecutive429 atomic.Int64

	betaCBSkipped  atomic.Int64
	betaCBPassed   atomic.Int64
	betaCBTripped  atomic.Int64
	betaDirectUsed atomic.Int64
	betaDirectOK   atomic.Int64
	betaFatigueSum atomic.Int64
	betaFatigueN   atomic.Int64
}

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
	s.lifecycle = scraper.NewBrowserLifecycle(cfg, func(c *config.Config) (*fetch.CamoufoxFetcher, error) {
		return scraper.NewSERPBrowserWithPool(c, c.SERP.Concurrency)
	}, "serp")
	return s
}

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

	// Touch health file first so Docker never sees us as unhealthy while
	// warm-up is in flight — otherwise a stuck fetch loops container restarts.
	go touchHealthFile(ctx, "/tmp/worker-healthy")

	googleEnabled := false
	for _, eng := range s.engines {
		if eng.Name() == "google" {
			googleEnabled = true
			break
		}
	}
	if googleEnabled {
		slog.Info("serp: warming up browser — visiting google.com")
		warmupCtx, warmupCancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := browser.Fetch(warmupCtx, &foxhound.Job{
			ID: "warmup", URL: "https://www.google.com/", Method: "GET",
			FetchMode: foxhound.FetchBrowser,
		})
		warmupCancel()
		if err != nil {
			slog.Warn("serp: warm-up failed, continuing anyway", "error", err)
		} else {
			time.Sleep(3 * time.Second)
			slog.Info("serp: warm-up done")
		}
	} else {
		slog.Info("serp: skipping warm-up (google not in engines)")
	}
	go s.heartbeat(ctx)
	go s.reconciler(ctx)
	go s.queryFeeder(ctx)

	// Start the DB-to-Redis buffer feeder.
	engineNames := make([]string, len(s.engines))
	for i, e := range s.engines {
		engineNames[i] = e.Name()
	}
	serpFeeder := feeder.NewSERPFeeder(s.db, s.redis, engineNames)
	go serpFeeder.Run(ctx)

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

// heartbeat upserts the workers table every 30s so the reconciler and
// Telegram /status command can report worker health.
func (s *SERPStage) heartbeat(ctx context.Context) {
	workerID := fmt.Sprintf("serp-%s", shortHostname())

	// Register on startup.
	s.db.Exec(`INSERT INTO workers (worker_id, worker_type, status, last_heartbeat, started_at)
		VALUES ($1, 'serp', 'idle', NOW(), NOW())
		ON CONFLICT (worker_id) DO UPDATE SET status = 'idle', last_heartbeat = NOW(), started_at = NOW()`,
		workerID)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.db.Exec(`UPDATE workers SET status = 'dead', last_heartbeat = NOW() WHERE worker_id = $1`, workerID)
			return
		case <-ticker.C:
			pages := s.pagesProcessed.Load()
			emails := s.urlsFound.Load()
			s.db.Exec(`UPDATE workers SET
				pages_delta = $1 - pages_prev,
				emails_delta = $2 - emails_prev,
				pages_prev = $1, emails_prev = $2,
				pages_processed = $1, emails_found = $2,
				delta_at = NOW(), last_heartbeat = NOW(),
				status = 'working'
			WHERE worker_id = $3`, pages, emails, workerID)
		}
	}
}

// queryFeeder pops queries from the Redis query queue, generates per-page
// serp_jobs in DB. The SERPFeeder goroutine picks them up and feeds them
// to serp:buffer for tab workers.
func (s *SERPStage) queryFeeder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

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

		// Backpressure: if too many pending serp jobs, wait.
		// Use a bounded count (LIMIT 50001) to avoid scanning millions of rows.
		var pendingCount int
		s.db.QueryRow(`SELECT COUNT(*) FROM (SELECT 1 FROM serp_jobs WHERE status = 'new' AND created_at > NOW() - INTERVAL '6 hours' LIMIT 50001) sub`).Scan(&pendingCount)
		if pendingCount > 50000 {
			slog.Info("serp: backpressure — too many pending serp jobs", "count", pendingCount)
			data, _ := json.Marshal(qMsg)
			s.redis.ZAdd(ctx, query.QueueKey, redis.Z{Score: float64(time.Now().Unix() + 60), Member: string(data)})
			s.db.Exec(`UPDATE queries SET status = 'pending', updated_at = NOW() WHERE id = $1`, qMsg.ID)
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
				continue
			}
		}

		var country string
		s.db.QueryRow(`SELECT COALESCE(country, '') FROM queries WHERE id = $1`, qMsg.ID).Scan(&country)
		locale := scraper.GetLocale(country)

		slog.Info("serp: generating jobs for query", "query", qMsg.Text, "id", qMsg.ID, "engines", len(s.engines), "country", country)

		for _, eng := range s.engines {
			maxPages := eng.MaxPages()
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

			var gl, hl string
			switch eng.Name() {
			case "google":
				gl, hl = locale.GoogleGL, locale.GoogleHL
			case "bing":
				gl, hl = locale.BingCC, locale.BingLang
			case "duckduckgo":
				gl, hl = locale.DDGKL, ""
			}

			for page := 0; page < maxPages; page++ {
				jobID := fmt.Sprintf("%s-%d-p%d", eng.Name(), qMsg.ID, page)
				serpURL := eng.BuildURL(qMsg.Text, page, s.cfg.SERP.ResultsPerPage, gl, hl)

				s.db.Exec(`
					INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, engine, status)
					VALUES ($1, $2, $3, $4, $5, 'new')
					ON CONFLICT (id) DO UPDATE SET
						status = 'new', attempt_count = 0, error_msg = '', locked_by = NULL,
						next_attempt_at = NULL, picked_at = NULL, updated_at = NOW()
					WHERE serp_jobs.status = 'failed'
				`, jobID, qMsg.ID, serpURL, page, eng.Name())
			}
		}
	}
}

// tabWorker pops from serp:buffer (Redis LIST), fetches SERP pages,
// and writes results directly to DB.
func (s *SERPStage) tabWorker(ctx context.Context, tabID int) {
	workerID := fmt.Sprintf("serp-%s-%d", shortHostname(), tabID)
	slog.Info("serp: tab worker starting", "tab", tabID)

	// Shared stealth fetcher for non-browser engines (Bing, DDG).
	// Avoids creating a new TLS connection + identity per request.
	stealth := scraper.NewStealth(s.cfg)
	stealthCount := 0
	stealthRecycleAfter := s.cfg.Fetch.StealthRecycleAfter
	defer stealth.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// BLPOP from serp:buffer.
		result, err := s.redis.BLPop(ctx, 5*time.Second, feeder.SERPBufferKey).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if len(result) < 2 {
			continue
		}

		var job feeder.SERPBufferItem
		if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
			slog.Warn("serp: invalid job in buffer", "error", err)
			continue
		}

		if job.Engine == "" {
			job.Engine = "google"
		}

		eng := scraper.GetEngine(job.Engine)
		if eng == nil {
			slog.Warn("serp: unknown engine, skipping", "engine", job.Engine, "job", job.ID)
			continue
		}

		// Guard: refuse jobs whose engine is not in the configured set. Legacy
		// rows (e.g. Google jobs from before SERP_ENGINES was narrowed) still
		// live in serp_jobs and would otherwise burn browser time on reCAPTCHA.
		engineEnabled := false
		for _, e := range s.engines {
			if e.Name() == job.Engine {
				engineEnabled = true
				break
			}
		}
		if !engineEnabled {
			slog.Warn("serp: engine disabled, marking dead", "engine", job.Engine, "job", job.ID)
			s.db.Exec(`UPDATE serp_jobs SET status='dead', error_msg='engine disabled in SERP_ENGINES', locked_by=NULL, updated_at=NOW() WHERE id=$1`, job.ID)
			s.redis.Del(ctx, "serp:lock:"+job.ID)
			continue
		}

		// Redis SETNX claim.
		ok, claimErr := s.redis.SetNX(ctx, "serp:lock:"+job.ID, workerID, 5*time.Minute).Result()
		if claimErr != nil || !ok {
			continue
		}

		// DB claim: set locked_by on the job the feeder already marked 'processing'.
		// The feeder transitions status new→processing when pushing to serp:buffer,
		// so we match on 'processing' (not 'new') to set ownership.
		s.db.Exec(`UPDATE serp_jobs SET locked_by = $1, locked_at = NOW(), updated_at = NOW() WHERE id = $2 AND status = 'processing'`, workerID, job.ID)

		// --- Fetch SERP page ---
		var body []byte
		var fetchErr error

		if eng.NeedsBrowser() {
			// Fix 3: Consecutive 429 cooldown — if too many 429s, back off all tabs.
			if c429 := s.consecutive429.Load(); c429 >= 5 {
				cooldown := time.Duration(2) * time.Minute
				slog.Warn("serp: 429 cooldown — too many rate limits, backing off", "consecutive", c429, "cooldown", cooldown, "tab", tabID)
				s.redis.Del(ctx, "serp:lock:"+job.ID)
				s.db.Exec(`UPDATE serp_jobs SET status = 'new', locked_by = NULL, picked_at = NULL, updated_at = NOW() WHERE id = $1`, job.ID)
				select {
				case <-ctx.Done():
					return
				case <-time.After(cooldown):
				}
				continue
			}

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
				slog.Warn("serp: browser is nil, releasing job", "tab", tabID, "job", job.ID)
				s.redis.Del(ctx, "serp:lock:"+job.ID)
				s.db.Exec(`UPDATE serp_jobs SET status = 'new', locked_by = NULL, picked_at = NULL, updated_at = NOW() WHERE id = $1`, job.ID)
				time.Sleep(5 * time.Second)
				continue
			}

			// Fix 1: Per-fetch timeout — prevent browser.Fetch from hanging forever.
			fetchCtx, fetchCancel := context.WithTimeout(ctx, 45*time.Second)

			if s.circuitBreaker != nil {
				cbFetcher := s.circuitBreaker.Wrap(browser)
				probeResp, probeErr := cbFetcher.Fetch(fetchCtx, &foxhound.Job{
					ID: job.ID, URL: job.URL, Method: "GET", FetchMode: foxhound.FetchBrowser,
				})
				if probeErr == nil && probeResp != nil && probeResp.StatusCode == 503 {
					s.betaCBSkipped.Add(1)
					directBody, directErr := s.fetchDirect(fetchCtx, job.ID, job.URL, eng)
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

			if body == nil && fetchErr == nil {
				steps := eng.FetchSteps()
				if len(steps) > 0 {
					body, fetchErr = scraper.FetchSERPWithEngine(fetchCtx, browser, job.URL, job.ID, steps)
				} else {
					body, fetchErr = scraper.FetchSERP(fetchCtx, browser, job.URL, job.ID)
				}
			}
			fetchCancel()
		} else {
			// Recycle stealth fetcher periodically to rotate identity/TLS fingerprint.
			stealthCount++
			if stealthCount >= stealthRecycleAfter {
				stealth.Close()
				stealth = scraper.NewStealth(s.cfg)
				stealthCount = 0
				slog.Info("serp: stealth recycled", "tab", tabID, "engine", job.Engine)
			}
			// Per-fetch timeout for stealth too.
			stealthCtx, stealthCancel := context.WithTimeout(ctx, 30*time.Second)
			body, fetchErr = scraper.FetchSERPStealth(stealthCtx, stealth, job.URL, job.ID)
			stealthCancel()
		}

		if fetchErr == nil && eng.IsCaptchaPage(body) {
			fetchErr = fmt.Errorf("captcha detected")
		}

		if fetchErr != nil {
			attemptKey := "serp:attempt:" + job.ID
			newAttempt, _ := s.redis.Incr(ctx, attemptKey).Result()
			s.redis.Expire(ctx, attemptKey, 1*time.Hour)

			errStr := fetchErr.Error()
			slog.Warn("serp: fetch failed", "job", job.ID, "attempt", newAttempt, "tab", tabID, "error", fetchErr)

			// Fix 2: 429 backoff — sleep this tab before picking next job.
			is429 := strings.Contains(errStr, "429")
			isCaptcha := strings.Contains(errStr, "captcha")
			if is429 || isCaptcha {
				s.consecutive429.Add(1)
				backoff429 := 30 * time.Second
				slog.Warn("serp: rate limited, tab backing off", "tab", tabID, "backoff", backoff429)
				select {
				case <-ctx.Done():
				case <-time.After(backoff429):
				}
			} else {
				// Not a 429 — reset consecutive counter.
				s.consecutive429.Store(0)
			}

			maxAttempts := int64(5)
			if newAttempt >= maxAttempts {
				s.db.Exec(`UPDATE serp_jobs SET status = 'failed', attempt_count = $1, error_msg = $2, locked_by = NULL, updated_at = NOW() WHERE id = $3`,
					int(newAttempt), errStr, job.ID)
			} else {
				shift := newAttempt - 1
				if shift < 0 {
					shift = 0
				}
				backoffSec := 30 * (1 << shift)
				s.db.Exec(`UPDATE serp_jobs SET status = 'new', attempt_count = $1, next_attempt_at = NOW() + interval '1 second' * $2, error_msg = $3, locked_by = NULL, picked_at = NULL, updated_at = NOW() WHERE id = $4`,
					int(newAttempt), backoffSec, errStr, job.ID)
			}

			s.redis.Del(ctx, "serp:lock:"+job.ID)

			if eng.NeedsBrowser() && s.lifecycle.IncrementAndCheck() {
				s.restartBrowser()
			}
			continue
		}

		// Success — reset 429 cooldown counter.
		s.consecutive429.Store(0)

		// Parse SERP results.
		urls, parseErr := eng.ParseResults(body)
		if parseErr != nil {
			slog.Warn("serp: parse failed", "job", job.ID, "error", parseErr)
			s.db.Exec(`UPDATE serp_jobs SET status = 'failed', error_msg = $1, locked_by = NULL, updated_at = NOW() WHERE id = $2`,
				parseErr.Error(), job.ID)
			s.redis.Del(ctx, "serp:lock:"+job.ID)
			continue
		}

		// DB direct writes — INSERT serp_results + UPDATE serp_jobs.
		tx, txErr := s.db.BeginTx(ctx, nil)
		if txErr != nil {
			slog.Warn("serp: begin tx failed", "error", txErr)
			s.redis.Del(ctx, "serp:lock:"+job.ID)
			continue
		}

		inserted := 0
		for _, u := range urls {
			urlHash := dedup.HashURL(u)
			domain := dedup.ExtractDomain(u)
			if domain == "" {
				continue
			}

			// INSERT into serp_results — trigger auto-creates enrichment_jobs.
			_, insertErr := tx.Exec(`
				INSERT INTO serp_results (url, url_hash, domain, source_query_id, source_serp_id)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (url_hash) DO NOTHING
			`, u, urlHash, domain, job.QueryID, job.ID)
			if insertErr != nil {
				slog.Debug("serp: insert serp_result failed", "url", u, "error", insertErr)
				continue
			}
			inserted++
		}

		tx.Exec(`UPDATE serp_jobs SET status = 'completed', result_count = $1, locked_by = NULL, updated_at = NOW() WHERE id = $2`,
			len(urls), job.ID)

		if err := tx.Commit(); err != nil {
			slog.Warn("serp: tx commit failed", "error", err)
			tx.Rollback()
		}

		s.urlsFound.Add(int64(inserted))
		s.pagesProcessed.Add(1)
		s.redis.Del(ctx, "serp:lock:"+job.ID)

		slog.Info("serp: page done", "job", job.ID, "engine", job.Engine, "found", len(urls), "new", inserted, "tab", tabID)

		if eng.NeedsBrowser() && s.lifecycle.IncrementAndCheck() {
			slog.Info("serp: page reuse limit reached, rotating browser", "tab", tabID)
			s.restartBrowser()
		}

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

func (s *SERPStage) restartBrowser() {
	s.browserMu.Lock()
	defer s.browserMu.Unlock()

	old := s.browser
	s.browser = nil

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
	if eng.IsCaptchaPage(body) {
		return nil, fmt.Errorf("captcha on direct")
	}

	s.betaDirectOK.Add(1)
	slog.Info("serp: direct fetch succeeded", "job", jobID, "engine", eng.Name())
	return body, nil
}

// reconciler runs periodically to handle recovery.
func (s *SERPStage) reconciler(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.cfg.Fetch.ReconcilerIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// 1. Reset stuck processing jobs (>5 min).
		res, err := s.db.Exec(`
			UPDATE serp_jobs SET status = 'new', locked_by = NULL, locked_at = NULL, picked_at = NULL, updated_at = NOW()
			WHERE id IN (
				SELECT id FROM serp_jobs
				WHERE status = 'processing' AND locked_at < NOW() - INTERVAL '5 minutes'
				LIMIT 200
			)
		`)
		if err == nil {
			if n, _ := res.RowsAffected(); n > 0 {
				slog.Info("serp: reconciler reset stuck jobs", "count", n)
			}
		}

		// 2. Mark queries completed when all their serp_jobs are done.
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
			}
		}

		// 3. Requeue zombie processing queries.
		// Use bounded count to avoid scanning millions of ancient recycled rows.
		var pendingCount int
		s.db.QueryRow(`SELECT COUNT(*) FROM (SELECT 1 FROM serp_jobs WHERE status = 'new' AND created_at > NOW() - INTERVAL '6 hours' LIMIT 5001) sub`).Scan(&pendingCount)
		if pendingCount < 5000 {
			stuckQRes, _ := s.db.Exec(`
				UPDATE queries SET status = 'pending', updated_at = NOW()
				WHERE id IN (
					SELECT q.id FROM queries q
					WHERE q.status = 'processing'
					  AND q.updated_at < NOW() - INTERVAL '10 minutes'
					  AND NOT EXISTS (
						SELECT 1 FROM serp_jobs s
						WHERE s.parent_job_id = q.id AND s.status IN ('new', 'processing')
					)
					LIMIT 500
				)
			`)
			if stuckQRes != nil {
				if n, _ := stuckQRes.RowsAffected(); n > 0 {
					slog.Info("serp: reconciler requeued stuck queries", "count", n)
					s.requeuePendingQueriesToRedis(ctx)
				}
			}
		}

		// 4. Auto-expand completed queries.
		s.expandCompletedQueries()

		// 5. Beta metrics.
		if s.circuitBreaker != nil || s.fatigue != nil {
			cbSkipped := s.betaCBSkipped.Load()
			cbPassed := s.betaCBPassed.Load()
			cbTripped := s.betaCBTripped.Load()
			fatigueN := s.betaFatigueN.Load()
			avgFatigueMs := int64(0)
			if fatigueN > 0 {
				avgFatigueMs = s.betaFatigueSum.Load() / fatigueN
			}
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
				"cb_skipped", cbSkipped, "cb_passed", cbPassed, "cb_tripped", cbTripped,
				"cb_skip_pct", fmt.Sprintf("%.1f%%", cbSkipPct),
				"direct_used", directUsed, "direct_ok", directOK,
				"direct_succ_pct", fmt.Sprintf("%.1f%%", directSuccPct),
				"fatigue_avg_ms", avgFatigueMs, "fatigue_samples", fatigueN,
			)
		}
	}
}

var queryExpanders = []string{
	"near me", "reviews", "classes", "best rated",
	"\"@gmail.com\"", "\"@yahoo.com\"", "email", "contact", "instagram",
}

func (s *SERPStage) expandCompletedQueries() {
	rows, err := s.db.Query(`
		SELECT id, text FROM queries
		WHERE status = 'completed' AND result_count > 0 AND expanded_at IS NULL
		ORDER BY id ASC LIMIT 50
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
				// Mark variant as already expanded so it won't be expanded again (depth-1 limit).
				s.db.Exec(`UPDATE queries SET expanded_at = NOW() WHERE text_hash = $1 AND expanded_at IS NULL`,
					dedup.HashQuery(variant))
			}
		}
		s.db.Exec(`UPDATE queries SET expanded_at = NOW() WHERE id = $1`, id)
	}
	if expanded > 0 {
		slog.Info("serp: auto-expanded completed queries into variants", "new_queries", expanded)
	}
}

func (s *SERPStage) requeueStuckJobs() {
	res, err := s.db.Exec(`
		UPDATE serp_jobs SET status = 'new', locked_by = NULL, locked_at = NULL, picked_at = NULL, updated_at = NOW()
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

func shortHostname() string {
	host, _ := os.Hostname()
	if len(host) > 12 {
		host = host[:12]
	}
	return host
}

func (s *SERPStage) QueriesProcessed() int64 { return s.queriesProcessed.Load() }
func (s *SERPStage) URLsFound() int64        { return s.urlsFound.Load() }
func (s *SERPStage) PagesProcessed() int64   { return s.pagesProcessed.Load() }
