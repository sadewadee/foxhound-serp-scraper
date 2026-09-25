//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
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
	pagesIrrelevant  atomic.Int64

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
			// Score in UnixMicro to MATCH pushQueryToQueue (the queue is scored in
			// micros); +60s places this behind currently-enqueued items. Scoring in
			// Unix seconds (~1.7e9) would sort BELOW every micro-scored item (~1.7e15)
			// and jump re-queues to the FRONT — the tight loop Invariant #3 forbids.
			s.redis.ZAdd(ctx, query.QueueKey, redis.Z{Score: float64(time.Now().Add(60 * time.Second).UnixMicro()), Member: string(data)})
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
						status = 'new', error_msg = '', locked_by = NULL,
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
			errStr := fetchErr.Error()

			// 429 backoff — sleep this tab before picking next job.
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

			// Atomic SQL increment: attempt_count increments in DB without relying on
			// transient Redis counters (Invariant #1). Cap against serp_jobs.max_attempts.
			var newAttempt, maxAttempts int
			var newStatus string
			rowErr := s.db.QueryRowContext(ctx, `
				UPDATE serp_jobs SET
					attempt_count = attempt_count + 1,
					status = CASE WHEN attempt_count + 1 >= max_attempts THEN 'failed' ELSE 'new' END,
					next_attempt_at = CASE WHEN attempt_count + 1 >= max_attempts THEN NULL
						ELSE NOW() + interval '1 second' * (30 * power(2, LEAST(6, attempt_count))) END,
					error_msg = $1,
					locked_by = NULL,
					picked_at = NULL,
					updated_at = NOW()
				WHERE id = $2
				RETURNING attempt_count, max_attempts, status
			`, errStr, job.ID).Scan(&newAttempt, &maxAttempts, &newStatus)
			if rowErr != nil {
				slog.Error("serp: update failed job error", "job", job.ID, "error", rowErr)
			} else {
				slog.Warn("serp: fetch failed", "job", job.ID, "attempt", newAttempt, "max_attempts", maxAttempts, "status", newStatus, "tab", tabID, "error", fetchErr)
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
		results, parseErr := eng.ParseResults(body)
		if parseErr != nil {
			slog.Warn("serp: parse failed", "job", job.ID, "error", parseErr)
			s.db.Exec(`UPDATE serp_jobs SET status = 'failed', error_msg = $1, locked_by = NULL, updated_at = NOW() WHERE id = $2`,
				parseErr.Error(), job.ID)
			s.redis.Del(ctx, "serp:lock:"+job.ID)
			continue
		}

		// Relevance guard: filter irrelevant results and detect poisoned SERP pages.
		var uQuery string
		if parsedURL, err := url.Parse(job.URL); err == nil {
			uQuery = parsedURL.Query().Get("q")
		}

		kept := results
		ratio := 1.0
		if s.cfg.SERP.RelevanceGuard && uQuery != "" {
			kept, ratio = scraper.FilterRelevant(results, uQuery)
			minRatio := s.cfg.SERP.RelevanceMin
			if minRatio <= 0 {
				minRatio = 0.3
			}

			if len(results) >= 3 && ratio < minRatio {
				s.pagesIrrelevant.Add(1)
				errStr := fmt.Sprintf("irrelevant SERP page (ratio %.2f < %.2f)", ratio, minRatio)

				// Atomic SQL increment: cap retries low specifically for irrelevant pages
				// (MaxIrrelevantAttempts = 3) because poisoned pages are query-deterministic
				// and rarely self-heal, so we fail fast to preserve crawl budget and proxy bandwidth.
				// Does NOT increment consecutive429 or sleep the tab, as this is not a rate limit.
				var newAttempt, maxAttempts int
				var newStatus string
				rowErr := s.db.QueryRowContext(ctx, `
					UPDATE serp_jobs SET
						attempt_count = attempt_count + 1,
						status = CASE WHEN attempt_count + 1 >= $1 OR attempt_count + 1 >= max_attempts THEN 'failed' ELSE 'new' END,
						next_attempt_at = CASE WHEN attempt_count + 1 >= $1 OR attempt_count + 1 >= max_attempts THEN NULL
							ELSE NOW() + interval '1 second' * (30 * power(2, LEAST(6, attempt_count))) END,
						error_msg = $2,
						locked_by = NULL,
						picked_at = NULL,
						updated_at = NOW()
					WHERE id = $3
					RETURNING attempt_count, max_attempts, status
				`, MaxIrrelevantAttempts, errStr, job.ID).Scan(&newAttempt, &maxAttempts, &newStatus)
				if rowErr != nil {
					slog.Error("serp: update irrelevant job error", "job", job.ID, "error", rowErr)
				} else {
					slog.Warn("serp: irrelevant SERP page — soft-blocked",
						"job", job.ID, "engine", job.Engine, "attempt", newAttempt, "max_attempts", maxAttempts,
						"status", newStatus, "found", len(results), "relevant", len(kept), "ratio", ratio, "min", minRatio)
				}

				s.redis.Del(ctx, "serp:lock:"+job.ID)

				if eng.NeedsBrowser() && s.lifecycle.IncrementAndCheck() {
					s.restartBrowser()
				}
				continue
			}
		}

		// DB direct writes — INSERT serp_results + UPDATE serp_jobs.
		tx, txErr := s.db.BeginTx(ctx, nil)
		if txErr != nil {
			slog.Warn("serp: begin tx failed", "error", txErr)
			s.redis.Del(ctx, "serp:lock:"+job.ID)
			continue
		}

		droppedIrrelevant := len(results) - len(kept)
		inserted, duplicates, skippedBlocked := 0, 0, 0
		for _, r := range kept {
			u := r.URL
			urlHash := dedup.HashURL(u)
			domain := dedup.ExtractDomain(u)
			if domain == "" {
				continue
			}
			// Pre-INSERT filter: drop off-niche / non-business hosts so the
			// trigger never spawns enrichment jobs we'd just skip later.
			// Without this, isSkipDomain only fires after a worker picks up
			// the locked job — wasted lifecycle.
			if isSkipDomain(domain) {
				skippedBlocked++
				continue
			}

			// INSERT into serp_results — trigger auto-creates enrichment_jobs.
			dbRes, insertErr := tx.Exec(`
				INSERT INTO serp_results (url, url_hash, domain, source_query_id, source_serp_id)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (url_hash) DO NOTHING
			`, u, urlHash, domain, job.QueryID, job.ID)
			if insertErr != nil {
				slog.Debug("serp: insert serp_result failed", "url", u, "error", insertErr)
				continue
			}
			if n, _ := dbRes.RowsAffected(); n > 0 {
				inserted++
			} else {
				duplicates++
			}
		}
		if skippedBlocked > 0 {
			slog.Info("serp: blocked domains filtered pre-INSERT",
				"engine", job.Engine,
				"job_id", job.ID,
				"skipped_blocked", skippedBlocked,
				"inserted", inserted,
				"total_urls", len(kept),
			)
		}

		tx.Exec(`UPDATE serp_jobs SET status = 'completed', result_count = $1, locked_by = NULL, updated_at = NOW() WHERE id = $2`,
			len(kept), job.ID)

		if err := tx.Commit(); err != nil {
			slog.Warn("serp: tx commit failed", "error", err)
			tx.Rollback()
		}

		s.urlsFound.Add(int64(inserted))
		s.pagesProcessed.Add(1)
		s.redis.Del(ctx, "serp:lock:"+job.ID)

		slog.Info("serp: page done",
			"job", job.ID,
			"engine", job.Engine,
			"found", len(results),
			"relevant", len(kept),
			"new", inserted,
			"duplicates", duplicates,
			"dropped_irrelevant", droppedIrrelevant,
			"tab", tabID,
		)

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

		// 2. Reconcile processing queries: complete finished queries, fail exhausted queries,
		// and requeue true zombies (0 serp_jobs), decoupled from the pendingCount gate.
		s.reconcileProcessingQueries(ctx)

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

// offTargetBeautySubstrings lists beauty/grooming substrings that identify
// off-target queries which must never be re-expanded. Match is lowercased
// Contains — ONLY these categories; wellness/spa/yoga/pilates remain on-target.
var offTargetBeautySubstrings = []string{
	"hair stylist",
	"hair salon",
	"barbershop",
	"barber",
	"nail salon",
	"nail technician",
	"beauty salon",
	"beauty therapist",
	"lash technician",
	"makeup artist",
	"esthetician",
	"skin therapist",
}

// isOffTargetQuery returns true if the query text contains a known
// beauty/grooming substring that marks it as off-target for this pipeline.
func isOffTargetQuery(text string) bool {
	lower := strings.ToLower(text)
	for _, sub := range offTargetBeautySubstrings {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
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

		// Defense-in-depth: skip re-expansion for off-target (beauty/grooming)
		// legacy queries. Mark expanded_at now so they never re-enter this loop.
		if isOffTargetQuery(text) {
			s.db.Exec(`UPDATE queries SET expanded_at = NOW() WHERE id = $1`, id)
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
	// Limit raised to 5000 so a fresh deploy drains a larger slice of the
	// boot backlog (previously capped at 500, leaving ~116K queries stuck).
	res, err := s.db.Exec(`
		UPDATE serp_jobs SET status = 'new', locked_by = NULL, locked_at = NULL, picked_at = NULL, updated_at = NOW()
		WHERE id IN (SELECT id FROM serp_jobs WHERE status = 'processing' LIMIT 5000)
	`)
	if err != nil {
		slog.Warn("serp: requeueStuckJobs failed", "error", err)
	} else if n, _ := res.RowsAffected(); n > 0 {
		slog.Info("serp: requeued stuck serp_jobs from previous run", "count", n)
	}

	qRes, qErr := s.db.Exec(`
		UPDATE queries SET status = 'pending', updated_at = NOW()
		WHERE id IN (SELECT id FROM queries WHERE status = 'processing' LIMIT 5000)
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
		// UnixMicro to match pushQueryToQueue's scoring unit; +60s future base so
		// recovered queries land at the BACK, +1s-per-item stagger (in micros) to
		// preserve batch order. (Unix-seconds scoring would jump these ahead of
		// every fresh micro-scored insert — Invariant #3.)
		s.redis.ZAdd(ctx, query.QueueKey, redis.Z{Score: float64(time.Now().Add(60*time.Second).UnixMicro() + int64(n)*1_000_000), Member: string(data)})
		n++
	}
	if n > 0 {
		slog.Info("serp: pushed pending queries to redis", "count", n)
	}
}

// ReconcileResult holds the counts of queries affected by a reconciliation pass.
type ReconcileResult struct {
	Completed int
	Failed    int
	Requeued  int
	Active    int
}

func (s *SERPStage) reconcileProcessingQueries(ctx context.Context) {
	enabledEngines := make([]string, 0, len(s.engines))
	for _, e := range s.engines {
		if e != nil && e.Name() != "" {
			enabledEngines = append(enabledEngines, e.Name())
		}
	}
	if len(enabledEngines) == 0 {
		enabledEngines = []string{"bing", "duckduckgo"}
	}

	res, err := ReconcileProcessingQueries(ctx, s.db, enabledEngines)
	if err != nil {
		slog.Warn("serp: reconcile processing queries failed", "error", err)
		return
	}

	if res.Requeued > 0 {
		s.requeuePendingQueriesToRedis(ctx)
	}
}

// ReconcileProcessingQueries resolves a batch of up to 500 'processing' queries:
//   - Marks jobs for disabled engines as 'dead'.
//   - Marks queries 'completed' if all enabled-engine jobs are terminal and SUM(result_count) > 0.
//   - Marks queries 'failed' if all enabled-engine jobs are terminal and SUM(result_count) == 0.
//   - Marks queries 'pending' (requeued) if they have 0 serp_jobs (true zombies).
//   - Touches updated_at on active queries so the queue scans forward.
func ReconcileProcessingQueries(ctx context.Context, db *sql.DB, enabledEngines []string) (ReconcileResult, error) {
	if len(enabledEngines) == 0 {
		enabledEngines = []string{"google", "bing", "duckduckgo"}
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
	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM queries
		WHERE status = 'processing'
		ORDER BY updated_at ASC
		LIMIT 500
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
	pqEnabled := pq.Array(enabledEngines)

	// 2. Mark any new/processing jobs for disabled engines as dead for these queries.
	if deadRes, err := tx.ExecContext(ctx, `
		UPDATE serp_jobs SET status = 'dead', error_msg = 'engine disabled in SERP_ENGINES', updated_at = NOW()
		WHERE parent_job_id = ANY($1)
		  AND engine != ALL($2)
		  AND status IN ('new', 'processing')
	`, pqQueryIDs, pqEnabled); err == nil {
		if n, _ := deadRes.RowsAffected(); n > 0 {
			slog.Info("serp: reconciler retired disabled-engine jobs to dead", "count", n)
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
			COUNT(s.id) FILTER (WHERE s.status IN ('new', 'processing') AND s.engine = ANY($2)) AS active_jobs,
			COALESCE(SUM(s.result_count), 0) AS total_results
		FROM serp_jobs s
		WHERE s.parent_job_id = ANY($1)
		GROUP BY s.parent_job_id
	`, pqQueryIDs, pqEnabled)
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
func (s *SERPStage) PagesIrrelevant() int64  { return s.pagesIrrelevant.Load() }
