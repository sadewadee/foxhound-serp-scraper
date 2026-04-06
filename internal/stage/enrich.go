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

	"github.com/lib/pq"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/directory"
	"github.com/sadewadee/serp-scraper/internal/feeder"
	internalScraper "github.com/sadewadee/serp-scraper/internal/scraper"
)

var blockedDomains = map[string]bool{
	"www.yelp.com": true, "m.yelp.com": true,
	"www.tripadvisor.com": true, "www.tripadvisor.co.uk": true, "www.tripadvisor.co.id": true,
	"www.tripadvisor.de": true, "www.tripadvisor.fr": true, "www.tripadvisor.com.au": true,
	"www.indeed.com": true, "de.indeed.com": true, "id.indeed.com": true,
	"www.ziprecruiter.com": true, "www.simplyhired.com": true,
	"www.glassdoor.com": true, "www.glassdoor.co.uk": true, "www.glassdoor.de": true,
	"www.linkedin.com": true,
	"www.facebook.com": true, "m.facebook.com": true,
	"www.instagram.com": true,
	"twitter.com":       true, "x.com": true,
	"www.tiktok.com": true, "www.youtube.com": true,
	"www.pinterest.com": true, "www.reddit.com": true,
	"www.quora.com": true, "www.researchgate.net": true,
	"pmc.ncbi.nlm.nih.gov": true,
	"journals.sagepub.com": true, "www.tandfonline.com": true,
	"rocketreach.co": true,
	"www.amazon.com": true, "www.walmart.com": true,
	"www.booking.com": true, "www.airbnb.com": true,
	"www.bbb.org": true, "maps.google.com": true,
	"www.yellowpages.com": true, "www.whitepages.com": true,
}

func isSkipDomain(domain string) bool {
	if blockedDomains[domain] {
		return true
	}
	for _, suffix := range []string{
		".edu", ".gov", ".mil", ".ac.uk", ".gov.uk", ".edu.au", ".gov.au",
		".ac.id", ".go.id", ".edu.sg", ".gov.sg", ".ac.jp", ".go.jp",
	} {
		if strings.HasSuffix(domain, suffix) {
			return true
		}
	}
	return false
}

type EnrichStage struct {
	cfg   *config.Config
	db    *sql.DB
	dedup *dedup.Store

	browserMu     sync.Mutex
	sharedBrowser *fetch.CamoufoxFetcher
	lifecycle     *internalScraper.BrowserLifecycle
	domainScorer  *fetch.DomainScorer

	pagesProcessed atomic.Int64
	emailsFound    atomic.Int64
	phonesFound    atomic.Int64

	betaDSStatic  atomic.Int64
	betaDSCaution atomic.Int64
	betaDSBrowser atomic.Int64
}

func NewEnrichStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *EnrichStage {
	browserFactory := func(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
		return internalScraper.NewBrowserWithPool(cfg, cfg.Enrich.Concurrency)
	}
	return &EnrichStage{
		cfg:          cfg,
		db:           database,
		dedup:        dd,
		lifecycle:    internalScraper.NewBrowserLifecycle(cfg, browserFactory, "enrich"),
		domainScorer: internalScraper.NewDomainScorer(cfg),
	}
}

func (c *EnrichStage) Run(ctx context.Context) error {
	// Requeue stuck processing jobs from previous run.
	res, err := c.db.Exec(`UPDATE enrichment_jobs SET status = 'pending', locked_by = NULL, locked_at = NULL, picked_at = NULL, updated_at = NOW() WHERE status = 'processing'`)
	if err != nil {
		slog.Warn("enrich: requeue stuck jobs failed", "error", err)
	} else {
		n, _ := res.RowsAffected()
		if n > 0 {
			slog.Info("enrich: requeued stuck processing jobs", "count", n)
		}
	}

	numWorkers := c.cfg.Enrich.Concurrency
	slog.Info("enrich: starting workers", "count", numWorkers)

	go touchHealthFile(ctx, "/tmp/worker-healthy")
	go c.heartbeat(ctx)

	browser, err := internalScraper.NewBrowserWithPool(c.cfg, numWorkers)
	if err != nil {
		slog.Warn("enrich: shared browser init failed, stealth-only mode", "error", err)
	} else {
		c.browserMu.Lock()
		c.sharedBrowser = browser
		c.browserMu.Unlock()
		defer func() {
			c.browserMu.Lock()
			if c.sharedBrowser != nil {
				c.sharedBrowser.Close()
			}
			c.browserMu.Unlock()
		}()
		slog.Info("enrich: shared browser ready", "pool_size", numWorkers)
	}

	go c.reconciler(ctx)

	// Start the DB-to-Redis buffer feeder.
	enrichFeeder := feeder.NewEnrichFeeder(c.db, c.dedup.Client())
	go enrichFeeder.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, workerID)
		}(i)
	}

	wg.Wait()
	slog.Info("enrich: all workers done",
		"pages", c.pagesProcessed.Load(),
		"emails", c.emailsFound.Load(),
		"phones", c.phonesFound.Load())
	return nil
}

// heartbeat upserts the workers table every 30s so the reconciler and
// Telegram /status command can report worker health.
func (c *EnrichStage) heartbeat(ctx context.Context) {
	host, _ := os.Hostname()
	if len(host) > 12 {
		host = host[:12]
	}
	workerID := fmt.Sprintf("enrich-%s", host)

	// Register on startup.
	c.db.Exec(`INSERT INTO workers (worker_id, worker_type, status, last_heartbeat, started_at)
		VALUES ($1, 'enrich', 'idle', NOW(), NOW())
		ON CONFLICT (worker_id) DO UPDATE SET status = 'idle', last_heartbeat = NOW(), started_at = NOW()`,
		workerID)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.db.Exec(`UPDATE workers SET status = 'dead', last_heartbeat = NOW() WHERE worker_id = $1`, workerID)
			return
		case <-ticker.C:
			pages := c.pagesProcessed.Load()
			emails := c.emailsFound.Load()
			c.db.Exec(`UPDATE workers SET
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

func (c *EnrichStage) reconciler(ctx context.Context) {
	interval := time.Duration(c.cfg.Fetch.ReconcilerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// 1. Reset stuck processing jobs (>5 min).
		res, err := c.db.Exec(`
			UPDATE enrichment_jobs SET status = 'pending', locked_by = NULL, locked_at = NULL, picked_at = NULL, updated_at = NOW()
			WHERE id IN (
				SELECT id FROM enrichment_jobs
				WHERE status = 'processing' AND locked_at < NOW() - INTERVAL '5 minutes'
				LIMIT 200
			)
		`)
		if err == nil {
			if n, _ := res.RowsAffected(); n > 0 {
				slog.Info("enrich: reconciler reset stuck jobs", "count", n)
			}
		}

		// 2a. Mark dead jobs with too many attempts as permanently dead (prevent infinite retry).
		permDeadRes, _ := c.db.Exec(`
			UPDATE enrichment_jobs SET error_msg = 'permanently dead: max resurrections reached', updated_at = NOW()
			WHERE status = 'dead'
			  AND attempt_count >= 15
			  AND error_msg NOT LIKE 'permanently dead%'
		`)
		if permDeadRes != nil {
			if n, _ := permDeadRes.RowsAffected(); n > 0 {
				slog.Info("enrich: reconciler marked jobs permanently dead", "count", n)
			}
		}

		// 2b. Resurrect dead jobs with transient errors (hourly).
		// Increment attempt_count (never reset to 0) so they eventually reach the cap.
		deadRes, _ := c.db.Exec(`
			UPDATE enrichment_jobs SET status = 'pending', attempt_count = attempt_count + 1, locked_by = NULL, picked_at = NULL, updated_at = NOW()
			WHERE id IN (
				SELECT id FROM enrichment_jobs
				WHERE status = 'dead'
				  AND attempt_count < 15
				  AND updated_at < NOW() - INTERVAL '1 hour'
				  AND error_msg NOT LIKE 'HTTP 403%'
				  AND error_msg NOT LIKE 'HTTP 404%'
				  AND error_msg NOT LIKE 'HTTP 410%'
				  AND error_msg NOT LIKE 'HTTP 451%'
				  AND error_msg NOT LIKE '%certificate%'
				  AND error_msg NOT LIKE '%x509%'
				  AND error_msg NOT LIKE '%no such host%'
				  AND error_msg NOT LIKE '%server misbehaving%'
				  AND error_msg NOT LIKE 'permanently dead%'
				LIMIT 1000
			)
		`)
		if deadRes != nil {
			if n, _ := deadRes.RowsAffected(); n > 0 {
				slog.Info("enrich: reconciler resurrected dead jobs", "count", n)
			}
		}

		// 3. Re-queue failed jobs whose retry window has elapsed.
		failedRes, _ := c.db.Exec(`
			UPDATE enrichment_jobs SET status = 'pending', locked_by = NULL, picked_at = NULL, updated_at = NOW()
			WHERE id IN (
				SELECT id FROM enrichment_jobs
				WHERE status = 'failed'
				  AND next_attempt_at IS NOT NULL AND next_attempt_at <= NOW()
				  AND attempt_count < max_attempts
				LIMIT 1000
			)
		`)
		if failedRes != nil {
			if n, _ := failedRes.RowsAffected(); n > 0 {
				slog.Info("enrich: reconciler re-queued failed retries", "count", n)
			}
		}

		// Beta metrics.
		if c.domainScorer != nil {
			dsStatic := c.betaDSStatic.Load()
			dsCaution := c.betaDSCaution.Load()
			dsBrowser := c.betaDSBrowser.Load()
			dsTotal := dsStatic + dsCaution + dsBrowser
			escalatePct := 0.0
			if dsTotal > 0 {
				escalatePct = float64(dsBrowser) / float64(dsTotal) * 100
			}
			slog.Info("beta-metrics: enrich",
				"ds_static", dsStatic, "ds_caution", dsCaution, "ds_browser", dsBrowser,
				"ds_escalate_pct", fmt.Sprintf("%.1f%%", escalatePct),
			)
		}
	}
}

func (c *EnrichStage) worker(ctx context.Context, workerID int) {
	host, _ := os.Hostname()
	if len(host) > 12 {
		host = host[:12]
	}
	workerIDStr := fmt.Sprintf("enrich-%s-%d", host, workerID)
	slog.Info("enrich: worker starting", "worker", workerID)

	stealth := internalScraper.NewStealth(c.cfg)
	stealthCount := 0
	stealthRecycleAfter := c.cfg.Fetch.StealthRecycleAfter

	defer stealth.Close()

	redisClient := c.dedup.Client()

	for {
		if ctx.Err() != nil {
			return
		}

		stealthCount++
		if stealthCount >= stealthRecycleAfter {
			stealth.Close()
			stealth = internalScraper.NewStealth(c.cfg)
			stealthCount = 0
			slog.Debug("enrich: stealth recycled", "worker", workerID)
		}

		// BLPOP from enrich:buffer.
		result, err := redisClient.BLPop(ctx, 5*time.Second, feeder.EnrichBufferKey).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if len(result) < 2 {
			continue
		}

		var job feeder.EnrichBufferItem
		if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
			slog.Warn("enrich: invalid job in buffer", "error", err)
			continue
		}

		pageURL := job.URL
		domain := job.Domain
		if domain == "" {
			domain = dedup.ExtractDomain(pageURL)
		}
		if domain == "" {
			continue
		}

		urlHash := job.URLHash
		if urlHash == "" {
			urlHash = dedup.HashURL(pageURL)
		}

		// Skip blocked domains — mark dead.
		if isSkipDomain(domain) {
			c.db.Exec(`UPDATE enrichment_jobs SET status = 'dead', error_msg = $1, updated_at = NOW() WHERE url_hash = $2`,
				"blocked domain: "+domain, urlHash)
			continue
		}

		// Redis SETNX claim.
		ok, claimErr := redisClient.SetNX(ctx, "enrich:lock:"+urlHash, workerIDStr, 5*time.Minute).Result()
		if claimErr != nil || !ok {
			continue
		}

		// DB claim: mark processing.
		c.db.Exec(`UPDATE enrichment_jobs SET status = 'processing', locked_by = $1, locked_at = NOW(), updated_at = NOW() WHERE url_hash = $2 AND status = 'pending'`,
			workerIDStr, urlHash)

		// Fetch page.
		timeout := time.Duration(c.cfg.Enrich.TimeoutMs) * time.Millisecond
		fetchCtx, cancel := context.WithTimeout(ctx, timeout)

		c.browserMu.Lock()
		currentBrowser := c.sharedBrowser
		c.browserMu.Unlock()

		var body string
		if c.domainScorer != nil && currentBrowser != nil {
			action := c.domainScorer.Recommend(domain)
			switch action {
			case fetch.ActionBrowserDirect:
				c.betaDSBrowser.Add(1)
				body, err = internalScraper.FetchWithBrowserString(fetchCtx, currentBrowser, pageURL, job.ID)
				if err == nil {
					c.domainScorer.RecordBrowser(domain, false)
				} else {
					c.domainScorer.RecordBrowser(domain, true)
				}
			case fetch.ActionStaticCautious:
				c.betaDSCaution.Add(1)
				cautionCtx, cautionCancel := context.WithTimeout(fetchCtx, 5*time.Second)
				body, err = internalScraper.FetchPage(cautionCtx, stealth, currentBrowser, pageURL, job.ID)
				cautionCancel()
				blocked := err != nil
				c.domainScorer.RecordStatic(domain, blocked)
			default:
				c.betaDSStatic.Add(1)
				body, err = internalScraper.FetchPage(fetchCtx, stealth, currentBrowser, pageURL, job.ID)
				blocked := err != nil
				c.domainScorer.RecordStatic(domain, blocked)
			}
		} else if currentBrowser != nil {
			body, err = internalScraper.FetchPage(fetchCtx, stealth, currentBrowser, pageURL, job.ID)
		} else {
			body, err = fetchStealthOnly(fetchCtx, stealth, pageURL, job.ID)
		}
		cancel()

		if currentBrowser != nil && c.lifecycle.IncrementAndCheck() {
			slog.Info("enrich: page reuse limit reached, restarting browser", "worker", workerID)
			c.browserMu.Lock()
			if c.sharedBrowser != currentBrowser {
				c.browserMu.Unlock()
			} else {
				oldBrowser := c.sharedBrowser
				c.sharedBrowser = nil
				c.browserMu.Unlock()
				newBrowser, restartErr := c.lifecycle.Restart(oldBrowser)
				c.browserMu.Lock()
				if restartErr != nil {
					slog.Error("enrich: lifecycle restart failed", "error", restartErr)
				} else {
					c.sharedBrowser = newBrowser
				}
				c.browserMu.Unlock()
			}
		}

		if err != nil {
			errMsg := err.Error()
			slog.Debug("enrich: fetch failed", "url", pageURL, "error", errMsg)

			if isPermanentError(errMsg) {
				c.db.Exec(`UPDATE enrichment_jobs SET status = 'dead', attempt_count = attempt_count + 1, error_msg = $1, updated_at = NOW() WHERE url_hash = $2`,
					errMsg, urlHash)
			} else {
				c.db.Exec(`UPDATE enrichment_jobs SET
					attempt_count = attempt_count + 1,
					error_msg = $1,
					status = CASE WHEN attempt_count + 1 >= max_attempts THEN 'dead' ELSE 'failed' END,
					next_attempt_at = CASE WHEN attempt_count + 1 >= max_attempts THEN NULL
						ELSE NOW() + interval '1 second' * 30 * power(2, attempt_count) END,
					locked_by = NULL, picked_at = NULL, updated_at = NOW()
				WHERE url_hash = $2`, errMsg, urlHash)
			}
			redisClient.Del(ctx, "enrich:lock:"+urlHash)
			continue
		}

		c.pagesProcessed.Add(1)

		// Directory path: extract business listings.
		if directory.IsDirectory(pageURL) {
			listings := directory.ExtractListings(pageURL, []byte(body))
			if len(listings) > 0 {
				slog.Info("enrich: directory listings extracted", "url", pageURL, "count", len(listings), "worker", workerID)
			}

			var rawEmails, rawPhones []string
			for _, l := range listings {
				if l.Email != "" {
					rawEmails = append(rawEmails, l.Email)
				}
				if l.Phone != "" {
					rawPhones = append(rawPhones, l.Phone)
				}
				// Queue individual listing URLs via DB INSERT (trigger creates enrichment_jobs).
				if l.URL != "" {
					listingDomain := dedup.ExtractDomain(l.URL)
					if listingDomain != "" {
						c.db.Exec(`INSERT INTO serp_results (url, url_hash, domain, source_query_id)
							VALUES ($1, $2, $3, $4) ON CONFLICT (url_hash) DO NOTHING`,
							l.URL, dedup.HashURL(l.URL), listingDomain, job.ParentQueryID)
					}
				}
			}

			allEmails := internalScraper.FilterEmails(rawEmails)
			allPhones := internalScraper.FilterPhones(rawPhones)
			socialLinks := buildSocialLinks(nil)
			socialJSON, _ := json.Marshal(socialLinks)

			c.db.Exec(`UPDATE enrichment_jobs SET
				status = 'completed', raw_emails = $1, raw_phones = $2, raw_social = $3,
				completed_at = NOW(), updated_at = NOW()
			WHERE url_hash = $4`,
				pq.Array(allEmails), pq.Array(allPhones), socialJSON, urlHash)

			redisClient.Del(ctx, "enrich:lock:"+urlHash)
			c.emailsFound.Add(int64(len(allEmails)))
			c.phonesFound.Add(int64(len(allPhones)))
			continue
		}

		// Extract contacts.
		cd := internalScraper.ExtractContacts([]byte(body))
		emails := internalScraper.FilterEmails(cd.Emails)
		phones := internalScraper.FilterPhones(cd.Phones)

		if c.cfg.Enrich.ValidateMX && len(emails) > 0 {
			if !internalScraper.ValidateMX(emails[0]) {
				emails = nil
			}
		}

		socialLinks := buildSocialLinks(cd)
		socialJSON, _ := json.Marshal(socialLinks)

		// DB direct write — trigger handles normalization + contact pages.
		c.db.Exec(`UPDATE enrichment_jobs SET
			status = 'completed',
			raw_emails = $1, raw_phones = $2, raw_social = $3,
			raw_business_name = $4, raw_category = $5, raw_address = $6,
			raw_page_title = $7,
			locked_by = NULL, completed_at = NOW(), updated_at = NOW()
		WHERE url_hash = $8`,
			pq.Array(emails), pq.Array(phones), socialJSON,
			cd.BusinessName, cd.BusinessCategory, cd.Address,
			cd.PageTitle, urlHash)

		redisClient.Del(ctx, "enrich:lock:"+urlHash)
		c.emailsFound.Add(int64(len(emails)))
		c.phonesFound.Add(int64(len(phones)))

		slog.Debug("enrich: page done", "url", pageURL, "emails", len(emails), "phones", len(phones), "worker", workerID)
	}
}

func buildSocialLinks(cd *internalScraper.ContactData) map[string]string {
	links := map[string]string{}
	if cd == nil {
		return links
	}
	if cd.Instagram != "" {
		links["instagram"] = cd.Instagram
	}
	if cd.Facebook != "" {
		links["facebook"] = cd.Facebook
	}
	if cd.Twitter != "" {
		links["twitter"] = cd.Twitter
	}
	if cd.LinkedIn != "" {
		links["linkedin"] = cd.LinkedIn
	}
	if cd.WhatsApp != "" {
		links["whatsapp"] = cd.WhatsApp
	}
	return links
}

func fetchStealthOnly(ctx context.Context, stealth *fetch.StealthFetcher, pageURL, jobID string) (string, error) {
	resp, err := stealth.Fetch(ctx, &foxhound.Job{ID: jobID, URL: pageURL, Method: "GET"})
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", fmt.Errorf("nil response")
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return string(resp.Body), nil
}

func isPermanentError(errMsg string) bool {
	permanent := []string{"HTTP 404", "HTTP 410", "HTTP 451", "certificate", "x509", "no such host", "server misbehaving"}
	lower := strings.ToLower(errMsg)
	for _, p := range permanent {
		if strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

func (c *EnrichStage) PagesProcessed() int64 { return c.pagesProcessed.Load() }
func (c *EnrichStage) EmailsFound() int64    { return c.emailsFound.Load() }
func (c *EnrichStage) PhonesFound() int64    { return c.phonesFound.Load() }
