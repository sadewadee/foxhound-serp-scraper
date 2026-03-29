//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/fetch"
	"github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/directory"
	internalScraper "github.com/sadewadee/serp-scraper/internal/scraper"
)

// contactPagePaths are common paths that contain contact information.
var contactPagePaths = []string{
	"/contact",
	"/contact-us",
	"/kontakt",
	"/about",
	"/about-us",
	"/hubungi-kami",
	"/team",
	"/impressum",
	"/contact.html",
}

// EnrichStage runs Stage 4: Contact enrichment workers.
type EnrichStage struct {
	cfg   *config.Config
	db    *sql.DB
	dedup *dedup.Store

	// Shared browser state — all workers share one browser process (pool of tabs).
	// browserMu guards sharedBrowser pointer replacement during lifecycle restarts.
	browserMu     sync.Mutex
	sharedBrowser *fetch.CamoufoxFetcher
	lifecycle     *internalScraper.BrowserLifecycle

	pagesProcessed atomic.Int64
	emailsFound    atomic.Int64
	phonesFound    atomic.Int64
}

// NewEnrichStage creates a new contact enrichment stage.
func NewEnrichStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *EnrichStage {
	// Browser factory that matches the pool size to the configured concurrency.
	browserFactory := func(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
		return internalScraper.NewBrowserWithPool(cfg, cfg.Enrich.Concurrency)
	}
	return &EnrichStage{
		cfg:       cfg,
		db:        database,
		dedup:     dd,
		lifecycle: internalScraper.NewBrowserLifecycle(cfg, browserFactory, "enrich"),
	}
}

// Run starts contact enrichment workers. Blocks until ctx is cancelled.
//
// Browser lifecycle:
//   - 1 shared Camoufox browser with N pooled tabs (N = worker count)
//   - Browser auto-recycles every maxBrowserRequests (default 300) via foxhound
//   - Stealth HTTP handles 95%+ of requests (no browser needed)
//   - Browser pool is fallback-only for JS-heavy sites
//   - Stealth fetchers are recycled every stealthRecycleInterval to prevent
//     memory growth from accumulated TLS sessions and connection pools
func (c *EnrichStage) Run(ctx context.Context) error {
	// Requeue stuck processing jobs from previous run.
	res, err := c.db.Exec(`UPDATE enrich_jobs SET status = 'pending', locked_by = NULL, locked_at = NULL, updated_at = NOW() WHERE status = 'processing'`)
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

	// Start healthcheck heartbeat.
	go touchHealthFile(ctx, "/tmp/worker-healthy")

	// One shared browser for all workers (fallback for JS-heavy sites).
	// Pool size = worker count so each goroutine can acquire a pre-warmed tab.
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

	// Start reconciler — resets stuck jobs and re-queues orphaned DB records.
	go c.reconciler(ctx)

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

// reconciler runs periodically to:
//  1. Reset enrich_jobs stuck in 'processing' (locked >5 min) back to 'pending'.
//  2. Re-queue 'pending' DB records that are NOT in the Redis queue (DB↔Redis mismatch).
//  3. Re-queue 'failed' jobs whose next_attempt_at has elapsed.
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

		queueKey := "serp:queue:enrich"

		// 1. Reset stuck processing jobs (>5 min) and immediately re-queue.
		// RETURNING gives us exactly the rows we reset — no separate SELECT race.
		stuckRows, err := c.db.Query(`
			UPDATE enrich_jobs SET status = 'pending', locked_by = NULL, locked_at = NULL, updated_at = NOW()
			WHERE status = 'processing' AND locked_at < NOW() - INTERVAL '5 minutes'
			RETURNING id, url, url_hash, parent_job_id
		`)
		if err == nil {
			n := 0
			for stuckRows.Next() {
				var id, pageURL, urlHash string
				var queryID int64
				if err := stuckRows.Scan(&id, &pageURL, &urlHash, &queryID); err == nil {
					c.pushToEnrichQueue(ctx, queueKey, pageURL, urlHash, queryID)
					n++
				}
			}
			stuckRows.Close()
			if n > 0 {
				slog.Info("enrich: reconciler reset+requeued stuck jobs", "count", n)
			}
		}

		// 2. Re-queue orphaned pending jobs (DB exists but missing from Redis).
		// UPDATE updated_at so we don't re-push the same jobs every tick.
		orphanRows, err := c.db.Query(`
			UPDATE enrich_jobs SET updated_at = NOW()
			WHERE status = 'pending' AND locked_by IS NULL
			  AND updated_at < NOW() - INTERVAL '2 minutes'
			RETURNING id, url, url_hash, parent_job_id
		`)
		if err == nil {
			requeued := 0
			for orphanRows.Next() {
				var id, pageURL, urlHash string
				var queryID int64
				if err := orphanRows.Scan(&id, &pageURL, &urlHash, &queryID); err == nil {
					c.pushToEnrichQueue(ctx, queueKey, pageURL, urlHash, queryID)
					requeued++
				}
			}
			orphanRows.Close()
			if requeued > 0 {
				slog.Info("enrich: reconciler re-queued orphaned pending jobs", "count", requeued)
			}
		}

		// 3. Resurrect dead jobs with transient errors (hourly).
		deadRows, deadErr := c.db.Query(`
			UPDATE enrich_jobs SET status = 'pending', attempt_count = 0, locked_by = NULL, updated_at = NOW()
			WHERE status = 'dead'
			  AND updated_at < NOW() - INTERVAL '1 hour'
			  AND error_msg NOT LIKE 'HTTP 403%'
			  AND error_msg NOT LIKE 'HTTP 404%'
			  AND error_msg NOT LIKE 'HTTP 410%'
			  AND error_msg NOT LIKE 'HTTP 451%'
			  AND error_msg NOT LIKE '%certificate%'
			  AND error_msg NOT LIKE '%x509%'
			  AND error_msg NOT LIKE '%no such host%'
			  AND error_msg NOT LIKE '%server misbehaving%'
			RETURNING id, url, url_hash, parent_job_id
		`)
		if deadErr == nil {
			n := 0
			for deadRows.Next() {
				var id, pageURL, urlHash string
				var queryID int64
				if err := deadRows.Scan(&id, &pageURL, &urlHash, &queryID); err == nil {
					c.pushToEnrichQueue(ctx, queueKey, pageURL, urlHash, queryID)
					n++
				}
			}
			deadRows.Close()
			if n > 0 {
				slog.Info("enrich: reconciler resurrected dead jobs", "count", n)
			}
		}

		// 4. Re-queue failed jobs whose retry window has elapsed.
		failedRows, err := c.db.Query(`
			UPDATE enrich_jobs SET status = 'pending', locked_by = NULL, updated_at = NOW()
			WHERE status = 'failed'
			  AND next_attempt_at IS NOT NULL AND next_attempt_at <= NOW()
			  AND attempt_count < max_attempts
			RETURNING id, url, url_hash, parent_job_id
		`)
		if err == nil {
			retried := 0
			for failedRows.Next() {
				var id, pageURL, urlHash string
				var queryID int64
				if err := failedRows.Scan(&id, &pageURL, &urlHash, &queryID); err == nil {
					c.pushToEnrichQueue(ctx, queueKey, pageURL, urlHash, queryID)
					retried++
				}
			}
			failedRows.Close()
			if retried > 0 {
				slog.Info("enrich: reconciler re-queued failed retries", "count", retried)
			}
		}
	}
}

func (c *EnrichStage) worker(ctx context.Context, workerID int) {
	workerIDStr := fmt.Sprintf("enrich-worker-%d", workerID)
	slog.Info("enrich: worker starting", "worker", workerID)

	// Each worker gets its own stealth HTTP fetcher (lightweight, per-worker identity).
	// Recycled every StealthRecycleAfter requests to prevent memory growth.
	stealth := internalScraper.NewStealth(c.cfg)
	stealthCount := 0
	stealthRecycleAfter := c.cfg.Fetch.StealthRecycleAfter

	defer stealth.Close()

	queueKey := "serp:queue:enrich"

	for {
		if ctx.Err() != nil {
			return
		}

		// Recycle stealth fetcher periodically — new identity, fresh TLS sessions.
		stealthCount++
		if stealthCount >= stealthRecycleAfter {
			stealth.Close()
			stealth = internalScraper.NewStealth(c.cfg)
			stealthCount = 0
			slog.Debug("enrich: stealth recycled", "worker", workerID)
		}

		// Pop from Redis enrich queue.
		job, err := popFromQueue(ctx, c.dedup.Client(), queueKey)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Debug("enrich: pop failed", "worker", workerID, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}
		if job == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		pageURL := job.URL
		domain := dedup.ExtractDomain(pageURL)
		if domain == "" {
			continue
		}

		// Domain-level dedup: if this is the first time we see this domain,
		// generate contact page candidate URLs and queue them for enrichment.
		if c.cfg.Enrich.ContactPages {
			isDomainNew, dedupErr := c.dedup.Add(ctx, dedup.KeyDomains, domain)
			if dedupErr != nil {
				slog.Warn("enrich: domain dedup failed", "error", dedupErr)
			} else if isDomainNew {
				c.queueContactPages(ctx, pageURL, domain, job)
			}
		}

		// Extract url_hash from meta to look up the enrich_job.
		urlHash, _ := job.Meta["url_hash"].(string)
		if urlHash == "" {
			urlHash = dedup.HashURL(pageURL)
		}

		// Atomic claim: lookup + lock in one statement.
		// Only succeeds if job exists AND is in a claimable state (pending/failed).
		// If another worker already claimed it (duplicate in Redis), this returns no rows → skip.
		var enrichJobID string
		claimErr := c.db.QueryRowContext(ctx, `
			UPDATE enrich_jobs SET status = 'processing', locked_by = $1, locked_at = NOW(), updated_at = NOW()
			WHERE id = (
				SELECT id FROM enrich_jobs
				WHERE url_hash = $2 AND status IN ('pending', 'failed')
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id
		`, workerIDStr, urlHash).Scan(&enrichJobID)
		if claimErr != nil {
			// Already claimed, completed, dead, or doesn't exist — skip.
			continue
		}

		// Fetch page.
		timeout := time.Duration(c.cfg.Enrich.TimeoutMs) * time.Millisecond
		fetchCtx, cancel := context.WithTimeout(ctx, timeout)

		// Snapshot the shared browser pointer for this fetch (the browser itself
		// is goroutine-safe for concurrent Fetch calls).
		c.browserMu.Lock()
		currentBrowser := c.sharedBrowser
		c.browserMu.Unlock()

		var body string
		if currentBrowser != nil {
			body, err = internalScraper.FetchPage(fetchCtx, stealth, currentBrowser, pageURL, job.ID)
		} else {
			body, err = fetchStealthOnly(fetchCtx, stealth, pageURL, job.ID)
		}
		cancel()

		// Check lifecycle after every fetch attempt (success or failure).
		// Only one goroutine wins the restart; others continue with the new browser.
		if currentBrowser != nil && c.lifecycle.IncrementAndCheck() {
			slog.Info("enrich: page reuse limit reached, restarting browser", "worker", workerID)
			c.browserMu.Lock()
			// Re-check: another worker may have already restarted.
			if c.sharedBrowser != currentBrowser {
				// Another worker already restarted — just use the new one.
				c.browserMu.Unlock()
			} else {
				// Mark nil so other workers fall back to stealth-only during restart.
				oldBrowser := c.sharedBrowser
				c.sharedBrowser = nil
				c.browserMu.Unlock()

				// Restart outside the lock — Close() may block waiting for in-flight fetches.
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

			// Classify error: permanent (no point retrying) vs transient (retry later).
			if isPermanentError(errMsg) {
				c.db.ExecContext(ctx, `
					UPDATE enrich_jobs SET
						attempt_count = attempt_count + 1,
						error_msg = $1,
						status = 'dead',
						updated_at = NOW()
					WHERE id = $2
				`, errMsg, enrichJobID)
			} else {
				c.db.ExecContext(ctx, `
					UPDATE enrich_jobs SET
						attempt_count = attempt_count + 1,
						error_msg = $1,
						status = CASE WHEN attempt_count + 1 >= max_attempts THEN 'dead' ELSE 'failed' END,
						next_attempt_at = CASE WHEN attempt_count + 1 >= max_attempts THEN NULL
							ELSE NOW() + interval '1 second' * 30 * power(2, attempt_count) END,
						updated_at = NOW()
					WHERE id = $2
				`, errMsg, enrichJobID)
			}
			continue
		}

		c.pagesProcessed.Add(1)

		// Directory path: extract business listings, queue individual URLs for enrichment.
		if directory.IsDirectory(pageURL) {
			listings := directory.ExtractListings(pageURL, []byte(body))
			if len(listings) > 0 {
				slog.Info("enrich: directory listings extracted",
					"url", pageURL, "count", len(listings), "worker", workerID)
			}

			// Collect emails/phones from directory page itself.
			var rawEmails, rawPhones []string
			for _, l := range listings {
				if l.Email != "" {
					rawEmails = append(rawEmails, l.Email)
				}
				if l.Phone != "" {
					rawPhones = append(rawPhones, l.Phone)
				}
				// Queue individual listing URLs for full enrichment.
				if l.URL != "" {
					listingHash := dedup.HashURL(l.URL)
					listingDomain := dedup.ExtractDomain(l.URL)
					if listingDomain == "" {
						continue
					}
					// Extract query_id from job meta.
					var qID int64
					if qid, ok := job.Meta["query_id"]; ok {
						switch v := qid.(type) {
						case float64:
							qID = int64(v)
						case int64:
							qID = v
						}
					}
					c.db.Exec(`
						INSERT INTO enrich_jobs (parent_job_id, domain, url, url_hash, status)
						VALUES ($1, $2, $3, $4, 'pending')
						ON CONFLICT (url_hash) DO NOTHING
					`, qID, listingDomain, l.URL, listingHash)

					pushData := &foxhound.Job{
						ID:        fmt.Sprintf("enrich-%s", listingHash[:12]),
						URL:       l.URL,
						Method:    "GET",
						Priority:  foxhound.PriorityNormal,
						CreatedAt: time.Now(),
						Meta:      map[string]any{"query_id": qID, "url_hash": listingHash},
					}
					data, _ := json.Marshal(pushData)
					micros := pushData.CreatedAt.UnixMicro()
					score := -(float64(pushData.Priority) * 1_000_000_000) + float64(micros)
					c.dedup.Client().ZAdd(ctx, queueKey, redis.Z{Score: score, Member: string(data)})
				}
			}

			allEmails := internalScraper.FilterEmails(rawEmails)
			allPhones := internalScraper.FilterPhones(rawPhones)

			socialLinks := buildSocialLinks(nil)
			socialJSON, _ := json.Marshal(socialLinks)

			_, updateErr := c.db.ExecContext(ctx, `
				UPDATE enrich_jobs SET
					emails = $1,
					phones = $2,
					social_links = $3,
					status = 'completed',
					completed_at = NOW(),
					updated_at = NOW()
				WHERE id = $4
			`, pq.Array(allEmails), pq.Array(allPhones), socialJSON, enrichJobID)
			if updateErr != nil {
				slog.Warn("enrich: directory update enrich_job failed", "error", updateErr)
			}

			c.emailsFound.Add(int64(len(allEmails)))
			c.phonesFound.Add(int64(len(allPhones)))

			slog.Debug("enrich: directory page done",
				"url", pageURL, "listings", len(listings), "worker", workerID)
			continue
		}

		// Extract contacts.
		cd := internalScraper.ExtractContacts([]byte(body))

		// Apply post-extraction filters before any further processing.
		emails := internalScraper.FilterEmails(cd.Emails)
		phones := internalScraper.FilterPhones(cd.Phones)

		// Optional MX validation — filter emails that fail MX check.
		var mxValid *bool
		if c.cfg.Enrich.ValidateMX && len(emails) > 0 {
			// Validate MX for the first email's domain (representative for the site).
			v := internalScraper.ValidateMX(emails[0])
			mxValid = &v
			if !v {
				// Discard emails from domains without MX records.
				emails = nil
			}
		}

		// Build social links JSON.
		socialLinks := buildSocialLinks(cd)
		socialJSON, _ := json.Marshal(socialLinks)

		// Determine website URL (main domain URL, not the specific page).
		websiteURL := fmt.Sprintf("https://%s", dedup.ExtractDomain(pageURL))

		// Update enrich_job with collected arrays + business info.
		_, updateErr := c.db.ExecContext(ctx, `
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
			WHERE id = $15
		`, cd.ContactName, cd.BusinessName, cd.BusinessCategory, cd.Description, websiteURL,
			pq.Array(emails), pq.Array(phones), socialJSON,
			cd.Address, cd.Location, cd.OpeningHours, cd.Rating, cd.PageTitle,
			mxValid, enrichJobID)
		if updateErr != nil {
			slog.Warn("enrich: update enrich_job failed", "url", pageURL, "error", updateErr)
			c.db.ExecContext(ctx, `
				UPDATE enrich_jobs SET
					attempt_count = attempt_count + 1,
					error_msg = $1,
					status = CASE WHEN attempt_count + 1 >= max_attempts THEN 'dead' ELSE 'failed' END,
					next_attempt_at = CASE WHEN attempt_count + 1 >= max_attempts THEN NULL
						ELSE NOW() + interval '1 second' * 30 * power(2, attempt_count) END,
					updated_at = NOW()
				WHERE id = $2
			`, updateErr.Error(), enrichJobID)
			continue
		}

		c.emailsFound.Add(int64(len(emails)))
		c.phonesFound.Add(int64(len(phones)))

		slog.Debug("enrich: page done",
			"url", pageURL,
			"emails", len(emails),
			"phones", len(phones),
			"worker", workerID)
	}
}

// queueContactPages generates contact page candidate URLs for a domain and pushes
// them to the enrich queue. Called once per new domain (domain-level dedup).
func (c *EnrichStage) queueContactPages(ctx context.Context, pageURL, domain string, job *foxhound.Job) {
	u, err := url.Parse(pageURL)
	if err != nil {
		return
	}

	// Extract query_id from job meta.
	var queryID int64
	if qid, ok := job.Meta["query_id"]; ok {
		switch v := qid.(type) {
		case float64:
			queryID = int64(v)
		case int64:
			queryID = v
		}
	}

	queueKey := "serp:queue:enrich"
	for _, path := range contactPagePaths {
		contactURL := fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, path)
		contactHash := dedup.HashURL(contactURL)

		// Insert website record for tracking.
		c.db.Exec(`
			INSERT INTO websites (domain, url, url_hash, source_query_id, page_type, status)
			VALUES ($1, $2, $3, $4, 'contact', 'pending')
			ON CONFLICT (url_hash) DO NOTHING
		`, domain, contactURL, contactHash, queryID)

		// Insert enrich_job + push to queue.
		c.db.Exec(`
			INSERT INTO enrich_jobs (parent_job_id, domain, url, url_hash, status)
			VALUES ($1, $2, $3, $4, 'pending')
			ON CONFLICT (url_hash) DO NOTHING
		`, queryID, domain, contactURL, contactHash)

		pushData := &foxhound.Job{
			ID:        fmt.Sprintf("enrich-%s", contactHash[:12]),
			URL:       contactURL,
			Method:    "GET",
			Priority:  foxhound.PriorityNormal,
			CreatedAt: time.Now(),
			Meta: map[string]any{
				"query_id": queryID,
				"url_hash": contactHash,
			},
		}
		data, _ := json.Marshal(pushData)
		micros := pushData.CreatedAt.UnixMicro()
		score := -(float64(pushData.Priority) * 1_000_000_000) + float64(micros)
		c.dedup.Client().ZAdd(ctx, queueKey, redis.Z{Score: score, Member: string(data)})
	}

	slog.Debug("enrich: contact pages queued", "domain", domain, "count", len(contactPagePaths))
}

// buildSocialLinks converts ContactData social fields into a map for JSONB storage.
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

// popFromQueue pops a job from a Redis sorted set queue (ZPOPMIN).
func popFromQueue(ctx context.Context, client *redis.Client, queueKey string) (*foxhound.Job, error) {
	results, err := client.ZPopMin(ctx, queueKey, 1).Result()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}

	raw, ok := results[0].Member.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected member type %T", results[0].Member)
	}

	var job foxhound.Job
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

func fetchStealthOnly(ctx context.Context, stealth *fetch.StealthFetcher, pageURL, jobID string) (string, error) {
	resp, err := stealth.Fetch(ctx, &foxhound.Job{
		ID:     jobID,
		URL:    pageURL,
		Method: "GET",
	})
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

// pushToEnrichQueue pushes a job to the Redis enrich queue.
func (c *EnrichStage) pushToEnrichQueue(ctx context.Context, queueKey, pageURL, urlHash string, queryID int64) {
	pushData := &foxhound.Job{
		ID:        fmt.Sprintf("enrich-%s", urlHash[:12]),
		URL:       pageURL,
		Method:    "GET",
		Priority:  foxhound.PriorityNormal,
		CreatedAt: time.Now(),
		Meta:      map[string]any{"query_id": queryID, "url_hash": urlHash},
	}
	data, _ := json.Marshal(pushData)
	micros := pushData.CreatedAt.UnixMicro()
	score := -(float64(pushData.Priority) * 1_000_000_000) + float64(micros)
	c.dedup.Client().ZAdd(ctx, queueKey, redis.Z{Score: score, Member: string(data)})
}

// isPermanentError returns true for HTTP errors that will never succeed on retry.
// These are not transient — retrying just wastes crawl budget.
func isPermanentError(errMsg string) bool {
	permanent := []string{
		"HTTP 403", // Forbidden — blocked by server
		"HTTP 404", // Not Found — page doesn't exist
		"HTTP 410", // Gone — permanently removed
		"HTTP 451", // Unavailable For Legal Reasons
		"certificate",     // SSL/TLS cert issues
		"x509",            // cert validation failure
		"no such host",    // DNS resolution failed — domain dead
		"server misbehaving", // DNS failure variant
	}
	lower := strings.ToLower(errMsg)
	for _, p := range permanent {
		if strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// PagesProcessed returns the count of processed pages.
func (c *EnrichStage) PagesProcessed() int64 {
	return c.pagesProcessed.Load()
}

// EmailsFound returns the count of discovered emails.
func (c *EnrichStage) EmailsFound() int64 {
	return c.emailsFound.Load()
}

// PhonesFound returns the count of discovered phones.
func (c *EnrichStage) PhonesFound() int64 {
	return c.phonesFound.Load()
}
