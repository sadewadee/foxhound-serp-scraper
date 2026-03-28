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

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/fetch"
	"github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/directory"
	internalScraper "github.com/sadewadee/serp-scraper/internal/scraper"
)

// ContactStage runs Stage 4: Contact enrichment workers.
type ContactStage struct {
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

// NewContactStage creates a new contact enrichment stage.
func NewContactStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *ContactStage {
	// Browser factory that matches the pool size to the configured worker count.
	browserFactory := func(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
		return internalScraper.NewBrowserWithPool(cfg, cfg.Contact.Workers)
	}
	return &ContactStage{
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
func (c *ContactStage) Run(ctx context.Context) error {
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

	numWorkers := c.cfg.Contact.Workers
	slog.Info("contact: starting workers", "count", numWorkers)

	// One shared browser for all workers (fallback for JS-heavy sites).
	// Pool size = worker count so each goroutine can acquire a pre-warmed tab.
	browser, err := internalScraper.NewBrowserWithPool(c.cfg, numWorkers)
	if err != nil {
		slog.Warn("contact: shared browser init failed, stealth-only mode", "error", err)
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
		slog.Info("contact: shared browser ready", "pool_size", numWorkers)
	}

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, workerID)
		}(i)
	}

	wg.Wait()
	slog.Info("contact: all workers done",
		"pages", c.pagesProcessed.Load(),
		"emails", c.emailsFound.Load(),
		"phones", c.phonesFound.Load())
	return nil
}

// stealthRecycleAfter controls how many requests before a stealth fetcher
// is recycled. TLS sessions and HTTP connection pools accumulate memory over
// time; recycling creates a fresh fetcher with a new identity.
const stealthRecycleAfter = 500

func (c *ContactStage) worker(ctx context.Context, workerID int) {
	workerIDStr := fmt.Sprintf("enrich-worker-%d", workerID)
	slog.Info("contact: worker starting", "worker", workerID)

	// Each worker gets its own stealth HTTP fetcher (lightweight, per-worker identity).
	// Recycled every stealthRecycleAfter requests to prevent memory growth.
	stealth := internalScraper.NewStealth(c.cfg)
	stealthCount := 0

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
			slog.Debug("contact: stealth recycled", "worker", workerID)
		}

		// Pop from Redis enrich queue.
		job, err := popFromQueue(ctx, c.dedup.Client(), queueKey)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Debug("contact: pop failed", "worker", workerID, "error", err)
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

		// Extract url_hash from meta to look up the enrich_job.
		urlHash, _ := job.Meta["url_hash"].(string)
		if urlHash == "" {
			urlHash = dedup.HashURL(pageURL)
		}

		// Look up enrich_job by url_hash.
		var enrichJobID string
		var attemptCount, maxAttempts int
		lookupErr := c.db.QueryRowContext(ctx, `
			SELECT id, attempt_count, max_attempts
			FROM enrich_jobs
			WHERE url_hash = $1
			LIMIT 1
		`, urlHash).Scan(&enrichJobID, &attemptCount, &maxAttempts)
		if lookupErr == sql.ErrNoRows {
			slog.Debug("contact: enrich_job not found, skipping", "url", pageURL)
			continue
		}
		if lookupErr != nil {
			slog.Warn("contact: lookup enrich_job failed", "url", pageURL, "error", lookupErr)
			continue
		}

		// Lock the enrich_job.
		c.db.ExecContext(ctx, `
			UPDATE enrich_jobs SET status = 'processing', locked_by = $1, locked_at = NOW(), updated_at = NOW()
			WHERE id = $2
		`, workerIDStr, enrichJobID)

		// Fetch page.
		timeout := time.Duration(c.cfg.Contact.TimeoutMs) * time.Millisecond
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
			slog.Debug("contact: fetch failed", "url", pageURL, "error", err)
			c.db.ExecContext(ctx, `
				UPDATE enrich_jobs SET
					attempt_count = attempt_count + 1,
					error_msg = $1,
					status = CASE WHEN attempt_count + 1 >= max_attempts THEN 'dead' ELSE 'failed' END,
					next_attempt_at = CASE WHEN attempt_count + 1 >= max_attempts THEN NULL
						ELSE NOW() + interval '1 second' * 30 * power(2, attempt_count) END,
					updated_at = NOW()
				WHERE id = $2
			`, err.Error(), enrichJobID)
			continue
		}

		c.pagesProcessed.Add(1)

		// Directory path: extract business listings and store their data directly.
		if directory.IsDirectory(pageURL) {
			listings := directory.ExtractListings(pageURL, []byte(body))
			if len(listings) > 0 {
				slog.Info("contact: directory listings extracted",
					"url", pageURL, "count", len(listings), "worker", workerID)
			}

			// Collect all emails and phones from directory listings.
			var rawEmails, rawPhones []string
			for _, l := range listings {
				if l.Email != "" {
					rawEmails = append(rawEmails, l.Email)
				}
				if l.Phone != "" {
					rawPhones = append(rawPhones, l.Phone)
				}
			}

			// Apply post-extraction filters to remove noise.
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
				slog.Warn("contact: directory update enrich_job failed", "error", updateErr)
			}

			c.emailsFound.Add(int64(len(allEmails)))
			c.phonesFound.Add(int64(len(allPhones)))

			slog.Debug("contact: directory page done",
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
		if c.cfg.Contact.ValidateMX && len(emails) > 0 {
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
				business_name = $1,
				business_category = $2,
				description = $3,
				website = $4,
				emails = $5,
				phones = $6,
				social_links = $7,
				address = $8,
				location = $9,
				opening_hours = $10,
				rating = $11,
				page_title = $12,
				mx_valid = $13,
				status = 'completed',
				completed_at = NOW(),
				updated_at = NOW()
			WHERE id = $14
		`, cd.BusinessName, cd.BusinessCategory, cd.Description, websiteURL,
			pq.Array(emails), pq.Array(phones), socialJSON,
			cd.Address, cd.Location, cd.OpeningHours, cd.Rating, cd.PageTitle,
			mxValid, enrichJobID)
		if updateErr != nil {
			slog.Warn("contact: update enrich_job failed", "url", pageURL, "error", updateErr)
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

		slog.Debug("contact: page done",
			"url", pageURL,
			"emails", len(emails),
			"phones", len(phones),
			"worker", workerID)
	}
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

// PagesProcessed returns the count of processed pages.
func (c *ContactStage) PagesProcessed() int64 {
	return c.pagesProcessed.Load()
}

// EmailsFound returns the count of discovered emails.
func (c *ContactStage) EmailsFound() int64 {
	return c.emailsFound.Load()
}

// PhonesFound returns the count of discovered phones.
func (c *ContactStage) PhonesFound() int64 {
	return c.phonesFound.Load()
}
