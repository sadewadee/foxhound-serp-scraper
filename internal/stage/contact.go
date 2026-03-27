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

	pagesProcessed atomic.Int64
	emailsFound    atomic.Int64
	phonesFound    atomic.Int64
}

// NewContactStage creates a new contact enrichment stage.
func NewContactStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *ContactStage {
	return &ContactStage{
		cfg:   cfg,
		db:    database,
		dedup: dd,
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
	numWorkers := c.cfg.Contact.Workers
	slog.Info("contact: starting workers", "count", numWorkers)

	// One shared browser for all workers (fallback for JS-heavy sites).
	// Pool size = worker count so each goroutine can acquire a pre-warmed tab.
	var sharedBrowser *fetch.CamoufoxFetcher
	browser, err := internalScraper.NewBrowserWithPool(c.cfg, numWorkers)
	if err != nil {
		slog.Warn("contact: shared browser init failed, stealth-only mode", "error", err)
	} else {
		sharedBrowser = browser
		defer sharedBrowser.Close()
		slog.Info("contact: shared browser ready", "pool_size", numWorkers)
	}

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, workerID, sharedBrowser)
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

func (c *ContactStage) worker(ctx context.Context, workerID int, sharedBrowser *fetch.CamoufoxFetcher) {
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

		var body string
		if sharedBrowser != nil {
			body, err = internalScraper.FetchPage(fetchCtx, stealth, sharedBrowser, pageURL, job.ID)
		} else {
			body, err = fetchStealthOnly(fetchCtx, stealth, pageURL, job.ID)
		}
		cancel()

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
			var allEmails, allPhones []string
			for _, l := range listings {
				if l.Email != "" {
					allEmails = append(allEmails, l.Email)
				}
				if l.Phone != "" {
					allPhones = append(allPhones, l.Phone)
				}
			}

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

		// Optional MX validation — filter emails that fail MX check.
		emails := cd.Emails
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

		// Update enrich_job with collected arrays.
		_, updateErr := c.db.ExecContext(ctx, `
			UPDATE enrich_jobs SET
				emails = $1,
				phones = $2,
				social_links = $3,
				address = $4,
				raw_context = $5,
				mx_valid = $6,
				status = 'completed',
				completed_at = NOW(),
				updated_at = NOW()
			WHERE id = $7
		`, pq.Array(emails), pq.Array(cd.Phones), socialJSON,
			cd.Address, cd.BusinessName, mxValid, enrichJobID)
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
		c.phonesFound.Add(int64(len(cd.Phones)))

		slog.Debug("contact: page done",
			"url", pageURL,
			"emails", len(emails),
			"phones", len(cd.Phones),
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
