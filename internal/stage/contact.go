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

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/fetch"

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
func (c *ContactStage) Run(ctx context.Context) error {
	numWorkers := c.cfg.Contact.Workers
	slog.Info("contact: starting workers", "count", numWorkers)

	// One shared browser for all workers (fallback for JS-heavy sites).
	// Pool size = worker count so each goroutine can acquire a pre-warmed tab.
	// Architecture: 1 browser process → N tabs, NOT N browser processes.
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

func (c *ContactStage) worker(ctx context.Context, workerID int, sharedBrowser *fetch.CamoufoxFetcher) {
	slog.Info("contact: worker starting", "worker", workerID)

	// Each worker gets its own stealth HTTP fetcher (lightweight, per-worker identity).
	stealth := internalScraper.NewStealth(c.cfg)
	defer stealth.Close()

	queueKey := "serp:queue:contacts"

	for {
		if ctx.Err() != nil {
			return
		}

		// Pop from Redis queue.
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

		// Extract query_id from meta.
		var queryID int64
		if qid, ok := job.Meta["query_id"]; ok {
			switch v := qid.(type) {
			case float64:
				queryID = int64(v)
			case int64:
				queryID = v
			}
		}

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
			for _, l := range listings {
				// Store listing contact data if email is present.
				if l.Email != "" {
					emailHash := dedup.HashEmail(l.Email)

					isNew, dedupErr := c.dedup.Add(ctx, dedup.KeyEmails, emailHash)
					if dedupErr != nil {
						slog.Warn("contact: listing email dedup failed", "error", dedupErr)
						continue
					}
					if !isNew {
						continue
					}

					phone := l.Phone
					_, insertErr := c.db.Exec(`
						INSERT INTO contacts (
							email, email_hash, phone, domain, source_url, source_query_id,
							address
						) VALUES ($1, $2, $3, $4, $5, $6, $7)
						ON CONFLICT (email_hash, domain) DO NOTHING
					`, l.Email, emailHash, phone,
						dedup.ExtractDomain(pageURL), pageURL, queryID,
						l.Address)
					if insertErr != nil {
						slog.Warn("contact: listing insert failed", "email", l.Email, "error", insertErr)
						continue
					}
					c.emailsFound.Add(1)
				} else if l.Phone != "" {
					// Phone-only listing.
					_, insertErr := c.db.Exec(`
						INSERT INTO contacts (
							phone, domain, source_url, source_query_id,
							address
						) VALUES ($1, $2, $3, $4, $5)
						ON CONFLICT DO NOTHING
					`, l.Phone, dedup.ExtractDomain(pageURL), pageURL, queryID,
						l.Address)
					if insertErr != nil {
						slog.Debug("contact: listing phone-only insert failed", "error", insertErr)
					}
					c.phonesFound.Add(1)
				}
			}
			slog.Debug("contact: directory page done",
				"url", pageURL, "listings", len(listings), "worker", workerID)
			continue
		}

		// Extract contacts.
		cd := internalScraper.ExtractContacts([]byte(body))

		// Store each email as a contact.
		for _, email := range cd.Emails {
			emailLower := dedup.HashEmail(email)

			// Redis email dedup.
			isNew, err := c.dedup.Add(ctx, dedup.KeyEmails, emailLower)
			if err != nil {
				slog.Warn("contact: email dedup failed", "error", err)
				continue
			}
			if !isNew {
				continue
			}

			// Optional MX validation.
			var mxValid *bool
			if c.cfg.Contact.ValidateMX {
				v := internalScraper.ValidateMX(email)
				mxValid = &v
				if !v {
					continue
				}
			}

			// Get first phone if available.
			phone := ""
			if len(cd.Phones) > 0 {
				phone = cd.Phones[0]
			}

			// Insert contact.
			_, err = c.db.Exec(`
				INSERT INTO contacts (
					email, email_hash, phone, domain, source_url, source_query_id,
					instagram, facebook, twitter, linkedin, whatsapp, address, mx_valid
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (email_hash, domain) DO NOTHING
			`, email, dedup.HashEmail(email), phone, domain, pageURL, queryID,
				cd.Instagram, cd.Facebook, cd.Twitter, cd.LinkedIn,
				cd.WhatsApp, cd.Address, mxValid)
			if err != nil {
				slog.Warn("contact: insert failed", "email", email, "error", err)
				continue
			}

			c.emailsFound.Add(1)
		}

		// Also store phone-only contacts if no email was found.
		if len(cd.Emails) == 0 && len(cd.Phones) > 0 {
			for _, phone := range cd.Phones {
				_, err = c.db.Exec(`
					INSERT INTO contacts (
						phone, domain, source_url, source_query_id,
						instagram, facebook, twitter, linkedin, whatsapp, address
					) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
					ON CONFLICT DO NOTHING
				`, phone, domain, pageURL, queryID,
					cd.Instagram, cd.Facebook, cd.Twitter, cd.LinkedIn,
					cd.WhatsApp, cd.Address)
				if err != nil {
					slog.Debug("contact: insert phone-only failed", "error", err)
				}
				c.phonesFound.Add(1)
			}
		}

		slog.Debug("contact: page done",
			"url", pageURL,
			"emails", len(cd.Emails),
			"phones", len(cd.Phones),
			"worker", workerID)
	}
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
