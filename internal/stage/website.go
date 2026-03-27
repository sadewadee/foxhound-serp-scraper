//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	foxhound "github.com/sadewadee/foxhound"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/directory"
	"github.com/sadewadee/serp-scraper/internal/scraper"
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

// WebsiteStage runs Stage 3: Website/contact-page discovery workers.
type WebsiteStage struct {
	cfg   *config.Config
	db    *sql.DB
	dedup *dedup.Store

	domainsProcessed atomic.Int64
	pagesQueued      atomic.Int64
}

// NewWebsiteStage creates a new website discovery stage.
func NewWebsiteStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *WebsiteStage {
	return &WebsiteStage{
		cfg:   cfg,
		db:    database,
		dedup: dd,
	}
}

// Run starts website discovery workers. Blocks until ctx is cancelled.
func (w *WebsiteStage) Run(ctx context.Context) error {
	// Requeue stuck websites.
	if res, err := w.db.Exec(`UPDATE websites SET status = 'pending' WHERE status = 'processing' AND page_type = 'serp_result'`); err == nil {
		if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("website: requeued processing", "count", n)
		}
	}

	numWorkers := w.cfg.Website.Workers
	slog.Info("website: starting workers", "count", numWorkers)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w.worker(ctx, workerID)
		}(i)
	}

	wg.Wait()
	slog.Info("website: all workers done",
		"domains", w.domainsProcessed.Load(),
		"pages_queued", w.pagesQueued.Load())
	return nil
}

func (w *WebsiteStage) worker(ctx context.Context, workerID int) {
	slog.Info("website: worker starting", "worker", workerID)

	// Stealth fetcher for directory page fetches.
	stealth := scraper.NewStealth(w.cfg)
	defer stealth.Close()

	queueKey := "serp:queue:websites"
	enrichQueueKey := "serp:queue:enrich"

	for {
		if ctx.Err() != nil {
			return
		}

		// Pop from Redis queue.
		job, err := popFromQueue(ctx, w.dedup.Client(), queueKey)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Debug("website: pop failed", "worker", workerID, "error", err)
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

		siteURL := job.URL
		domain := dedup.ExtractDomain(siteURL)
		if domain == "" {
			continue
		}

		// Domain-level dedup: skip if already processed.
		isDomainNew, err := w.dedup.Add(ctx, dedup.KeyDomains, domain)
		if err != nil {
			slog.Warn("website: domain dedup failed", "error", err)
			continue
		}
		if !isDomainNew {
			slog.Debug("website: domain already processed", "domain", domain)
			continue
		}

		w.domainsProcessed.Add(1)

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

		// Directory path: fetch the page and extract individual business listings.
		// Each listing with a URL is pushed directly to the enrich queue for enrichment.
		if directory.IsDirectory(siteURL) {
			slog.Debug("website: directory detected, fetching listings", "url", siteURL, "worker", workerID)

			timeout := time.Duration(w.cfg.Website.TimeoutMs) * time.Millisecond
			fetchCtx, cancel := context.WithTimeout(ctx, timeout)
			body, fetchErr := fetchStealthOnly(fetchCtx, stealth, siteURL, fmt.Sprintf("dir-%s", dedup.HashURL(siteURL)[:8]))
			cancel()

			if fetchErr != nil {
				slog.Warn("website: directory fetch failed", "url", siteURL, "error", fetchErr)
			} else {
				listings := directory.ExtractListings(siteURL, []byte(body))
				slog.Info("website: directory listings extracted",
					"url", siteURL, "count", len(listings), "worker", workerID)

				for _, l := range listings {
					if l.URL == "" {
						continue
					}
					if err := w.pushEnrichJob(ctx, enrichQueueKey, l.URL, queryID); err != nil {
						slog.Warn("website: push directory listing failed", "url", l.URL, "error", err)
					} else {
						w.pagesQueued.Add(1)
					}
				}
			}

			slog.Debug("website: directory domain processed", "domain", domain, "worker", workerID)
			continue
		}

		// Always push the original URL to enrich queue.
		if err := w.pushEnrichJob(ctx, enrichQueueKey, siteURL, queryID); err != nil {
			slog.Warn("website: push enrich failed", "url", siteURL, "error", err)
		} else {
			w.pagesQueued.Add(1)
		}

		// Generate and push contact page candidates.
		if w.cfg.Website.ContactPages {
			u, err := url.Parse(siteURL)
			if err != nil {
				continue
			}

			for _, path := range contactPagePaths {
				contactURL := fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, path)
				contactHash := dedup.HashURL(contactURL)

				// Insert website record for the contact page (DB dedup via url_hash UNIQUE).
				w.db.Exec(`
					INSERT INTO websites (domain, url, url_hash, source_query_id, page_type, status)
					VALUES ($1, $2, $3, $4, 'contact', 'pending')
					ON CONFLICT (url_hash) DO NOTHING
				`, domain, contactURL, contactHash, queryID)

				// Push to enrich queue.
				if err := w.pushEnrichJob(ctx, enrichQueueKey, contactURL, queryID); err != nil {
					slog.Warn("website: push contact page failed", "url", contactURL, "error", err)
				} else {
					w.pagesQueued.Add(1)
				}
			}
		}

		slog.Debug("website: domain processed", "domain", domain, "worker", workerID)
	}
}

// pushEnrichJob inserts an enrich_job record and pushes the URL to the enrich Redis queue.
func (w *WebsiteStage) pushEnrichJob(ctx context.Context, queueKey, contactURL string, queryID int64) error {
	urlHash := dedup.HashURL(contactURL)
	domain := dedup.ExtractDomain(contactURL)

	// Lookup website ID for the source_website_id FK (best-effort — may be nil).
	var websiteID sql.NullInt64
	w.db.QueryRowContext(ctx, `SELECT id FROM websites WHERE url_hash = $1 LIMIT 1`, urlHash).Scan(&websiteID)

	// Insert enrich_job row.
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO enrich_jobs (parent_job_id, source_website_id, domain, url, url_hash, status)
		VALUES ($1, $2, $3, $4, $5, 'pending')
		ON CONFLICT (url_hash) DO NOTHING
	`, queryID, websiteID, domain, contactURL, urlHash)
	if err != nil {
		return fmt.Errorf("insert enrich_job: %w", err)
	}

	// Push to Redis queue.
	job := &foxhound.Job{
		ID:        fmt.Sprintf("enrich-%s", urlHash[:12]),
		URL:       contactURL,
		Method:    "GET",
		Priority:  foxhound.PriorityNormal,
		CreatedAt: time.Now(),
		Meta: map[string]any{
			"query_id": queryID,
			"url_hash": urlHash,
		},
	}

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	micros := job.CreatedAt.UnixMicro()
	score := -(float64(job.Priority) * 1_000_000_000) + float64(micros)

	return w.dedup.Client().ZAdd(ctx, queueKey, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
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

// DomainsProcessed returns the count of processed domains.
func (w *WebsiteStage) DomainsProcessed() int64 {
	return w.domainsProcessed.Load()
}

// PagesQueued returns the count of pages pushed to enrich queue.
func (w *WebsiteStage) PagesQueued() int64 {
	return w.pagesQueued.Load()
}
