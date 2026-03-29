//go:build playwright

package standalone

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/behavior"
	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/directory"
	"github.com/sadewadee/serp-scraper/internal/scraper"
	"github.com/sadewadee/serp-scraper/internal/validate"
)

// Result holds a single contact result for CSV export.
type Result struct {
	Email            string
	Phone            string
	Domain           string
	SourceURL        string
	BusinessName     string
	BusinessCategory string
	Description      string
	Location         string
	Address          string
	Instagram        string
	Facebook         string
	Twitter          string
	LinkedIn         string
	WhatsApp         string
	OpeningHours     string
	Rating           string
	EmailStatus      string // safe, risky, invalid, unknown (from mordibouncer)
}

// Runner executes the full pipeline in-memory without PG/Redis.
type Runner struct {
	cfg    *config.Config
	query  string
	output string

	// Email validation.
	validator *validate.MordibouncerClient

	// In-memory state.
	urlSeen    sync.Map // map[string]bool — URL hash dedup
	domainSeen sync.Map // map[string]bool — domain dedup
	emailSeen  sync.Map // map[string]bool — email dedup
	results    []Result
	resultsMu  sync.Mutex

	// Channel as queue.
	contactQueue chan string

	// Metrics.
	serpURLs       atomic.Int64
	domainsFound   atomic.Int64
	pagesProcessed atomic.Int64
	emailsFound    atomic.Int64
	phonesFound    atomic.Int64

	startTime time.Time
}

// New creates a standalone runner.
func New(cfg *config.Config, query, output string) *Runner {
	return &Runner{
		cfg:          cfg,
		query:        query,
		output:       output,
		validator:    validate.NewMordibouncer(&cfg.Mordibouncer),
		contactQueue: make(chan string, 50000),
	}
}

// Run executes the full pipeline: SERP → Enrichment → CSV.
func (r *Runner) Run(ctx context.Context) error {
	r.startTime = time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.Info("standalone: starting pipeline",
		"query", r.query,
		"pages", r.cfg.SERP.PagesPerQuery,
		"output", r.output)

	var wg sync.WaitGroup

	// SERP discovery (1 worker, sequential pages) → pushes directly to contactQueue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(r.contactQueue)
		r.runSERP(ctx)
	}()

	// Enrichment workers (handle domain dedup, contact page gen, and extraction).
	var enrichWg sync.WaitGroup
	for i := 0; i < r.cfg.Enrich.Concurrency; i++ {
		enrichWg.Add(1)
		go func(id int) {
			defer enrichWg.Done()
			r.runEnrichment(ctx, id)
		}(i)
	}

	// Start metrics ticker.
	go r.metricsTicker(ctx)

	// Wait for all work to complete, then cancel to stop ticker.
	wg.Wait()
	enrichWg.Wait()
	cancel() // Stop the ticker

	// Final stats.
	elapsed := time.Since(r.startTime)
	slog.Info("standalone: pipeline complete",
		"elapsed", elapsed.Round(time.Second),
		"serp_urls", r.serpURLs.Load(),
		"domains", r.domainsFound.Load(),
		"pages", r.pagesProcessed.Load(),
		"emails", r.emailsFound.Load(),
		"phones", r.phonesFound.Load())

	// Write CSV.
	return r.writeCSV()
}

// runSERP scrapes Google SERP and pushes URLs to websiteQueue.
// Uses persistent session browser, consent banner handling, and careful timing.
func (r *Runner) runSERP(ctx context.Context) {
	slog.Info("serp: starting browser (persistent session + page pooling)")

	browser, err := scraper.NewSERPBrowser(r.cfg)
	if err != nil {
		slog.Error("serp: browser init failed", "error", err)
		return
	}
	defer browser.Close()

	// Use careful timing profile for Google — longer, more human-like delays.
	timing := behavior.NewTiming(behavior.CarefulProfile().Timing)

	for page := 0; page < r.cfg.SERP.PagesPerQuery; page++ {
		if ctx.Err() != nil {
			return
		}

		serpURL := scraper.BuildSERPURL(r.query, page, r.cfg.SERP.ResultsPerPage)
		slog.Info("serp: fetching page", "page", page+1, "url", serpURL)

		// Use FetchSERP which handles consent banners and waits for results.
		body, err := scraper.FetchSERP(ctx, browser, serpURL, fmt.Sprintf("serp-p%d", page))
		if err != nil {
			slog.Warn("serp: fetch failed", "page", page+1, "error", err)
			continue
		}

		urls, err := scraper.ParseSERPResults(body)
		if err != nil {
			slog.Warn("serp: parse failed", "page", page+1, "error", err)
			continue
		}

		newCount := 0
		for _, u := range urls {
			hash := dedup.HashURL(u)
			if _, loaded := r.urlSeen.LoadOrStore(hash, true); loaded {
				continue
			}
			r.contactQueue <- u
			newCount++
		}

		r.serpURLs.Add(int64(newCount))
		slog.Info("serp: page done", "page", page+1, "found", len(urls), "new", newCount)

		// Human-like delay between SERP pages — careful profile uses longer pauses.
		if page < r.cfg.SERP.PagesPerQuery-1 {
			delay := timing.SearchDelay() // 5-20s with careful profile
			slog.Debug("serp: waiting before next page", "delay", delay.Round(time.Millisecond))
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}

	slog.Info("serp: all pages done", "total_urls", r.serpURLs.Load())
}

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

// runEnrichment reads URLs from contactQueue, handles domain dedup + contact page
// generation, fetches pages, and extracts contacts.
// Standalone mode uses stealth HTTP only (no browser) for speed and clean shutdown.
// Stealth fetcher is recycled every 500 requests to prevent memory growth.
func (r *Runner) runEnrichment(ctx context.Context, workerID int) {
	stealth := scraper.NewStealth(r.cfg)
	defer stealth.Close()
	requestCount := 0

	for pageURL := range r.contactQueue {
		if ctx.Err() != nil {
			return
		}

		domain := dedup.ExtractDomain(pageURL)
		if domain == "" {
			continue
		}

		// Domain-level dedup: first time seeing this domain, generate contact page URLs.
		if _, loaded := r.domainSeen.LoadOrStore(domain, true); !loaded {
			r.domainsFound.Add(1)
			if r.cfg.Enrich.ContactPages {
				u, parseErr := url.Parse(pageURL)
				if parseErr == nil {
					for _, path := range contactPagePaths {
						contactURL := fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, path)
						hash := dedup.HashURL(contactURL)
						if _, seen := r.urlSeen.LoadOrStore(hash, true); !seen {
							select {
							case r.contactQueue <- contactURL:
							default:
							}
						}
					}
				}
			}
		}

		// Recycle stealth fetcher periodically.
		requestCount++
		if requestCount >= 500 {
			stealth.Close()
			stealth = scraper.NewStealth(r.cfg)
			requestCount = 0
		}

		timeout := time.Duration(r.cfg.Enrich.TimeoutMs) * time.Millisecond
		fetchCtx, cancel := context.WithTimeout(ctx, timeout)

		body, err := fetchStealth(fetchCtx, stealth, pageURL, fmt.Sprintf("enrich-%d", workerID))
		cancel()

		if err != nil {
			slog.Debug("enrichment: fetch failed", "url", pageURL, "error", err)
			continue
		}

		r.pagesProcessed.Add(1)

		// Check if this is a directory page — extract listings instead of contacts.
		if listings := directory.ExtractListings(pageURL, []byte(body)); len(listings) > 0 {
			slog.Info("enrichment: directory detected",
				"url", pageURL, "listings", len(listings), "source", listings[0].Source)
			for _, l := range listings {
				if l.URL != "" && !directory.IsDirectoryDomain(dedup.ExtractDomain(l.URL)) {
					// Push business website URLs to contact queue for enrichment.
					// Skip if URL is itself a directory (avoid loops).
					hash := dedup.HashURL(l.URL)
					if _, loaded := r.urlSeen.LoadOrStore(hash, true); !loaded {
						// Non-blocking send — contactQueue might be full or closing.
						select {
						case r.contactQueue <- l.URL:
						default:
						}
					}
				}
				// Store listing data directly if it has email or phone.
				if l.Email != "" || l.Phone != "" {
					r.resultsMu.Lock()
					r.results = append(r.results, Result{
						Email:            l.Email,
						Phone:            l.Phone,
						Domain:           dedup.ExtractDomain(pageURL),
						SourceURL:        pageURL,
						BusinessName:     l.Name,
						BusinessCategory: l.Category,
						Address:          l.Address,
						Rating:           l.Rating,
					})
					r.resultsMu.Unlock()
					if l.Email != "" {
						r.emailsFound.Add(1)
					}
				}
			}
			continue
		}

		// Regular page — extract contacts.
		cd := scraper.ExtractContacts([]byte(body))

		// Store results.
		for _, email := range cd.Emails {
			emailKey := strings.ToLower(email) + "|" + domain
			if _, loaded := r.emailSeen.LoadOrStore(emailKey, true); loaded {
				continue
			}

			// Optional MX validation (local DNS check).
			if r.cfg.Enrich.ValidateMX && !scraper.ValidateMX(email) {
				continue
			}

			// Optional Mordibouncer email validation (SMTP-level check).
			emailStatus := ""
			if r.cfg.Enrich.ValidateEmail && r.validator != nil {
				result, err := r.validator.Check(ctx, email)
				if err != nil {
					slog.Debug("enrichment: mordibouncer check failed", "email", email, "error", err)
				} else {
					emailStatus = result.IsReachable
					if !result.IsGoodEmail() {
						slog.Debug("enrichment: email rejected by mordibouncer",
							"email", email, "status", result.IsReachable, "sub", result.SubStatus)
						continue
					}
				}
			}

			phone := ""
			if len(cd.Phones) > 0 {
				phone = cd.Phones[0]
			}

			r.resultsMu.Lock()
			r.results = append(r.results, Result{
				Email:            email,
				Phone:            phone,
				Domain:           domain,
				SourceURL:        pageURL,
				BusinessName:     cd.BusinessName,
				BusinessCategory: cd.BusinessCategory,
				Description:      cd.Description,
				Location:         cd.Location,
				Address:          cd.Address,
				Instagram:        cd.Instagram,
				Facebook:         cd.Facebook,
				Twitter:          cd.Twitter,
				LinkedIn:         cd.LinkedIn,
				WhatsApp:         cd.WhatsApp,
				OpeningHours:     cd.OpeningHours,
				Rating:           cd.Rating,
				EmailStatus:      emailStatus,
			})
			r.resultsMu.Unlock()

			r.emailsFound.Add(1)
		}

		// Phone-only contacts.
		if len(cd.Emails) == 0 && len(cd.Phones) > 0 {
			for _, phone := range cd.Phones {
				r.resultsMu.Lock()
				r.results = append(r.results, Result{
					Phone:            phone,
					Domain:           domain,
					SourceURL:        pageURL,
					BusinessName:     cd.BusinessName,
					BusinessCategory: cd.BusinessCategory,
					Description:      cd.Description,
					Location:         cd.Location,
					Address:          cd.Address,
					Instagram:        cd.Instagram,
					Facebook:         cd.Facebook,
					Twitter:          cd.Twitter,
					LinkedIn:         cd.LinkedIn,
					WhatsApp:         cd.WhatsApp,
					OpeningHours:     cd.OpeningHours,
					Rating:           cd.Rating,
				})
				r.resultsMu.Unlock()
				r.phonesFound.Add(1)
			}
		}

		slog.Debug("enrichment: page done",
			"url", pageURL,
			"emails", len(cd.Emails),
			"phones", len(cd.Phones),
			"worker", workerID)
	}
}

func fetchStealth(ctx context.Context, stealth *fetch.StealthFetcher, pageURL, jobID string) (string, error) {
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

// writeCSV writes results to a CSV file.
func (r *Runner) writeCSV() error {
	r.resultsMu.Lock()
	results := make([]Result, len(r.results))
	copy(results, r.results)
	r.resultsMu.Unlock()

	if len(results) == 0 {
		slog.Warn("standalone: no results to export")
		return nil
	}

	f, err := os.Create(r.output)
	if err != nil {
		return fmt.Errorf("standalone: creating %s: %w", r.output, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header.
	w.Write([]string{
		"email", "email_status", "phone", "domain", "source_url",
		"business_name", "business_category", "description", "location", "address",
		"instagram", "facebook", "twitter", "linkedin", "whatsapp",
		"opening_hours", "rating",
	})

	for _, res := range results {
		w.Write([]string{
			res.Email, res.EmailStatus, res.Phone, res.Domain, res.SourceURL,
			res.BusinessName, res.BusinessCategory, res.Description, res.Location, res.Address,
			res.Instagram, res.Facebook, res.Twitter, res.LinkedIn, res.WhatsApp,
			res.OpeningHours, res.Rating,
		})
	}

	fmt.Printf("\nExported %d contacts to %s\n", len(results), r.output)
	return nil
}

// metricsTicker prints stats to stderr every 10 seconds.
func (r *Runner) metricsTicker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(r.startTime).Seconds()
			emailRate := float64(0)
			if elapsed > 0 {
				emailRate = float64(r.emailsFound.Load()) / elapsed * 3600
			}

			fmt.Fprintf(os.Stderr,
				"\r[%s] serp: %d urls | domains: %d | enrich: %d pages, %d emails (%.0f/hr), %d phones | queue: %d",
				time.Since(r.startTime).Round(time.Second),
				r.serpURLs.Load(),
				r.domainsFound.Load(),
				r.pagesProcessed.Load(),
				r.emailsFound.Load(),
				emailRate,
				r.phonesFound.Load(),
				len(r.contactQueue),
			)
		}
	}
}
