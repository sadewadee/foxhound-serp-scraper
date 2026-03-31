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

	"github.com/redis/go-redis/v9"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/directory"
	"github.com/sadewadee/serp-scraper/internal/persist"
	internalScraper "github.com/sadewadee/serp-scraper/internal/scraper"
)

// contactPagePaths are common paths that contain contact information.
// Reduced to 4 highest-yield paths to minimize 404 waste.
var contactPagePaths = []string{
	"/contact",
	"/contact-us",
	"/about",
	"/about-us",
}

// blockedDomains are sites with aggressive bot detection or no useful email content.
// Fetching these wastes crawl budget — they never yield personal wellness emails.
var blockedDomains = map[string]bool{
	// Directories with bot detection
	"www.yelp.com": true, "m.yelp.com": true,
	"www.tripadvisor.com": true, "www.tripadvisor.co.uk": true, "www.tripadvisor.co.id": true,
	"www.tripadvisor.de": true, "www.tripadvisor.fr": true, "www.tripadvisor.com.au": true,
	// Job boards
	"www.indeed.com": true, "de.indeed.com": true, "id.indeed.com": true,
	"www.ziprecruiter.com": true, "www.simplyhired.com": true,
	"www.glassdoor.com": true, "www.glassdoor.co.uk": true, "www.glassdoor.de": true,
	// Social media (no scrapeable emails)
	"www.linkedin.com": true,
	"www.facebook.com": true, "m.facebook.com": true,
	"www.instagram.com": true,
	"twitter.com": true, "x.com": true,
	"www.tiktok.com": true,
	"www.youtube.com": true,
	"www.pinterest.com": true,
	"www.reddit.com": true,
	// Q&A / research
	"www.quora.com": true,
	"www.researchgate.net": true,
	"pmc.ncbi.nlm.nih.gov": true,
	"journals.sagepub.com": true, "www.tandfonline.com": true,
	// Aggregators / no personal emails
	"rocketreach.co": true,
	"www.amazon.com": true, "www.walmart.com": true,
	"www.booking.com": true, "www.airbnb.com": true,
	"www.bbb.org": true,
	"maps.google.com": true,
	"www.yellowpages.com": true, "www.whitepages.com": true,
}

// isSkipDomain returns true for domains that should not be fetched.
func isSkipDomain(domain string) bool {
	if blockedDomains[domain] {
		return true
	}
	// Academic, government, military — not wellness/fitness target.
	// Includes international variants.
	for _, suffix := range []string{
		".edu", ".gov", ".mil",
		".ac.uk", ".gov.uk", ".edu.au", ".gov.au",
		".ac.id", ".go.id",
		".edu.sg", ".gov.sg",
		".ac.jp", ".go.jp",
	} {
		if strings.HasSuffix(domain, suffix) {
			return true
		}
	}
	return false
}

// EnrichStage runs Stage 4: Contact enrichment workers.
// Hot path is zero-DB: all claims via Redis SETNX, results pushed to
// Redis lists, persister drains to DB in background batches.
type EnrichStage struct {
	cfg   *config.Config
	db    *sql.DB
	dedup *dedup.Store

	// Shared browser state — all workers share one browser process (pool of tabs).
	browserMu     sync.Mutex
	sharedBrowser *fetch.CamoufoxFetcher
	lifecycle     *internalScraper.BrowserLifecycle

	pagesProcessed atomic.Int64
	emailsFound    atomic.Int64
	phonesFound    atomic.Int64
}

// NewEnrichStage creates a new contact enrichment stage.
func NewEnrichStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *EnrichStage {
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
func (c *EnrichStage) Run(ctx context.Context) error {
	// Requeue stuck processing jobs from previous run (startup only — uses DB).
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

// reconciler runs periodically to handle recovery (NOT hot path — uses DB).
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
					c.dedup.Client().Del(ctx, "enrich:lock:"+urlHash) // clear stale lock
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

// worker is the hot-path goroutine. Zero DB calls — claims via Redis SETNX,
// results pushed to Redis lists for persister to drain.
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

	queueKey := "serp:queue:enrich"
	redisClient := c.dedup.Client()

	for {
		if ctx.Err() != nil {
			return
		}

		// Recycle stealth fetcher periodically.
		stealthCount++
		if stealthCount >= stealthRecycleAfter {
			stealth.Close()
			stealth = internalScraper.NewStealth(c.cfg)
			stealthCount = 0
			slog.Debug("enrich: stealth recycled", "worker", workerID)
		}

		// Pop from Redis enrich queue.
		job, err := popFromQueue(ctx, redisClient, queueKey)
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

		// Skip domains that always block or are off-target — mark dead so reconciler doesn't re-queue.
		if isSkipDomain(domain) {
			urlHash, _ := job.Meta["url_hash"].(string)
			if urlHash == "" {
				urlHash = dedup.HashURL(pageURL)
			}
			result := persist.EnrichResult{
				URLHash:     urlHash,
				Status:      "dead",
				ErrorMsg:    "blocked domain: " + domain,
				IsPermanent: true,
				AttemptIncr: 1,
			}
			data, _ := json.Marshal(result)
			redisClient.RPush(ctx, persist.KeyResultEnrich, string(data))
			continue
		}

		// Domain-level dedup: queue contact pages for new domains.
		if c.cfg.Enrich.ContactPages {
			isDomainNew, dedupErr := c.dedup.Add(ctx, dedup.KeyDomains, domain)
			if dedupErr != nil {
				slog.Warn("enrich: domain dedup failed", "error", dedupErr)
			} else if isDomainNew {
				c.queueContactPages(ctx, pageURL, domain, job)
			}
		}

		// Extract url_hash from meta.
		urlHash, _ := job.Meta["url_hash"].(string)
		if urlHash == "" {
			urlHash = dedup.HashURL(pageURL)
		}

		// Redis SETNX claim — only one worker wins. If already claimed, skip.
		ok, claimErr := redisClient.SetNX(ctx, "enrich:lock:"+urlHash, workerIDStr, 5*time.Minute).Result()
		if claimErr != nil || !ok {
			continue
		}

		// Fetch page.
		timeout := time.Duration(c.cfg.Enrich.TimeoutMs) * time.Millisecond
		fetchCtx, cancel := context.WithTimeout(ctx, timeout)

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

		// Check lifecycle after every fetch attempt.
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

			// Push failure to Redis result queue (persister handles DB update).
			result := persist.EnrichResult{
				URLHash:     urlHash,
				Status:      "failed",
				ErrorMsg:    errMsg,
				AttemptIncr: 1,
				IsPermanent: isPermanentError(errMsg),
			}
			data, _ := json.Marshal(result)
			redisClient.RPush(ctx, persist.KeyResultEnrich, string(data))
			redisClient.Del(ctx, "enrich:lock:"+urlHash) // release lock
			continue
		}

		c.pagesProcessed.Add(1)

		// Directory path: extract business listings, queue individual URLs.
		if directory.IsDirectory(pageURL) {
			listings := directory.ExtractListings(pageURL, []byte(body))
			if len(listings) > 0 {
				slog.Info("enrich: directory listings extracted",
					"url", pageURL, "count", len(listings), "worker", workerID)
			}

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
					var qID int64
					if qid, ok := job.Meta["query_id"]; ok {
						switch v := qid.(type) {
						case float64:
							qID = int64(v)
						case int64:
							qID = v
						}
					}

					// Push directory listing to persister (DB INSERT deferred).
					dirResult := persist.EnrichResult{
						URLHash:     listingHash,
						Status:      "directory_listing",
						Website:     listingDomain, // reuse: domain
						Address:     l.URL,         // reuse: url
						AttemptIncr: int(qID),      // reuse: queryID
					}
					dirData, _ := json.Marshal(dirResult)
					redisClient.RPush(ctx, persist.KeyResultEnrich, string(dirData))

					// Push to enrich queue immediately.
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
					redisClient.ZAdd(ctx, queueKey, redis.Z{Score: score, Member: string(data)})
				}
			}

			allEmails := internalScraper.FilterEmails(rawEmails)
			allPhones := internalScraper.FilterPhones(rawPhones)

			socialLinks := buildSocialLinks(nil)

			// Push directory completion to persister.
			result := persist.EnrichResult{
				URLHash:     urlHash,
				Status:      "completed",
				Emails:      allEmails,
				Phones:      allPhones,
				SocialLinks: socialLinks,
			}
			data, _ := json.Marshal(result)
			redisClient.RPush(ctx, persist.KeyResultEnrich, string(data))
			redisClient.Del(ctx, "enrich:lock:"+urlHash) // release lock

			c.emailsFound.Add(int64(len(allEmails)))
			c.phonesFound.Add(int64(len(allPhones)))

			slog.Debug("enrich: directory page done",
				"url", pageURL, "listings", len(listings), "worker", workerID)
			continue
		}

		// Extract contacts.
		cd := internalScraper.ExtractContacts([]byte(body))
		emails := internalScraper.FilterEmails(cd.Emails)
		phones := internalScraper.FilterPhones(cd.Phones)

		// Optional MX validation.
		var mxValid *bool
		if c.cfg.Enrich.ValidateMX && len(emails) > 0 {
			v := internalScraper.ValidateMX(emails[0])
			mxValid = &v
			if !v {
				emails = nil
			}
		}

		socialLinks := buildSocialLinks(cd)
		websiteURL := fmt.Sprintf("https://%s", dedup.ExtractDomain(pageURL))

		// Push completion to Redis result queue (persister handles DB update).
		result := persist.EnrichResult{
			URLHash:          urlHash,
			Status:           "completed",
			Emails:           emails,
			Phones:           phones,
			SocialLinks:      socialLinks,
			ContactName:      cd.ContactName,
			BusinessName:     cd.BusinessName,
			BusinessCategory: cd.BusinessCategory,
			Description:      cd.Description,
			Website:          websiteURL,
			Address:          cd.Address,
			Location:         cd.Location,
			OpeningHours:     cd.OpeningHours,
			Rating:           cd.Rating,
			PageTitle:        cd.PageTitle,
			MXValid:          mxValid,
		}
		data, _ := json.Marshal(result)
		redisClient.RPush(ctx, persist.KeyResultEnrich, string(data))
		redisClient.Del(ctx, "enrich:lock:"+urlHash) // release lock

		c.emailsFound.Add(int64(len(emails)))
		c.phonesFound.Add(int64(len(phones)))

		slog.Debug("enrich: page done",
			"url", pageURL,
			"emails", len(emails),
			"phones", len(phones),
			"worker", workerID)
	}
}

// queueContactPages generates contact page candidate URLs and queues them.
// Zero DB — pushes to Redis result queue for persister + enrich queue for workers.
func (c *EnrichStage) queueContactPages(ctx context.Context, pageURL, domain string, job *foxhound.Job) {
	u, err := url.Parse(pageURL)
	if err != nil {
		return
	}

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
	redisClient := c.dedup.Client()

	for _, path := range contactPagePaths {
		contactURL := fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, path)
		contactHash := dedup.HashURL(contactURL)

		// Push contact page creation to persister (DB INSERTs deferred).
		cpResult := persist.EnrichResult{
			URLHash:     contactHash,
			Status:      "contact_page",
			Website:     domain,      // reuse: domain
			Address:     contactURL,  // reuse: url
			AttemptIncr: int(queryID), // reuse: queryID
		}
		cpData, _ := json.Marshal(cpResult)
		redisClient.RPush(ctx, persist.KeyResultEnrich, string(cpData))

		// Push to enrich queue immediately.
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
		redisClient.ZAdd(ctx, queueKey, redis.Z{Score: score, Member: string(data)})
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
// NOTE: HTTP 403 removed — with rotating proxy, a different IP may succeed.
// Blocked domains are handled by isSkipDomain() before fetching.
func isPermanentError(errMsg string) bool {
	permanent := []string{
		"HTTP 404",
		"HTTP 410",
		"HTTP 451",
		"certificate",
		"x509",
		"no such host",
		"server misbehaving",
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
