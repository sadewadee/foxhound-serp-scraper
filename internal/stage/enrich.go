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

// blockedDomains: exact-match host blocklist. Domains here are skipped at
// SERP-result-INSERT time, so they never spawn enrichment jobs.
//
// Categories captured (kept inline for fast lookup + audit trail):
//   - Directory aggregators (Yelp, TripAdvisor, Indeed, Glassdoor, BBB)
//   - Social/UGC (LinkedIn, Facebook, Twitter, TikTok, YouTube, Pinterest, Reddit, Quora)
//   - Marketplaces (Amazon TLD variants, eBay, Etsy, AliExpress, Vinted, Idealo, Mobile.de)
//   - Q&A platforms (Zhihu, Baidu Zhidao/Jingyan, Stack Overflow/Exchange, Yahoo Chiebukuro)
//   - News/media (CNBC, Bloomberg, Reuters, BBC, NYT, Guardian, WaPo, WSJ, CNN, Fox)
//   - Software/corporate help (Adobe, Canva, Dell, MS support, Apple support, GitHub)
//   - Travel (Booking, Expedia, Agoda, Kayak, Priceline, Airbnb)
//   - Health/medical (PsychologyToday, Drugs.com, WebMD, Healthline, Mayo Clinic)
//   - Adult (sample from prod logs)
//   - Gaming (Poki, JeuxVideo, MLB, Steam community)
//   - Brand/corporate (Nike, Adidas, UnderArmour, Peloton, Lululemon, REI, IKEA)
//   - Image/document/CDN (IMDB, Scribd, Shutterstock, Getty)
//   - Logistics (UPS, FedEx, DHL)
//   - Academic (PMC NCBI, SAGE, Taylor & Francis)
//   - Other (RocketReach contacts aggregator, Yellowpages, Whitepages, Walmart)
var blockedDomains = map[string]bool{
	// Directory aggregators
	"www.yelp.com": true, "m.yelp.com": true,
	"www.tripadvisor.com": true, "www.tripadvisor.co.uk": true, "www.tripadvisor.co.id": true,
	"www.tripadvisor.de": true, "www.tripadvisor.fr": true, "www.tripadvisor.com.au": true,
	"www.indeed.com": true, "de.indeed.com": true, "id.indeed.com": true,
	"www.ziprecruiter.com": true, "www.simplyhired.com": true,
	"www.glassdoor.com": true, "www.glassdoor.co.uk": true, "www.glassdoor.de": true,
	"www.bbb.org": true, "maps.google.com": true,
	"www.yellowpages.com": true, "www.whitepages.com": true,
	"rocketreach.co": true,

	// Social / UGC
	"www.linkedin.com": true,
	"www.facebook.com": true, "m.facebook.com": true,
	"www.instagram.com": true,
	"twitter.com":       true, "x.com": true,
	"www.tiktok.com": true, "www.youtube.com": true,
	"www.pinterest.com": true, "www.reddit.com": true,
	"www.quora.com": true, "www.researchgate.net": true,

	// Academic
	"pmc.ncbi.nlm.nih.gov": true,
	"journals.sagepub.com": true, "www.tandfonline.com": true,

	// Amazon TLD variants (off-niche e-commerce — variants previously bocor)
	"www.amazon.com": true, "www.amazon.de": true, "www.amazon.fr": true,
	"www.amazon.co.uk": true, "www.amazon.ca": true, "www.amazon.com.au": true,
	"www.amazon.it": true, "www.amazon.es": true, "www.amazon.com.mx": true,
	"www.amazon.com.br": true, "www.amazon.co.jp": true, "www.amazon.in": true,
	"www.amazon.nl": true, "www.amazon.com.tr": true, "www.amazon.sg": true,
	"www.amazon.ae": true,

	// Marketplaces global
	"www.walmart.com": true,
	"www.etsy.com":    true,
	"www.alibaba.com": true, "www.aliexpress.com": true, "www.aliexpress.us": true,
	"www.taobao.com": true, "www.tmall.com": true,
	"www.ebay.com": true, "www.ebay.de": true, "www.ebay.co.uk": true,
	"www.ebay.fr": true, "www.ebay.it": true, "www.ebay.es": true,
	"www.idealo.de": true, "www.idealo.fr": true, "www.idealo.it": true,
	"www.kleinanzeigen.de": true, "www.mobile.de": true,
	"www.vinted.com": true, "www.vinted.fr": true, "www.vinted.de": true,
	"www.vinted.it": true, "www.vinted.es": true,
	"www.leboncoin.fr": true, "www.immobilienscout24.de": true,

	// Q&A / forum platforms (huge contamination per prod data)
	"www.zhihu.com": true, "zhuanlan.zhihu.com": true,
	"zhidao.baidu.com": true, "jingyan.baidu.com": true,
	"www.justanswer.com": true, "www.chegg.com": true,
	"stackoverflow.com": true, "superuser.com": true, "askubuntu.com": true,
	"stackexchange.com": true, "math.stackexchange.com": true,
	"answers.microsoft.com": true, "answers.yahoo.com": true,
	"detail.chiebukuro.yahoo.co.jp": true, "chiebukuro.yahoo.co.jp": true,
	// Forum hosts that don't match prefix patterns (creative naming) but
	// appeared in prod top 60 — exact-match entries catch them.
	"www.tenforums.com":       true, // Windows 10 forum
	"www.fxp.co.il":           true, // Israeli forum
	"www.52pojie.cn":          true, // Chinese tech forum
	"www.juraforum.de":        true, // German legal forum
	"www.motor-talk.de":       true, // German auto forum
	"www.radioforen.de":       true, // German radio forum
	"www.datev-community.de":  true, // German business community
	"telekomhilft.telekom.de": true, // German telco support
	"www.60millions-mag.com":  true, // French consumer
	"web2.cylex.de":           true, // German business directory
	"www.bankier.pl":          true, // Polish banking forum

	// News/media
	"www.cnbc.com": true, "www.bloomberg.com": true, "www.reuters.com": true,
	"www.bbc.com": true, "www.nytimes.com": true, "www.theguardian.com": true,
	"www.washingtonpost.com": true, "www.wsj.com": true, "edition.cnn.com": true,
	"www.foxnews.com": true, "join1440.com": true, "tecnobits.com": true,

	// Software / corporate help
	"www.adobe.com": true, "www.canva.com": true, "www.dell.com": true,
	"www.microsoft.com": true, "support.microsoft.com": true,
	"support.google.com": true, "support.apple.com": true, "www.apple.com": true,
	"github.com": true, "gitlab.com": true, "bitbucket.org": true,
	"www.chip.de": true, "quillbot.com": true,
	"apkpure.com": true, "steamcommunity.com": true,

	// Travel
	"www.booking.com": true, "www.airbnb.com": true,
	"www.expedia.com": true, "www.agoda.com": true,
	"www.kayak.com": true, "www.priceline.com": true,

	// Health/medical
	"www.psychologytoday.com": true,
	"www.drugs.com":           true, "www.webmd.com": true, "www.healthline.com": true,
	"www.mayoclinic.org": true, "medlineplus.gov": true,
	"insurancetoday.cc": true, "goodins.life": true,

	// Adult
	"sexfinder.com": true, "www.sexfinder.com": true,
	"friendfinder.com": true, "www.friendfinder.com": true,
	"bakecaincontrii.com": true, "cosenza.bakecaincontrii.com": true,

	// Gaming
	"poki.com": true, "www.poki.com": true,
	"www.jeuxvideo.com": true, "www.mlb.com": true,

	// Fitness brands corporate (no public business contact)
	"www.nike.com": true, "www.adidas.com": true, "www.underarmour.com": true,
	"www.onepeloton.com": true, "www.lululemon.com": true,
	"www.rei.com": true, "www.dickssportinggoods.com": true,

	// Other
	"www.ikea.com": true,
	"www.imdb.com": true, "www.scribd.com": true,
	"www.shutterstock.com": true, "www.gettyimages.com": true,
	"www.ups.com": true, "www.fedex.com": true, "www.dhl.com": true,
}

// isSkipDomain reports whether a domain should be skipped from enrichment.
// Three matching layers, in order of precedence:
//  1. Exact host match against blockedDomains (fast O(1) map lookup)
//  2. Suffix match for gov/edu/mil TLDs (English + non-English country codes)
//  3. Subdomain prefix/infix patterns for forum/community/Q&A/support hosts
//
// Layer 3 uses STRICT subdomain matching (HasPrefix or ".pat." infix), not
// substring contains — substring contains incorrectly flagged hosts like
// www.apparelsupport.com (matched "support." in path) as forums/support.
func isSkipDomain(domain string) bool {
	if blockedDomains[domain] {
		return true
	}
	suffixes := []string{
		// English-speaking gov/edu
		".edu", ".gov", ".mil",
		".ac.uk", ".gov.uk", ".edu.au", ".gov.au",
		// SE Asia gov/edu
		".ac.id", ".go.id", ".edu.sg", ".gov.sg", ".ac.jp", ".go.jp",
		// EU + LATAM gov
		".gouv.fr", ".gov.de", ".gob.mx", ".gob.es",
		// East Asia gov/edu
		".ac.cn", ".gov.cn", ".edu.cn", ".ac.kr", ".go.kr",
	}
	for _, suffix := range suffixes {
		if strings.HasSuffix(domain, suffix) {
			return true
		}
	}
	// Subdomain-prefix patterns: matches hosts like forum.example.com,
	// support.google.com — strict prefix only, NOT substring (avoids
	// www.apparelsupport.com false-positive on "support.").
	subdomainPrefixes := []string{
		"forum.", "forums.", "community.",
		"diskuse.", "discussions.",
		"support.", "help.",
	}
	for _, p := range subdomainPrefixes {
		if strings.HasPrefix(domain, p) {
			return true
		}
	}
	// Subdomain-infix patterns: matches hosts where the pattern appears as
	// a separate subdomain segment — e.g. abc.forum.example.com. The leading
	// dot in the pattern guarantees match only at segment boundaries.
	subdomainInfixes := []string{
		".forum.", ".forums.", ".community.",
	}
	for _, p := range subdomainInfixes {
		if strings.Contains(domain, p) {
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
		// All textual fields go through nullIfEmpty so an empty extraction
		// from a re-enrichment cannot clobber a prior valid value via the
		// trigger's COALESCE merge (COALESCE treats '' as truthy; only NULL
		// falls through to the existing column).
		_, updErr := c.db.Exec(`UPDATE enrichment_jobs SET
			status = 'completed',
			raw_emails = $1, raw_phones = $2, raw_social = $3,
			raw_business_name = $4, raw_category = $5, raw_address = $6,
			raw_page_title = $7,
			raw_description = $8, raw_location = $9, raw_country = $10,
			raw_city = $11, raw_contact_name = $12,
			raw_opening_hours = $13, raw_rating = $14,
			raw_tiktok = $15, raw_youtube = $16, raw_telegram = $17,
			locked_by = NULL, completed_at = NOW(), updated_at = NOW()
		WHERE url_hash = $18`,
			pq.Array(emails), pq.Array(phones), socialJSON,
			nullIfEmpty(cd.BusinessName), nullIfEmpty(cd.BusinessCategory), nullIfEmpty(cd.Address),
			nullIfEmpty(cd.PageTitle),
			nullIfEmpty(cd.Description), nullIfEmpty(cd.Location), nullIfEmpty(cd.Country),
			nullIfEmpty(cd.City), nullIfEmpty(cd.ContactName),
			nullIfEmpty(cd.OpeningHours), nullIfEmpty(cd.Rating),
			nullIfEmpty(cd.TikTok), nullIfEmpty(cd.YouTube), nullIfEmpty(cd.Telegram),
			urlHash)
		if updErr != nil {
			// Don't leave the job in 'processing' — reconciler would re-fetch
			// in 5 min thinking it stalled, wasting bandwidth. Mark failed so
			// the resurrection logic governs retries with attempt_count cap.
			slog.Error("enrich: completion UPDATE failed, marking job failed",
				"url_hash", urlHash, "url", pageURL, "error", updErr)
			c.db.Exec(`UPDATE enrichment_jobs SET status = 'failed',
				error_msg = $1, locked_by = NULL, updated_at = NOW()
			WHERE url_hash = $2`, "completion-update-failed: "+updErr.Error(), urlHash)
		}

		redisClient.Del(ctx, "enrich:lock:"+urlHash)
		c.emailsFound.Add(int64(len(emails)))
		c.phonesFound.Add(int64(len(phones)))

		// Only log pages that actually yielded contacts — skip empty ones
		// to avoid log spam for the ~70% of pages that return nothing.
		if len(emails) > 0 || len(phones) > 0 {
			firstEmail := ""
			if len(emails) > 0 {
				firstEmail = emails[0]
			}
			slog.Info("enrich: page done",
				"url", pageURL,
				"emails", len(emails),
				"phones", len(phones),
				"first_email", firstEmail,
				"business", cd.BusinessName,
				"worker", workerID)
		}
	}
}

// buildSocialLinks builds the JSONB social_links blob. Platforms with
// dedicated TEXT columns on business_listings (tiktok, youtube, telegram)
// are NOT duplicated into the JSONB to keep a single source of truth —
// architect review found that dual storage diverges across re-enrichments.
// Old socials (fb/ig/tw/li/wa) stay in JSONB for backward-compatibility
// with existing consumers and to avoid a wider schema change.
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

// nullIfEmpty returns sql.NullString so empty strings round-trip as SQL NULL.
// Avoids overwriting a pre-existing column value with ” on subsequent
// enrichments where the extractor failed to capture this field.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
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
	contentType := ""
	if resp.Headers != nil {
		contentType = resp.Headers.Get("Content-Type")
	}
	return decodeBodyUTF8(resp.Body, contentType), nil
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
