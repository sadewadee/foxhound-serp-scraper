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

	"github.com/redis/go-redis/v9"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/behavior"
	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/query"
	"github.com/sadewadee/serp-scraper/internal/scraper"
)

// SERPStage runs Stage 2: SERP discovery workers.
type SERPStage struct {
	cfg     *config.Config
	db      *sql.DB
	dedup   *dedup.Store
	queryRepo *query.Repository
	timing  *behavior.Timing

	// Metrics.
	queriesProcessed atomic.Int64
	urlsFound        atomic.Int64
}

// NewSERPStage creates a new SERP discovery stage.
func NewSERPStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *SERPStage {
	return &SERPStage{
		cfg:       cfg,
		db:        database,
		dedup:     dd,
		queryRepo: query.NewRepository(database),
		timing: behavior.NewTiming(behavior.TimingConfig{
			Mu:    1.0,
			Sigma: 0.8,
			Min:   2 * time.Second,
			Max:   30 * time.Second,
		}),
	}
}

// Run starts SERP discovery workers. Blocks until ctx is cancelled or work is done.
func (s *SERPStage) Run(ctx context.Context) error {
	// Requeue any queries stuck in 'processing' from previous run.
	if n, err := s.queryRepo.RequeueProcessing(); err != nil {
		slog.Warn("serp: requeue failed", "error", err)
	} else if n > 0 {
		slog.Info("serp: requeued processing queries", "count", n)
	}

	numWorkers := s.cfg.SERP.Workers
	slog.Info("serp: starting workers", "count", numWorkers)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.worker(ctx, workerID)
		}(w)
	}

	wg.Wait()
	slog.Info("serp: all workers done",
		"queries", s.queriesProcessed.Load(),
		"urls", s.urlsFound.Load())
	return nil
}

func (s *SERPStage) worker(ctx context.Context, workerID int) {
	slog.Info("serp: worker starting", "worker", workerID)

	// Each worker gets its own browser instance.
	browser, err := scraper.NewBrowser(s.cfg)
	if err != nil {
		slog.Error("serp: worker browser init failed", "worker", workerID, "error", err)
		return
	}
	defer browser.Close()

	for {
		if ctx.Err() != nil {
			return
		}

		// Get next pending query from PG (atomic claim).
		q, err := s.queryRepo.MarkProcessing()
		if err != nil {
			slog.Error("serp: mark processing failed", "worker", workerID, "error", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if q == nil {
			// No more pending queries — check periodically.
			slog.Debug("serp: no pending queries, waiting", "worker", workerID)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}

		slog.Info("serp: processing query", "worker", workerID, "query", q.Text, "id", q.ID)
		resultCount := s.processQuery(ctx, browser, q)

		status := "completed"
		errMsg := ""
		if resultCount == 0 {
			status = "completed" // Still completed, just no results.
		}
		if err := s.queryRepo.UpdateStatus(q.ID, status, resultCount, errMsg); err != nil {
			slog.Error("serp: update status failed", "error", err)
		}

		s.queriesProcessed.Add(1)

		// Delay between queries.
		delay := s.timing.SearchDelay()
		slog.Debug("serp: delay between queries", "delay", delay, "worker", workerID)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (s *SERPStage) processQuery(ctx context.Context, browser *fetch.CamoufoxFetcher, q *db.Query) int {
	totalURLs := 0
	queueKey := "serp:queue:websites"

	for page := 0; page < s.cfg.SERP.PagesPerQuery; page++ {
		if ctx.Err() != nil {
			break
		}

		serpURL := scraper.BuildSERPURL(q.Text, page, s.cfg.SERP.ResultsPerPage)

		// Insert SERP seed.
		var seedID int64
		err := s.db.QueryRow(`
			INSERT INTO serp_seeds (query_id, google_url, page_num, status)
			VALUES ($1, $2, $3, 'processing')
			ON CONFLICT (query_id, page_num) DO UPDATE SET status = 'processing'
			RETURNING id
		`, q.ID, serpURL, page).Scan(&seedID)
		if err != nil {
			slog.Error("serp: insert seed failed", "error", err)
			continue
		}

		// Fetch SERP page via browser.
		slog.Debug("serp: fetching page", "query", q.Text, "page", page, "url", serpURL)
		body, err := scraper.FetchWithBrowser(ctx, browser, serpURL, fmt.Sprintf("serp-%d-%d", q.ID, page))
		if err != nil {
			slog.Warn("serp: fetch failed", "query", q.Text, "page", page, "error", err)
			s.db.Exec(`UPDATE serp_seeds SET status = 'failed' WHERE id = $1`, seedID)
			continue
		}

		// Parse results.
		urls, err := scraper.ParseSERPResults(body)
		if err != nil {
			slog.Warn("serp: parse failed", "query", q.Text, "page", page, "error", err)
			s.db.Exec(`UPDATE serp_seeds SET status = 'failed' WHERE id = $1`, seedID)
			continue
		}

		// Dedup and insert websites.
		inserted := 0
		for _, u := range urls {
			urlHash := dedup.HashURL(u)

			// Redis URL dedup.
			isNew, err := s.dedup.Add(ctx, dedup.KeyURLs, urlHash)
			if err != nil {
				slog.Warn("serp: dedup check failed", "error", err)
				continue
			}
			if !isNew {
				continue
			}

			domain := dedup.ExtractDomain(u)
			if domain == "" {
				continue
			}

			// Insert website.
			_, err = s.db.Exec(`
				INSERT INTO websites (domain, url, url_hash, source_query_id, source_serp_id, page_type, status)
				VALUES ($1, $2, $3, $4, $5, 'serp_result', 'pending')
				ON CONFLICT (url_hash) DO NOTHING
			`, domain, u, urlHash, q.ID, seedID)
			if err != nil {
				slog.Warn("serp: insert website failed", "url", u, "error", err)
				continue
			}

			// Push to Redis queue for Stage 3.
			if err := pushToQueue(ctx, s.dedup.Client(), queueKey, u, q.ID, seedID); err != nil {
				slog.Warn("serp: push to queue failed", "url", u, "error", err)
			}

			inserted++
		}

		totalURLs += inserted
		s.urlsFound.Add(int64(inserted))

		// Update seed.
		s.db.Exec(`UPDATE serp_seeds SET status = 'completed', result_count = $1 WHERE id = $2`,
			len(urls), seedID)

		slog.Info("serp: page done",
			"query", q.Text, "page", page,
			"found", len(urls), "new", inserted)

		// Delay between pages.
		if page < s.cfg.SERP.PagesPerQuery-1 {
			delay := s.timing.PaginationDelay()
			select {
			case <-ctx.Done():
				return totalURLs
			case <-time.After(delay):
			}
		}
	}

	return totalURLs
}

// pushToQueue pushes a URL job to a Redis sorted set queue.
func pushToQueue(ctx context.Context, client *redis.Client, queueKey, urlStr string, queryID, serpID int64) error {
	job := &foxhound.Job{
		ID:        fmt.Sprintf("web-%d-%s", queryID, dedup.HashURL(urlStr)[:8]),
		URL:       urlStr,
		Method:    "GET",
		Priority:  foxhound.PriorityNormal,
		CreatedAt: time.Now(),
		Meta: map[string]any{
			"query_id": queryID,
			"serp_id":  serpID,
		},
	}

	micros := job.CreatedAt.UnixMicro()
	score := -(float64(job.Priority) * 1_000_000_000) + float64(micros)

	data := fmt.Sprintf(`{"id":"%s","url":"%s","method":"GET","priority":%d,"meta":{"query_id":%d,"serp_id":%d}}`,
		job.ID, urlStr, job.Priority, queryID, serpID)

	return client.ZAdd(ctx, queueKey, redis.Z{
		Score:  score,
		Member: data,
	}).Err()
}

// QueriesProcessed returns the count of processed queries.
func (s *SERPStage) QueriesProcessed() int64 {
	return s.queriesProcessed.Load()
}

// URLsFound returns the count of discovered URLs.
func (s *SERPStage) URLsFound() int64 {
	return s.urlsFound.Load()
}
