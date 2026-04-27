package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/sadewadee/serp-scraper/internal/reenrich"
)

// reenrichRequest is the body shape for POST /api/v2/enrichments/reenrich.
//
// Filter selects the eligibility query (no_valid_email | no_email | specific_domains).
// Domains is required when Filter == "specific_domains" and ignored otherwise.
// Limit=0 means unlimited (consistent with the scheduler default — global scraping).
type reenrichRequest struct {
	Filter     string   `json:"filter"`
	Domains    []string `json:"domains"`
	MaxAgeDays int      `json:"max_age_days"`
	GapDays    int      `json:"gap_days"`
	Limit      int      `json:"limit"`
	ExtraPaths []string `json:"extra_paths"`
	DryRun     bool     `json:"dry_run"`
}

type reenrichResponse struct {
	Filter         string `json:"filter"`
	Eligible       int    `json:"eligible"`
	Queued         int    `json:"queued"`
	PathsPerDomain int    `json:"paths_per_domain"`
	EstimatedHours int    `json:"estimated_completion_hours"`
	DryRun         bool   `json:"dry_run"`
}

// handleV2Reenrich queues a selective re-enrichment run. Admin-only because
// it can fan out into millions of jobs depending on filter + path count.
//
// dry_run=true returns the eligible count without enqueueing — useful for
// estimating cost before committing.
func (s *Server) handleV2Reenrich(w http.ResponseWriter, r *http.Request) {
	// Re-enrich runs can take seconds to compute eligibility on a 10M table;
	// give them a generous timeout independent of v2RequestContext.
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	var req reenrichRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeV2Error(w, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}

	opts := reenrich.DefaultOptions()
	if req.Filter != "" {
		opts.Filter = reenrich.FilterMode(req.Filter)
	}
	if req.MaxAgeDays > 0 {
		opts.MaxAgeDays = req.MaxAgeDays
	}
	if req.GapDays > 0 {
		opts.GapDays = req.GapDays
	}
	opts.Limit = req.Limit // 0 = unlimited; pass through caller intent
	opts.Domains = req.Domains
	opts.ExtraPaths = req.ExtraPaths

	domains, err := reenrich.EligibleDomains(ctx, s.db, opts)
	if err != nil {
		slog.Error("v2: reenrich eligibility query failed", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "eligibility_failed", err.Error())
		return
	}

	pathsPerDomain := len(reenrich.ContactPaths) + len(opts.ExtraPaths)
	queued := 0
	if !req.DryRun {
		n, qErr := reenrich.EnqueueDomains(ctx, s.db, domains, opts)
		if qErr != nil {
			slog.Warn("v2: reenrich enqueue partial failure", "error", qErr, "queued", n)
		}
		queued = n
	}

	// Conservative estimate: enrich rate observed in practice ~5200/h per
	// 4-worker container. Caller can divide by their actual replica count.
	estimatedHours := 0
	if queued > 0 {
		estimatedHours = (queued + 5199) / 5200
	}

	writeV2Single(w, reenrichResponse{
		Filter:         string(opts.Filter),
		Eligible:       len(domains),
		Queued:         queued,
		PathsPerDomain: pathsPerDomain,
		EstimatedHours: estimatedHours,
		DryRun:         req.DryRun,
	})
}

// handleV2ReenrichStats reports current re-enrich queue depth + recent
// throughput. Read-only, viewer-accessible — useful for dashboard polling.
func (s *Server) handleV2ReenrichStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	var pending, processing, completedToday, failedToday int
	_ = s.db.QueryRowContext(ctx, `
		SELECT
		  COUNT(*) FILTER (WHERE status = 'pending'),
		  COUNT(*) FILTER (WHERE status = 'processing'),
		  COUNT(*) FILTER (WHERE status = 'completed' AND completed_at >= date_trunc('day', NOW())),
		  COUNT(*) FILTER (WHERE status = 'failed'    AND updated_at  >= date_trunc('day', NOW()))
		FROM enrichment_jobs
		WHERE source = 'reenrich'
	`).Scan(&pending, &processing, &completedToday, &failedToday)

	writeV2Single(w, map[string]int{
		"reenrich_pending":         pending,
		"reenrich_processing":      processing,
		"reenrich_completed_today": completedToday,
		"reenrich_failed_today":    failedToday,
	})
}
