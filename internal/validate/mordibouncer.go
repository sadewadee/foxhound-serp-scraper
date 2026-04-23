package validate

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/sadewadee/serp-scraper/internal/config"
)

// MordibouncerClient validates emails via the Mordibouncer API.
type MordibouncerClient struct {
	apiURL     string
	secret     string
	httpClient *http.Client
}

// NewMordibouncer creates a new Mordibouncer validation client.
// Returns nil if not configured (no API URL or secret).
func NewMordibouncer(cfg *config.MordibouncerConfig) *MordibouncerClient {
	if cfg.APIURL == "" || cfg.Secret == "" {
		return nil
	}
	return &MordibouncerClient{
		apiURL: cfg.APIURL,
		secret: cfg.Secret,
		httpClient: &http.Client{
			Timeout: 60 * time.Second, // SMTP checks can be slow
		},
	}
}

// checkRequest is the request body for the Mordibouncer API.
type checkRequest struct {
	ToEmail string `json:"to_email"`
}

// CheckResult holds the parsed Mordibouncer API response.
type CheckResult struct {
	Email       string `json:"input"`
	IsReachable string `json:"is_reachable"` // "safe", "risky", "invalid", "unknown"
	SubStatus   string `json:"sub_status"`   // "role_based", "disposable", etc.

	MX struct {
		AcceptsMail bool     `json:"accepts_mail"`
		Records     []string `json:"records"`
	} `json:"mx"`

	SMTP struct {
		CanConnect    bool `json:"can_connect_smtp"`
		IsDeliverable bool `json:"is_deliverable"`
		IsCatchAll    bool `json:"is_catch_all"`
		IsDisabled    bool `json:"is_disabled"`
		HasFullInbox  bool `json:"has_full_inbox"`
	} `json:"smtp"`

	Misc struct {
		IsDisposable   bool `json:"is_disposable"`
		IsFreeProvider bool `json:"is_free_provider"`
		IsRoleAccount  bool `json:"is_role_account"`
		IsHoneypot     bool `json:"is_honeypot"`
		IsB2C          bool `json:"is_b2c"`
	} `json:"misc"`

	Syntax struct {
		IsValidSyntax bool   `json:"is_valid_syntax"`
		Domain        string `json:"domain"`
		Username      string `json:"username"`
	} `json:"syntax"`
}

// Check validates an email address via the Mordibouncer API.
func (c *MordibouncerClient) Check(ctx context.Context, email string) (*CheckResult, error) {
	body, err := json.Marshal(checkRequest{ToEmail: email})
	if err != nil {
		return nil, fmt.Errorf("mordibouncer: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.apiURL+"/v1/check_email", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("mordibouncer: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-mordibouncer-secret", c.secret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mordibouncer: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("mordibouncer: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result CheckResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("mordibouncer: decode response: %w", err)
	}

	return &result, nil
}

// FilterValid checks emails via the Mordibouncer API and returns only valid ones.
// Fail-open: on API error, the email is kept to avoid data loss.
// Uses a 10s per-email timeout to avoid blocking callers for too long.
func (c *MordibouncerClient) FilterValid(ctx context.Context, emails []string) []string {
	var valid []string
	for _, email := range emails {
		emailCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, err := c.Check(emailCtx, email)
		cancel()
		if err != nil {
			valid = append(valid, email)
			continue
		}
		if result.IsGoodEmail() {
			valid = append(valid, email)
		} else {
			slog.Debug("mordibouncer: email rejected", "email", email, "status", result.IsReachable)
		}
	}
	return valid
}

// emailRow is a single email awaiting validation.
type emailRow struct {
	id    int64
	email string
}

// bulkSubmitRequest is the body for POST /v1/bulk.
type bulkSubmitRequest struct {
	Input []string `json:"input"`
}

// bulkSubmitResponse is the response from POST /v1/bulk.
type bulkSubmitResponse struct {
	JobID int64 `json:"job_id"`
}

// bulkProgressResponse is the response from GET /v1/bulk/{job_id}.
type bulkProgressResponse struct {
	JobID          int64  `json:"job_id"`
	TotalRecords   int    `json:"total_records"`
	TotalProcessed int    `json:"total_processed"`
	JobStatus      string `json:"job_status"` // "Running", "Completed"
}

// SubmitBulk submits a batch of emails for async validation.
func (c *MordibouncerClient) SubmitBulk(ctx context.Context, emails []string) (int64, error) {
	body, err := json.Marshal(bulkSubmitRequest{Input: emails})
	if err != nil {
		return 0, fmt.Errorf("mordibouncer: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.apiURL+"/v1/bulk", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("mordibouncer: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-mordibouncer-secret", c.secret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("mordibouncer: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("mordibouncer: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result bulkSubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("mordibouncer: decode: %w", err)
	}
	return result.JobID, nil
}

// PollBulk checks if a bulk job is done.
func (c *MordibouncerClient) PollBulk(ctx context.Context, jobID int64) (*bulkProgressResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/bulk/%d", c.apiURL, jobID), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-mordibouncer-secret", c.secret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result bulkProgressResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// FetchBulkResults gets the full results for a completed bulk job using pagination.
func (c *MordibouncerClient) FetchBulkResults(ctx context.Context, jobID int64) ([]CheckResult, error) {
	const pageSize = 200
	var all []CheckResult

	for offset := 0; ; offset += pageSize {
		req, err := http.NewRequestWithContext(ctx, "GET",
			fmt.Sprintf("%s/v1/bulk/%d/results?limit=%d&offset=%d", c.apiURL, jobID, pageSize, offset), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("x-mordibouncer-secret", c.secret)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}

		var wrapper struct {
			Results []CheckResult `json:"results"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		all = append(all, wrapper.Results...)
		if len(wrapper.Results) < pageSize {
			break
		}
	}
	return all, nil
}

// BackfillValidation scans existing emails with pending validation status
// and validates them via Mordibouncer bulk API.
// Flow: fetch 500 pending → submit bulk → poll until done → fetch results → write DB.
func BackfillValidation(ctx context.Context, db *sql.DB, client *MordibouncerClient) {
	const batchSize = 500

	slog.Info("backfill: starting email validation (bulk mode)")

	// Small delay to let other services start first.
	select {
	case <-ctx.Done():
		return
	case <-time.After(30 * time.Second):
	}

	var validated, removed atomic.Int64

	// Progress logger.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				slog.Info("backfill: progress",
					"validated", validated.Load(),
					"removed", removed.Load())
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			break
		}

		// 1. Fetch batch of pending emails.
		rows, err := db.QueryContext(ctx, `
			SELECT id, email
			FROM emails
			WHERE validation_status = 'pending'
			ORDER BY created_at ASC
			LIMIT $1
		`, batchSize)
		if err != nil {
			slog.Warn("backfill: query failed", "error", err)
			time.Sleep(30 * time.Second)
			continue
		}

		var batch []emailRow
		for rows.Next() {
			var e emailRow
			if err := rows.Scan(&e.id, &e.email); err == nil {
				batch = append(batch, e)
			}
		}
		rows.Close()

		if len(batch) == 0 {
			slog.Info("backfill: caught up, sleeping 60s",
				"validated", validated.Load(), "removed", removed.Load())
			select {
			case <-ctx.Done():
				return
			case <-time.After(60 * time.Second):
			}
			continue
		}

		// Build email list and id lookup.
		emails := make([]string, len(batch))
		idByEmail := make(map[string]int64, len(batch))
		for i, e := range batch {
			emails[i] = e.email
			idByEmail[e.email] = e.id
		}

		// 2. Submit bulk job.
		jobID, submitErr := client.SubmitBulk(ctx, emails)
		if submitErr != nil {
			slog.Warn("backfill: bulk submit failed, marking batch unknown", "error", submitErr)
			for _, e := range batch {
				db.ExecContext(ctx, `UPDATE emails SET validation_status = 'unknown', validated_at = NOW() WHERE id = $1`, e.id)
				validated.Add(1)
			}
			time.Sleep(10 * time.Second)
			continue
		}

		slog.Info("backfill: bulk job submitted", "job_id", jobID, "count", len(batch))

		// 3. Poll until completed (max 5 min).
		deadline := time.Now().Add(5 * time.Minute)
		completed := false
		for time.Now().Before(deadline) {
			if ctx.Err() != nil {
				return
			}
			progress, pollErr := client.PollBulk(ctx, jobID)
			if pollErr != nil {
				slog.Warn("backfill: poll failed", "job_id", jobID, "error", pollErr)
				time.Sleep(5 * time.Second)
				continue
			}
			if progress.JobStatus == "Completed" {
				completed = true
				break
			}
			time.Sleep(3 * time.Second)
		}

		if !completed {
			slog.Warn("backfill: bulk job timed out, marking batch unknown", "job_id", jobID)
			for _, e := range batch {
				db.ExecContext(ctx, `UPDATE emails SET validation_status = 'unknown', validated_at = NOW() WHERE id = $1`, e.id)
				validated.Add(1)
			}
			continue
		}

		// 4. Fetch results.
		results, fetchErr := client.FetchBulkResults(ctx, jobID)
		if fetchErr != nil {
			slog.Warn("backfill: fetch results failed", "job_id", jobID, "error", fetchErr)
			for _, e := range batch {
				db.ExecContext(ctx, `UPDATE emails SET validation_status = 'unknown', validated_at = NOW() WHERE id = $1`, e.id)
				validated.Add(1)
			}
			continue
		}

		// 5. Write results to DB.
		seen := make(map[string]bool, len(results))
		for _, r := range results {
			seen[r.Email] = true
			eid, ok := idByEmail[r.Email]
			if !ok {
				continue
			}
			score := r.Score()
			if r.IsGoodEmail() {
				db.ExecContext(ctx, `
					UPDATE emails SET
						validation_status = 'valid',
						score = $1, is_acceptable = TRUE,
						mx_valid = $2, deliverable = $3, disposable = $4,
						role_account = $5, free_email = $6, catch_all = $7,
						validated_at = NOW()
					WHERE id = $8
				`, score, r.MX.AcceptsMail, r.SMTP.IsDeliverable, r.Misc.IsDisposable,
					r.Misc.IsRoleAccount, r.Misc.IsFreeProvider, r.SMTP.IsCatchAll, eid)
			} else {
				db.ExecContext(ctx, `
					UPDATE emails SET
						validation_status = 'invalid',
						score = $1, is_acceptable = FALSE,
						reason = $2,
						validated_at = NOW()
					WHERE id = $3
				`, score, r.IsReachable, eid)
				removed.Add(1)
			}
			validated.Add(1)
		}

		// 6. Mark emails not returned by API as unknown.
		missing := 0
		for _, e := range batch {
			if !seen[e.email] {
				db.ExecContext(ctx, `UPDATE emails SET validation_status = 'unknown', validated_at = NOW() WHERE id = $1`, e.id)
				validated.Add(1)
				missing++
			}
		}

		slog.Info("backfill: bulk job done", "job_id", jobID, "results", len(results), "missing", missing)
	}

	slog.Info("backfill: finished",
		"validated", validated.Load(),
		"removed", removed.Load())
}

// Score computes a deliverability score in [0.0, 1.0] from validation flags.
// Hard disqualifiers (invalid/honeypot/disabled/bad-syntax) force 0.
func (r *CheckResult) Score() float32 {
	if r.IsReachable == "invalid" || !r.Syntax.IsValidSyntax ||
		r.Misc.IsHoneypot || r.SMTP.IsDisabled {
		return 0.0
	}
	var s float32 = 0.05 // valid syntax baseline
	if r.MX.AcceptsMail {
		s += 0.25
	}
	if r.SMTP.IsDeliverable {
		s += 0.35
	}
	if !r.Misc.IsDisposable {
		s += 0.15
	}
	if !r.Misc.IsRoleAccount {
		s += 0.10
	}
	if !r.SMTP.IsCatchAll {
		s += 0.10
	}
	return s
}

// IsGoodEmail returns true if the email is safe or risky-but-deliverable.
// Rejects: invalid, undeliverable, disposable, honeypot, disabled.
func (r *CheckResult) IsGoodEmail() bool {
	if r.IsReachable == "invalid" {
		return false
	}
	if !r.Syntax.IsValidSyntax {
		return false
	}
	if !r.MX.AcceptsMail {
		return false
	}
	if r.Misc.IsDisposable || r.Misc.IsHoneypot {
		return false
	}
	if r.SMTP.IsDisabled {
		return false
	}
	// "safe" = fully deliverable
	// "risky" = deliverable but catch-all or role-based (still useful for leads)
	// "unknown" = couldn't determine (keep, let user decide)
	return true
}
