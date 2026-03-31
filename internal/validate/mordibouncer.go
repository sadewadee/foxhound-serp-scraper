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
	"time"

	"github.com/lib/pq"
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

// BackfillValidation scans existing completed enrich_jobs with emails but no
// validation (mx_valid IS NULL) and validates them via Mordibouncer.
// Runs as a background goroutine in the manager container.
func BackfillValidation(ctx context.Context, db *sql.DB, client *MordibouncerClient) {
	slog.Info("backfill: starting email validation for existing records")

	// Small delay to let other services start first.
	select {
	case <-ctx.Done():
		return
	case <-time.After(30 * time.Second):
	}

	validated, removed := 0, 0
	for {
		if ctx.Err() != nil {
			break
		}

		// Fetch batch of unvalidated jobs (oldest first).
		rows, err := db.QueryContext(ctx, `
			SELECT id, emails
			FROM enrich_jobs
			WHERE status = 'completed'
			  AND mx_valid IS NULL
			  AND array_length(emails, 1) > 0
			ORDER BY completed_at ASC
			LIMIT 100
		`)
		if err != nil {
			slog.Warn("backfill: query failed", "error", err)
			time.Sleep(30 * time.Second)
			continue
		}

		type job struct {
			id     string
			emails []string
		}
		var batch []job
		for rows.Next() {
			var j job
			var emails pq.StringArray
			if err := rows.Scan(&j.id, &emails); err == nil {
				j.emails = emails
				batch = append(batch, j)
			}
		}
		rows.Close()

		if len(batch) == 0 {
			slog.Info("backfill: all existing emails validated", "total_validated", validated, "total_removed", removed)
			return // Done — no more unvalidated records.
		}

		for _, j := range batch {
			if ctx.Err() != nil {
				return
			}

			validEmails := client.FilterValid(ctx, j.emails)
			removed += len(j.emails) - len(validEmails)
			mxValid := len(validEmails) > 0 && len(validEmails) == len(j.emails)
			db.ExecContext(ctx, `
				UPDATE enrich_jobs SET emails = $1, mx_valid = $2, updated_at = NOW()
				WHERE id = $3
			`, pq.Array(validEmails), mxValid, j.id)

			validated++
			if validated%500 == 0 {
				slog.Info("backfill: progress", "validated", validated, "removed", removed)
			}

			// Rate limit — don't hammer Mordibouncer API.
			time.Sleep(100 * time.Millisecond)
		}
	}

	slog.Info("backfill: finished", "validated", validated, "removed", removed)
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
