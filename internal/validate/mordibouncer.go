package validate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
