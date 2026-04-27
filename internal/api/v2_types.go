package api

import (
	"encoding/json"
	"time"
)

// V2BusinessListing is the API response shape for a business listing with all columns.
type V2BusinessListing struct {
	ID              int64           `json:"id"`
	Domain          string          `json:"domain"`
	URL             string          `json:"url"`
	BusinessName    string          `json:"business_name"`
	Category        string          `json:"category"`
	Description     string          `json:"description"`
	Address         string          `json:"address"`
	Location        string          `json:"location"`
	City            string          `json:"city"`
	Country         string          `json:"country"`
	ContactName     string          `json:"contact_name"`
	Phone           string          `json:"phone"`
	Phones          []string        `json:"phones"`
	Website         string          `json:"website"`
	PageTitle       string          `json:"page_title"`
	SocialLinks     json.RawMessage `json:"social_links"`
	TikTok          string          `json:"tiktok"`
	YouTube         string          `json:"youtube"`
	Telegram        string          `json:"telegram"`
	OpeningHours    string          `json:"opening_hours"`
	Rating          string          `json:"rating"`
	SourceQueryID   *int64          `json:"source_query_id"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	Emails          []string        `json:"emails"`
	EmailsWithInfo  []V2EmailInfo   `json:"emails_with_info"`
	TotalEmailCount int             `json:"total_email_count"`
	ValidEmailCount int             `json:"valid_email_count"`
}

// V2EmailInfo is the API response shape for a single email with all validation columns.
type V2EmailInfo struct {
	Email        string     `json:"email"`
	Domain       string     `json:"domain"`
	Status       string     `json:"status"`
	Score        *float32   `json:"score"`
	IsAcceptable *bool      `json:"is_acceptable"`
	MXValid      *bool      `json:"mx_valid"`
	Deliverable  *bool      `json:"deliverable"`
	Disposable   *bool      `json:"disposable"`
	RoleAccount  *bool      `json:"role_account"`
	FreeEmail    *bool      `json:"free_email"`
	CatchAll     *bool      `json:"catch_all"`
	Reason       string     `json:"reason"`
	Source       string     `json:"source"`
	ValidatedAt  *time.Time `json:"validated_at"`
}

// V2ResultsStats is the API response shape for results stats.
type V2ResultsStats struct {
	TotalListings int            `json:"total_listings"`
	WithEmail     int            `json:"with_email"`
	WithPhone     int            `json:"with_phone"`
	UniqueDomains int            `json:"unique_domains"`
	TotalEmails   int            `json:"total_emails"`
	ValidEmails   int            `json:"valid_emails"`
	PendingEmails int            `json:"pending_emails"`
	InvalidEmails int            `json:"invalid_emails"`
	EmailsPerHour int            `json:"emails_per_hour"`
	EmailsPer24h  int            `json:"emails_per_24h"`
	TopProviders  map[string]int `json:"top_providers"`
}

// V2CategoryStats is the API response shape for a category with counts.
type V2CategoryStats struct {
	Category      string `json:"category"`
	BusinessCount int    `json:"business_count"`
	EmailCount    int    `json:"email_count"`
}

// V2DomainStats is the API response shape for a domain with counts.
type V2DomainStats struct {
	Domain     string `json:"domain"`
	EmailCount int    `json:"email_count"`
}
