package db

import "time"

// --- Kept from original ---

type Query struct {
	ID          int64     `json:"id"`
	Text        string    `json:"text"`
	TextHash    string    `json:"text_hash"`
	Status      string    `json:"status"`
	Country     string    `json:"country,omitempty"`
	ResultCount int       `json:"result_count"`
	ErrorMsg    string    `json:"error_msg,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type SERPJob struct {
	ID            string     `json:"id"`
	ParentJobID   int64      `json:"parent_job_id"`
	Priority      int        `json:"priority"`
	SearchURL     string     `json:"search_url"`
	PageNum       int        `json:"page_num"`
	Engine        string     `json:"engine"`
	Status        string     `json:"status"`
	AttemptCount  int        `json:"attempt_count"`
	MaxAttempts   int        `json:"max_attempts"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	LockedBy      string     `json:"locked_by,omitempty"`
	LockedAt      *time.Time `json:"locked_at,omitempty"`
	PickedAt      *time.Time `json:"picked_at,omitempty"`
	ResultCount   int        `json:"result_count"`
	ErrorMsg      string     `json:"error_msg,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// --- New tables ---

type SERPResult struct {
	ID            int64     `json:"id"`
	URL           string    `json:"url"`
	URLHash       string    `json:"url_hash"`
	Domain        string    `json:"domain"`
	SourceQueryID int64     `json:"source_query_id"`
	SourceSerpID  string    `json:"source_serp_id"`
	CreatedAt     time.Time `json:"created_at"`
}

type EnrichmentJob struct {
	ID            string     `json:"id"`
	URL           string     `json:"url"`
	URLHash       string     `json:"url_hash"`
	Domain        string     `json:"domain"`
	ParentQueryID int64      `json:"parent_query_id"`
	Source        string     `json:"source"`
	Status        string     `json:"status"`
	AttemptCount  int        `json:"attempt_count"`
	MaxAttempts   int        `json:"max_attempts"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	LockedBy      string     `json:"locked_by,omitempty"`
	LockedAt      *time.Time `json:"locked_at,omitempty"`
	PickedAt      *time.Time `json:"picked_at,omitempty"`
	ErrorMsg      string     `json:"error_msg,omitempty"`
	RawEmails     []string   `json:"raw_emails"`
	RawPhones     []string   `json:"raw_phones"`
	RawSocial     any        `json:"raw_social"`
	RawBizName    string     `json:"raw_business_name,omitempty"`
	RawCategory   string     `json:"raw_category,omitempty"`
	RawAddress    string     `json:"raw_address,omitempty"`
	RawPageTitle  string     `json:"raw_page_title,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

type BusinessListing struct {
	ID            int64     `json:"id"`
	Domain        string    `json:"domain"`
	URL           string    `json:"url"`
	BusinessName  string    `json:"business_name,omitempty"`
	Category      string    `json:"category,omitempty"`
	Description   string    `json:"description,omitempty"`
	Address       string    `json:"address,omitempty"`
	Location      string    `json:"location,omitempty"`
	Phone         string    `json:"phone,omitempty"`
	Website       string    `json:"website,omitempty"`
	PageTitle     string    `json:"page_title,omitempty"`
	SocialLinks   any       `json:"social_links"`
	OpeningHours  string    `json:"opening_hours,omitempty"`
	Rating        string    `json:"rating,omitempty"`
	SourceQueryID int64     `json:"source_query_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Email struct {
	ID               int64      `json:"id"`
	EmailAddr        string     `json:"email"`
	Domain           string     `json:"domain"`
	LocalPart        string     `json:"local_part"`
	ValidationStatus string     `json:"validation_status"`
	MXValid          *bool      `json:"mx_valid,omitempty"`
	Deliverable      *bool      `json:"deliverable,omitempty"`
	Disposable       *bool      `json:"disposable,omitempty"`
	RoleAccount      *bool      `json:"role_account,omitempty"`
	FreeEmail        *bool      `json:"free_email,omitempty"`
	CatchAll         *bool      `json:"catch_all,omitempty"`
	Reason           string     `json:"reason,omitempty"`
	Score            *float32   `json:"score,omitempty"`
	IsAcceptable     *bool      `json:"is_acceptable,omitempty"`
	ValidatedAt      *time.Time `json:"validated_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

type BusinessEmail struct {
	BusinessID int64     `json:"business_id"`
	EmailID    int64     `json:"email_id"`
	Source     string    `json:"source"`
	CreatedAt  time.Time `json:"created_at"`
}

type Worker struct {
	WorkerID       string    `json:"worker_id"`
	WorkerType     string    `json:"worker_type"`
	ContainerID    string    `json:"container_id,omitempty"`
	Status         string    `json:"status"`
	CurrentJobID   string    `json:"current_job_id,omitempty"`
	CurrentURL     string    `json:"current_url,omitempty"`
	PagesProcessed int64     `json:"pages_processed"`
	EmailsFound    int64     `json:"emails_found"`
	ErrorsCount    int64     `json:"errors_count"`
	LastHeartbeat  time.Time `json:"last_heartbeat"`
	StartedAt      time.Time `json:"started_at"`
}
