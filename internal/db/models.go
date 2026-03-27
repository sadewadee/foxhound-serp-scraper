package db

import "time"

type Query struct {
	ID          int64     `json:"id"`
	Text        string    `json:"text"`
	TextHash    string    `json:"text_hash"`
	Status      string    `json:"status"`
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
	Status        string     `json:"status"`
	AttemptCount  int        `json:"attempt_count"`
	MaxAttempts   int        `json:"max_attempts"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	LockedBy      string     `json:"locked_by,omitempty"`
	LockedAt      *time.Time `json:"locked_at,omitempty"`
	ResultCount   int        `json:"result_count"`
	ErrorMsg      string     `json:"error_msg,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type Website struct {
	ID            int64     `json:"id"`
	Domain        string    `json:"domain"`
	URL           string    `json:"url"`
	URLHash       string    `json:"url_hash"`
	SourceQueryID int64     `json:"source_query_id,omitempty"`
	SourceSerpID  string    `json:"source_serp_id,omitempty"`
	PageType      string    `json:"page_type"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type EnrichJob struct {
	ID              string     `json:"id"`
	ParentJobID     int64      `json:"parent_job_id,omitempty"`
	SourceWebsiteID int64      `json:"source_website_id,omitempty"`
	Domain          string     `json:"domain"`
	URL             string     `json:"url"`
	URLHash         string     `json:"url_hash"`
	Status          string     `json:"status"`
	AttemptCount    int        `json:"attempt_count"`
	MaxAttempts     int        `json:"max_attempts"`
	NextAttemptAt   *time.Time `json:"next_attempt_at,omitempty"`
	LockedBy        string     `json:"locked_by,omitempty"`
	LockedAt        *time.Time `json:"locked_at,omitempty"`
	ErrorMsg        string     `json:"error_msg,omitempty"`
	Emails          []string   `json:"emails"`
	Phones          []string   `json:"phones"`
	SocialLinks     any        `json:"social_links"`
	Address         string     `json:"address,omitempty"`
	RawContext      string     `json:"raw_context,omitempty"`
	MXValid         *bool      `json:"mx_valid,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
}

type PipelineState struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}
