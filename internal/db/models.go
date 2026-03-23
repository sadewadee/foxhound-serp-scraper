package db

import "time"

type Query struct {
	ID          int64     `json:"id"`
	Text        string    `json:"text"`
	TextHash    string    `json:"text_hash"`
	TemplateID  string    `json:"template_id,omitempty"`
	Status      string    `json:"status"`
	ResultCount int       `json:"result_count"`
	ErrorMsg    string    `json:"error_msg,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type SERPSeed struct {
	ID          int64     `json:"id"`
	QueryID     int64     `json:"query_id"`
	GoogleURL   string    `json:"google_url"`
	PageNum     int       `json:"page_num"`
	Status      string    `json:"status"`
	ResultCount int       `json:"result_count"`
	CreatedAt   time.Time `json:"created_at"`
}

type Website struct {
	ID            int64     `json:"id"`
	Domain        string    `json:"domain"`
	URL           string    `json:"url"`
	URLHash       string    `json:"url_hash"`
	SourceQueryID int64     `json:"source_query_id,omitempty"`
	SourceSERPID  int64     `json:"source_serp_id,omitempty"`
	PageType      string    `json:"page_type"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
}

type Contact struct {
	ID            int64     `json:"id"`
	Email         string    `json:"email,omitempty"`
	EmailHash     string    `json:"email_hash,omitempty"`
	Phone         string    `json:"phone,omitempty"`
	Domain        string    `json:"domain"`
	WebsiteID     int64     `json:"website_id,omitempty"`
	SourceURL     string    `json:"source_url"`
	SourceQueryID int64     `json:"source_query_id,omitempty"`
	Instagram     string    `json:"instagram,omitempty"`
	Facebook      string    `json:"facebook,omitempty"`
	Twitter       string    `json:"twitter,omitempty"`
	LinkedIn      string    `json:"linkedin,omitempty"`
	WhatsApp      string    `json:"whatsapp,omitempty"`
	Address       string    `json:"address,omitempty"`
	RawContext    string    `json:"raw_context,omitempty"`
	MXValid       *bool     `json:"mx_valid,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type PipelineState struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}
