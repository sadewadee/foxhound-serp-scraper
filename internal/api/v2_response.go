package api

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"
)

// PaginatedResponse wraps list endpoints with data + meta.
type PaginatedResponse struct {
	Data any            `json:"data"`
	Meta PaginationMeta `json:"meta"`
}

// PaginationMeta holds pagination details.
type PaginationMeta struct {
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	Total      int `json:"total"`
	TotalPages int `json:"total_pages"`
}

// SingleResponse wraps single-object endpoints.
type SingleResponse struct {
	Data any `json:"data"`
}

// ErrorResponse wraps error responses.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail holds error code and message.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeV2Paginated writes a paginated JSON response.
func writeV2Paginated(w http.ResponseWriter, data any, total, page, perPage int) {
	totalPages := 0
	if perPage > 0 {
		totalPages = int(math.Ceil(float64(total) / float64(perPage)))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(PaginatedResponse{
		Data: data,
		Meta: PaginationMeta{
			Page:       page,
			PerPage:    perPage,
			Total:      total,
			TotalPages: totalPages,
		},
	})
}

// writeV2Single writes a single-object JSON response.
func writeV2Single(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(SingleResponse{Data: data})
}

// v2RequestContext returns a context with an 18-second timeout for API requests.
// Must stay STRICTLY GREATER than any per-handler `SET LOCAL statement_timeout`
// (currently max 15s in dashboard stats, 12s in filtered results) so PG-side
// timeout fires first and returns a clean cancel error instead of context-cancel.
func v2RequestContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 18*time.Second)
}

// v2DownloadContext returns a context with a 5-minute timeout for download/export requests.
func v2DownloadContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 5*time.Minute)
}

// writeV2Error writes an error JSON response.
func writeV2Error(w http.ResponseWriter, httpStatus int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: ErrorDetail{Code: code, Message: message},
	})
}
