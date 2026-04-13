package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/sadewadee/serp-scraper/internal/query"
)

// handleV2ListQueries returns paginated queries wrapped in V2 format.
func (s *Server) handleV2ListQueries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := queryInt(q, "page", 1)
	perPage := queryInt(q, "per_page", 50)
	if perPage > 200 {
		perPage = 200
	}
	offset := (page - 1) * perPage

	where := "WHERE 1=1"
	args := []any{}
	argIdx := 1

	if status := q.Get("status"); status != "" {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if search := q.Get("search"); search != "" {
		where += fmt.Sprintf(" AND text ILIKE $%d", argIdx)
		args = append(args, "%"+search+"%")
		argIdx++
	}

	var total int
	s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM queries %s", where), args...).Scan(&total)

	dataQuery := fmt.Sprintf(`
		SELECT id, text, status, result_count, COALESCE(error_msg,''), created_at
		FROM queries %s
		ORDER BY id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := s.db.Query(dataQuery, args...)
	if err != nil {
		slog.Error("v2: list queries error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch queries")
		return
	}
	defer rows.Close()

	type queryRow struct {
		ID          int64     `json:"id"`
		Text        string    `json:"text"`
		Status      string    `json:"status"`
		ResultCount int       `json:"result_count"`
		Error       string    `json:"error"`
		CreatedAt   time.Time `json:"created_at"`
	}

	queries := []queryRow{}
	for rows.Next() {
		var qr queryRow
		rows.Scan(&qr.ID, &qr.Text, &qr.Status, &qr.ResultCount, &qr.Error, &qr.CreatedAt)
		queries = append(queries, qr)
	}

	writeV2Paginated(w, queries, total, page, perPage)
}

// handleV2CreateQueries creates new queries.
func (s *Server) handleV2CreateQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queries []string `json:"queries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeV2Error(w, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}
	var cleaned []string
	for _, q := range req.Queries {
		q = strings.TrimSpace(q)
		if q != "" {
			cleaned = append(cleaned, q)
		}
	}
	if len(cleaned) == 0 {
		writeV2Error(w, http.StatusBadRequest, "empty_queries", "queries array is empty")
		return
	}
	if len(cleaned) > 500 {
		writeV2Error(w, http.StatusBadRequest, "too_many_queries", "max 500 queries per request")
		return
	}

	inserted, err := s.queryRepo.InsertBatch(cleaned)
	if err != nil {
		slog.Error("v2: create queries error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to create queries")
		return
	}

	writeV2Single(w, map[string]any{
		"inserted":   inserted,
		"duplicates": len(cleaned) - inserted,
		"total":      len(cleaned),
	})
}

// handleV2DeleteQueries deletes queries by IDs.
func (s *Server) handleV2DeleteQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []int64 `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.IDs) == 0 {
		writeV2Error(w, http.StatusBadRequest, "invalid_ids", "ids array is required")
		return
	}
	if len(req.IDs) > 1000 {
		writeV2Error(w, http.StatusBadRequest, "too_many_ids", "max 1000 ids per request")
		return
	}

	deleted, err := s.queryRepo.DeleteByIDs(req.IDs)
	if err != nil {
		slog.Error("v2: delete queries error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to delete queries")
		return
	}

	writeV2Single(w, map[string]any{"deleted": deleted})
}

// handleV2RetryQueries retries errored queries.
func (s *Server) handleV2RetryQueries(w http.ResponseWriter, r *http.Request) {
	retried, err := s.queryRepo.RetryErrors()
	if err != nil {
		slog.Error("v2: retry queries error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to retry queries")
		return
	}

	writeV2Single(w, map[string]any{"retried": retried})
}

// handleV2GenerateQueries generates queries for a country.
func (s *Server) handleV2GenerateQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Country string `json:"country"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeV2Error(w, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}
	if req.Country == "" {
		writeV2Error(w, http.StatusBadRequest, "missing_country", "country is required (name or \"all\")")
		return
	}

	var keywords []string
	if strings.ToLower(req.Country) == "all" {
		keywords = query.GenerateAllKeywords()
	} else {
		keywords = query.GenerateKeywordsForCountry(req.Country)
		if keywords == nil {
			writeV2Error(w, http.StatusBadRequest, "country_not_found", "country not found")
			return
		}
	}

	const batchSize = 500
	totalInserted := 0
	for i := 0; i < len(keywords); i += batchSize {
		end := i + batchSize
		if end > len(keywords) {
			end = len(keywords)
		}
		inserted, err := s.queryRepo.InsertBatch(keywords[i:end])
		if err != nil {
			slog.Error("v2: generate queries error", "error", err)
			writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to generate queries")
			return
		}
		totalInserted += inserted
	}

	writeV2Single(w, map[string]any{
		"country":    req.Country,
		"generated":  len(keywords),
		"inserted":   totalInserted,
		"duplicates": len(keywords) - totalInserted,
	})
}
