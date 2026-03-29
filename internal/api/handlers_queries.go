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

func (s *Server) handleListQueries(w http.ResponseWriter, r *http.Request) {
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
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer rows.Close()

	var queries []map[string]any
	for rows.Next() {
		var id int64
		var text, status, errMsg string
		var resultCount int
		var createdAt time.Time
		rows.Scan(&id, &text, &status, &resultCount, &errMsg, &createdAt)
		queries = append(queries, map[string]any{
			"id": id, "text": text, "status": status,
			"result_count": resultCount, "error": errMsg, "created_at": createdAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data":     queries,
		"total":    total,
		"page":     page,
		"per_page": perPage,
	})
}

func (s *Server) handleCreateQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queries []string `json:"queries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
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
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "queries array is empty"})
		return
	}
	if len(cleaned) > 500 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 500 queries per request"})
		return
	}
	inserted, err := s.queryRepo.InsertBatch(cleaned)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"inserted":   inserted,
		"duplicates": len(cleaned) - inserted,
		"total":      len(cleaned),
	})
}

func (s *Server) handleDeleteQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []int64 `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.IDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ids array is required"})
		return
	}
	if len(req.IDs) > 1000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 1000 ids per request"})
		return
	}
	deleted, err := s.queryRepo.DeleteByIDs(req.IDs)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

func (s *Server) handleRetryQueries(w http.ResponseWriter, r *http.Request) {
	retried, err := s.queryRepo.RetryErrors()
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": retried})
}

func (s *Server) handleQueryStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.queryRepo.CountByStatus()
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	counts["total"] = total
	writeJSON(w, http.StatusOK, counts)
}

func (s *Server) handleGenerateQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Country string `json:"country"` // country name or "all"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.Country == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "country is required (name or \"all\")"})
		return
	}

	var keywords []string
	if strings.ToLower(req.Country) == "all" {
		keywords = query.GenerateAllKeywords()
	} else {
		keywords = query.GenerateKeywordsForCountry(req.Country)
		if keywords == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "country not found"})
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
			slog.Error("handler error", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}
		totalInserted += inserted
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"country":    req.Country,
		"generated":  len(keywords),
		"inserted":   totalInserted,
		"duplicates": len(keywords) - totalInserted,
	})
}
