package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	pq "github.com/lib/pq"
)

func (s *Server) handleListContacts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := queryInt(q, "page", 1)
	perPage := queryInt(q, "per_page", 50)
	if perPage > 200 {
		perPage = 200
	}
	offset := (page - 1) * perPage

	// Build WHERE clause from filters.
	where := "WHERE status = 'completed'"
	args := []any{}
	argIdx := 1

	if domain := q.Get("domain"); domain != "" {
		where += fmt.Sprintf(" AND domain = $%d", argIdx)
		args = append(args, domain)
		argIdx++
	}
	if hasEmail := q.Get("has_email"); hasEmail == "true" {
		where += " AND array_length(emails, 1) > 0"
	}
	if email := q.Get("email"); email != "" {
		where += fmt.Sprintf(" AND $%d = ANY(emails)", argIdx)
		args = append(args, email)
		argIdx++
	}
	if provider := q.Get("email_provider"); provider != "" {
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM unnest(emails) AS e WHERE e LIKE $%d)", argIdx)
		args = append(args, "%@"+provider)
		argIdx++
	}

	// Count total.
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM enrich_jobs %s", where)
	s.db.QueryRow(countQuery, args...).Scan(&total)

	// Fetch rows.
	dataQuery := fmt.Sprintf(`
		SELECT id, COALESCE(contact_name,''), COALESCE(business_name,''),
		       COALESCE(business_category,''), COALESCE(description,''),
		       COALESCE(website,''), emails, phones, domain, url, social_links,
		       COALESCE(address,''), COALESCE(location,''),
		       COALESCE(opening_hours,''), COALESCE(rating,''),
		       COALESCE(page_title,''), status, created_at
		FROM enrich_jobs %s
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

	var contacts []map[string]any
	for rows.Next() {
		var id string
		var contactName, businessName, businessCategory, description, website string
		var emails, phones []string
		var domain, url, address, location, openingHours, rating, pageTitle, status string
		var socialLinksJSON []byte
		var createdAt time.Time

		rows.Scan(&id, &contactName, &businessName, &businessCategory, &description, &website,
			pq.Array(&emails), pq.Array(&phones), &domain, &url, &socialLinksJSON,
			&address, &location, &openingHours, &rating, &pageTitle, &status, &createdAt)

		contacts = append(contacts, map[string]any{
			"id": id, "contact_name": contactName,
			"business_name": businessName, "business_category": businessCategory,
			"description": description, "website": website,
			"emails": emails, "phones": phones, "domain": domain,
			"url": url, "social_links": json.RawMessage(socialLinksJSON),
			"address": address, "location": location, "opening_hours": openingHours,
			"rating": rating, "page_title": pageTitle,
			"status": status, "created_at": createdAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data":     contacts,
		"total":    total,
		"page":     page,
		"per_page": perPage,
	})
}

func (s *Server) handleExportContacts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	format := q.Get("format")
	if format == "" {
		format = "json"
	}

	where := "WHERE status = 'completed'"
	if q.Get("has_email") == "true" {
		where += " AND array_length(emails, 1) > 0"
	}
	if provider := q.Get("email_provider"); provider != "" {
		// Only allow alphanumeric + dots to prevent SQL injection.
		safe := true
		for _, c := range provider {
			if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.') {
				safe = false
				break
			}
		}
		if safe {
			where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM unnest(emails) AS e WHERE e LIKE '%%@%s')", provider)
		}
	}

	exportQuery := fmt.Sprintf(`
		SELECT COALESCE(contact_name,''), COALESCE(business_name,''),
		       COALESCE(business_category,''), COALESCE(website,''),
		       emails, phones, domain, social_links,
		       COALESCE(address,''), COALESCE(location,'')
		FROM enrich_jobs %s ORDER BY id ASC
	`, where)

	rows, err := s.db.Query(exportQuery)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer rows.Close()

	if format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=contacts.csv")
		fmt.Fprintln(w, "contact_name,business_name,business_category,website,emails,phones,domain,social_links,address,location")
		for rows.Next() {
			var contactName, businessName, businessCategory, website string
			var emails, phones []string
			var domain, address, location string
			var socialLinksJSON []byte
			rows.Scan(&contactName, &businessName, &businessCategory, &website,
				pq.Array(&emails), pq.Array(&phones), &domain, &socialLinksJSON,
				&address, &location)
			fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				csvEscape(contactName), csvEscape(businessName), csvEscape(businessCategory),
				csvEscape(website), csvEscape(strings.Join(emails, ";")),
				csvEscape(strings.Join(phones, ";")), csvEscape(domain),
				csvEscape(string(socialLinksJSON)), csvEscape(address), csvEscape(location))
		}
		return
	}

	// JSON export — stream to avoid accumulating the full result set in memory.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("["))
	enc := json.NewEncoder(w)
	first := true
	for rows.Next() {
		var contactName, businessName, businessCategory, website string
		var emails, phones []string
		var domain, address, location string
		var socialLinksJSON []byte
		rows.Scan(&contactName, &businessName, &businessCategory, &website,
			pq.Array(&emails), pq.Array(&phones), &domain, &socialLinksJSON,
			&address, &location)
		if !first {
			w.Write([]byte(","))
		}
		first = false
		enc.Encode(map[string]any{
			"contact_name": contactName, "business_name": businessName,
			"business_category": businessCategory, "website": website,
			"emails": emails, "phones": phones, "domain": domain,
			"social_links": json.RawMessage(socialLinksJSON),
			"address": address, "location": location,
		})
	}
	w.Write([]byte("]"))
}

func (s *Server) handleContactStats(w http.ResponseWriter, r *http.Request) {
	var total, withEmail, uniqueDomains, uniqueEmails, lastHour, last24h int

	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed'").Scan(&total)
	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND array_length(emails, 1) > 0").Scan(&withEmail)
	s.db.QueryRow("SELECT COUNT(DISTINCT domain) FROM enrich_jobs WHERE status = 'completed'").Scan(&uniqueDomains)
	s.db.QueryRow("SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'").Scan(&uniqueEmails)
	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'").Scan(&lastHour)
	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours'").Scan(&last24h)

	// Top email providers.
	providers := map[string]int{}
	providerRows, _ := s.db.Query(`
		SELECT split_part(e, '@', 2) AS provider, COUNT(*) AS cnt
		FROM enrich_jobs, unnest(emails) AS e
		WHERE status = 'completed'
		GROUP BY provider
		ORDER BY cnt DESC
		LIMIT 10
	`)
	if providerRows != nil {
		defer providerRows.Close()
		for providerRows.Next() {
			var provider string
			var cnt int
			providerRows.Scan(&provider, &cnt)
			providers[provider] = cnt
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"total":          total,
		"with_email":     withEmail,
		"unique_domains": uniqueDomains,
		"unique_emails":  uniqueEmails,
		"last_hour":      lastHour,
		"last_24h":       last24h,
		"providers":      providers,
	})
}

func (s *Server) handleDeleteContacts(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs    []string `json:"ids"`
		Domain string   `json:"domain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if len(req.IDs) == 0 && req.Domain == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide ids or domain"})
		return
	}
	if len(req.IDs) > 0 && req.Domain != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide ids or domain, not both"})
		return
	}
	if len(req.IDs) > 1000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 1000 ids per request"})
		return
	}
	var res sql.Result
	var err error
	if req.Domain != "" {
		res, err = s.db.Exec("DELETE FROM enrich_jobs WHERE domain = $1", req.Domain)
	} else {
		res, err = s.db.Exec("DELETE FROM enrich_jobs WHERE id = ANY($1)", pq.Array(req.IDs))
	}
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	n, _ := res.RowsAffected()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": n})
}

func (s *Server) handleListDomains(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`
		SELECT domain, COUNT(*) as contact_count,
		       COUNT(CASE WHEN array_length(emails, 1) > 0 THEN 1 END) as email_count
		FROM enrich_jobs
		WHERE status = 'completed'
		GROUP BY domain
		ORDER BY contact_count DESC
		LIMIT 500
	`)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer rows.Close()

	var domains []map[string]any
	for rows.Next() {
		var domain string
		var contactCount, emailCount int
		rows.Scan(&domain, &contactCount, &emailCount)
		domains = append(domains, map[string]any{
			"domain": domain, "contacts": contactCount, "emails": emailCount,
		})
	}
	writeJSON(w, http.StatusOK, domains)
}

func (s *Server) handleListCategories(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, []any{})
}
