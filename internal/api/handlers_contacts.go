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
	where := "WHERE 1=1"
	args := []any{}
	argIdx := 1

	if domain := q.Get("domain"); domain != "" {
		where += fmt.Sprintf(" AND bl.domain = $%d", argIdx)
		args = append(args, domain)
		argIdx++
	}
	if hasEmail := q.Get("has_email"); hasEmail == "true" {
		where += " AND EXISTS (SELECT 1 FROM business_emails be WHERE be.business_id = bl.id)"
	}
	if email := q.Get("email"); email != "" {
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.email = $%d)", argIdx)
		args = append(args, email)
		argIdx++
	}
	if provider := q.Get("email_provider"); provider != "" {
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.email LIKE $%d)", argIdx)
		args = append(args, "%@"+provider)
		argIdx++
	}

	// Count total.
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM business_listings bl %s", where)
	s.db.QueryRow(countQuery, args...).Scan(&total)

	// Fetch rows.
	dataQuery := fmt.Sprintf(`
		SELECT bl.id, COALESCE(bl.business_name,''), COALESCE(bl.category,''),
		       COALESCE(bl.description,''), COALESCE(bl.website,''),
		       bl.domain, bl.url, bl.social_links,
		       COALESCE(bl.address,''), COALESCE(bl.location,''),
		       COALESCE(bl.opening_hours,''), COALESCE(bl.rating,''),
		       COALESCE(bl.page_title,''), bl.created_at,
		       COALESCE(
		         (SELECT array_agg(e.email) FROM business_emails be JOIN emails e ON e.id = be.email_id
		          WHERE be.business_id = bl.id AND e.validation_status IN ('valid', 'pending')),
		         '{}'
		       ) AS emails,
		       COALESCE(bl.phone, '') AS phone
		FROM business_listings bl %s
		ORDER BY bl.id DESC
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
		var id int64
		var businessName, category, description, website string
		var domain, url, address, location, openingHours, rating, pageTitle, phone string
		var socialLinksJSON []byte
		var createdAt time.Time
		var emails []string

		rows.Scan(&id, &businessName, &category, &description, &website,
			&domain, &url, &socialLinksJSON,
			&address, &location, &openingHours, &rating, &pageTitle, &createdAt,
			pq.Array(&emails), &phone)

		var phones []string
		if phone != "" {
			phones = []string{phone}
		}

		contacts = append(contacts, map[string]any{
			"id": id,
			"business_name": businessName, "business_category": category,
			"description": description, "website": website,
			"emails": emails, "phones": phones, "domain": domain,
			"url": url, "social_links": json.RawMessage(socialLinksJSON),
			"address": address, "location": location, "opening_hours": openingHours,
			"rating": rating, "page_title": pageTitle,
			"created_at": createdAt,
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

	where := "WHERE EXISTS (SELECT 1 FROM business_emails be WHERE be.business_id = bl.id)"
	if provider := q.Get("email_provider"); provider != "" {
		safe := true
		for _, c := range provider {
			if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.') {
				safe = false
				break
			}
		}
		if safe {
			where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.email LIKE '%%@%s')", provider)
		}
	}

	exportQuery := fmt.Sprintf(`
		SELECT COALESCE(bl.business_name,''), COALESCE(bl.category,''),
		       COALESCE(bl.website,''), bl.domain, bl.social_links,
		       COALESCE(bl.address,''), COALESCE(bl.location,''),
		       COALESCE(bl.phone,''),
		       COALESCE(
		         (SELECT array_agg(e.email) FROM business_emails be JOIN emails e ON e.id = be.email_id
		          WHERE be.business_id = bl.id AND e.validation_status IN ('valid', 'pending')),
		         '{}'
		       ) AS emails
		FROM business_listings bl %s ORDER BY bl.id ASC
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
		fmt.Fprintln(w, "business_name,category,website,emails,domain,social_links,address,location,phone")
		for rows.Next() {
			var businessName, category, website, domain, address, location, phone string
			var socialLinksJSON []byte
			var emails []string
			rows.Scan(&businessName, &category, &website, &domain, &socialLinksJSON,
				&address, &location, &phone, pq.Array(&emails))
			fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				csvEscape(businessName), csvEscape(category),
				csvEscape(website), csvEscape(strings.Join(emails, ";")),
				csvEscape(domain), csvEscape(string(socialLinksJSON)),
				csvEscape(address), csvEscape(location), csvEscape(phone))
		}
		return
	}

	// JSON export.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("["))
	enc := json.NewEncoder(w)
	first := true
	for rows.Next() {
		var businessName, category, website, domain, address, location, phone string
		var socialLinksJSON []byte
		var emails []string
		rows.Scan(&businessName, &category, &website, &domain, &socialLinksJSON,
			&address, &location, &phone, pq.Array(&emails))
		if !first {
			w.Write([]byte(","))
		}
		first = false
		enc.Encode(map[string]any{
			"business_name": businessName, "business_category": category,
			"website": website, "emails": emails, "domain": domain,
			"social_links": json.RawMessage(socialLinksJSON),
			"address": address, "location": location, "phone": phone,
		})
	}
	w.Write([]byte("]"))
}

func (s *Server) handleContactStats(w http.ResponseWriter, r *http.Request) {
	var totalBiz, withEmail, uniqueDomains, totalEmails, lastHour, last24h int
	var validEmails, pendingEmails, invalidEmails int

	s.db.QueryRow("SELECT COUNT(*) FROM business_listings").Scan(&totalBiz)
	s.db.QueryRow("SELECT COUNT(DISTINCT bl.id) FROM business_listings bl JOIN business_emails be ON be.business_id = bl.id").Scan(&withEmail)
	s.db.QueryRow("SELECT COUNT(DISTINCT domain) FROM business_listings").Scan(&uniqueDomains)
	s.db.QueryRow("SELECT COUNT(*) FROM emails").Scan(&totalEmails)
	s.db.QueryRow("SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '1 hour'").Scan(&lastHour)
	s.db.QueryRow("SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '24 hours'").Scan(&last24h)
	s.db.QueryRow("SELECT COUNT(*) FROM emails WHERE validation_status = 'valid'").Scan(&validEmails)
	s.db.QueryRow("SELECT COUNT(*) FROM emails WHERE validation_status = 'pending'").Scan(&pendingEmails)
	s.db.QueryRow("SELECT COUNT(*) FROM emails WHERE validation_status = 'invalid'").Scan(&invalidEmails)

	// Top email providers.
	providers := map[string]int{}
	providerRows, _ := s.db.Query(`
		SELECT domain, COUNT(*) AS cnt
		FROM emails
		GROUP BY domain
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
		"total":          totalBiz,
		"with_email":     withEmail,
		"unique_domains": uniqueDomains,
		"unique_emails":  totalEmails,
		"last_hour":      lastHour,
		"last_24h":       last24h,
		"providers":      providers,
		"validation": map[string]int{
			"valid":   validEmails,
			"pending": pendingEmails,
			"invalid": invalidEmails,
		},
	})
}

func (s *Server) handleDeleteContacts(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs    []int64 `json:"ids"`
		Domain string  `json:"domain"`
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
		res, err = s.db.Exec("DELETE FROM business_listings WHERE domain = $1", req.Domain)
	} else {
		res, err = s.db.Exec("DELETE FROM business_listings WHERE id = ANY($1)", pq.Array(req.IDs))
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
		SELECT bl.domain,
		       COUNT(DISTINCT be.email_id) as email_count
		FROM business_listings bl
		LEFT JOIN business_emails be ON be.business_id = bl.id
		GROUP BY bl.domain
		ORDER BY email_count DESC
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
		var emailCount int
		rows.Scan(&domain, &emailCount)
		domains = append(domains, map[string]any{
			"domain": domain, "emails": emailCount,
		})
	}
	writeJSON(w, http.StatusOK, domains)
}

func (s *Server) handleListCategories(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`
		SELECT category, COUNT(*) AS cnt
		FROM business_listings
		WHERE category IS NOT NULL AND category != ''
		GROUP BY category ORDER BY cnt DESC LIMIT 50
	`)
	if err != nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	defer rows.Close()

	var cats []map[string]any
	for rows.Next() {
		var cat string
		var cnt int
		rows.Scan(&cat, &cnt)
		cats = append(cats, map[string]any{"category": cat, "count": cnt})
	}
	if cats == nil {
		cats = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, cats)
}
