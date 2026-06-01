package api

import (
	"context"
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
		// Match on e.domain (exact) rather than LIKE '%@provider' — avoids
		// a full-index scan of the emails table on large datasets.
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.domain = $%d)", argIdx)
		args = append(args, provider)
		argIdx++
	}

	// Count total — bounded context + statement_timeout to prevent pool starvation
	// when business_listings is large (>1M rows).  5s matches the enrich reconciler
	// budget; a timeout silently leaves total=0 (pagination still works).
	countCtx, countCancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer countCancel()
	var total int
	countTx, err := s.db.BeginTx(countCtx, nil)
	if err == nil {
		countTx.ExecContext(countCtx, "SET LOCAL statement_timeout = '5000'")
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM business_listings bl %s", where)
		countTx.QueryRowContext(countCtx, countQuery, args...).Scan(&total)
		countTx.Rollback() // read-only, always rollback
	}

	// Query 1: Fetch page of listings WITHOUT a correlated email subquery.
	// Emails are fetched in a single batch query below (avoids N+1).
	dataQuery := fmt.Sprintf(`
		SELECT bl.id, COALESCE(bl.business_name,''), COALESCE(bl.category,''),
		       COALESCE(bl.description,''), COALESCE(bl.website,''),
		       bl.domain, bl.url, bl.social_links,
		       COALESCE(bl.address,''), COALESCE(bl.location,''),
		       COALESCE(bl.opening_hours,''), COALESCE(bl.rating,''),
		       COALESCE(bl.page_title,''), bl.created_at,
		       COALESCE(bl.phone, '') AS phone
		FROM business_listings bl %s
		ORDER BY bl.id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := s.db.QueryContext(r.Context(), dataQuery, args...)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer rows.Close()

	// Temporary struct to hold listings before email stitching.
	type listingRow struct {
		id                                                   int64
		businessName, category, description, website         string
		domain, url, address, location, openingHours, rating string
		pageTitle, phone                                     string
		socialLinksJSON                                      []byte
		createdAt                                            time.Time
	}
	var listings []listingRow
	var listingIDs []int64

	for rows.Next() {
		var lr listingRow
		rows.Scan(&lr.id, &lr.businessName, &lr.category, &lr.description, &lr.website,
			&lr.domain, &lr.url, &lr.socialLinksJSON,
			&lr.address, &lr.location, &lr.openingHours, &lr.rating, &lr.pageTitle,
			&lr.createdAt, &lr.phone)
		listings = append(listings, lr)
		listingIDs = append(listingIDs, lr.id)
	}
	rows.Close()

	// Query 2: Batch-fetch emails for all listing IDs in one round-trip.
	// This replaces the correlated array_agg subquery that fired once per row.
	emailMap := make(map[int64][]string, len(listingIDs))
	if len(listingIDs) > 0 {
		emailRows, err := s.db.QueryContext(r.Context(), `
			SELECT be.business_id, e.email
			FROM business_emails be
			JOIN emails e ON e.id = be.email_id
			WHERE be.business_id = ANY($1)
			  AND e.validation_status IN ('valid', 'pending', 'unknown')
			ORDER BY be.business_id, e.email
		`, pq.Array(listingIDs))
		if err != nil {
			slog.Error("v1: batch email fetch error", "error", err)
			// Continue without emails — don't fail the whole response.
		} else {
			defer emailRows.Close()
			for emailRows.Next() {
				var bizID int64
				var email string
				if err := emailRows.Scan(&bizID, &email); err == nil {
					emailMap[bizID] = append(emailMap[bizID], email)
				}
			}
		}
	}

	// Stitch emails back into contact records.
	var contacts []map[string]any
	for _, lr := range listings {
		emails := emailMap[lr.id]
		if emails == nil {
			emails = []string{}
		}
		var phones []string
		if lr.phone != "" {
			phones = []string{lr.phone}
		}
		contacts = append(contacts, map[string]any{
			"id":            lr.id,
			"business_name": lr.businessName, "business_category": lr.category,
			"description": lr.description, "website": lr.website,
			"emails": emails, "phones": phones, "domain": lr.domain,
			"url": lr.url, "social_links": json.RawMessage(lr.socialLinksJSON),
			"address": lr.address, "location": lr.location, "opening_hours": lr.openingHours,
			"rating": lr.rating, "page_title": lr.pageTitle,
			"created_at": lr.createdAt,
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

	// Hard cap: unbounded export on a >1M-row table holds a DB conn for
	// minutes and starves the 2-conn pool.  Default 50 000; caller may
	// lower via ?limit=N but never raise above the cap.
	const exportHardLimit = 50_000
	exportLimit := queryInt(q, "limit", exportHardLimit)
	if exportLimit <= 0 || exportLimit > exportHardLimit {
		exportLimit = exportHardLimit
	}

	// Build WHERE clause — parameterised to avoid SQL injection.
	where := "WHERE EXISTS (SELECT 1 FROM business_emails be WHERE be.business_id = bl.id)"
	args := []any{}
	argIdx := 1
	if provider := q.Get("email_provider"); provider != "" {
		safe := true
		for _, c := range provider {
			if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.') {
				safe = false
				break
			}
		}
		if safe {
			// Match on e.domain (exact) instead of LIKE '%@provider'.
			where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be2 JOIN emails e ON e.id = be2.email_id WHERE be2.business_id = bl.id AND e.domain = $%d)", argIdx)
			args = append(args, provider)
			argIdx++
		}
	}

	// Query 1: listings page (no correlated subquery).  LIMIT is enforced here.
	listQuery := fmt.Sprintf(`
		SELECT bl.id, COALESCE(bl.business_name,''), COALESCE(bl.category,''),
		       COALESCE(bl.website,''), bl.domain, bl.social_links,
		       COALESCE(bl.address,''), COALESCE(bl.location,''),
		       COALESCE(bl.phone,'')
		FROM business_listings bl %s
		ORDER BY bl.id ASC
		LIMIT $%d
	`, where, argIdx)
	args = append(args, exportLimit)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, listQuery, args...)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer rows.Close()

	type exportRow struct {
		id              int64
		businessName    string
		category        string
		website         string
		domain          string
		socialLinksJSON []byte
		address         string
		location        string
		phone           string
	}
	var exportRows []exportRow
	var exportIDs []int64
	for rows.Next() {
		var er exportRow
		rows.Scan(&er.id, &er.businessName, &er.category, &er.website, &er.domain,
			&er.socialLinksJSON, &er.address, &er.location, &er.phone)
		exportRows = append(exportRows, er)
		exportIDs = append(exportIDs, er.id)
	}
	rows.Close()

	// Query 2: Batch-fetch emails for all IDs — replaces per-row array_agg subquery.
	emailMap := make(map[int64][]string, len(exportIDs))
	if len(exportIDs) > 0 {
		emailRows, err := s.db.QueryContext(ctx, `
			SELECT be.business_id, e.email
			FROM business_emails be
			JOIN emails e ON e.id = be.email_id
			WHERE be.business_id = ANY($1)
			  AND e.validation_status IN ('valid', 'pending', 'unknown')
			ORDER BY be.business_id, e.email
		`, pq.Array(exportIDs))
		if err != nil {
			slog.Error("v1: export batch email fetch error", "error", err)
			// Continue — export rows with empty email lists rather than 500.
		} else {
			defer emailRows.Close()
			for emailRows.Next() {
				var bizID int64
				var email string
				if err := emailRows.Scan(&bizID, &email); err == nil {
					emailMap[bizID] = append(emailMap[bizID], email)
				}
			}
		}
	}

	if format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=contacts.csv")
		fmt.Fprintln(w, "business_name,category,website,emails,domain,social_links,address,location,phone")
		for _, er := range exportRows {
			emails := emailMap[er.id]
			fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				csvEscape(er.businessName), csvEscape(er.category),
				csvEscape(er.website), csvEscape(strings.Join(emails, ";")),
				csvEscape(er.domain), csvEscape(string(er.socialLinksJSON)),
				csvEscape(er.address), csvEscape(er.location), csvEscape(er.phone))
		}
		return
	}

	// JSON export.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("["))
	enc := json.NewEncoder(w)
	for i, er := range exportRows {
		if i > 0 {
			w.Write([]byte(","))
		}
		emails := emailMap[er.id]
		if emails == nil {
			emails = []string{}
		}
		enc.Encode(map[string]any{
			"business_name": er.businessName, "business_category": er.category,
			"website": er.website, "emails": emails, "domain": er.domain,
			"social_links": json.RawMessage(er.socialLinksJSON),
			"address":      er.address, "location": er.location, "phone": er.phone,
		})
	}
	w.Write([]byte("]"))
}

func (s *Server) handleContactStats(w http.ResponseWriter, r *http.Request) {
	// 10s budget — must exceed the 8s statement_timeout below.
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var totalBiz, withEmail, uniqueDomains, totalEmails, lastHour, last24h int
	var validEmails, pendingEmails, invalidEmails int

	// 9 serial QueryRow on a 2-conn pool was the cascade root: 1 slow query
	// held both pool conns ~40s and v2 endpoints saw 500 with context deadline.
	// Fold into 3 queries (business, email, providers) inside one tx with
	// statement_timeout to bound the damage on the pool.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v1: contact-stats tx error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '8000'"); err != nil {
		slog.Error("v1: contact-stats set timeout error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	// Business listings: 2 aggregates in 1 pass.
	tx.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(DISTINCT domain)
		FROM business_listings
	`).Scan(&totalBiz, &uniqueDomains)

	// with_email needs a join; separate query (still inside tx).
	tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT business_id) FROM business_emails`).Scan(&withEmail)

	// Emails: 6 aggregates in 1 pass.
	tx.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE validation_status = 'valid'),
		       COUNT(*) FILTER (WHERE validation_status = 'pending'),
		       COUNT(*) FILTER (WHERE validation_status = 'invalid'),
		       COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 hour'),
		       COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours')
		FROM emails
	`).Scan(&totalEmails, &validEmails, &pendingEmails, &invalidEmails, &lastHour, &last24h)

	// Top email providers.
	providers := map[string]int{}
	providerRows, _ := tx.QueryContext(ctx, `
		SELECT domain, COUNT(*) AS cnt
		FROM emails
		GROUP BY domain
		ORDER BY cnt DESC
		LIMIT 10
	`)
	if providerRows != nil {
		for providerRows.Next() {
			var provider string
			var cnt int
			providerRows.Scan(&provider, &cnt)
			providers[provider] = cnt
		}
		providerRows.Close()
	}

	tx.Commit()

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
	// GROUP BY on business_listings can be expensive at scale; bound with a
	// transaction-local statement_timeout so a slow scan can't stall the pool.
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '7000'"); err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	rows, err := tx.QueryContext(ctx, `
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
	rows.Close()
	tx.Commit()
	writeJSON(w, http.StatusOK, domains)
}

func (s *Server) handleListCategories(w http.ResponseWriter, r *http.Request) {
	// COUNT(*) GROUP BY on a large table: bound with statement_timeout to
	// prevent pool starvation (see Operational Invariants §2).
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	defer tx.Rollback()
	tx.ExecContext(ctx, "SET LOCAL statement_timeout = '7000'")

	rows, err := tx.QueryContext(ctx, `
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
	rows.Close()
	tx.Commit()
	if cats == nil {
		cats = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, cats)
}
