package api

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pq "github.com/lib/pq"
)

// resultsCountTTL is the Redis cache window for filtered COUNT(*) results.
// Polling clients re-hit the same filter combo within this window for free.
const resultsCountTTL = 60 * time.Second

// cachedFilteredCount returns COUNT(*) for the given WHERE+args, served from
// Redis when fresh. On query timeout/error, returns (-1, err) so the caller can
// still serve the page with a "many" sentinel instead of HTTP 500.
//
// Cache key shape: "v2:count:<sha1(where|args)>" — args serialized via fmt to
// keep numbers/strings/timestamps stable across calls.
func (s *Server) cachedFilteredCount(ctx context.Context, where string, args []any) (int, error) {
	key := buildCountCacheKey(where, args)

	if s.redis != nil {
		if v, err := s.redis.Get(ctx, key).Result(); err == nil {
			if n, perr := strconv.Atoi(v); perr == nil {
				return n, nil
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return -1, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '12000'"); err != nil {
		return -1, err
	}

	var total int
	q := fmt.Sprintf("SELECT COUNT(*) FROM business_listings bl %s", where)
	if err := tx.QueryRowContext(ctx, q, args...).Scan(&total); err != nil {
		return -1, err
	}
	tx.Commit()

	if s.redis != nil {
		// Best-effort SETEX; cache miss isn't worth failing the request over.
		_ = s.redis.Set(ctx, key, strconv.Itoa(total), resultsCountTTL).Err()
	}
	return total, nil
}

func buildCountCacheKey(where string, args []any) string {
	h := sha1.New()
	h.Write([]byte(where))
	for _, a := range args {
		fmt.Fprintf(h, "|%v", a)
	}
	return "v2:count:" + hex.EncodeToString(h.Sum(nil))
}

// buildResultsFilter builds a WHERE clause + args from query parameters.
// Returns (whereClause, args, nextArgIdx).
func buildResultsFilter(q url.Values) (string, []any, int) {
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
	if emailStatus := q.Get("email_status"); emailStatus != "" {
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.validation_status = $%d)", argIdx)
		args = append(args, emailStatus)
		argIdx++
	}
	if search := q.Get("search"); search != "" {
		where += fmt.Sprintf(" AND bl.business_name ILIKE $%d", argIdx)
		args = append(args, "%"+search+"%")
		argIdx++
	}

	return where, args, argIdx
}

// handleV2ListResults returns paginated business listings with full email info.
// Two-query strategy: listings first, then batch email fetch.
func (s *Server) handleV2ListResults(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	q := r.URL.Query()
	page := queryInt(q, "page", 1)
	perPage := queryInt(q, "per_page", 50)
	if perPage > 200 {
		perPage = 200
	}
	offset := (page - 1) * perPage

	where, args, argIdx := buildResultsFilter(q)

	// Count total via Redis-cached helper. On timeout/error the helper returns -1
	// so we serve the page with a sentinel instead of 500.
	total, err := s.cachedFilteredCount(ctx, where, args)
	if err != nil {
		slog.Warn("v2: count failed (returning -1 sentinel)", "error", err)
	}

	// Query 1: Fetch paginated listings (all columns).
	dataQuery := fmt.Sprintf(`
		SELECT bl.id, COALESCE(bl.business_name,''), COALESCE(bl.category,''),
		       COALESCE(bl.description,''), COALESCE(bl.website,''),
		       bl.domain, bl.url, COALESCE(bl.social_links,'{}'),
		       COALESCE(bl.address,''), COALESCE(bl.location,''),
		       COALESCE(bl.opening_hours,''), COALESCE(bl.rating,''),
		       COALESCE(bl.page_title,''), COALESCE(bl.phone,''),
		       bl.source_query_id, bl.created_at, bl.updated_at
		FROM business_listings bl %s
		ORDER BY bl.id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := s.db.QueryContext(ctx, dataQuery, args...)
	if err != nil {
		slog.Error("v2: list error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch results")
		return
	}
	defer rows.Close()

	var listings []V2BusinessListing
	var listingIDs []int64
	for rows.Next() {
		var l V2BusinessListing
		var socialLinksJSON []byte
		var phone string
		err := rows.Scan(&l.ID, &l.BusinessName, &l.Category, &l.Description, &l.Website,
			&l.Domain, &l.URL, &socialLinksJSON,
			&l.Address, &l.Location, &l.OpeningHours, &l.Rating,
			&l.PageTitle, &phone, &l.SourceQueryID, &l.CreatedAt, &l.UpdatedAt)
		if err != nil {
			slog.Error("v2: scan error", "error", err)
			continue
		}
		l.SocialLinks = json.RawMessage(socialLinksJSON)
		l.Emails = []string{}
		l.EmailsWithInfo = []V2EmailInfo{}
		l.Phones = []string{}
		if phone != "" {
			l.Phones = []string{phone}
		}
		listings = append(listings, l)
		listingIDs = append(listingIDs, l.ID)
	}

	// Query 2: Batch-fetch ALL email columns for those listing IDs.
	if len(listingIDs) > 0 {
		emailRows, err := s.db.QueryContext(ctx, `
			SELECT be.business_id, e.email, COALESCE(e.domain,''),
			       COALESCE(e.validation_status,'pending'),
			       e.score, e.is_acceptable, e.mx_valid, e.deliverable,
			       e.disposable, e.role_account, e.free_email, e.catch_all,
			       COALESCE(e.reason,''), COALESCE(be.source,'enrichment'),
			       e.validated_at
			FROM business_emails be
			JOIN emails e ON e.id = be.email_id
			WHERE be.business_id = ANY($1)
			ORDER BY be.business_id, e.email
		`, pq.Array(listingIDs))
		if err != nil {
			slog.Error("v2: email fetch error", "error", err)
			// Continue without emails — don't fail the whole response.
		} else {
			defer emailRows.Close()

			emailMap := make(map[int64][]V2EmailInfo)
			for emailRows.Next() {
				var businessID int64
				var ei V2EmailInfo
				err := emailRows.Scan(&businessID, &ei.Email, &ei.Domain, &ei.Status,
					&ei.Score, &ei.IsAcceptable, &ei.MXValid, &ei.Deliverable,
					&ei.Disposable, &ei.RoleAccount, &ei.FreeEmail, &ei.CatchAll,
					&ei.Reason, &ei.Source, &ei.ValidatedAt)
				if err != nil {
					slog.Error("v2: email scan error", "error", err)
					continue
				}
				emailMap[businessID] = append(emailMap[businessID], ei)
			}

			// Attach emails to listings.
			for i := range listings {
				if emails, ok := emailMap[listings[i].ID]; ok {
					listings[i].EmailsWithInfo = emails
					for _, e := range emails {
						listings[i].Emails = append(listings[i].Emails, e.Email)
					}
					listings[i].TotalEmailCount = len(emails)
					for _, e := range emails {
						if e.Status == "valid" {
							listings[i].ValidEmailCount++
						}
					}
				}
			}
		}
	}

	if listings == nil {
		listings = []V2BusinessListing{}
	}

	writeV2Paginated(w, listings, total, page, perPage)
}

// handleV2ResultsStats returns clear, unambiguous stats for results.
// Consolidates into 3 queries (business stats, email stats, providers) instead of 10+ serial ones.
func (s *Server) handleV2ResultsStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	stats := V2ResultsStats{
		TopProviders: map[string]int{},
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v2: stats tx error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch stats")
		return
	}
	defer tx.Rollback()

	// 15s timeout for stats — emails table (828K+) with 6 FILTER passes needs more than 5s.
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '15000'"); err != nil {
		slog.Error("v2: set timeout error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to set timeout")
		return
	}

	// Combined business stats — 1 query instead of 4.
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE phone IS NOT NULL AND phone != ''),
		       COUNT(DISTINCT domain)
		FROM business_listings
	`).Scan(&stats.TotalListings, &stats.WithPhone, &stats.UniqueDomains); err != nil {
		slog.Error("v2: business stats error", "error", err)
	}

	// with_email needs a join, separate query.
	tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT business_id) FROM business_emails`).Scan(&stats.WithEmail)

	// Combined email stats — 1 query instead of 6.
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE validation_status = 'valid'),
		       COUNT(*) FILTER (WHERE validation_status = 'pending'),
		       COUNT(*) FILTER (WHERE validation_status = 'invalid'),
		       COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 hour'),
		       COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours')
		FROM emails
	`).Scan(&stats.TotalEmails, &stats.ValidEmails, &stats.PendingEmails,
		&stats.InvalidEmails, &stats.EmailsPerHour, &stats.EmailsPer24h); err != nil {
		slog.Error("v2: email stats error", "error", err)
	}

	// Top providers.
	providerRows, err := tx.QueryContext(ctx, `
		SELECT domain, COUNT(*) AS cnt
		FROM emails
		GROUP BY domain ORDER BY cnt DESC LIMIT 10
	`)
	if err == nil {
		defer providerRows.Close()
		for providerRows.Next() {
			var provider string
			var cnt int
			providerRows.Scan(&provider, &cnt)
			stats.TopProviders[provider] = cnt
		}
	}

	tx.Commit()

	writeV2Single(w, stats)
}

// handleV2ResultsCount returns a lightweight count with filters.
func (s *Server) handleV2ResultsCount(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	where, args, _ := buildResultsFilter(r.URL.Query())

	total, err := s.cachedFilteredCount(ctx, where, args)
	if err != nil {
		slog.Warn("v2: count failed (returning -1 sentinel)", "error", err)
	}

	writeV2Single(w, map[string]int{"count": total})
}

// handleV2Categories returns categories with business and email counts.
func (s *Server) handleV2Categories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v2: categories tx error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch categories")
		return
	}
	defer tx.Rollback()
	tx.ExecContext(ctx, "SET LOCAL statement_timeout = '5000'")

	rows, err := tx.QueryContext(ctx, `
		SELECT bl.category, COUNT(DISTINCT bl.id) AS biz_count,
		       COUNT(DISTINCT be.email_id) AS email_count
		FROM business_listings bl
		LEFT JOIN business_emails be ON be.business_id = bl.id
		WHERE bl.category IS NOT NULL AND bl.category != ''
		GROUP BY bl.category
		ORDER BY biz_count DESC
		LIMIT 100
	`)
	if err != nil {
		slog.Error("v2: categories error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch categories")
		return
	}
	defer rows.Close()

	cats := []V2CategoryStats{}
	for rows.Next() {
		var c V2CategoryStats
		rows.Scan(&c.Category, &c.BusinessCount, &c.EmailCount)
		cats = append(cats, c)
	}
	tx.Commit()

	writeV2Single(w, cats)
}

// handleV2Domains returns domains with email counts.
func (s *Server) handleV2Domains(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v2: domains tx error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch domains")
		return
	}
	defer tx.Rollback()
	tx.ExecContext(ctx, "SET LOCAL statement_timeout = '5000'")

	rows, err := tx.QueryContext(ctx, `
		SELECT bl.domain, COUNT(DISTINCT be.email_id) AS email_count
		FROM business_listings bl
		LEFT JOIN business_emails be ON be.business_id = bl.id
		GROUP BY bl.domain
		ORDER BY email_count DESC
		LIMIT 500
	`)
	if err != nil {
		slog.Error("v2: domains error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch domains")
		return
	}
	defer rows.Close()

	domains := []V2DomainStats{}
	for rows.Next() {
		var d V2DomainStats
		rows.Scan(&d.Domain, &d.EmailCount)
		domains = append(domains, d)
	}
	tx.Commit()

	writeV2Single(w, domains)
}

// handleV2Download streams results as CSV or JSON with full email info.
// Uses batched processing (1000 listings at a time) to avoid OOM on large exports.
func (s *Server) handleV2Download(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2DownloadContext(r)
	defer cancel()

	q := r.URL.Query()
	format := q.Get("format")
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "csv" {
		writeV2Error(w, http.StatusBadRequest, "invalid_format", "format must be json or csv")
		return
	}

	where, args, argIdx := buildResultsFilter(q)

	const batchSize = 1000
	lastID := int64(0)
	firstItem := true

	if format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=results.csv")
		fmt.Fprintln(w, "id,business_name,category,domain,website,address,location,phone,emails,email_count,valid_email_count,created_at")
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=results.json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("["))
	}

	enc := json.NewEncoder(w)

	for {
		// Fetch a batch of listings using keyset pagination (id > lastID).
		batchWhere := where + fmt.Sprintf(" AND bl.id > $%d", argIdx)
		batchArgs := append(append([]any{}, args...), lastID)

		batchQuery := fmt.Sprintf(`
			SELECT bl.id, COALESCE(bl.business_name,''), COALESCE(bl.category,''),
			       COALESCE(bl.description,''), COALESCE(bl.website,''),
			       bl.domain, bl.url, COALESCE(bl.social_links,'{}'),
			       COALESCE(bl.address,''), COALESCE(bl.location,''),
			       COALESCE(bl.opening_hours,''), COALESCE(bl.rating,''),
			       COALESCE(bl.page_title,''), COALESCE(bl.phone,''),
			       bl.source_query_id, bl.created_at, bl.updated_at
			FROM business_listings bl %s
			ORDER BY bl.id ASC
			LIMIT %d
		`, batchWhere, batchSize)

		rows, err := s.db.QueryContext(ctx, batchQuery, batchArgs...)
		if err != nil {
			slog.Error("v2: download batch error", "error", err)
			break
		}

		var batch []V2BusinessListing
		var batchIDs []int64
		for rows.Next() {
			var l V2BusinessListing
			var socialLinksJSON []byte
			var phone string
			if err := rows.Scan(&l.ID, &l.BusinessName, &l.Category, &l.Description, &l.Website,
				&l.Domain, &l.URL, &socialLinksJSON,
				&l.Address, &l.Location, &l.OpeningHours, &l.Rating,
				&l.PageTitle, &phone, &l.SourceQueryID, &l.CreatedAt, &l.UpdatedAt); err != nil {
				slog.Error("v2: download scan error", "error", err)
				continue
			}
			l.SocialLinks = json.RawMessage(socialLinksJSON)
			l.Emails = []string{}
			l.EmailsWithInfo = []V2EmailInfo{}
			l.Phones = []string{}
			if phone != "" {
				l.Phones = []string{phone}
			}
			batch = append(batch, l)
			batchIDs = append(batchIDs, l.ID)
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		// Batch-fetch emails for this batch.
		emailRows, err := s.db.QueryContext(ctx, `
			SELECT be.business_id, e.email, COALESCE(e.domain,''),
			       COALESCE(e.validation_status,'pending'),
			       e.score, e.is_acceptable, e.mx_valid, e.deliverable,
			       e.disposable, e.role_account, e.free_email, e.catch_all,
			       COALESCE(e.reason,''), COALESCE(be.source,'enrichment'),
			       e.validated_at
			FROM business_emails be
			JOIN emails e ON e.id = be.email_id
			WHERE be.business_id = ANY($1)
			ORDER BY be.business_id, e.email
		`, pq.Array(batchIDs))
		if err == nil {
			emailMap := make(map[int64][]V2EmailInfo)
			for emailRows.Next() {
				var businessID int64
				var ei V2EmailInfo
				emailRows.Scan(&businessID, &ei.Email, &ei.Domain, &ei.Status,
					&ei.Score, &ei.IsAcceptable, &ei.MXValid, &ei.Deliverable,
					&ei.Disposable, &ei.RoleAccount, &ei.FreeEmail, &ei.CatchAll,
					&ei.Reason, &ei.Source, &ei.ValidatedAt)
				emailMap[businessID] = append(emailMap[businessID], ei)
			}
			emailRows.Close()

			for i := range batch {
				if emails, ok := emailMap[batch[i].ID]; ok {
					batch[i].EmailsWithInfo = emails
					for _, e := range emails {
						batch[i].Emails = append(batch[i].Emails, e.Email)
					}
					batch[i].TotalEmailCount = len(emails)
					for _, e := range emails {
						if e.Status == "valid" {
							batch[i].ValidEmailCount++
						}
					}
				}
			}
		}

		// Write batch to output.
		for _, l := range batch {
			if format == "csv" {
				fmt.Fprintf(w, "%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s\n",
					l.ID,
					csvEscape(l.BusinessName), csvEscape(l.Category),
					csvEscape(l.Domain), csvEscape(l.Website),
					csvEscape(l.Address), csvEscape(l.Location),
					csvEscape(l.Phone),
					csvEscape(strings.Join(l.Emails, ";")),
					l.TotalEmailCount, l.ValidEmailCount,
					l.CreatedAt.Format(time.RFC3339))
			} else {
				if !firstItem {
					w.Write([]byte(","))
				}
				firstItem = false
				enc.Encode(l)
			}
		}

		lastID = batch[len(batch)-1].ID
		if len(batch) < batchSize {
			break
		}
	}

	if format == "json" {
		w.Write([]byte("]"))
	}
}

// handleV2DeleteResults deletes results by IDs or domain.
func (s *Server) handleV2DeleteResults(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	var req struct {
		IDs    []int64 `json:"ids"`
		Domain string  `json:"domain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeV2Error(w, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}
	if len(req.IDs) == 0 && req.Domain == "" {
		writeV2Error(w, http.StatusBadRequest, "missing_params", "provide ids or domain")
		return
	}
	if len(req.IDs) > 0 && req.Domain != "" {
		writeV2Error(w, http.StatusBadRequest, "invalid_params", "provide ids or domain, not both")
		return
	}
	if len(req.IDs) > 1000 {
		writeV2Error(w, http.StatusBadRequest, "too_many_ids", "max 1000 ids per request")
		return
	}

	var res sql.Result
	var err error
	if req.Domain != "" {
		res, err = s.db.ExecContext(ctx, "DELETE FROM business_listings WHERE domain = $1", req.Domain)
	} else {
		res, err = s.db.ExecContext(ctx, "DELETE FROM business_listings WHERE id = ANY($1)", pq.Array(req.IDs))
	}
	if err != nil {
		slog.Error("v2: delete error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to delete results")
		return
	}
	n, _ := res.RowsAffected()
	writeV2Single(w, map[string]int64{"deleted": n})
}
