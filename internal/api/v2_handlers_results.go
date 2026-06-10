package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	pq "github.com/lib/pq"
)

// resultsCountTTL is the cache TTL for filtered COUNT(*) on business_listings.
const resultsCountTTL = 60 * time.Second

// cachedFilteredCount is the existing call signature, now thin wrapper over cachedCount.
func (s *Server) cachedFilteredCount(ctx context.Context, where string, args []any) (int, error) {
	return s.cachedCount(ctx, "business_listings bl", where, args, resultsCountTTL, 12*time.Second)
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
		// emails.domain stores the exact after-@ part (e.g. "gmail.com") and is
		// indexed by idx_emails_domain — avoids the leading-wildcard LIKE scan on
		// the full email address that the previous e.email LIKE '%@provider' path
		// caused (non-sargable on any btree index).
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.domain = $%d)", argIdx)
		args = append(args, provider)
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
	if country := q.Get("country"); country != "" {
		// Stored values are ISO alpha-2 ("ID", "DE", ...) — accept any case
		// from the caller and normalize via UPPER on both sides.
		where += fmt.Sprintf(" AND UPPER(bl.country) = UPPER($%d)", argIdx)
		args = append(args, country)
		argIdx++
	}
	if city := q.Get("city"); city != "" {
		// Prefix ILIKE so "?city=Berlin" matches "Berlin", "Berlin-Mitte", etc.
		// Cheaper than substring %x% and friendlier to a btree(city) index.
		where += fmt.Sprintf(" AND bl.city ILIKE $%d", argIdx)
		args = append(args, city+"%")
		argIdx++
	}
	if hasPhone := q.Get("has_phone"); hasPhone == "true" {
		where += " AND bl.phone IS NOT NULL AND bl.phone <> ''"
	}
	if hasSocial := q.Get("has_social"); hasSocial == "true" {
		where += " AND bl.social_links IS NOT NULL AND bl.social_links::text NOT IN ('{}', '')"
	}
	// off_niche default-filtered out so consumer doesn't see polluted rows
	// (Hotel, AutoDealer, Dentist, ...) unless they explicitly opt in.
	// `include_off_niche=true` opts in for full view; anything else preserves
	// the default safe path.
	if q.Get("include_off_niche") != "true" {
		where += " AND bl.off_niche IS NOT TRUE"
	}
	if niche := q.Get("niche"); niche != "" {
		where += fmt.Sprintf(" AND bl.niche_category = $%d", argIdx)
		args = append(args, niche)
		argIdx++
	}
	if category := q.Get("category"); category != "" {
		// Exact-match on bl.category; backed by idx_bl_category (added in migrate.go).
		where += fmt.Sprintf(" AND bl.category = $%d", argIdx)
		args = append(args, category)
		argIdx++
	}
	if source := q.Get("source"); source != "" {
		// Filter to listings that have at least one email with the given source
		// label (e.g. "enrichment", "directory", "manual").
		where += fmt.Sprintf(" AND EXISTS (SELECT 1 FROM business_emails be WHERE be.business_id = bl.id AND be.source = $%d)", argIdx)
		args = append(args, source)
		argIdx++
	}

	return where, args, argIdx
}

// resultsOrderBy returns a safe ORDER BY clause for the results endpoint.
// Whitelist-only to prevent SQL injection — q.Get("sort") is user input.
// Defaults to id_desc for compat with the original hardcoded ordering.
func resultsOrderBy(sort string) string {
	switch sort {
	case "id_asc":
		return "ORDER BY bl.id ASC"
	case "updated_desc":
		return "ORDER BY bl.updated_at DESC, bl.id DESC"
	case "updated_asc":
		return "ORDER BY bl.updated_at ASC, bl.id ASC"
	case "created_desc":
		return "ORDER BY bl.created_at DESC, bl.id DESC"
	case "created_asc":
		return "ORDER BY bl.created_at ASC, bl.id ASC"
	default:
		return "ORDER BY bl.id DESC"
	}
}

// isIDDescOrder reports whether the OFFSET ordering is keyset-compatible with
// the cursor (which walks bl.id DESC). Only the default order and the explicit
// id_desc alias qualify — any other sort would make an id-keyed cursor skip or
// duplicate rows, so we must not hand one out.
func isIDDescOrder(sort string) bool {
	return sort == "" || sort == "id_desc"
}

// offsetNextCursor decides whether an OFFSET-mode response can hand the client
// a keyset cursor to escape deep-OFFSET timeouts (issue #32, Bug 3 — keyset was
// previously unreachable because no next_cursor was ever emitted). Returns the
// opaque cursor, or "" when keyset is not offerable for this page.
//
// The cursor is keyed on bl.id DESC, so it is only valid when the page itself
// is ordered id-descending (isIDDescOrder). When the filtered total is known we
// offer a cursor only if rows remain after this page; when the count failed
// (total < 0 — issue #32 Bug 2) we offer one whenever the page came back full,
// so a consumer with an unreliable total still has a forward path.
func offsetNextCursor(sort string, page, perPage, total, returned int, lastID int64) string {
	if returned == 0 || !isIDDescOrder(sort) {
		return ""
	}
	more := false
	if total >= 0 {
		more = page*perPage < total
	} else {
		more = returned >= perPage
	}
	if !more {
		return ""
	}
	return encodeCursor(lastID)
}

// isStatementTimeout reports whether err is a Postgres statement_timeout / query
// cancellation (or the request-context deadline firing). Used to map a deep
// OFFSET scan that blew the timeout to a clean 4xx (offset_too_deep) instead of
// a generic 500 (issue #32, fix #3).
func isStatementTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "57014" // query_canceled (raised by statement_timeout)
	}
	return strings.Contains(strings.ToLower(err.Error()), "statement timeout")
}

// isMatviewNotPopulated reports whether err is Postgres' "materialized view not
// populated" condition. category_stats is created WITH NO DATA, so two distinct
// SQLSTATEs can surface until it is first seeded:
//   - 55000 (object_not_in_prerequisite_state): SELECT on an unpopulated matview
//     ("materialized view ... has not been populated").
//   - 0A000 (feature_not_supported): REFRESH ... CONCURRENTLY on an unpopulated
//     matview ("CONCURRENTLY cannot be used when ... not populated").
//
// The categories handler treats it as "warming up" and returns an empty list
// instead of a 500.
func isMatviewNotPopulated(err error) bool {
	if err == nil {
		return false
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		// 42P01 (undefined_table): the matview briefly doesn't exist while a
		// versioned migration DROPs + recreates it — same warm-up semantics.
		if pqErr.Code == "42P01" {
			return true
		}
		return (pqErr.Code == "55000" || pqErr.Code == "0A000") && strings.Contains(pqErr.Message, "populated")
	}
	return strings.Contains(err.Error(), "not populated") || strings.Contains(err.Error(), "not been populated")
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

	// Cursor mode (?cursor=base64): keyset pagination — O(log N) at any depth
	// via the existing bl.id PK index. Skips COUNT entirely and returns
	// {next_cursor, has_more} instead of a total. Detected by presence of the
	// cursor param; if absent we fall through to the existing OFFSET path so
	// existing consumers keep working unchanged.
	cursorParam := q.Get("cursor")
	useCursor := cursorParam != ""

	if useCursor {
		cursorID, derr := decodeCursor(cursorParam)
		if derr != nil {
			writeV2Error(w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
			return
		}
		dir := q.Get("cursor_dir")
		if dir == "prev" {
			where += fmt.Sprintf(" AND bl.id > $%d", argIdx)
		} else {
			where += fmt.Sprintf(" AND bl.id < $%d", argIdx)
		}
		args = append(args, cursorID)
		argIdx++
	}

	// Count only applies in OFFSET mode. On timeout/error the helper returns -1
	// so we serve the page with a sentinel instead of 500.
	var total int
	if !useCursor {
		var err error
		total, err = s.cachedFilteredCount(ctx, where, args)
		if err != nil {
			slog.Warn("v2: count failed (returning -1 sentinel)", "error", err)
		}
	}

	// Cursor mode fetches perPage+1 to detect has_more without a second query.
	limit := perPage
	if useCursor {
		limit = perPage + 1
	}

	// Cursor mode is keyed on bl.id, so ?sort= only takes effect in OFFSET mode.
	// Forcing id-order in cursor mode keeps the keyset boundary consistent —
	// mixing sort keys with a cursor would skip or duplicate rows.
	orderBy := "ORDER BY bl.id DESC"
	if !useCursor {
		orderBy = resultsOrderBy(q.Get("sort"))
	}

	// Query 1: Fetch paginated listings (all columns).
	dataQuery := fmt.Sprintf(`
		SELECT bl.id, COALESCE(bl.business_name,''), COALESCE(bl.category,''),
		       COALESCE(bl.niche_category,''), COALESCE(bl.off_niche, FALSE),
		       COALESCE(bl.description,''), COALESCE(bl.website,''),
		       bl.domain, bl.url, COALESCE(bl.social_links,'{}'),
		       COALESCE(bl.address,''), COALESCE(bl.location,''),
		       COALESCE(bl.city,''), COALESCE(bl.country,''), COALESCE(bl.contact_name,''),
		       COALESCE(bl.opening_hours,''), COALESCE(bl.rating,''),
		       COALESCE(bl.page_title,''), COALESCE(bl.phone,''), COALESCE(bl.phones,'{}'),
		       COALESCE(bl.tiktok,''), COALESCE(bl.youtube,''), COALESCE(bl.telegram,''),
		       bl.source_query_id, bl.created_at, bl.updated_at
		FROM business_listings bl %s
		%s
		LIMIT $%d`, where, orderBy, argIdx)
	args = append(args, limit)
	argIdx++

	if !useCursor {
		dataQuery += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, offset)
	}

	rows, err := s.db.QueryContext(ctx, dataQuery, args...)
	if err != nil {
		// A deep OFFSET scan that blows the statement timeout used to surface as
		// a generic 500 (issue #32, Bug 1). Return a clean 4xx pointing the
		// client at keyset pagination instead — the next_cursor emitted on a
		// shallower page is the supported escape hatch.
		if !useCursor && offset > 0 && isStatementTimeout(err) {
			slog.Warn("v2: results OFFSET timed out — advising keyset cursor", "offset", offset, "error", err)
			writeV2Error(w, http.StatusBadRequest, "offset_too_deep",
				"this page is too deep to fetch by offset under load; switch to keyset pagination using the next_cursor returned on a shallower page")
			return
		}
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
		var phones pq.StringArray
		err := rows.Scan(&l.ID, &l.BusinessName, &l.Category,
			&l.NicheCategory, &l.OffNiche,
			&l.Description, &l.Website,
			&l.Domain, &l.URL, &socialLinksJSON,
			&l.Address, &l.Location, &l.City, &l.Country, &l.ContactName,
			&l.OpeningHours, &l.Rating,
			&l.PageTitle, &phone, &phones,
			&l.TikTok, &l.YouTube, &l.Telegram,
			&l.SourceQueryID, &l.CreatedAt, &l.UpdatedAt)
		if err != nil {
			slog.Error("v2: scan error", "error", err)
			continue
		}
		l.SocialLinks = json.RawMessage(socialLinksJSON)
		l.Emails = []string{}
		l.EmailsWithInfo = []V2EmailInfo{}
		// Prefer multi-phone array; fall back to single phone for old rows
		// where the array column is empty (legacy data pre-2026-04-27).
		if len(phones) > 0 {
			l.Phones = []string(phones)
		} else if phone != "" {
			l.Phones = []string{phone}
		} else {
			l.Phones = []string{}
		}
		l.Phone = phone
		listings = append(listings, l)
		listingIDs = append(listingIDs, l.ID)
	}

	// Cursor mode: detect has_more via the sentinel row, then trim it BEFORE
	// the email batch fetch so we don't waste a query on a row we're dropping.
	hasMore := false
	if useCursor && len(listings) > perPage {
		hasMore = true
		listings = listings[:perPage]
		listingIDs = listingIDs[:perPage]
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

	if useCursor {
		// Cursor envelope: explicit fields instead of pagination meta. Skip
		// writeV2Single because that wraps in "data" and we also need
		// next_cursor + has_more at the same level as the JSON-RPC-ish v2 style.
		var nextCursor string
		if hasMore && len(listings) > 0 {
			nextCursor = encodeCursor(listings[len(listings)-1].ID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data":        listings,
			"next_cursor": nextCursor,
			"has_more":    hasMore,
		})
		return
	}

	// OFFSET mode: emit a keyset next_cursor alongside the pagination meta so a
	// client can switch off OFFSET before it hits the deep-page timeout (issue
	// #32, fix #1 — previously keyset was unreachable). Empty (omitted) when the
	// sort isn't id-keyed or there are no further rows.
	var lastID int64
	if len(listings) > 0 {
		lastID = listings[len(listings)-1].ID
	}
	nextCursor := offsetNextCursor(q.Get("sort"), page, perPage, total, len(listings), lastID)
	writeV2PaginatedCursor(w, listings, total, page, perPage, nextCursor)
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

// handleV2ResultsCount returns a lightweight count with filters. When the
// underlying COUNT(*) times out, returns count=0 + count_known=false instead
// of HTTP 500 so polling clients stop retrying a query that will never
// succeed in the time budget.
func (s *Server) handleV2ResultsCount(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	where, args, _ := buildResultsFilter(r.URL.Query())

	total, err := s.cachedFilteredCount(ctx, where, args)
	known := err == nil && total >= 0
	if err != nil {
		slog.Warn("v2: count failed (returning unknown sentinel)", "error", err)
	}
	if !known {
		total = 0
	}

	writeV2Single(w, map[string]any{
		"count":       total,
		"count_known": known,
	})
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

	// Read precomputed counts from the category_stats materialized view (refreshed
	// off the request path by the manager's RefreshCategoryStatsLoop). The live
	// aggregation — business_listings (1.2M) ⋈ business_emails (4.3M), GROUP BY
	// 130K+ free-text categories with 2× COUNT(DISTINCT) — blew past the 5s budget
	// and returned 500s (Invariant #2).
	rows, err := tx.QueryContext(ctx, `
		SELECT category, biz_count, email_count
		FROM category_stats
		ORDER BY biz_count DESC
		LIMIT 100
	`)
	if err != nil {
		// Before the first background refresh the matview is unpopulated (WITH NO
		// DATA → SQLSTATE 55000). That's a transient warm-up state, not a failure:
		// return an empty list with 200 so the dashboard renders cleanly.
		if isMatviewNotPopulated(err) {
			_ = tx.Rollback()
			writeV2Single(w, []V2CategoryStats{})
			return
		}
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
		fmt.Fprintln(w, "id,business_name,category,niche_category,off_niche,domain,website,address,location,phone,emails,email_count,valid_email_count,created_at")
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
			       COALESCE(bl.niche_category,''), COALESCE(bl.off_niche, FALSE),
			       COALESCE(bl.description,''), COALESCE(bl.website,''),
			       bl.domain, bl.url, COALESCE(bl.social_links,'{}'),
			       COALESCE(bl.address,''), COALESCE(bl.location,''),
			       COALESCE(bl.city,''), COALESCE(bl.country,''), COALESCE(bl.contact_name,''),
			       COALESCE(bl.opening_hours,''), COALESCE(bl.rating,''),
			       COALESCE(bl.page_title,''), COALESCE(bl.phone,''), COALESCE(bl.phones,'{}'),
			       COALESCE(bl.tiktok,''), COALESCE(bl.youtube,''), COALESCE(bl.telegram,''),
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
			var phones pq.StringArray
			if err := rows.Scan(&l.ID, &l.BusinessName, &l.Category,
				&l.NicheCategory, &l.OffNiche,
				&l.Description, &l.Website,
				&l.Domain, &l.URL, &socialLinksJSON,
				&l.Address, &l.Location, &l.City, &l.Country, &l.ContactName,
				&l.OpeningHours, &l.Rating,
				&l.PageTitle, &phone, &phones,
				&l.TikTok, &l.YouTube, &l.Telegram,
				&l.SourceQueryID, &l.CreatedAt, &l.UpdatedAt); err != nil {
				slog.Error("v2: download scan error", "error", err)
				continue
			}
			l.SocialLinks = json.RawMessage(socialLinksJSON)
			l.Emails = []string{}
			l.EmailsWithInfo = []V2EmailInfo{}
			if len(phones) > 0 {
				l.Phones = []string(phones)
			} else if phone != "" {
				l.Phones = []string{phone}
			} else {
				l.Phones = []string{}
			}
			l.Phone = phone
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
				fmt.Fprintf(w, "%d,%s,%s,%s,%t,%s,%s,%s,%s,%s,%s,%d,%d,%s\n",
					l.ID,
					csvEscape(l.BusinessName), csvEscape(l.Category),
					csvEscape(l.NicheCategory), l.OffNiche,
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
