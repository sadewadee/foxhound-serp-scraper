package db

// Geo lineage (2026-06-10, first slice of the v4 normalized schema — see
// docs/foxhound-schema-normalized.md). 86% of active business_listings had
// empty country (88% empty city) — NOT a scraping-coverage gap (Jakarta/Bali
// alone had 44K+ queries) but lost attribution: the wellness generator knew
// city+country per keyword, yet only the text was persisted, and page
// extraction can't recover it (555K of the 600K country-empty rows have no
// address at all; Indonesian sites rarely print "Indonesia" for a domestic
// audience).
//
// Design (v4 direction): countries (ISO-2 lookup) + geo_cities (city→country)
// become first-class reference tables, seeded from internal/query at manager
// boot. From them:
//   - InsertBatch resolves queries.country (ISO-2) + queries.city at INSERT
//     time for every path (generate, import, telegram, API, auto-expansion).
//   - trg_normalize_enrichment inherits the query's geo into listings whose
//     page gave none — country (legacy full-name), country_code (v4 FK-ready),
//     city — marked geo_source='query_inference'.
//   - BackfillQueryGeo retro-fills 3.45M legacy queries from the city token in
//     their text (single-regex alternation, id-windowed).
//   - BackfillListingGeoInherit retro-fills ~582K legacy listings from their
//     source query (id-windowed).
//   - BackfillListingCountryCode maps existing page-extracted full-name
//     countries (~97K rows) to ISO-2 country_code.
// Inference NEVER overwrites page-extracted geo — extracted values win, and
// geo_source distinguishes the two.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Country is one row of the countries lookup (ISO-3166-1 alpha-2).
type Country struct {
	Code string // "ID"
	Name string // "Indonesia"
}

// GeoCity is one row of the geo_cities reference table.
type GeoCity struct {
	CityLower   string // lowercased token as it appears in query text
	City        string // canonical proper-case form ("Jakarta")
	CountryCode string // ISO-2 → countries.code
}

// SeedCountries idempotently upserts the countries lookup. Runs on every
// manager boot (tiny) so list updates flow on deploy.
func SeedCountries(ctx context.Context, db *sql.DB, rows []Country) {
	if len(rows) == 0 {
		return
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		slog.Warn("db: countries seed: begin failed", "error", err)
		return
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO countries (code, name, enabled) VALUES ($1, $2, TRUE)
		ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
	`)
	if err != nil {
		slog.Warn("db: countries seed: prepare failed", "error", err)
		return
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx, r.Code, r.Name); err != nil {
			slog.Warn("db: countries seed: insert failed", "code", r.Code, "error", err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		slog.Warn("db: countries seed: commit failed", "error", err)
		return
	}
	slog.Info("db: countries seeded", "rows", len(rows))
}

// SeedGeoCities idempotently upserts the city→country reference rows. Runs on
// every manager boot (≈600 tiny rows) so wellness.go city-list updates flow to
// the DB on deploy without a versioned migration. Must run AFTER SeedCountries
// (FK on country_code).
func SeedGeoCities(ctx context.Context, db *sql.DB, rows []GeoCity) {
	if len(rows) == 0 {
		return
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		slog.Warn("db: geo_cities seed: begin failed", "error", err)
		return
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO geo_cities (city_lower, city, country_code) VALUES ($1, $2, $3)
		ON CONFLICT (city_lower) DO UPDATE SET city = EXCLUDED.city, country_code = EXCLUDED.country_code
	`)
	if err != nil {
		slog.Warn("db: geo_cities seed: prepare failed", "error", err)
		return
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx, r.CityLower, r.City, r.CountryCode); err != nil {
			slog.Warn("db: geo_cities seed: insert failed", "city", r.CityLower, "error", err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		slog.Warn("db: geo_cities seed: commit failed", "error", err)
		return
	}
	slog.Info("db: geo_cities seeded", "rows", len(rows))
}

// buildCityAlternation compiles the geo_cities tokens into one word-bounded
// POSIX alternation: \m(kuta lombok|new york|...|york)\M. Longest-first so the
// more specific token wins at the same match position ("new york" over "york").
// Tokens are regex-escaped; the result is matched against lower(text).
func buildCityAlternation(rows []GeoCity) string {
	tokens := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.CityLower != "" {
			tokens = append(tokens, regexp.QuoteMeta(r.CityLower))
		}
	}
	sort.Slice(tokens, func(i, j int) bool {
		if len(tokens[i]) != len(tokens[j]) {
			return len(tokens[i]) > len(tokens[j])
		}
		return tokens[i] < tokens[j]
	})
	return `\m(` + strings.Join(tokens, "|") + `)\M`
}

// queryGeoBackfillVersion gates the one-time legacy queries.country/city fill.
const queryGeoBackfillVersion = "2026_06_10_query_geo_backfill"

// BackfillQueryGeo retro-fills queries.country (ISO-2) + queries.city for
// legacy rows by matching the city token embedded in the templated text
// ("<keyword> <city> <operator>") against geo_cities. One regexp_match per row
// (single compiled alternation), then a PK join to geo_cities on the captured
// token — no per-city scans. Background (manager only), id-windowed so each
// statement is small, resumable: if interrupted the version is not recorded
// and the next boot re-walks the id range cheaply (already-filled rows are
// skipped by the country=” filter).
func BackfillQueryGeo(ctx context.Context, db *sql.DB, rows []GeoCity) {
	var done bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, queryGeoBackfillVersion,
	).Scan(&done); err != nil || done {
		if err != nil {
			slog.Warn("db: query geo backfill version check failed", "error", err)
		}
		return
	}

	alternation := buildCityAlternation(rows)
	var minID, maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(id), MAX(id) FROM queries`).Scan(&minID, &maxID); err != nil || !maxID.Valid {
		slog.Warn("db: query geo backfill: id range failed", "error", err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		slog.Warn("db: query geo backfill: acquire conn failed", "error", err)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = '60s'`); err != nil {
		slog.Warn("db: query geo backfill: set timeout failed", "error", err)
		return
	}

	const window = int64(50000)
	slog.Info("db: query geo backfill starting (background, id-windowed)",
		"min_id", minID.Int64, "max_id", maxID.Int64, "window", window)
	var total int64
	for lo := minID.Int64 - 1; lo < maxID.Int64; lo += window {
		res, err := conn.ExecContext(ctx, `
			UPDATE queries q
			SET country = g.country_code, city = g.city, updated_at = NOW()
			FROM geo_cities g
			WHERE q.id > $1 AND q.id <= $2
			  AND COALESCE(q.country, '') = ''
			  AND g.city_lower = (regexp_match(lower(q.text), $3))[1]
		`, lo, lo+window, alternation)
		if err != nil {
			slog.Warn("db: query geo backfill window failed — resumes next boot", "lo", lo, "error", err, "filled_so_far", total)
			return
		}
		n, _ := res.RowsAffected()
		total += n
		select {
		case <-ctx.Done():
			slog.Info("db: query geo backfill interrupted (resumes next boot)", "filled_so_far", total)
			return
		case <-time.After(100 * time.Millisecond): // gentle pacing between windows
		}
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
		queryGeoBackfillVersion, fmt.Sprintf("fill queries.country (ISO-2) + city from city token in text via geo_cities (%d rows)", total),
	); err != nil {
		slog.Warn("db: query geo backfill: version record failed", "error", err)
		return
	}
	slog.Info("db: query geo backfill complete", "filled", total)
}

// listingGeoInheritVersion gates the one-time listings geo inheritance.
const listingGeoInheritVersion = "2026_06_10_listing_geo_inherit"

// BackfillListingGeoInherit retro-fills business_listings country/country_code
// (and city, when empty) from the originating query for rows whose page
// extraction gave no country. Marked geo_source='query_inference' so inferred
// geo is always distinguishable from page-extracted geo. Runs AFTER
// BackfillQueryGeo in the manager boot chain; id-windowed and resumable.
func BackfillListingGeoInherit(ctx context.Context, db *sql.DB) {
	var done bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, listingGeoInheritVersion,
	).Scan(&done); err != nil || done {
		if err != nil {
			slog.Warn("db: listing geo inherit version check failed", "error", err)
		}
		return
	}

	var minID, maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(id), MAX(id) FROM business_listings`).Scan(&minID, &maxID); err != nil || !maxID.Valid {
		slog.Warn("db: listing geo inherit: id range failed", "error", err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		slog.Warn("db: listing geo inherit: acquire conn failed", "error", err)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = '60s'`); err != nil {
		slog.Warn("db: listing geo inherit: set timeout failed", "error", err)
		return
	}

	const window = int64(50000)
	slog.Info("db: listing geo inherit starting (background, id-windowed)",
		"min_id", minID.Int64, "max_id", maxID.Int64, "window", window)
	var total int64
	for lo := minID.Int64 - 1; lo < maxID.Int64; lo += window {
		res, err := conn.ExecContext(ctx, `
			UPDATE business_listings bl
			SET country      = c.name,
			    country_code = q.country,
			    city         = CASE WHEN COALESCE(bl.city, '') = '' AND q.city <> '' THEN q.city ELSE bl.city END,
			    geo_source   = 'query_inference',
			    updated_at   = NOW()
			FROM queries q
			JOIN countries c ON c.code = q.country
			WHERE bl.id > $1 AND bl.id <= $2
			  AND COALESCE(bl.country, '') = ''
			  AND bl.source_query_id = q.id
			  AND COALESCE(q.country, '') <> ''
		`, lo, lo+window)
		if err != nil {
			slog.Warn("db: listing geo inherit window failed — resumes next boot", "lo", lo, "error", err, "filled_so_far", total)
			return
		}
		n, _ := res.RowsAffected()
		total += n
		select {
		case <-ctx.Done():
			slog.Info("db: listing geo inherit interrupted (resumes next boot)", "filled_so_far", total)
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
		listingGeoInheritVersion, fmt.Sprintf("inherit country/country_code/city from source query for geo-less listings, geo_source=query_inference (%d rows)", total),
	); err != nil {
		slog.Warn("db: listing geo inherit: version record failed", "error", err)
		return
	}
	slog.Info("db: listing geo inherit complete", "filled", total)
}

// listingCountryCodeVersion gates the one-time legacy name→ISO-2 mapping.
const listingCountryCodeVersion = "2026_06_10_listing_country_code"

// BackfillListingCountryCode maps existing page-extracted full-name countries
// ("United States", "Indonesia", ...) to the v4 FK-ready ISO-2 country_code
// via the countries lookup. Touches only rows with a country but no code
// (~97K). Id-windowed and resumable like the other geo backfills.
func BackfillListingCountryCode(ctx context.Context, db *sql.DB) {
	var done bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, listingCountryCodeVersion,
	).Scan(&done); err != nil || done {
		if err != nil {
			slog.Warn("db: listing country_code backfill version check failed", "error", err)
		}
		return
	}

	var minID, maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(id), MAX(id) FROM business_listings`).Scan(&minID, &maxID); err != nil || !maxID.Valid {
		slog.Warn("db: listing country_code backfill: id range failed", "error", err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		slog.Warn("db: listing country_code backfill: acquire conn failed", "error", err)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = '60s'`); err != nil {
		slog.Warn("db: listing country_code backfill: set timeout failed", "error", err)
		return
	}

	const window = int64(50000)
	var total int64
	for lo := minID.Int64 - 1; lo < maxID.Int64; lo += window {
		res, err := conn.ExecContext(ctx, `
			UPDATE business_listings bl
			SET country_code = c.code
			FROM countries c
			WHERE bl.id > $1 AND bl.id <= $2
			  AND bl.country_code IS NULL
			  AND COALESCE(bl.country, '') <> ''
			  AND LOWER(c.name) = LOWER(bl.country)
		`, lo, lo+window)
		if err != nil {
			slog.Warn("db: listing country_code backfill window failed — resumes next boot", "lo", lo, "error", err, "filled_so_far", total)
			return
		}
		n, _ := res.RowsAffected()
		total += n
		select {
		case <-ctx.Done():
			slog.Info("db: listing country_code backfill interrupted (resumes next boot)", "filled_so_far", total)
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
		listingCountryCodeVersion, fmt.Sprintf("map legacy full-name business_listings.country to ISO-2 country_code via countries lookup (%d rows)", total),
	); err != nil {
		slog.Warn("db: listing country_code backfill: version record failed", "error", err)
		return
	}
	slog.Info("db: listing country_code backfill complete", "filled", total)
}
