package db

// Niche lineage backfill (2026-06-10, same proven pattern as the geo lineage
// slice). 87% of active business_listings had niche_category NULL — not because
// the niche is unknowable, but because trg_normalize_enrichment classifies from
// PAGE content (raw_business_name + raw_page_title + raw_description) and many
// contact pages never repeat the keyword. The niche IS in the originating query
// text ("yoga studio jakarta contact"), exactly like the city token the geo
// backfill recovered. This fills niche_category for code-less ACTIVE rows from
// their source query, using the SAME keyword buckets the trigger applies to page
// content — so a backfilled bucket equals what the trigger would have produced
// on that text.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// nicheBuckets mirrors the niche_category CASE in trg_normalize_enrichment
// (internal/db/migrate.go) — identical patterns, identical precedence (first
// match wins). If the trigger CASE changes, update this slice AND the test
// (niche_backfill_test.go) to match; the same manual-sync contract the off-niche
// list already follows. Word boundaries use Postgres \m…\M; matched against the
// LOWER()'d query text. The "ayurveda" bucket uses a \m-prefix (no trailing \M)
// so it matches "ayurveda"/"ayurvedic" — kept in lockstep with the trigger.
var nicheBuckets = []struct {
	pattern string // Postgres ARE, matched against lowercased text
	bucket  string
}{
	{`\myoga|asana|vinyasa|ashtanga|kundalini|iyengar|hatha|bikram|jivamukti\M`, "yoga"},
	{`\mpilates|reformer\M`, "pilates"},
	{`\mcrossfit|bootcamp|hiit|barre|spin\M`, "fitness"},
	{`\m(gym|fitness)\M`, "fitness"},
	{`\mmeditation|mindfulness|breathwork\M`, "meditation"},
	{`\mreiki|sound healing|energy healing|healing\M`, "healing"},
	{`\mayurved`, "ayurveda"},
	{`\m(spa|massage|thermal)\M`, "spa"},
	{`\m(wellness|holistic)\M`, "wellness"},
}

// buildNicheCaseSQL renders the niche CASE over an already-lowercased text
// expression, e.g. buildNicheCaseSQL("LOWER(q.text)"). No pattern contains a
// '%' or a "'", so direct interpolation is safe (same as the trigger's inline
// CASE and the geo backfill's interpolated alternation).
func buildNicheCaseSQL(lowerTextExpr string) string {
	var b strings.Builder
	b.WriteString("CASE")
	for _, nb := range nicheBuckets {
		b.WriteString(fmt.Sprintf(" WHEN %s ~ '%s' THEN '%s'", lowerTextExpr, nb.pattern, nb.bucket))
	}
	b.WriteString(" ELSE NULL END")
	return b.String()
}

// listingNicheInheritVersion gates the one-time niche inheritance.
const listingNicheInheritVersion = "2026_06_10_listing_niche_inherit"

// BackfillListingNicheInherit fills business_listings.niche_category for ACTIVE
// (off_niche IS NOT TRUE) rows the page classifier left NULL, by applying the
// niche keyword buckets to the source query's text via source_query_id. Runs in
// the BACKGROUND (manager only), id-windowed + resumable, version-gated → no-op
// after a clean pass. PersonalNiches with no bucket (hypnotherapist, personal
// trainer, health coach, …) resolve to NULL and are left untouched.
//
// Marked niche_source='query_inference' (mirrors geo_source): the upsert
// trigger's ON CONFLICT lets a later re-enrich that finds a real page keyword
// SUPERSEDE the inferred bucket (and clear the marker), so inference never
// permanently shadows page-extracted truth.
func BackfillListingNicheInherit(ctx context.Context, db *sql.DB) {
	var done bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, listingNicheInheritVersion,
	).Scan(&done); err != nil || done {
		if err != nil {
			slog.Warn("db: listing niche inherit version check failed", "error", err)
		}
		return
	}

	var minID, maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(id), MAX(id) FROM business_listings`).Scan(&minID, &maxID); err != nil || !maxID.Valid {
		slog.Warn("db: listing niche inherit: id range failed", "error", err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		slog.Warn("db: listing niche inherit: acquire conn failed", "error", err)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = '60s'`); err != nil {
		slog.Warn("db: listing niche inherit: set timeout failed", "error", err)
		return
	}

	// The CASE is interpolated into both SET and WHERE (proven geo-queries
	// pattern) so only rows whose query text resolves to a bucket are touched.
	nicheCase := buildNicheCaseSQL("LOWER(q.text)")
	stmt := fmt.Sprintf(`
		UPDATE business_listings bl
		SET niche_category = %s, niche_source = 'query_inference', updated_at = NOW()
		FROM queries q
		WHERE bl.id > $1 AND bl.id <= $2
		  AND bl.niche_category IS NULL
		  AND bl.off_niche IS NOT TRUE
		  AND bl.source_query_id = q.id
		  AND %s IS NOT NULL
	`, nicheCase, nicheCase)

	const window = int64(50000)
	slog.Info("db: listing niche inherit starting (background, id-windowed)",
		"min_id", minID.Int64, "max_id", maxID.Int64, "window", window)
	var total int64
	var failedWindows int
	for lo := minID.Int64 - 1; lo < maxID.Int64; lo += window {
		n, err := execWindowRetry(ctx, conn, stmt, lo, lo+window)
		if err != nil {
			failedWindows++
			slog.Warn("db: listing niche inherit window failed after retries — continuing", "lo", lo, "error", err, "filled_so_far", total)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		total += n
		select {
		case <-ctx.Done():
			slog.Info("db: listing niche inherit interrupted (resumes next boot)", "filled_so_far", total)
			return
		case <-time.After(100 * time.Millisecond): // gentle pacing between windows
		}
	}
	if failedWindows > 0 {
		slog.Warn("db: listing niche inherit pass partial — version NOT recorded, re-walks next boot",
			"failed_windows", failedWindows, "filled_this_pass", total)
		return
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
		listingNicheInheritVersion, fmt.Sprintf("inherit niche_category from source query text for active page-unclassified listings (%d rows)", total),
	); err != nil {
		slog.Warn("db: listing niche inherit: version record failed", "error", err)
		return
	}
	slog.Info("db: listing niche inherit complete", "filled", total)
}
