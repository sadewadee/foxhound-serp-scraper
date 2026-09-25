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
	"os"
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
	// 2026-09-25: wrapped in \m(...)\M — the un-grouped form (`\ma|b|c\M`)
	// only anchors the FIRST alternative at word-start and the LAST at
	// word-end; every alternative in between matches as an unanchored
	// substring (`spin` inside "spinach", `barre` inside "barrel", `hatha`/
	// `asana` mid-word). See the 2026-09-25 gotcha entry.
	{`\m(yoga|asana|vinyasa|ashtanga|kundalini|iyengar|hatha|bikram|jivamukti|yogi|yogis)\M`, "yoga"},
	{`\m(pilates|reformer)\M`, "pilates"},
	{`\m(crossfit|bootcamp|hiit|barre|spin)\M`, "fitness"},
	{`\m(gym|fitness)\M`, "fitness"},
	// fitness broadened (v0.9.8 broad-wellness scope): personal training,
	// coaching-by-discipline, combat/dance/aquatic fitness.
	{`\m(personal train|strength coach|conditioning coach|functional train|kickbox|boxing|martial art|swimming|zumba|pole danc|pole fit|pole instructor|dance)`, "fitness"},
	{`\m(meditation|mindfulness|breathwork)\M`, "meditation"},
	{`\m(reiki|sound healing|energy healing|healing)\M`, "healing"},
	{`\mayurved`, "ayurveda"},
	{`\m(spa|massage|thermal)\M`, "spa"},
	{`\m(wellness|holistic)\M`, "wellness"},
	// Health-adjacent buckets (v0.9.8 broad-wellness scope). \m-prefix groups
	// (no trailing \M) so stems match their inflections (osteopath/osteopathy,
	// naturopath/naturopathy, dietit→dietitian). Ordered AFTER the core wellness
	// buckets so a page/query mentioning a core keyword keeps its core bucket.
	{`\m(osteopath|physiotherap|physical therap|chiropract|acupunctur|craniosacral|reflexolog|kinesiolog)`, "bodywork"},
	{`\m(hypnotherap|psychotherap|counsel)`, "therapy"},
	{`\m(dietit|dietician|nutrition)`, "nutrition"},
	{`\m(naturopath|herbal|homeopath|homoeopath)`, "naturopathy"},
	{`\m(life coach|health coach|mindset coach)`, "coaching"},
}

// beautyOffNichePattern matches off-target beauty/grooming niches (nail, barber,
// esthetician, …) that the v0.9.8 broad-wellness scope decision (2026-06-11)
// EXCLUDES from results (off_niche=TRUE). The wellness generator never seeds
// these (they entered via a legacy broad-niche import). Kept in lockstep with
// the trigger off_niche CASE. \m-prefix groups, matched against lowercased text.
const beautyOffNichePattern = `\m(nail salon|manicure|pedicure|esthetic|aesthetic|beautician|cosmetolog|barber|hairdress|hair salon|makeup|make-up|eyelash|lash extension|eyebrow|microblad|waxing salon|tattoo)`

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

// nicheTaxonomyV2Version gates the v0.9.8 broad-wellness taxonomy backfill.
const nicheTaxonomyV2Version = "2026_06_11_niche_taxonomy_v2"

// BackfillNicheTaxonomyV2 applies the v0.9.8 broad-wellness scope decision to the
// rows the first niche-inherit pass left NULL (niche_category IS NULL AND
// off_niche IS NOT TRUE), via source-query lineage:
//
//  1. off_niche = TRUE for off-target beauty/grooming queries (nail, barber,
//     esthetician, …) — beautyOffNichePattern. Runs FIRST so those rows are
//     excluded from the bucketing pass.
//  2. niche_category = the EXPANDED bucket (bodywork/therapy/nutrition/
//     naturopathy/coaching + broadened fitness) for the rest, marked
//     niche_source='query_inference'. The original 8 buckets never re-match here
//     (those rows were already filled or genuinely don't match), so this only
//     classifies the newly-added health/fitness buckets.
//
// Background (manager only), id-windowed + resumable, version-gated → no-op after
// a clean pass.
func BackfillNicheTaxonomyV2(ctx context.Context, db *sql.DB) {
	var done bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, nicheTaxonomyV2Version,
	).Scan(&done); err != nil || done {
		if err != nil {
			slog.Warn("db: niche taxonomy v2 version check failed", "error", err)
		}
		return
	}

	var minID, maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(id), MAX(id) FROM business_listings`).Scan(&minID, &maxID); err != nil || !maxID.Valid {
		slog.Warn("db: niche taxonomy v2: id range failed", "error", err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		slog.Warn("db: niche taxonomy v2: acquire conn failed", "error", err)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = '60s'`); err != nil {
		slog.Warn("db: niche taxonomy v2: set timeout failed", "error", err)
		return
	}

	beautyStmt := fmt.Sprintf(`
		UPDATE business_listings bl
		SET off_niche = TRUE, updated_at = NOW()
		FROM queries q
		WHERE bl.id > $1 AND bl.id <= $2
		  AND bl.niche_category IS NULL AND bl.off_niche IS NOT TRUE
		  AND bl.source_query_id = q.id
		  AND LOWER(q.text) ~ '%s'
	`, beautyOffNichePattern)

	nicheCase := buildNicheCaseSQL("LOWER(q.text)")
	nicheStmt := fmt.Sprintf(`
		UPDATE business_listings bl
		SET niche_category = %s, niche_source = 'query_inference', updated_at = NOW()
		FROM queries q
		WHERE bl.id > $1 AND bl.id <= $2
		  AND bl.niche_category IS NULL AND bl.off_niche IS NOT TRUE
		  AND bl.source_query_id = q.id
		  AND %s IS NOT NULL
	`, nicheCase, nicheCase)

	const window = int64(50000)
	slog.Info("db: niche taxonomy v2 starting (background, id-windowed)",
		"min_id", minID.Int64, "max_id", maxID.Int64, "window", window)
	var beautied, bucketed int64
	var failedWindows int
	for lo := minID.Int64 - 1; lo < maxID.Int64; lo += window {
		nb, err := execWindowRetry(ctx, conn, beautyStmt, lo, lo+window)
		if err != nil {
			failedWindows++
			slog.Warn("db: niche taxonomy v2 beauty window failed after retries — continuing", "lo", lo, "error", err)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		nn, err := execWindowRetry(ctx, conn, nicheStmt, lo, lo+window)
		if err != nil {
			failedWindows++
			slog.Warn("db: niche taxonomy v2 bucket window failed after retries — continuing", "lo", lo, "error", err)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		beautied += nb
		bucketed += nn
		select {
		case <-ctx.Done():
			slog.Info("db: niche taxonomy v2 interrupted (resumes next boot)", "off_niche_so_far", beautied, "bucketed_so_far", bucketed)
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if failedWindows > 0 {
		slog.Warn("db: niche taxonomy v2 pass partial — version NOT recorded, re-walks next boot",
			"failed_windows", failedWindows, "off_niche_this_pass", beautied, "bucketed_this_pass", bucketed)
		return
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
		nicheTaxonomyV2Version, fmt.Sprintf("broad-wellness taxonomy: off_niche %d beauty rows, bucket %d health/fitness rows via query lineage", beautied, bucketed),
	); err != nil {
		slog.Warn("db: niche taxonomy v2: version record failed", "error", err)
		return
	}
	slog.Info("db: niche taxonomy v2 complete", "off_niche_beauty", beautied, "bucketed", bucketed)
}

// -----------------------------------------------------------------------
// 2026-09-25 — off_niche keyword reclassification.
//
// Root cause (see .dev-squad/gotchas.md, "schema.org @type overrides keyword
// niche classification"): trg_normalize_enrichment used to force
// off_niche=TRUE unconditionally for a hard @type list (Physician,
// MedicalClinic, HealthAndBeautyBusiness, ...) and for content/page @types
// (Article, ContactPage, ...) — even when the SAME row's business_name /
// page_title / description carried unambiguous niche keyword evidence
// ("physiotherapy clinic", "Yoga Studio"). The extractor also picked the
// FIRST @type off a JSON-LD document instead of the most specific one, and
// never flattened @graph, so WordPress/Yoast sites lost their real business
// @type and JSON-LD-derived fields entirely. Both are fixed going forward
// (trg_normalize_enrichment + internal/scraper/contact.go); this is the
// one-time cleanup for rows written before the fix landed. off_niche was
// also sticky in the old ON CONFLICT (`OR`), so a wrong TRUE could never
// self-heal via re-enrichment either — also fixed in the trigger.
// -----------------------------------------------------------------------

// reclassifyLowerTextExpr is the LOWER()'d business_name/page_title/
// description union used by BOTH the WHERE filter and the SET niche_category
// expression in ReclassifyOffNicheByKeyword — a single constant so the two
// can never drift out of sync with each other.
const reclassifyLowerTextExpr = `LOWER(COALESCE(business_name,'') || ' ' || COALESCE(page_title,'') || ' ' || COALESCE(description,''))`

// buildReclassifyPredicateSQL renders the shared WHERE predicate: an
// off_niche=TRUE row with no beauty/grooming evidence (that exclusion is
// intentional and stays) AND at least one niche keyword bucket match. Reused
// by the count (dry-run) and UPDATE (write) SQL builders below and by the
// backup INSERT so all three windows agree on exactly the same row set.
func buildReclassifyPredicateSQL() string {
	nicheCase := buildNicheCaseSQL(reclassifyLowerTextExpr)
	return fmt.Sprintf(
		"off_niche = TRUE AND %s !~ '%s' AND %s IS NOT NULL",
		reclassifyLowerTextExpr, beautyOffNichePattern, nicheCase,
	)
}

// buildReclassifyCountSQL renders the id-windowed, parameterized COUNT used
// by the dry-run path. Pure string building — no DB required to test it.
func buildReclassifyCountSQL() string {
	return "SELECT COUNT(*) FROM business_listings WHERE id > $1 AND id <= $2 AND " + buildReclassifyPredicateSQL()
}

// buildReclassifyBackupInsertSQL renders the id-windowed INSERT that copies
// the pre-mutation state of every row about to change into the backup table
// created by ReclassifyOffNicheByKeyword, immediately before that window's
// UPDATE runs.
func buildReclassifyBackupInsertSQL() string {
	return fmt.Sprintf(`
		INSERT INTO business_listings_offniche_backup_20260925 (id, off_niche, niche_category, niche_source, category, updated_at)
		SELECT id, off_niche, niche_category, niche_source, category, updated_at
		FROM business_listings
		WHERE id > $1 AND id <= $2 AND %s
	`, buildReclassifyPredicateSQL())
}

// buildReclassifyUpdateSQL renders the id-windowed UPDATE for
// ReclassifyOffNicheByKeyword. off_niche flips to FALSE; niche_category is
// only set when it's currently empty or still carries a query-inferred
// (not page-extracted) value — a page-extracted bucket from a later
// re-enrich must never be clobbered by this one-time sweep. niche_source is
// cleared to mark the (now-set) bucket page-authoritative, mirroring the
// trigger's own ON CONFLICT niche_category/niche_source precedence.
// Exposed standalone (not inlined into the caller) so its shape can be
// asserted in a unit test without a live Postgres.
func buildReclassifyUpdateSQL() string {
	nicheCase := buildNicheCaseSQL(reclassifyLowerTextExpr)
	return fmt.Sprintf(`
		UPDATE business_listings
		SET off_niche = FALSE,
		    niche_category = CASE
		                        WHEN niche_category IS NULL OR niche_source = 'query_inference'
		                          THEN %s
		                        ELSE niche_category
		                      END,
		    niche_source = CASE
		                      WHEN niche_category IS NULL OR niche_source = 'query_inference'
		                        THEN NULL
		                      ELSE niche_source
		                    END,
		    updated_at = NOW()
		WHERE id > $1 AND id <= $2 AND %s
	`, nicheCase, buildReclassifyPredicateSQL())
}

// nicheReclassifyDryRunEnabled reports whether ReclassifyOffNicheByKeyword is
// restricted to counting-only mode. Default is TRUE (dry-run, safe) when
// NICHE_RECLASSIFY_DRY_RUN is unset — INVERTED from COUNTRY_CLEANUP_DRY_RUN
// (which defaults OFF / cleanup-runs) because this migration flips off_niche
// FALSE (surfaces previously-hidden rows to consumers) rather than nulling
// suspect data, so the safer default is "count only until an operator
// explicitly opts in". Only the literal value "false" (case-insensitive)
// enables the write path.
func nicheReclassifyDryRunEnabled() bool {
	return strings.ToLower(strings.TrimSpace(os.Getenv("NICHE_RECLASSIFY_DRY_RUN"))) != "false"
}

// offNicheKeywordReclassifyVersion gates the one-time off_niche keyword
// reclassification.
const offNicheKeywordReclassifyVersion = "2026_09_25_offniche_keyword_reclassify"

// ReclassifyOffNicheByKeyword flips off_niche back to FALSE for existing rows
// that were wrongly excluded by a stale schema.org @type (Article,
// ContactPage, Hotel, Physician, ...) even though the page's own
// business_name/page_title/description carries clear niche keyword evidence.
// See the "2026-09-25 — off_niche keyword reclassification" block comment
// above for the root cause. Never touches beauty/grooming off_niche rows
// (intentionally sticky) and never touches rows with no keyword evidence —
// those may be genuinely off-niche, a human decision, not this migration's.
//
// Dry-run gated via nicheReclassifyDryRunEnabled (default ON): in dry-run,
// logs the bounded per-window count of rows that WOULD change and does NOT
// record the version, so it re-counts on every boot until
// NICHE_RECLASSIFY_DRY_RUN=false. On the write path: backup-first (per
// CLAUDE.md — never mutate without a reversible copy), id-windowed (50k,
// Invariant #2: bounded + statement_timeout, no unbounded COUNT(*)),
// resumable on partial failure (failed pass -> version not recorded ->
// retries next boot). Mirrors BackfillSchemaTypeDenylist / the 2026-05-22
// off-niche backfill's shape.
//
// Rollback (documented for ops):
//
//	UPDATE business_listings bl
//	SET off_niche = b.off_niche, niche_category = b.niche_category,
//	    niche_source = b.niche_source, updated_at = b.updated_at
//	FROM business_listings_offniche_backup_20260925 b
//	WHERE bl.id = b.id;
//	DELETE FROM schema_migrations WHERE version = '2026_09_25_offniche_keyword_reclassify';
func ReclassifyOffNicheByKeyword(ctx context.Context, db *sql.DB) {
	var done bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, offNicheKeywordReclassifyVersion,
	).Scan(&done); err != nil || done {
		if err != nil {
			slog.Warn("db: off-niche keyword reclassify version check failed", "error", err)
		}
		return
	}

	var minID, maxID sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(id), MAX(id) FROM business_listings`).Scan(&minID, &maxID); err != nil || !maxID.Valid {
		slog.Warn("db: off-niche keyword reclassify: id range failed", "error", err)
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		slog.Warn("db: off-niche keyword reclassify: acquire conn failed", "error", err)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = '60s'`); err != nil {
		slog.Warn("db: off-niche keyword reclassify: set timeout failed", "error", err)
		return
	}

	const window = int64(50000)

	if nicheReclassifyDryRunEnabled() {
		slog.Info("db: off-niche keyword reclassify DRY-RUN mode — counting affected rows only, no mutation applied",
			"min_id", minID.Int64, "max_id", maxID.Int64)
		countSQL := buildReclassifyCountSQL()
		var total int64
		for lo := minID.Int64 - 1; lo < maxID.Int64; lo += window {
			var n int64
			if err := conn.QueryRowContext(ctx, countSQL, lo, lo+window).Scan(&n); err != nil {
				slog.Warn("db: off-niche keyword reclassify dry-run count window failed — continuing", "lo", lo, "error", err)
				if ctx.Err() != nil {
					return
				}
				continue
			}
			total += n
			select {
			case <-ctx.Done():
				slog.Info("db: off-niche keyword reclassify dry-run interrupted", "would_change_so_far", total)
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		slog.Info("db: off-niche keyword reclassify dry-run complete — rows that would flip off_niche TRUE->FALSE (set NICHE_RECLASSIFY_DRY_RUN=false to apply)",
			"would_change", total)
		return
	}

	// BACKUP table — created empty here (IF NOT EXISTS, so a retried partial
	// pass never errors); rows are copied in per-window immediately before
	// that window's UPDATE below.
	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS business_listings_offniche_backup_20260925 AS
		SELECT id, off_niche, niche_category, niche_source, category, updated_at
		FROM business_listings WHERE false
	`); err != nil {
		slog.Warn("db: off-niche keyword reclassify backup table create failed — aborting for safety", "error", err)
		return
	}

	backupSQL := buildReclassifyBackupInsertSQL()
	updateSQL := buildReclassifyUpdateSQL()

	slog.Info("db: off-niche keyword reclassify starting (background, id-windowed, write mode)",
		"min_id", minID.Int64, "max_id", maxID.Int64, "window", window)
	var total int64
	var failedWindows int
	for lo := minID.Int64 - 1; lo < maxID.Int64; lo += window {
		if _, err := execWindowRetry(ctx, conn, backupSQL, lo, lo+window); err != nil {
			failedWindows++
			slog.Warn("db: off-niche keyword reclassify backup window failed — continuing", "lo", lo, "error", err)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		n, err := execWindowRetry(ctx, conn, updateSQL, lo, lo+window)
		if err != nil {
			failedWindows++
			slog.Warn("db: off-niche keyword reclassify update window failed after retries — continuing", "lo", lo, "error", err, "changed_so_far", total)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		total += n
		select {
		case <-ctx.Done():
			slog.Info("db: off-niche keyword reclassify interrupted (resumes next boot)", "changed_so_far", total)
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if failedWindows > 0 {
		slog.Warn("db: off-niche keyword reclassify pass partial — version NOT recorded, re-walks next boot",
			"failed_windows", failedWindows, "changed_this_pass", total)
		return
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
		offNicheKeywordReclassifyVersion,
		fmt.Sprintf("flip off_niche TRUE->FALSE for %d rows with page keyword evidence a stale schema.org @type wrongly excluded", total),
	); err != nil {
		slog.Warn("db: off-niche keyword reclassify: version record failed", "error", err)
		return
	}
	slog.Info("db: off-niche keyword reclassify complete", "changed", total)
}
