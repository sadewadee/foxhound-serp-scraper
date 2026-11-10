// Command repair-emails fixes rows in the emails table whose address got page
// text glued on during extraction (phone number before the local part, website
// text after the TLD), using the shared internal/emailfix.Repair.
//
// Modes:
//
//	-mode scan   (default) read-only; writes id,old_email,new_email,action,target_id,rules
//	             to -out and logs per-rule counts plus 20 random samples per rule
//	-mode apply  scan + repair in place, one transaction per 50k-id window
//	-mode map    -in <file> -out <file>: old_email,new_email,action for a list of
//	             emails, no database (used to build the CRM correction list); only the
//	             context-free rules (phone, tld) apply here
//	-mode revert -in <file> -backup YYYYMMDD [-commit] [-out <csv>]: puts back the rows
//	             of one apply run (ids from -in, plus rows that merged into them). Dry run
//	             unless -commit; the whole revert is one transaction. See revert.go
//
// Actions: split (a field holding several addresses becomes one row per valid
// address; the original row is marked malformed), update (fixed address is new, row is rewritten and re-queued for
// validation), merge (fixed address already exists, links move to that row and
// the broken row is marked malformed), reject (not recoverable, row is marked
// malformed). Rules: phone, tld (context-free); label, prefix, middomain only fire
// when the fixed address exists on its own or the domain is the listing's own. Rows are never deleted. Affected rows and their business_emails
// links are copied to emails_backup_YYYYMMDD / business_emails_backup_YYYYMMDD
// first. Safe to re-run; resume a stopped run with -from-id.
//
// Usage:
//
//	POSTGRES_DSN=... ./repair-emails -mode scan -out fixes.csv
//	POSTGRES_DSN=... ./repair-emails -mode apply -out applied.csv [-from-id N]
//	./repair-emails -mode map -in emails.txt -out corrections.csv
//	POSTGRES_DSN=... ./repair-emails -mode revert -in ids.txt -backup 20261110 -out revert.csv
//	POSTGRES_DSN=... ./repair-emails -mode revert -in ids.txt -backup 20261110 -commit
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/emailfix"
)

const windowSize = 50000

const (
	actUpdate = "update"
	actMerge  = "merge"
	actReject = "reject"
	actSplit  = "split"
)

type fix struct {
	id       int64
	old, new string
	action   string
	rules    []string
	target   int64    // merge only
	pieces   []string // split only: the addresses the row is replaced with
}

type emailRow struct {
	id    int64
	email string
}

// classify returns the repaired address and action for an email, or "" when the
// address needs no change. Addresses Repair rejects without having changed them
// are left alone unless their TLD is invalid (glue with no valid TLD prefix);
// otherwise odd-but-real addresses such as a single-char local part would be
// marked malformed.
func classify(email string, c emailfix.Context) (fixed, action string, rules []string) {
	// Several addresses in one field ("a@x.de; b@x.de") are split into rows of
	// their own. "@@" is never split. Anything with no valid piece falls through
	// to the rejection below.
	if strings.Count(email, "@") > 1 && !strings.Contains(email, "@@") {
		if pieces := emailfix.CleanList([]string{email}); len(pieces) > 0 {
			return strings.Join(pieces, ";"), actSplit, []string{"split"}
		}
	}
	fixed, rules, ok := emailfix.RepairCtx(email, c)
	changed := len(rules) > 0
	switch {
	case ok && !changed:
		return "", "", nil
	case ok:
		return fixed, actUpdate, rules
	case changed || !validTLD(fixed):
		return fixed, actReject, rules
	}
	return "", "", nil
}

// splitPieces returns the addresses of a split fix, or nil for other actions.
func splitPieces(action, fixed string) []string {
	if action != actSplit {
		return nil
	}
	return strings.Split(fixed, ";")
}

func validTLD(email string) bool {
	dot := strings.LastIndex(email, ".")
	if dot < 0 || dot < strings.LastIndex(email, "@") {
		return false
	}
	return emailfix.IsTLD(email[dot+1:])
}

func main() {
	mode := flag.String("mode", "scan", "scan | apply | map | revert")
	out := flag.String("out", "", "output CSV path (required for scan and map)")
	in := flag.String("in", "", "map and revert mode: input file, one id or email per line, or first CSV column")
	fromID := flag.Int64("from-id", 0, "resume: start scanning at this emails.id")
	backup := flag.String("backup", "", "revert mode: YYYYMMDD suffix of the apply run to revert")
	commit := flag.Bool("commit", false, "revert mode: commit; without it the revert is a dry run that rolls back")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	var err error
	switch *mode {
	case "map":
		err = runMap(*in, *out)
	case "revert":
		err = runRevert(*in, *backup, *commit, *out)
	case "scan", "apply":
		if *mode == "scan" && *out == "" {
			err = fmt.Errorf("-out is required for scan")
			break
		}
		err = runDB(*mode == "apply", *out, *fromID)
	default:
		err = fmt.Errorf("unknown -mode %q", *mode)
	}
	if err != nil {
		slog.Error("repair-emails failed", "error", err)
		os.Exit(1)
	}
}

func openCSV(path string, header []string) (*csv.Writer, *os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, nil, err
	}
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, nil, err
	}
	return w, f, nil
}

func runMap(in, out string) error {
	if in == "" || out == "" {
		return fmt.Errorf("-in and -out are required for map")
	}
	src, err := os.Open(in)
	if err != nil {
		return err
	}
	defer src.Close()
	w, f, err := openCSV(out, []string{"old_email", "new_email", "action"})
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	counts := map[string]int{}
	for first := true; sc.Scan(); first = false {
		e := strings.TrimSpace(strings.SplitN(sc.Text(), ",", 2)[0])
		e = strings.Trim(e, `"`)
		if e == "" || (first && !strings.Contains(e, "@")) { // blank line or header
			continue
		}
		if fixed, action, _ := classify(e, emailfix.Context{}); action != "" {
			counts[action]++
			if err := w.Write([]string{e, fixed, action}); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	w.Flush()
	slog.Info("map done", "update", counts[actUpdate], "split", counts[actSplit], "reject", counts[actReject])
	return w.Error()
}

func runDB(apply bool, out string, fromID int64) error {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	var w *csv.Writer
	if out != "" {
		var f *os.File
		if w, f, err = openCSV(out, []string{"id", "old_email", "new_email", "action", "target_id", "rules"}); err != nil {
			return err
		}
		defer f.Close()
	}

	suffix := time.Now().UTC().Format("20060102")
	if apply {
		if err := createBackups(ctx, db, suffix); err != nil {
			return err
		}
	}

	var maxID int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM emails`).Scan(&maxID); err != nil {
		return err
	}
	slog.Info("starting", "mode", map[bool]string{true: "apply", false: "scan"}[apply], "from_id", fromID, "max_id", maxID)

	counts := map[string]int{}
	rc := newRuleStats()
	for lo := fromID; lo <= maxID; lo += windowSize {
		fixes, err := processWindow(ctx, db, lo, lo+windowSize, apply, suffix)
		if err != nil {
			return fmt.Errorf("window starting at id %d (resume with -from-id %d): %w", lo, lo, err)
		}
		for _, x := range fixes {
			counts[x.action]++
			rc.add(x)
			if w == nil {
				continue
			}
			target := ""
			if x.action == actMerge {
				target = strconv.FormatInt(x.target, 10)
			}
			if err := w.Write([]string{strconv.FormatInt(x.id, 10), x.old, x.new, x.action, target, strings.Join(x.rules, "+")}); err != nil {
				return err
			}
		}
		if w != nil {
			w.Flush()
		}
		slog.Info("window done", "from_id", lo, "found", len(fixes))
	}
	rc.log()
	slog.Info("done", "apply", apply, "update", counts[actUpdate], "merge", counts[actMerge], "split", counts[actSplit], "reject", counts[actReject])
	if w != nil {
		return w.Error()
	}
	return nil
}

// createBackups makes the dated backup tables with the original constraints so
// the INSERT ... ON CONFLICT DO NOTHING backups below are idempotent.
func createBackups(ctx context.Context, db *sql.DB, suffix string) error {
	for _, t := range []string{"emails", "business_emails"} {
		q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_backup_%s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)`, t, suffix, t)
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("create %s backup: %w", t, err)
		}
	}
	return nil
}

// processWindow scans emails with lo <= id < hi inside one transaction and, in
// apply mode, repairs what it found before committing.
func processWindow(ctx context.Context, db *sql.DB, lo, hi int64, apply bool, suffix string) ([]fix, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: !apply})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL statement_timeout = '30000'`); err != nil {
		return nil, err
	}

	// Rows already marked malformed by an earlier apply are skipped, which makes
	// re-runs find nothing to do.
	rows, err := tx.QueryContext(ctx, `
		SELECT id, email FROM emails
		WHERE id >= $1 AND id < $2 AND validation_status IS DISTINCT FROM 'malformed'
		ORDER BY id`, lo, hi)
	if err != nil {
		return nil, err
	}
	var batch []emailRow
	for rows.Next() {
		var r emailRow
		if err := rows.Scan(&r.id, &r.email); err != nil {
			rows.Close()
			return nil, err
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	fixes, err := classifyRows(ctx, tx, batch)
	if err != nil {
		return nil, err
	}
	if len(fixes) == 0 {
		return nil, nil
	}

	// Decide update vs merge: a fixed address that already exists (or that an
	// earlier row of this window is being rewritten to) is a merge.
	var cands []string
	for _, x := range fixes {
		if x.action == actUpdate {
			cands = append(cands, x.new)
		}
	}
	existing := map[string]int64{}
	if len(cands) > 0 {
		r, err := tx.QueryContext(ctx, `SELECT email, id FROM emails WHERE email = ANY($1)`, pq.Array(cands))
		if err != nil {
			return nil, err
		}
		for r.Next() {
			var e string
			var id int64
			if err := r.Scan(&e, &id); err != nil {
				r.Close()
				return nil, err
			}
			existing[e] = id
		}
		if err := r.Err(); err != nil {
			return nil, err
		}
		r.Close()
	}
	for i := range fixes {
		x := &fixes[i]
		if x.action != actUpdate {
			continue
		}
		if id, ok := existing[x.new]; ok {
			x.action, x.target = actMerge, id
		} else {
			existing[x.new] = x.id
		}
	}

	if !apply {
		return fixes, nil
	}
	if err := applyFixes(ctx, tx, fixes, suffix); err != nil {
		return nil, err
	}
	return fixes, tx.Commit()
}

// classifyRows runs the repair rules over a window. Pass 1 uses an empty context
// that records every address a rule would look up; only rows that reached such a
// lookup get a pass 2 with real database answers (existing addresses, and the
// listings each row is linked to).
func classifyRows(ctx context.Context, tx *sql.Tx, batch []emailRow) ([]fix, error) {
	wanted := map[string]bool{}
	var pending []emailRow
	var fixes []fix
	for _, r := range batch {
		touched := false
		look := func(e string) bool { touched, wanted[e] = true, true; return false }
		fixed, action, rules := classify(r.email, emailfix.Context{Exists: look, ExistsLinked: look})
		if touched {
			pending = append(pending, r)
		} else if action != "" {
			fixes = append(fixes, fix{id: r.id, old: r.email, new: fixed, action: action, rules: rules, pieces: splitPieces(action, fixed)})
		}
	}
	if len(pending) == 0 {
		return fixes, nil
	}

	ids := make([]int64, len(pending))
	for i, r := range pending {
		ids[i] = r.id
	}
	bizOf := map[int64]map[int64]bool{} // email id -> linked business ids
	domainsOf := map[int64][]string{}   // email id -> linked listing domains
	rs, err := tx.QueryContext(ctx, `
		SELECT be.email_id, be.business_id, COALESCE(bl.domain, '')
		FROM business_emails be JOIN business_listings bl ON bl.id = be.business_id
		WHERE be.email_id = ANY($1)`, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	for rs.Next() {
		var eid, bid int64
		var dom string
		if err := rs.Scan(&eid, &bid, &dom); err != nil {
			rs.Close()
			return nil, err
		}
		if bizOf[eid] == nil {
			bizOf[eid] = map[int64]bool{}
		}
		bizOf[eid][bid] = true
		domainsOf[eid] = append(domainsOf[eid], dom)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	rs.Close()

	cands := make([]string, 0, len(wanted))
	for e := range wanted {
		cands = append(cands, e)
	}
	exists := map[string]bool{}
	candBiz := map[string]map[int64]bool{}
	rs, err = tx.QueryContext(ctx, `
		SELECT e.email, COALESCE(be.business_id, 0)
		FROM emails e LEFT JOIN business_emails be ON be.email_id = e.id
		WHERE e.email = ANY($1) AND e.validation_status IS DISTINCT FROM 'malformed'`, pq.Array(cands))
	if err != nil {
		return nil, err
	}
	for rs.Next() {
		var e string
		var bid int64
		if err := rs.Scan(&e, &bid); err != nil {
			rs.Close()
			return nil, err
		}
		exists[e] = true
		if candBiz[e] == nil {
			candBiz[e] = map[int64]bool{}
		}
		candBiz[e][bid] = true
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	rs.Close()

	for _, r := range pending {
		c := emailfix.Context{
			ListingDomains: domainsOf[r.id],
			Exists:         func(e string) bool { return exists[e] },
			ExistsLinked: func(e string) bool {
				for b := range candBiz[e] {
					if bizOf[r.id][b] {
						return true
					}
				}
				return false
			},
		}
		if fixed, action, rules := classify(r.email, c); action != "" {
			fixes = append(fixes, fix{id: r.id, old: r.email, new: fixed, action: action, rules: rules, pieces: splitPieces(action, fixed)})
		}
	}
	sort.Slice(fixes, func(i, j int) bool { return fixes[i].id < fixes[j].id })
	return fixes, nil
}

// ruleStats counts fixes per rule and keeps a uniform random sample of each.
type ruleStats struct {
	count   map[string]int
	samples map[string][]fix
}

const samplesPerRule = 20

func newRuleStats() *ruleStats {
	return &ruleStats{count: map[string]int{}, samples: map[string][]fix{}}
}

func (s *ruleStats) add(x fix) {
	for _, r := range x.rules {
		s.count[r]++
		if len(s.samples[r]) < samplesPerRule {
			s.samples[r] = append(s.samples[r], x)
		} else if j := rand.IntN(s.count[r]); j < samplesPerRule { // reservoir sampling
			s.samples[r][j] = x
		}
	}
}

func (s *ruleStats) log() {
	rules := make([]string, 0, len(s.count))
	for r := range s.count {
		rules = append(rules, r)
	}
	sort.Strings(rules)
	for _, r := range rules {
		slog.Info("rule", "rule", r, "count", s.count[r])
		for _, x := range s.samples[r] {
			slog.Info("sample", "rule", r, "id", x.id, "old", x.old, "new", x.new, "action", x.action, "target", x.target)
		}
	}
}

func applyFixes(ctx context.Context, tx *sql.Tx, fixes []fix, suffix string) error {
	var all, upID, mgID, mgTarget, rjID, spID []int64
	var upOld, upNew []string
	var spSrc []int64
	var spEmail []string
	for _, x := range fixes {
		all = append(all, x.id)
		switch x.action {
		case actSplit:
			spID = append(spID, x.id)
			for _, p := range x.pieces {
				spSrc, spEmail = append(spSrc, x.id), append(spEmail, p)
			}
		case actUpdate:
			upID, upOld, upNew = append(upID, x.id), append(upOld, x.old), append(upNew, x.new)
		case actMerge:
			mgID, mgTarget = append(mgID, x.id), append(mgTarget, x.target)
		case actReject:
			rjID = append(rjID, x.id)
		}
	}

	// (a) backups of every affected row and its links.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO emails_backup_%s SELECT * FROM emails WHERE id = ANY($1) ON CONFLICT DO NOTHING`, suffix), pq.Array(all)); err != nil {
		return fmt.Errorf("backup emails: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO business_emails_backup_%s SELECT * FROM business_emails WHERE email_id = ANY($1) ON CONFLICT DO NOTHING`, suffix), pq.Array(all)); err != nil {
		return fmt.Errorf("backup business_emails: %w", err)
	}

	// (b) rewrite the address and reset validation so the validator re-checks it
	// (it picks rows with validation_status = 'pending'). The old-email guard
	// keeps a re-run from touching a row that changed since the scan.
	if len(upID) > 0 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE emails e SET
				email = v.new_email,
				local_part = split_part(v.new_email, '@', 1),
				domain = split_part(v.new_email, '@', 2),
				validation_status = 'pending', mx_valid = NULL, deliverable = NULL,
				disposable = NULL, role_account = NULL, free_email = NULL, catch_all = NULL,
				reason = NULL, score = NULL, is_acceptable = NULL, validated_at = NULL
			FROM unnest($1::bigint[], $2::text[], $3::text[]) AS v(id, old_email, new_email)
			WHERE e.id = v.id AND e.email = v.old_email`,
			pq.Array(upID), pq.Array(upOld), pq.Array(upNew)); err != nil {
			return fmt.Errorf("update: %w", err)
		}
	}

	// (c) move links to the existing row, then mark the broken one malformed.
	if len(mgID) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO business_emails (business_id, email_id, source, created_at)
			SELECT be.business_id, v.target_id, be.source, be.created_at
			FROM business_emails be
			JOIN unnest($1::bigint[], $2::bigint[]) AS v(id, target_id) ON be.email_id = v.id
			ON CONFLICT DO NOTHING`, pq.Array(mgID), pq.Array(mgTarget)); err != nil {
			return fmt.Errorf("merge links: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM business_emails WHERE email_id = ANY($1)`, pq.Array(mgID)); err != nil {
			return fmt.Errorf("merge unlink: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE emails e SET is_acceptable = FALSE, validation_status = 'malformed',
				reason = 'merged_into:' || v.target_id
			FROM unnest($1::bigint[], $2::bigint[]) AS v(id, target_id)
			WHERE e.id = v.id`, pq.Array(mgID), pq.Array(mgTarget)); err != nil {
			return fmt.Errorf("merge mark: %w", err)
		}
	}

	// (c2) split: copy the links to each piece, unlink the broken row, mark it.
	if len(spID) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO emails (email, domain, local_part)
			SELECT DISTINCT v.email, split_part(v.email, '@', 2), split_part(v.email, '@', 1)
			FROM unnest($1::bigint[], $2::text[]) AS v(src_id, email)
			ON CONFLICT (email) DO NOTHING`, pq.Array(spSrc), pq.Array(spEmail)); err != nil {
			return fmt.Errorf("split insert: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO business_emails (business_id, email_id, source, created_at)
			SELECT be.business_id, e.id, be.source, be.created_at
			FROM unnest($1::bigint[], $2::text[]) AS v(src_id, email)
			JOIN emails e ON e.email = v.email
			JOIN business_emails be ON be.email_id = v.src_id
			ON CONFLICT DO NOTHING`, pq.Array(spSrc), pq.Array(spEmail)); err != nil {
			return fmt.Errorf("split links: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM business_emails WHERE email_id = ANY($1)`, pq.Array(spID)); err != nil {
			return fmt.Errorf("split unlink: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE emails SET is_acceptable = FALSE, validation_status = 'malformed', reason = 'split'
			WHERE id = ANY($1)`, pq.Array(spID)); err != nil {
			return fmt.Errorf("split mark: %w", err)
		}
	}

	// (d) unrecoverable.
	if len(rjID) > 0 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE emails SET is_acceptable = FALSE, validation_status = 'malformed',
				reason = 'unrecoverable_glue'
			WHERE id = ANY($1)`, pq.Array(rjID)); err != nil {
			return fmt.Errorf("reject: %w", err)
		}
	}
	return nil
}
