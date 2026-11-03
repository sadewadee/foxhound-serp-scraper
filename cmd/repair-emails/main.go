// Command repair-emails fixes rows in the emails table whose address got page
// text glued on during extraction (phone number before the local part, website
// text after the TLD), using the shared internal/emailfix.Repair.
//
// Modes:
//
//	-mode scan   (default) read-only; writes id,old_email,new_email,action,target_id to -out
//	-mode apply  scan + repair in place, one transaction per 50k-id window
//	-mode map    -in <file> -out <file>: old_email,new_email,action for a list of
//	             emails, no database (used to build the CRM correction list)
//
// Actions: update (fixed address is new, row is rewritten and re-queued for
// validation), merge (fixed address already exists, links move to that row and
// the broken row is marked malformed), reject (not recoverable, row is marked
// malformed). Rows are never deleted. Affected rows and their business_emails
// links are copied to emails_backup_YYYYMMDD / business_emails_backup_YYYYMMDD
// first. Safe to re-run; resume a stopped run with -from-id.
//
// Usage:
//
//	POSTGRES_DSN=... ./repair-emails -mode scan -out fixes.csv
//	POSTGRES_DSN=... ./repair-emails -mode apply -out applied.csv [-from-id N]
//	./repair-emails -mode map -in emails.txt -out corrections.csv
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"golang.org/x/net/publicsuffix"

	"github.com/sadewadee/serp-scraper/internal/emailfix"
)

const windowSize = 50000

const (
	actUpdate = "update"
	actMerge  = "merge"
	actReject = "reject"
)

type fix struct {
	id       int64
	old, new string
	action   string
	target   int64 // merge only
}

// classify returns the repaired address and action for an email, or "" when the
// address needs no change. Addresses Repair rejects without having changed them
// are left alone unless their TLD is invalid (glue with no valid TLD prefix);
// otherwise odd-but-real addresses such as a single-char local part would be
// marked malformed.
func classify(email string) (fixed, action string) {
	fixed, changed, ok := emailfix.Repair(email)
	switch {
	case ok && !changed:
		return "", ""
	case ok:
		return fixed, actUpdate
	case changed || !validTLD(fixed):
		return fixed, actReject
	}
	return "", ""
}

func validTLD(email string) bool {
	dot := strings.LastIndex(email, ".")
	if dot < 0 || dot < strings.LastIndex(email, "@") {
		return false
	}
	_, icann := publicsuffix.PublicSuffix(email[dot+1:])
	return icann
}

func main() {
	mode := flag.String("mode", "scan", "scan | apply | map")
	out := flag.String("out", "", "output CSV path (required for scan and map)")
	in := flag.String("in", "", "map mode: input file, one email per line or first CSV column")
	fromID := flag.Int64("from-id", 0, "resume: start scanning at this emails.id")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	var err error
	switch *mode {
	case "map":
		err = runMap(*in, *out)
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
		if fixed, action := classify(e); action != "" {
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
	slog.Info("map done", "update", counts[actUpdate], "reject", counts[actReject])
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
		if w, f, err = openCSV(out, []string{"id", "old_email", "new_email", "action", "target_id"}); err != nil {
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
	for lo := fromID; lo <= maxID; lo += windowSize {
		fixes, err := processWindow(ctx, db, lo, lo+windowSize, apply, suffix)
		if err != nil {
			return fmt.Errorf("window starting at id %d (resume with -from-id %d): %w", lo, lo, err)
		}
		for _, x := range fixes {
			counts[x.action]++
			if w == nil {
				continue
			}
			target := ""
			if x.action == actMerge {
				target = strconv.FormatInt(x.target, 10)
			}
			if err := w.Write([]string{strconv.FormatInt(x.id, 10), x.old, x.new, x.action, target}); err != nil {
				return err
			}
		}
		if w != nil {
			w.Flush()
		}
		slog.Info("window done", "from_id", lo, "found", len(fixes))
	}
	slog.Info("done", "apply", apply, "update", counts[actUpdate], "merge", counts[actMerge], "reject", counts[actReject])
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
	tx, err := db.BeginTx(ctx, nil)
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
	var fixes []fix
	for rows.Next() {
		var x fix
		if err := rows.Scan(&x.id, &x.old); err != nil {
			rows.Close()
			return nil, err
		}
		if x.new, x.action = classify(x.old); x.action != "" {
			fixes = append(fixes, x)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
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

func applyFixes(ctx context.Context, tx *sql.Tx, fixes []fix, suffix string) error {
	var all, upID, mgID, mgTarget, rjID []int64
	var upOld, upNew []string
	for _, x := range fixes {
		all = append(all, x.id)
		switch x.action {
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
