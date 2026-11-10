package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// Revert puts the rows of one apply run back to their pre-apply state, using the
// backup tables that run wrote (emails_backup_<suffix>, business_emails_backup_<suffix>).
//
// Algorithm, all in one transaction that is rolled back unless -commit is set:
//  1. every requested id must exist in the emails backup; a row that was split
//     is refused (split reverts are not supported).
//  2. expansion: a row that merged into a reverted row is reverted too, because
//     its links point at a row that is going back to its old address.
//  3. conflict check: a restored address must not be held by any other live row.
//     Any conflict aborts before a single write.
//  4. links moved by a merge are removed when they were written by this run
//     (xmin of the business_emails row is the xid of the run's merge writes).
//     A business that a kept merge into the same target still holds stays linked.
//  5. emails rows are restored in full from the backup, then the backed-up links
//     are re-inserted (ON CONFLICT DO NOTHING).

const (
	revertUpdate  = "revert_update"
	revertMerge   = "revert_merge"
	revertReject  = "revert_reject"
	revertRestore = "revert_restore"

	mergedPrefix = "merged_into:"
)

var backupSuffixRe = regexp.MustCompile(`^\d{8}$`)

type revertRow struct {
	id                          int64
	action                      string
	currentEmail, restoredEmail string
	target                      int64 // revertMerge only: the row the links were moved to
	expanded                    bool  // pulled in because a row it merged into is reverted
}

type revertStats struct {
	requested, expanded         int
	byAction                    map[string]int
	linksRestored, linksDeleted int64
	linksShared, linksLeft      int
	warnings                    []string
}

// revertBackup reverts the rows of the run with the given backup suffix. ids are
// the rows to put back. Rows are returned sorted by id.
func revertBackup(ctx context.Context, db *sql.DB, ids []int64, suffix string, commit bool) ([]revertRow, *revertStats, error) {
	if !backupSuffixRe.MatchString(suffix) {
		return nil, nil, fmt.Errorf("-backup must be a YYYYMMDD suffix, got %q", suffix)
	}
	if len(ids) == 0 {
		return nil, nil, fmt.Errorf("no ids to revert")
	}
	emailsBk, linksBk := "emails_backup_"+suffix, "business_emails_backup_"+suffix

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL statement_timeout = '300000'`); err != nil {
		return nil, nil, err
	}
	for _, t := range []string{emailsBk, linksBk} {
		var reg sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1)::text`, t).Scan(&reg); err != nil {
			return nil, nil, err
		}
		if !reg.Valid {
			return nil, nil, fmt.Errorf("backup table %s does not exist", t)
		}
	}

	// (1) every requested id must be in the backup.
	known, err := queryInt64s(ctx, tx, fmt.Sprintf(`SELECT id FROM %s WHERE id = ANY($1)`, emailsBk), pq.Array(ids))
	if err != nil {
		return nil, nil, err
	}
	have := map[int64]bool{}
	for _, id := range known {
		have[id] = true
	}
	var missing []string
	for _, id := range ids {
		if !have[id] {
			missing = append(missing, strconv.FormatInt(id, 10))
		}
	}
	if len(missing) > 0 {
		return nil, nil, fmt.Errorf("%d id(s) are not in %s, nothing changed: %s", len(missing), emailsBk, strings.Join(missing, ","))
	}

	// (2) expansion: rows currently merged into another row, from this backup.
	pool := map[int64]int64{} // merged row id -> target id
	prs, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT e.id, e.reason FROM emails e JOIN %s b ON b.id = e.id WHERE e.reason LIKE $1`, emailsBk), mergedPrefix+"%")
	if err != nil {
		return nil, nil, err
	}
	for prs.Next() {
		var id int64
		var reason string
		if err := prs.Scan(&id, &reason); err != nil {
			prs.Close()
			return nil, nil, err
		}
		if t, ok := mergeTarget(reason); ok {
			pool[id] = t
		}
	}
	if err := prs.Err(); err != nil {
		prs.Close()
		return nil, nil, err
	}
	prs.Close()

	inZ := map[int64]bool{}
	for _, id := range ids {
		inZ[id] = true
	}
	expanded := map[int64]bool{}
	for changed := true; changed; {
		changed = false
		for id, t := range pool {
			if !inZ[id] && inZ[t] {
				inZ[id], expanded[id], changed = true, true, true
			}
		}
	}
	zs := sortedIDs(inZ)

	// Current and backup state of every row to revert.
	type state struct{ curEmail, curReason, bkEmail string }
	states := map[int64]state{}
	srs, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT e.id, e.email, COALESCE(e.reason, ''), b.email
		FROM emails e JOIN %s b ON b.id = e.id
		WHERE e.id = ANY($1)`, emailsBk), pq.Array(zs))
	if err != nil {
		return nil, nil, err
	}
	for srs.Next() {
		var id int64
		var st state
		if err := srs.Scan(&id, &st.curEmail, &st.curReason, &st.bkEmail); err != nil {
			srs.Close()
			return nil, nil, err
		}
		states[id] = st
	}
	if err := srs.Err(); err != nil {
		srs.Close()
		return nil, nil, err
	}
	srs.Close()
	if len(states) != len(zs) {
		return nil, nil, fmt.Errorf("%d row(s) to revert no longer exist in emails, nothing changed", len(zs)-len(states))
	}

	stats := &revertStats{requested: len(ids), byAction: map[string]int{}}
	out := make([]revertRow, 0, len(zs))
	for _, id := range zs {
		st := states[id]
		if st.curReason == "split" {
			return nil, nil, fmt.Errorf("row %d was split by the apply run; split reverts are not supported, remove it from the input", id)
		}
		r := revertRow{id: id, currentEmail: st.curEmail, restoredEmail: st.bkEmail, expanded: expanded[id]}
		switch t, merged := mergeTarget(st.curReason); {
		case merged:
			r.action, r.target = revertMerge, t
		case st.curEmail != st.bkEmail:
			r.action = revertUpdate
		case st.curReason == "unrecoverable_glue":
			r.action = revertReject
		default:
			r.action = revertRestore
		}
		out = append(out, r)
		stats.byAction[r.action]++
		if r.expanded {
			stats.expanded++
		}
	}
	if stats.expanded > 0 {
		stats.warnings = append(stats.warnings, fmt.Sprintf("%d row(s) pulled in because they merged into a reverted row", stats.expanded))
	}

	// Chains are refused. A row that is both merged and a merge target (or a merged
	// row whose target is merged) received links from a merge that this backup may
	// have captured, and those links cannot be told apart from pre-apply links.
	// Reverting the chain's links exactly would need xmin evidence the backup does
	// not keep, so nothing is changed.
	targets := map[int64]bool{}
	for _, t := range pool {
		targets[t] = true
	}
	inChain := func(id int64) bool {
		_, merged := pool[id]
		return merged && targets[id]
	}
	for _, r := range out {
		if inChain(r.id) || (r.action == revertMerge && inChain(r.target)) {
			return nil, nil, fmt.Errorf("row %d is part of a merge chain; this revert refuses chains, nothing changed", r.id)
		}
	}

	// (3) conflict check: no other live row may hold a restored address.
	owners := map[string][]int64{}
	for _, r := range out {
		owners[r.restoredEmail] = append(owners[r.restoredEmail], r.id)
	}
	var conflicts []string
	emailList := make([]string, 0, len(owners))
	for e, rowIDs := range owners {
		emailList = append(emailList, e)
		if len(rowIDs) > 1 {
			conflicts = append(conflicts, fmt.Sprintf("%s is the restored address of rows %v", e, rowIDs))
		}
	}
	hrs, err := tx.QueryContext(ctx, `SELECT id, email FROM emails WHERE email = ANY($1)`, pq.Array(emailList))
	if err != nil {
		return nil, nil, err
	}
	for hrs.Next() {
		var hid int64
		var e string
		if err := hrs.Scan(&hid, &e); err != nil {
			hrs.Close()
			return nil, nil, err
		}
		for _, rid := range owners[e] {
			if rid != hid {
				conflicts = append(conflicts, fmt.Sprintf("row %d holds %s, so row %d cannot be restored to it", hid, e, rid))
			}
		}
	}
	if err := hrs.Err(); err != nil {
		hrs.Close()
		return nil, nil, err
	}
	hrs.Close()
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return nil, nil, fmt.Errorf("revert would clash with live rows, nothing changed: %s", strings.Join(conflicts, "; "))
	}

	// (4) links. Business links of the rows to revert, and of kept rows merged
	// into the same targets (those links must survive).
	need := append([]int64(nil), zs...)
	for k := range pool {
		if !inZ[k] {
			need = append(need, k)
		}
	}
	bizOf := map[int64][]int64{}
	lrows, err := queryInts(ctx, tx, 2, fmt.Sprintf(`SELECT email_id, business_id FROM %s WHERE email_id = ANY($1)`, linksBk), pq.Array(need))
	if err != nil {
		return nil, nil, err
	}
	for _, r := range lrows {
		bizOf[r[0]] = append(bizOf[r[0]], r[1])
	}
	keep := map[int64]map[int64]bool{} // target -> businesses still held by kept merges
	for k, t := range pool {
		if inZ[k] {
			continue
		}
		if keep[t] == nil {
			keep[t] = map[int64]bool{}
		}
		for _, b := range bizOf[k] {
			keep[t][b] = true
		}
	}
	type linkPair struct{ email, business int64 }
	seen := map[linkPair]bool{}
	var cands []linkPair
	for _, r := range out {
		if r.action != revertMerge {
			continue
		}
		for _, b := range bizOf[r.id] {
			p := linkPair{r.target, b}
			switch {
			case keep[r.target][b]:
				stats.linksShared++
			case !seen[p]:
				seen[p] = true
				cands = append(cands, p)
			}
		}
	}

	// xids of the apply windows that merged rows. A link written by a merge
	// carries the xid of the window that wrote it.
	poolIDs := make([]int64, 0, len(pool))
	for k := range pool {
		poolIDs = append(poolIDs, k)
	}
	var xids []int64
	xidSet := map[int64]bool{}
	if len(poolIDs) > 0 {
		xids, err = queryInt64s(ctx, tx, `SELECT DISTINCT xmin::text::bigint FROM emails WHERE id = ANY($1)`, pq.Array(poolIDs))
		if err != nil {
			return nil, nil, err
		}
		for _, x := range xids {
			xidSet[x] = true
		}
	}

	var del []linkPair
	if len(cands) > 0 {
		var cEmail, cBiz []int64
		for _, p := range cands {
			cEmail, cBiz = append(cEmail, p.email), append(cBiz, p.business)
		}
		prs2, err := queryInts(ctx, tx, 3, `
			SELECT d.email_id, d.business_id, be.xmin::text::bigint
			FROM unnest($1::bigint[], $2::bigint[]) AS d(email_id, business_id)
			JOIN business_emails be ON be.email_id = d.email_id AND be.business_id = d.business_id`,
			pq.Array(cEmail), pq.Array(cBiz))
		if err != nil {
			return nil, nil, err
		}
		for _, r := range prs2 {
			p := linkPair{r[0], r[1]}
			if xidSet[r[2]] {
				del = append(del, p)
			} else {
				stats.linksLeft++
				stats.warnings = append(stats.warnings, fmt.Sprintf("link business %d -> email %d was not written by this run, left in place", r[1], r[0]))
			}
		}
	}
	if len(del) > 0 {
		var dEmail, dBiz []int64
		for _, p := range del {
			dEmail, dBiz = append(dEmail, p.email), append(dBiz, p.business)
		}
		res, err := tx.ExecContext(ctx, `
			DELETE FROM business_emails be
			USING unnest($1::bigint[], $2::bigint[]) AS d(email_id, business_id)
			WHERE be.email_id = d.email_id AND be.business_id = d.business_id
			  AND be.xmin::text::bigint = ANY($3::bigint[])`,
			pq.Array(dEmail), pq.Array(dBiz), pq.Array(xids))
		if err != nil {
			return nil, nil, fmt.Errorf("merge unlink: %w", err)
		}
		stats.linksDeleted, _ = res.RowsAffected()
	}

	// (5) restore the emails rows from the backup, then their links.
	cols, err := queryStrings(ctx, tx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'emails' AND column_name <> 'id'
		ORDER BY ordinal_position`)
	if err != nil {
		return nil, nil, err
	}
	var lhs, rhs []string
	for _, c := range cols {
		q := pq.QuoteIdentifier(c)
		lhs, rhs = append(lhs, q), append(rhs, "b."+q)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE emails e SET (%s) = (SELECT %s FROM %s b WHERE b.id = e.id)
		WHERE e.id = ANY($1)`, strings.Join(lhs, ", "), strings.Join(rhs, ", "), emailsBk), pq.Array(zs)); err != nil {
		return nil, nil, fmt.Errorf("restore emails: %w", err)
	}
	res, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO business_emails (business_id, email_id, source, created_at)
		SELECT business_id, email_id, source, created_at FROM %s
		WHERE email_id = ANY($1)
		ON CONFLICT DO NOTHING`, linksBk), pq.Array(zs))
	if err != nil {
		return nil, nil, fmt.Errorf("restore links: %w", err)
	}
	stats.linksRestored, _ = res.RowsAffected()

	if commit {
		if err := tx.Commit(); err != nil {
			return nil, nil, err
		}
	}
	return out, stats, nil
}

func mergeTarget(reason string) (int64, bool) {
	if !strings.HasPrefix(reason, mergedPrefix) {
		return 0, false
	}
	t, err := strconv.ParseInt(strings.TrimPrefix(reason, mergedPrefix), 10, 64)
	return t, err == nil
}

func sortedIDs(m map[int64]bool) []int64 {
	out := make([]int64, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// queryInts returns the rows of q as slices of n int64 columns.
func queryInts(ctx context.Context, tx *sql.Tx, n int, q string, args ...any) ([][]int64, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]int64
	for rows.Next() {
		vals := make([]int64, n)
		ptrs := make([]any, n)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

func queryInt64s(ctx context.Context, tx *sql.Tx, q string, args ...any) ([]int64, error) {
	rows, err := queryInts(ctx, tx, 1, q, args...)
	if err != nil {
		return nil, err
	}
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r[0]
	}
	return out, nil
}

func queryStrings(ctx context.Context, tx *sql.Tx, q string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// readIDs reads row ids, one per line or in the first CSV column. A non-numeric
// first line is taken as a header.
func readIDs(path string) ([]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	seen := map[int64]bool{}
	var ids []int64
	line := 0
	for sc.Scan() {
		line++
		s := strings.Trim(strings.TrimSpace(strings.SplitN(sc.Text(), ",", 2)[0]), `"`)
		if s == "" {
			continue
		}
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			if line == 1 {
				continue
			}
			return nil, fmt.Errorf("line %d: %q is not an id", line, s)
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids, sc.Err()
}

func runRevert(in, suffix string, commit bool, out string) error {
	if in == "" || suffix == "" {
		return fmt.Errorf("-in and -backup are required for revert")
	}
	ids, err := readIDs(in)
	if err != nil {
		return err
	}
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

	rows, st, err := revertBackup(context.Background(), db, ids, suffix, commit)
	if err != nil {
		return err
	}
	if out != "" {
		w, f, err := openCSV(out, []string{"id", "action", "current_email", "restored_email", "target_id", "expanded"})
		if err != nil {
			return err
		}
		defer f.Close()
		for _, r := range rows {
			target := ""
			if r.action == revertMerge {
				target = strconv.FormatInt(r.target, 10)
			}
			if err := w.Write([]string{strconv.FormatInt(r.id, 10), r.action, r.currentEmail, r.restoredEmail, target, strconv.FormatBool(r.expanded)}); err != nil {
				return err
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	}

	for _, w := range st.warnings {
		slog.Warn("revert", "warning", w)
	}
	slog.Info("revert done",
		"commit", commit, "requested", st.requested, "rows", len(rows), "expanded", st.expanded,
		"update", st.byAction[revertUpdate], "merge", st.byAction[revertMerge],
		"reject", st.byAction[revertReject], "restore", st.byAction[revertRestore],
		"links_restored", st.linksRestored, "links_deleted", st.linksDeleted,
		"links_shared_kept", st.linksShared, "links_left", st.linksLeft)
	if !commit {
		slog.Info("dry run: rolled back, nothing changed; rerun with -commit to apply")
	}
	return nil
}
