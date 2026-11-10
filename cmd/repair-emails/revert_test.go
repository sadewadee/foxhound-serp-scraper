package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	appdb "github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/testpg"
)

// Scenarios run against a real Postgres with the production schema. Each one
// applies a repair with processWindow(apply=true) and then reverts it.

const (
	revertSuffix = "20991231"
	revertHiAll  = int64(1) << 40
	revertGlued  = "9346082info@acme-yoga.de" // repairs to info@acme-yoga.de
	revertGlued2 = "9346083info@acme-yoga.de" // repairs to info@acme-yoga.de
	revertClean  = "info@acme-yoga.de"
)

func newRevertDB(t *testing.T) *sql.DB {
	t.Helper()
	db := testpg.Start(t, testpg.StartOpts{Prefix: "revert"}).DB()
	if err := appdb.Migrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := createBackups(context.Background(), db, revertSuffix); err != nil {
		t.Fatalf("createBackups: %v", err)
	}
	return db
}

func seedEmail(t *testing.T, db *sql.DB, email string) int64 {
	t.Helper()
	at := strings.LastIndex(email, "@")
	var id int64
	if err := db.QueryRow(`INSERT INTO emails (email, domain, local_part) VALUES ($1, $2, $3) RETURNING id`,
		email, email[at+1:], email[:at]).Scan(&id); err != nil {
		t.Fatalf("seed email %s: %v", email, err)
	}
	return id
}

func seedBiz(t *testing.T, db *sql.DB, domain string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO business_listings (domain, url) VALUES ($1, $2) RETURNING id`,
		domain, "https://"+domain).Scan(&id); err != nil {
		t.Fatalf("seed business %s: %v", domain, err)
	}
	return id
}

func link(t *testing.T, db *sql.DB, biz, email int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO business_emails (business_id, email_id) VALUES ($1, $2)`, biz, email); err != nil {
		t.Fatalf("link: %v", err)
	}
}

func hasLink(t *testing.T, db *sql.DB, biz, email int64) bool {
	t.Helper()
	var ok bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM business_emails WHERE business_id = $1 AND email_id = $2)`,
		biz, email).Scan(&ok); err != nil {
		t.Fatalf("hasLink: %v", err)
	}
	return ok
}

// emailState returns the address, reason and validation status of a row.
func emailState(t *testing.T, db *sql.DB, id int64) (email, reason, status string) {
	t.Helper()
	if err := db.QueryRow(`SELECT email, COALESCE(reason, ''), COALESCE(validation_status, '') FROM emails WHERE id = $1`,
		id).Scan(&email, &reason, &status); err != nil {
		t.Fatalf("emailState %d: %v", id, err)
	}
	return
}

// applyAll repairs every row with one apply window and returns the fixes.
func applyAll(t *testing.T, db *sql.DB) []fix {
	t.Helper()
	fixes, err := processWindow(context.Background(), db, 1, revertHiAll, true, revertSuffix)
	if err != nil {
		t.Fatalf("processWindow: %v", err)
	}
	return fixes
}

func wantFix(t *testing.T, fixes []fix, id int64, action string) fix {
	t.Helper()
	for _, x := range fixes {
		if x.id == id {
			if x.action != action {
				t.Fatalf("row %d: action %s, want %s (fixes %+v)", id, x.action, action, fixes)
			}
			return x
		}
	}
	t.Fatalf("row %d has no fix (fixes %+v)", id, fixes)
	return fix{}
}

func revertIDs(db *sql.DB, ids []int64, commit bool) ([]revertRow, *revertStats, error) {
	return revertBackup(context.Background(), db, ids, revertSuffix, commit)
}

func wantRow(t *testing.T, rows []revertRow, id int64, action string, expanded bool) {
	t.Helper()
	for _, r := range rows {
		if r.id == id {
			if r.action != action || r.expanded != expanded {
				t.Fatalf("row %d: action %s expanded %v, want %s expanded %v", id, r.action, r.expanded, action, expanded)
			}
			return
		}
	}
	t.Fatalf("row %d missing from revert plan %+v", id, rows)
}

// (a) a merged row goes back: its links return, the target keeps its own
// links and loses the ones the merge wrote.
func TestRevertSimpleMerge(t *testing.T) {
	db := newRevertDB(t)
	target := seedEmail(t, db, revertClean)
	dup := seedEmail(t, db, revertGlued)
	a, b := seedBiz(t, db, "a.example"), seedBiz(t, db, "b.example")
	link(t, db, a, target)
	link(t, db, b, dup)

	fixes := applyAll(t, db)
	if x := wantFix(t, fixes, dup, actMerge); x.target != target {
		t.Fatalf("merge target %d, want %d", x.target, target)
	}
	if hasLink(t, db, b, dup) || !hasLink(t, db, b, target) {
		t.Fatal("apply should move b's link from the broken row to the target")
	}

	rows, st, err := revertIDs(db, []int64{dup}, true)
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	wantRow(t, rows, dup, revertMerge, false)
	if st.linksDeleted != 1 || st.linksRestored != 1 {
		t.Fatalf("links deleted %d restored %d, want 1 and 1", st.linksDeleted, st.linksRestored)
	}
	if email, reason, status := emailState(t, db, dup); email != revertGlued || reason != "" || status != "pending" {
		t.Fatalf("dup after revert: email %q reason %q status %q", email, reason, status)
	}
	if !hasLink(t, db, b, dup) {
		t.Fatal("dup lost its link")
	}
	if hasLink(t, db, b, target) {
		t.Fatal("merge link to target was not removed")
	}
	if !hasLink(t, db, a, target) {
		t.Fatal("target lost its own link")
	}
}

// (b) an update is reverted with the merge that depends on it: u was rewritten
// to the clean address and x merged into it in the same window. Reverting u
// pulls x in.
func TestRevertUpdateExpandsMerged(t *testing.T) {
	db := newRevertDB(t)
	u := seedEmail(t, db, revertGlued)
	x := seedEmail(t, db, revertGlued2)
	uBiz, xBiz := seedBiz(t, db, "u.example"), seedBiz(t, db, "x.example")
	link(t, db, uBiz, u)
	link(t, db, xBiz, x)

	fixes := applyAll(t, db)
	wantFix(t, fixes, u, actUpdate)
	if x := wantFix(t, fixes, x, actMerge); x.target != u {
		t.Fatalf("x merged into %d, want %d", x.target, u)
	}
	if email, _, _ := emailState(t, db, u); email != revertClean {
		t.Fatalf("u after apply: %q", email)
	}

	rows, st, err := revertIDs(db, []int64{u}, true)
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	wantRow(t, rows, u, revertUpdate, false)
	wantRow(t, rows, x, revertMerge, true)
	if st.expanded != 1 {
		t.Fatalf("expanded %d, want 1", st.expanded)
	}
	if email, reason, _ := emailState(t, db, u); email != revertGlued || reason != "" {
		t.Fatalf("u after revert: email %q reason %q", email, reason)
	}
	if email, reason, _ := emailState(t, db, x); email != revertGlued2 || reason != "" {
		t.Fatalf("x after revert: email %q reason %q", email, reason)
	}
	if !hasLink(t, db, uBiz, u) || !hasLink(t, db, xBiz, x) {
		t.Fatal("original links not restored")
	}
	if hasLink(t, db, xBiz, u) {
		t.Fatal("link from x's business to u was not removed")
	}
}

// (c) a chain across two windows with the same suffix: x merges into t in
// window 1, then t (glued again by a test-only SQL change) merges into t2 in
// window 2. The chain is refused: window 2 copies the link x's merge wrote into
// t, and the backup cannot tell that link from a pre-apply one.
func TestRevertRefusesChain(t *testing.T) {
	db := newRevertDB(t)
	ctx := context.Background()
	tRow := seedEmail(t, db, revertGlued)
	x := seedEmail(t, db, revertGlued2)
	tBiz, xBiz, t2Biz := seedBiz(t, db, "t.example"), seedBiz(t, db, "x.example"), seedBiz(t, db, "t2.example")
	link(t, db, tBiz, tRow)
	link(t, db, xBiz, x)

	fixes1, err := processWindow(ctx, db, 1, revertHiAll, true, revertSuffix)
	if err != nil {
		t.Fatalf("window 1: %v", err)
	}
	wantFix(t, fixes1, tRow, actUpdate)
	if x := wantFix(t, fixes1, x, actMerge); x.target != tRow {
		t.Fatalf("x merged into %d, want %d", x.target, tRow)
	}

	// Test-only: glue t again and add the row it will merge into.
	if _, err := db.Exec(`UPDATE emails SET email = '9346084info@acme-yoga.de' WHERE id = $1`, tRow); err != nil {
		t.Fatal(err)
	}
	t2 := seedEmail(t, db, revertClean)
	link(t, db, t2Biz, t2)

	fixes2, err := processWindow(ctx, db, 1, revertHiAll, true, revertSuffix)
	if err != nil {
		t.Fatalf("window 2: %v", err)
	}
	if x := wantFix(t, fixes2, tRow, actMerge); x.target != t2 {
		t.Fatalf("t merged into %d, want %d", x.target, t2)
	}
	if !hasLink(t, db, tBiz, t2) {
		t.Fatal("window 2 should copy t's link to t2")
	}

	// Both ends of the chain are refused, in dry run and in commit mode, and
	// nothing changes.
	for _, ids := range [][]int64{{tRow}, {x}} {
		for _, commit := range []bool{false, true} {
			if _, _, err := revertIDs(db, ids, commit); err == nil || !strings.Contains(err.Error(), "merge chain") {
				t.Fatalf("ids %v commit=%v: err %v, want a merge chain refusal", ids, commit, err)
			}
		}
	}
	if _, reason, _ := emailState(t, db, tRow); reason != fmt.Sprintf("merged_into:%d", t2) {
		t.Fatalf("t reason changed by a refused revert: %q", reason)
	}
	if email, reason, _ := emailState(t, db, x); email != revertGlued2 || reason != fmt.Sprintf("merged_into:%d", tRow) {
		t.Fatalf("x changed by a refused revert: email %q reason %q", email, reason)
	}
	if !hasLink(t, db, tBiz, t2) || !hasLink(t, db, xBiz, t2) {
		t.Fatal("window 2 links changed by a refused revert")
	}
}

// (d) a business shared with a kept merge keeps its link to the target. Y
// merged into F and stays merged; t, also merged into F, is reverted.
func TestRevertKeepsSharedBusiness(t *testing.T) {
	db := newRevertDB(t)
	f := seedEmail(t, db, revertClean)
	fBiz := seedBiz(t, db, "f.example")
	link(t, db, fBiz, f)
	y := seedEmail(t, db, revertGlued)
	tRow := seedEmail(t, db, revertGlued2)
	sBiz, shared, tBiz := seedBiz(t, db, "s.example"), seedBiz(t, db, "shared.example"), seedBiz(t, db, "tx.example")
	link(t, db, sBiz, y)
	link(t, db, shared, y)
	link(t, db, shared, tRow)
	link(t, db, tBiz, tRow)

	fixes := applyAll(t, db)
	wantFix(t, fixes, y, actMerge)
	wantFix(t, fixes, tRow, actMerge)

	rows, st, err := revertIDs(db, []int64{tRow}, true)
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	wantRow(t, rows, tRow, revertMerge, false)
	if st.linksShared != 1 {
		t.Fatalf("linksShared %d, want 1", st.linksShared)
	}
	if !hasLink(t, db, shared, f) {
		t.Fatal("shared business lost its link to f, which kept merge y still needs")
	}
	if hasLink(t, db, tBiz, f) {
		t.Fatal("t's own link to f was not removed")
	}
	if !hasLink(t, db, shared, tRow) || !hasLink(t, db, tBiz, tRow) {
		t.Fatal("t's links not restored")
	}
	if !hasLink(t, db, sBiz, f) {
		t.Fatal("y's link to f disappeared")
	}
}

// (e) a restored address held by another live row aborts the revert before
// any write, in dry run and in commit mode.
func TestRevertUniqueConflictAborts(t *testing.T) {
	db := newRevertDB(t)
	u := seedEmail(t, db, revertGlued)
	uBiz := seedBiz(t, db, "u.example")
	link(t, db, uBiz, u)
	applyAll(t, db)
	seedEmail(t, db, revertGlued) // a live row now holds u's old address

	for _, commit := range []bool{false, true} {
		_, _, err := revertIDs(db, []int64{u}, commit)
		if err == nil || !strings.Contains(err.Error(), "clash") {
			t.Fatalf("commit=%v: err %v, want a clash error", commit, err)
		}
	}
	if email, _, _ := emailState(t, db, u); email != revertClean {
		t.Fatalf("u changed by a refused revert: %q", email)
	}
	if !hasLink(t, db, uBiz, u) {
		t.Fatal("link changed by a refused revert")
	}
}

// (f) a dry run changes nothing and reports the plan; an unknown id is refused
// before any write; a later commit still works.
func TestRevertDryRunAndUnknownID(t *testing.T) {
	db := newRevertDB(t)
	u := seedEmail(t, db, revertGlued)
	uBiz := seedBiz(t, db, "u.example")
	link(t, db, uBiz, u)
	applyAll(t, db)

	rows, _, err := revertIDs(db, []int64{u}, false)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	wantRow(t, rows, u, revertUpdate, false)
	if rows[0].restoredEmail != revertGlued || rows[0].currentEmail != revertClean {
		t.Fatalf("plan %+v", rows[0])
	}
	if email, _, status := emailState(t, db, u); email != revertClean || status != "pending" {
		t.Fatalf("dry run changed u: email %q status %q", email, status)
	}
	if !hasLink(t, db, uBiz, u) {
		t.Fatal("dry run changed links")
	}

	if _, _, err := revertIDs(db, []int64{u, 999999}, true); err == nil || !strings.Contains(err.Error(), "999999") {
		t.Fatalf("unknown id: err %v", err)
	}
	if email, _, _ := emailState(t, db, u); email != revertClean {
		t.Fatalf("unknown id changed u: %q", email)
	}

	if _, _, err := revertIDs(db, []int64{u}, true); err != nil {
		t.Fatalf("commit after dry run: %v", err)
	}
	if email, _, _ := emailState(t, db, u); email != revertGlued {
		t.Fatalf("u after commit: %q", email)
	}
}
