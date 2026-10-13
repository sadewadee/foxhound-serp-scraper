package prodir

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pointAt aims the package endpoints at srv and shrinks the backoff; it
// restores the production values when the test ends.
func pointAt(t *testing.T, srv *httptest.Server) {
	t.Helper()
	oa, oi, os_, rb, rj := ACEBase, ICFBase, ICFSearchURL, RetryBase, RetryJitter
	ACEBase, ICFBase, ICFSearchURL = srv.URL, srv.URL+"/eweb/", srv.URL+"/api/search"
	RetryBase, RetryJitter = time.Millisecond, 0
	t.Cleanup(func() { ACEBase, ICFBase, ICFSearchURL, RetryBase, RetryJitter = oa, oi, os_, rb, rj })
}

func writeLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func fileLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	for _, bad := range []int{403, 429, 500, 503} {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) < 3 {
				w.WriteHeader(bad)
				return
			}
			fmt.Fprint(w, "ok")
		}))
		pointAt(t, srv)
		resp, err := newClient().do(context.Background(), "GET", srv.URL, nil, nil)
		srv.Close()
		if err != nil || resp.status != 200 || string(resp.body) != "ok" {
			t.Fatalf("status %d: resp=%+v err=%v", bad, resp, err)
		}
		if hits.Load() != 3 {
			t.Errorf("status %d: %d requests, want 3", bad, hits.Load())
		}
	}
}

func TestDoGivesUpAfterFiveAttempts(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(503)
	}))
	defer srv.Close()
	pointAt(t, srv)
	if _, err := newClient().do(context.Background(), "GET", srv.URL, nil, nil); err == nil {
		t.Fatal("want an error after exhausting retries")
	}
	if hits.Load() != 5 {
		t.Errorf("%d requests, want 5", hits.Load())
	}
}

func TestDoFailsFastOnOther4xx(t *testing.T) {
	for _, code := range []int{400, 401, 404, 410} {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(code)
		}))
		pointAt(t, srv)
		resp, err := newClient().do(context.Background(), "GET", srv.URL, nil, nil)
		srv.Close()
		if err != nil || resp.status != code {
			t.Fatalf("status %d: resp=%+v err=%v", code, resp, err)
		}
		if hits.Load() != 1 {
			t.Errorf("status %d: %d requests, want 1 (no retry)", code, hits.Load())
		}
	}
}

func TestRunPoolBreaker(t *testing.T) {
	items := make([]int, 100)
	var calls atomic.Int32
	res := runPool(context.Background(), items, 1, "t", 1000, func(ctx context.Context, _ int) error {
		calls.Add(1)
		return fmt.Errorf("blocked")
	})
	if !res.tripped || calls.Load() != breakerLimit {
		t.Fatalf("tripped=%v calls=%d, want tripped after %d", res.tripped, calls.Load(), breakerLimit)
	}
	if got := exitCode(res, context.Background()); got != ExitBreaker {
		t.Errorf("exit code %d, want %d", got, ExitBreaker)
	}

	// A success resets the streak: 19 failures, 1 success, repeated, never trips.
	var n int
	res = runPool(context.Background(), make([]int, 200), 1, "t", 1000, func(ctx context.Context, _ int) error {
		n++
		if n%20 == 0 {
			return nil
		}
		return fmt.Errorf("flaky")
	})
	if res.tripped {
		t.Error("breaker tripped although successes interleave")
	}
	if got := exitCode(res, context.Background()); got != ExitErrors {
		t.Errorf("exit code %d, want %d", got, ExitErrors)
	}
}

func TestExitCodeInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := exitCode(poolResult{}, ctx); got != ExitInterrupted {
		t.Errorf("exit code %d, want %d", got, ExitInterrupted)
	}
}

// aceListRow is a minimal listing row.
func aceListRow(id string) string {
	return fmt.Sprintf(`{"EncryptedCustomerNumber":%q,"ProfileURL":"/pro/%s/","FullName":"Fixture %s","City":"Exampleville","StateCode":"NY","CountryName":"US"}`, id, id, id)
}

func TestACEProfilesRedirectToListingIsAnError(t *testing.T) {
	removed := readFixture(t, "ace_removed_SYNTHETIC.html")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/pro/") {
			http.Redirect(w, r, "/resources/everyone/find-ace-pro/", http.StatusFound)
			return
		}
		w.Write(removed)
	}))
	defer srv.Close()
	pointAt(t, srv)

	dir := t.TempDir()
	writeLines(t, filepath.Join(dir, "ace_list.jsonl"), aceListRow("aaa"), aceListRow("bbb"))
	code, err := ACEProfiles(context.Background(), dir, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if code != ExitErrors {
		t.Errorf("exit code %d, want %d", code, ExitErrors)
	}
	if got := fileLines(t, filepath.Join(dir, "ace_profiles.jsonl")); len(got) != 0 {
		t.Errorf("profiles file has %d lines, want none: %v", len(got), got)
	}
	errs := fileLines(t, filepath.Join(dir, "ace_profiles_errors.jsonl"))
	if len(errs) != 2 || !strings.Contains(errs[0], "profile block not found") || !strings.Contains(errs[0], `"id":"aaa"`) {
		t.Errorf("errors file = %v", errs)
	}
}

func TestACEProfilesBreakerExitCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html>no profile here</html>")
	}))
	defer srv.Close()
	pointAt(t, srv)

	dir := t.TempDir()
	var rows []string
	for i := 0; i < 50; i++ {
		rows = append(rows, aceListRow(fmt.Sprintf("id%02d", i)))
	}
	writeLines(t, filepath.Join(dir, "ace_list.jsonl"), rows...)
	code, err := ACEProfiles(context.Background(), dir, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if code != ExitBreaker {
		t.Errorf("exit code %d, want %d", code, ExitBreaker)
	}
	if n := len(fileLines(t, filepath.Join(dir, "ace_profiles_errors.jsonl"))); n != breakerLimit {
		t.Errorf("%d errors recorded, want %d (the run stops at the breaker)", n, breakerLimit)
	}
}

func TestACEProfilesResumeAndMissing(t *testing.T) {
	good := readFixture(t, "ace_profile_SYNTHETIC.html")
	var hits pathSet
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.add(r.URL.Path)
		switch r.URL.Path {
		case "/pro/gone/":
			w.WriteHeader(404)
		default:
			w.Write(good)
		}
	}))
	defer srv.Close()
	pointAt(t, srv)

	dir := t.TempDir()
	writeLines(t, filepath.Join(dir, "ace_list.jsonl"), aceListRow("done"), aceListRow("fresh"), aceListRow("gone"))
	// "done" is already in the state file, with a truncated last line from a hard kill.
	if err := os.WriteFile(filepath.Join(dir, "ace_profiles.jsonl"),
		[]byte(`{"id":"done","url":"u","tokens":[]}`+"\n"+`{"id":"tru`), 0o644); err != nil {
		t.Fatal(err)
	}
	code, err := ACEProfiles(context.Background(), dir, 2, 0)
	if err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if hits.has("/pro/done/") {
		t.Error("a profile already in the state file was fetched again")
	}
	if !hits.has("/pro/fresh/") || !hits.has("/pro/gone/") {
		t.Errorf("missing fetches: %v", hits.paths())
	}
	profs, err := aceProfiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p := profs["gone"]; !p.Missing || p.Tokens != nil {
		t.Errorf("gone = %+v, want missing", p)
	}
	if p := profs["fresh"]; p.Missing || p.Tokens == nil || len(*p.Tokens) == 0 {
		t.Errorf("fresh = %+v, want tokens", p)
	}
	// The tokens interpret to the same profile as parsing the page directly.
	entries, err := loadACE(dir)
	if err != nil || len(entries) != 3 {
		t.Fatalf("loadACE: %v, %d entries", err, len(entries))
	}
	for _, e := range entries {
		if e.id == "fresh" && (e.prof == nil || e.prof.Phone != "(555) 010-0199") {
			t.Errorf("fresh profile = %+v", e.prof)
		}
		if e.id == "gone" && !e.missing {
			t.Error("gone should be missing")
		}
	}
}

func TestACEListWindowsAndResume(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		q := r.URL.Query()
		if r.URL.Path != "/api/FindACEProByName/" || q.Get("firstName") != "_" || q.Get("lastName") != "_" ||
			r.Header.Get("X-Requested-With") != "XMLHttpRequest" {
			http.Error(w, "bad request", 400)
			return
		}
		var lo, hi int
		fmt.Sscan(q.Get("lowerLimit"), &lo)
		fmt.Sscan(q.Get("upperLimit"), &hi)
		if hi-lo != 24 {
			http.Error(w, "bad window", 400)
			return
		}
		n := 27 // total results
		var rows []string
		for i := lo; i <= min(hi, n); i++ {
			rows = append(rows, aceListRow(fmt.Sprintf("r%03d", i)))
		}
		fmt.Fprintf(w, `{"TotalResults":%d,"Results":[%s]}`, n, strings.Join(rows, ","))
	}))
	defer srv.Close()
	pointAt(t, srv)

	dir := t.TempDir()
	code, err := ACEList(context.Background(), dir, 2, 0)
	if err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	ids, _, err := aceUniqueList(dir)
	if err != nil || len(ids) != 27 {
		t.Fatalf("unique ids = %d, err=%v; want 27", len(ids), err)
	}
	lines := fileLines(t, filepath.Join(dir, "ace_list.jsonl"))
	if len(lines) != 27 || !strings.HasSuffix(lines[0], `"_page":1}`) || !strings.HasSuffix(lines[26], `"_page":26}`) {
		t.Errorf("list lines = %d, first=%s", len(lines), lines[0])
	}
	for _, l := range lines {
		if !json.Valid([]byte(l)) {
			t.Fatalf("invalid json line: %s", l)
		}
	}

	// A second run only re-fetches window 1 (for TotalResults) and writes nothing.
	hits.Store(0)
	if code, err = ACEList(context.Background(), dir, 2, 0); err != nil || code != ExitOK {
		t.Fatalf("resume: code=%d err=%v", code, err)
	}
	if hits.Load() != 1 {
		t.Errorf("resume made %d requests, want 1", hits.Load())
	}
	if got := fileLines(t, filepath.Join(dir, "ace_list.jsonl")); len(got) != 27 {
		t.Errorf("resume changed the file: %d lines", len(got))
	}
}

func icfListRow(key string) string {
	return fmt.Sprintf(`{"key":%q,"fullName":"Coach %s","headline":"h","description":"d","location":"Exampletown, NY UNITED STATES"}`, key, key)
}

func TestICFProfilesMissingAndFields(t *testing.T) {
	good := readFixture(t, "icf_detail_enhanced_SYNTHETIC.html")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("coachcstkey") {
		case "REDIR":
			http.Redirect(w, r, "/eweb/errorpage.aspx?aspxerrorpath=/x", http.StatusFound)
		case "GONE":
			w.WriteHeader(404)
		case "BLANK":
			fmt.Fprint(w, "<html>blocked</html>")
		default:
			w.Write(good)
		}
	}))
	defer srv.Close()
	pointAt(t, srv)
	dir := t.TempDir()
	writeLines(t, filepath.Join(dir, "icf_list.jsonl"), icfListRow("OK1"), icfListRow("REDIR"), icfListRow("GONE"), icfListRow("BLANK"))
	code, err := ICFProfiles(context.Background(), dir, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if code != ExitErrors { // BLANK parsed with no fields
		t.Errorf("exit code %d, want %d", code, ExitErrors)
	}
	profs, err := icfProfiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"REDIR", "GONE"} {
		if !profs[k].Missing {
			t.Errorf("%s = %+v, want missing", k, profs[k])
		}
	}
	if _, ok := profs["BLANK"]; ok {
		t.Error("a page without fields must not be stored as a profile")
	}
	ok := profs["OK1"]
	if ok.Missing || ok.Email != "Coach.Fixture@Example.TEST" || ok.Phone != "+1 555 010 0188" ||
		ok.Location != "Exampletown, NY UNITED STATES" {
		t.Errorf("OK1 = %+v", ok)
	}
	if errs := fileLines(t, filepath.Join(dir, "icf_profiles_errors.jsonl")); len(errs) != 1 || !strings.Contains(errs[0], `"key":"BLANK"`) {
		t.Errorf("errors file = %v", errs)
	}

	// The record round-trips through the state file with its field order intact.
	line := fileLines(t, filepath.Join(dir, "icf_profiles.jsonl"))[0]
	if !strings.HasPrefix(line, `{"key":"OK1","url":`) || !strings.Contains(line, `"fields":{"Web Site":["http://www.fixture-coaching.example"],"Email Address":`) {
		t.Errorf("profile line = %s", line)
	}
}

func TestICFListPaging(t *testing.T) {
	var searches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "GetCountriesStatesRegions"):
			fmt.Fprint(w, `{"d":[{"code":"AUSTRALIA","name":"Australia","region":"Asia Pacific"}]}`)
		case r.URL.Path == "/api/search":
			var body struct {
				Skip, Take int
			}
			json.NewDecoder(r.Body).Decode(&body)
			searches.Add(1)
			if body.Take != 1000 {
				http.Error(w, "take", 400)
				return
			}
			fmt.Fprintf(w, `{"resultCount":2,"results":[%s,%s]}`, icfListRow("K1"), icfListRow("K2"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	pointAt(t, srv)

	dir := t.TempDir()
	code, err := ICFList(context.Background(), dir)
	if err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if searches.Load() != 1 {
		t.Errorf("%d search requests, want 1", searches.Load())
	}
	recs, err := icfUniqueList(dir)
	if err != nil || len(recs) != 2 {
		t.Fatalf("recs=%d err=%v", len(recs), err)
	}
	if cs, err := loadICFCountries(dir); err != nil || len(cs) != 1 || cs[0].Code != "AUSTRALIA" {
		t.Errorf("countries = %v, err=%v", cs, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "icf_list.jsonl.tmp")); err == nil {
		t.Error("the .tmp list file was left behind")
	}
}

// pathSet is a tiny concurrent set of requested paths.
type pathSet struct {
	mu sync.Mutex
	m  map[string]bool
}

func (s *pathSet) add(p string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string]bool{}
	}
	s.m[p] = true
}

func (s *pathSet) has(p string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[p]
}

func (s *pathSet) paths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for p := range s.m {
		out = append(out, p)
	}
	return out
}
