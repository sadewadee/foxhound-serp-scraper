package prodir

// The shared pipeline of every generic Source (see source.go): rate-limited
// fetching, the list / profiles stages with their resumable JSONL state, the
// CSV export and the Record -> Listing mapping used by load and load -dry-run.
//
// State files in the -state dir (JSON shape: Record):
//
//	<slug>_list.jsonl             Records from List. Re-running list may append
//	                              duplicates; readers dedupe by ID, last wins.
//	<slug>_profiles.jsonl         Records after Detail (list record merged with
//	                              the detail result). Resumable: IDs already
//	                              present are skipped; a 404/410 is stored with
//	                              missing=true and never fetched again.
//	<slug>_profiles_errors.jsonl  {"id","url","error"} per failed profile.

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------- fetching ----------

// gate spaces requests: every caller, from any worker, gets its own slot at
// least `every` after the previous one.
type gate struct {
	mu    sync.Mutex
	next  time.Time
	every time.Duration
}

func (g *gate) wait(ctx context.Context) error {
	g.mu.Lock()
	slot := time.Now()
	if g.next.After(slot) {
		slot = g.next
	}
	g.next = slot.Add(g.every)
	g.mu.Unlock()

	d := time.Until(slot)
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// srcFetcher is the Fetcher of one run of one source.
type srcFetcher struct {
	c *client
	g *gate
}

func newFetcher(c *client, every time.Duration) *srcFetcher {
	if every <= 0 {
		every = DefaultMinInterval
	}
	return &srcFetcher{c: c, g: &gate{every: every}}
}

func (f *srcFetcher) Get(ctx context.Context, url string, hdr map[string]string) (Page, error) {
	return f.do(ctx, "GET", url, hdr, nil)
}

func (f *srcFetcher) Post(ctx context.Context, url string, hdr map[string]string, body []byte) (Page, error) {
	return f.do(ctx, "POST", url, hdr, body)
}

func (f *srcFetcher) do(ctx context.Context, method, url string, hdr map[string]string, body []byte) (Page, error) {
	if f.g.wait(ctx) != nil {
		return Page{}, errStopped
	}
	r, err := f.c.do(ctx, method, url, hdr, body)
	if err != nil {
		return Page{}, err
	}
	return Page{Status: r.status, URL: r.url, Body: r.body}, nil
}

// ---------- records ----------

// errListLimit is returned by emit once -limit records were written.
var errListLimit = errors.New("list limit reached")

// check rejects a record a source must not store.
func (r Record) check() error {
	if r.ID == "" {
		return errors.New("record without ID")
	}
	if _, ok := categoryNiche[r.Category]; r.Category != "" && !ok {
		return fmt.Errorf("record %s: unknown category %q", r.ID, r.Category)
	}
	return nil
}

// mergeRecord overlays detail on base: a non-empty detail field wins, Social
// merges per platform and Meta per key (the detail's values replace the list's
// for a key both carry). ID is always base's.
func mergeRecord(base, detail Record) Record {
	out := base
	set := func(dst *string, v string) {
		if v = clean(v); v != "" {
			*dst = v
		}
	}
	set(&out.URL, detail.URL)
	set(&out.Name, detail.Name)
	set(&out.Business, detail.Business)
	set(&out.Address, detail.Address)
	set(&out.City, detail.City)
	set(&out.Region, detail.Region)
	set(&out.PostalCode, detail.PostalCode)
	set(&out.Country, detail.Country)
	set(&out.Bio, detail.Bio)
	set(&out.Category, detail.Category)
	if len(detail.Emails) > 0 {
		out.Emails = detail.Emails
	}
	if len(detail.Phones) > 0 {
		out.Phones = detail.Phones
	}
	if len(detail.Websites) > 0 {
		out.Websites = detail.Websites
	}
	if len(detail.Social) > 0 {
		out.Social = map[string]string{}
		for _, m := range []map[string]string{base.Social, detail.Social} {
			for k, v := range m {
				out.Social[k] = v
			}
		}
	}
	if len(detail.Meta) > 0 {
		out.Meta = map[string][]string{}
		for _, m := range []map[string][]string{base.Meta, detail.Meta} {
			for k, v := range m {
				out.Meta[k] = v
			}
		}
	}
	out.Missing = base.Missing || detail.Missing
	out.ID = base.ID
	return out
}

func listPath(dir string, s *Source) string     { return filepath.Join(dir, s.Slug+"_list.jsonl") }
func profilesPath(dir string, s *Source) string { return filepath.Join(dir, s.Slug+"_profiles.jsonl") }

// readRecords reads a state file: records deduped by ID (last wins), in order
// of first appearance. A missing file is empty; corrupt lines are skipped.
func readRecords(path string) ([]Record, error) {
	var order []string
	byID := map[string]Record{}
	err := readJSONL(path, func(line []byte) {
		var r Record
		if json.Unmarshal(line, &r) != nil || r.ID == "" {
			return
		}
		if _, ok := byID[r.ID]; !ok {
			order = append(order, r.ID)
		}
		byID[r.ID] = r
	})
	out := make([]Record, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out, err
}

// loadRecords is the list joined with its profiles (a profile replaces its list
// record), in list order. IDs only in the profiles file are ignored: the list
// is the directory.
func loadRecords(dir string, s *Source) ([]Record, error) {
	list, err := readRecords(listPath(dir, s))
	if err != nil {
		return nil, err
	}
	profs, err := readRecords(profilesPath(dir, s))
	if err != nil {
		return nil, err
	}
	byID := make(map[string]Record, len(profs))
	for _, p := range profs {
		byID[p.ID] = p
	}
	for i, r := range list {
		if p, ok := byID[r.ID]; ok {
			list[i] = p
		}
	}
	return list, nil
}

// ---------- stages ----------

// SourceList runs the list stage: List appends its records to <slug>_list.jsonl.
// limit > 0 stops after that many records.
func SourceList(ctx context.Context, dir string, s *Source, limit int) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ExitErrors, err
	}
	w, err := openAppend(listPath(dir, s))
	if err != nil {
		return ExitErrors, err
	}
	defer w.Close()

	n := 0
	emit := func(r Record) error {
		if err := r.check(); err != nil {
			return err
		}
		if limit > 0 && n >= limit {
			return errListLimit
		}
		if err := w.write(r); err != nil {
			return err
		}
		n++
		if limit > 0 && n >= limit {
			return errListLimit
		}
		return nil
	}
	err = s.List(ctx, newFetcher(newClient(), s.MinInterval), emit)
	fmt.Printf("list records written this run: %d\n", n)
	switch {
	case err == nil || errors.Is(err, errListLimit):
		return ExitOK, nil
	case errors.Is(err, errStopped) || ctx.Err() != nil:
		return ExitInterrupted, nil
	}
	return ExitErrors, err
}

// SourceProfiles runs the profiles stage over the list records that have no
// profile yet.
func SourceProfiles(ctx context.Context, dir string, s *Source, workers, limit int) (int, error) {
	if s.Detail == nil {
		fmt.Printf("%s has no detail stage: the list carries everything, nothing to do\n", s.Slug)
		return ExitOK, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ExitErrors, err
	}
	list, err := readRecords(listPath(dir, s))
	if err != nil {
		return ExitErrors, err
	}
	have, err := readRecords(profilesPath(dir, s))
	if err != nil {
		return ExitErrors, err
	}
	done := make(map[string]bool, len(have))
	for _, p := range have {
		done[p.ID] = true
	}
	var todo []Record
	for _, r := range list {
		if !done[r.ID] {
			todo = append(todo, r)
		}
	}
	if limit > 0 && len(todo) > limit {
		todo = todo[:limit]
	}
	profW, err := openAppend(profilesPath(dir, s))
	if err != nil {
		return ExitErrors, err
	}
	defer profW.Close()
	errW, err := openAppend(filepath.Join(dir, s.Slug+"_profiles_errors.jsonl"))
	if err != nil {
		return ExitErrors, err
	}
	defer errW.Close()

	f := newFetcher(newClient(), s.MinInterval)
	res := runPool(ctx, todo, workers, s.Slug+"-profiles", 200, func(ctx context.Context, r Record) error {
		fail := func(err error) error {
			if werr := errW.write(struct {
				ID    string `json:"id"`
				URL   string `json:"url"`
				Error string `json:"error"`
			}{r.ID, r.URL, err.Error()}); werr != nil {
				slog.Error("prodir: write errors file", "error", werr)
			}
			return fmt.Errorf("%s: %w", r.ID, err)
		}
		d, err := s.Detail(ctx, f, r)
		switch {
		case errors.Is(err, errStopped):
			return err
		case errors.Is(err, ErrNotFound):
			r.Missing = true
			return profW.write(r)
		case err != nil:
			return fail(err)
		}
		m := mergeRecord(r, d)
		if err := m.check(); err != nil {
			return fail(err)
		}
		return profW.write(m)
	})
	fmt.Printf("profiles written this run: %d, errors: %d, still pending: %d\n",
		res.done, res.errs, len(todo)-res.done-res.errs)
	fmt.Printf("unique ids in list: %d\n", len(list))
	return exitCode(res, ctx), nil
}

// ---------- export ----------

var genericExportHead = []string{"id", "url", "name", "business", "address", "city", "region", "postal_code",
	"country", "emails", "phones", "websites", "social", "category", "bio"}

// SourceExport writes the joined records as CSV: the fixed columns, then one
// column per Meta key seen (sorted), multiple values joined with "; ".
// out "" means <dir>/<slug>.csv.
func SourceExport(dir string, s *Source, out string) error {
	recs, err := loadRecords(dir, s)
	if err != nil {
		return err
	}
	if out == "" {
		out = filepath.Join(dir, s.Slug+".csv")
	}
	keys := map[string]bool{}
	for _, r := range recs {
		for k := range r.Meta {
			keys[k] = true
		}
	}
	var metaKeys []string
	for k := range keys {
		metaKeys = append(metaKeys, k)
	}
	sort.Strings(metaKeys)

	w, err := newCSVWriter(out)
	if err != nil {
		return err
	}
	w.row(append(append([]string{}, genericExportHead...), metaKeys...))
	for _, r := range recs {
		var social []string
		for _, p := range sortedKeys(r.Social) {
			social = append(social, p+"="+r.Social[p])
		}
		row := []string{r.ID, r.URL, r.Name, r.Business, r.Address, r.City, r.Region, r.PostalCode, r.Country,
			strings.Join(r.Emails, "; "), strings.Join(r.Phones, "; "), strings.Join(r.Websites, "; "),
			strings.Join(social, "; "), firstNonEmpty(r.Category, s.Category), r.Bio}
		for _, k := range metaKeys {
			row = append(row, strings.Join(r.Meta[k], "; "))
		}
		w.row(row)
	}
	if err := w.close(); err != nil {
		return err
	}
	fmt.Printf("wrote %s (%d records)\n", out, len(recs))
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------- Record -> Listing ----------

var dnsLabelRe = regexp.MustCompile(`^[a-z0-9-]{1,63}$`)

// sourceDomain is the synthetic unique domain "<id>.<slug>.<host>" of a record:
// directory rows never collide with SERP-pipeline rows. An id that is no
// DNS label is replaced by the first 16 hex chars of its sha1.
func sourceDomain(s *Source, id string) string {
	label := id
	if !dnsLabelRe.MatchString(id) {
		sum := sha1.Sum([]byte(id))
		label = hex.EncodeToString(sum[:])[:16]
	}
	return label + "." + s.Slug + "." + s.Host
}

// usableSite reports whether u can be an entry's own website: a valid host that
// is neither a directory nor a social platform.
func usableSite(u string) bool {
	h := urlHost(u)
	return validHost(h) && !isOwnDomain(h) && socialPlatform(u) == ""
}

// recordListing maps one record to the row load writes. Structured values come
// first; contact details found in Bio are added by extractContacts.
func recordListing(s *Source, r Record, res *countryResolver) Listing {
	profileURL := firstNonEmpty(clean(r.URL), "https://"+s.Host)
	category := firstNonEmpty(r.Category, s.Category)
	name := firstNonEmpty(clean(r.Name), clean(r.Business))

	var loc []string
	for _, p := range []string{r.City, r.Region, r.Country} {
		if p = clean(p); p != "" {
			loc = append(loc, p)
		}
	}
	fd := extractContacts(r.Bio)

	l := Listing{
		Domain: sourceDomain(s, r.ID), URL: profileURL, Name: name,
		Category: category, Niche: categoryNiche[category],
		Website: profileURL, Address: clean(r.Address), Location: strings.Join(loc, ", "), City: clean(r.City),
		Description: clean(r.Bio), RawCountry: clean(r.Country),
		Social: map[string]string{}, Meta: map[string][]string{},
	}

	// Websites: the first usable structured site is the own website, the other
	// usable ones (and the sites in Bio) go to other_websites. A social profile
	// typed into a website field is a social link.
	var own string
	var others []string
	for _, w := range r.Websites {
		w = clean(w)
		switch {
		case w == "":
		case socialPlatform(w) != "":
			if p := socialPlatform(w); socialProfile(w) && l.Social[p] == "" {
				l.Social[p] = withScheme(w)
			}
		case !usableSite(w):
		case own == "":
			own = w
		default:
			others = append(others, w)
		}
	}
	l.setWebsites(own, append(others, fd.Websites...))

	for _, e := range r.Emails {
		for _, em := range structuredEmails(e) {
			l.Emails = addString(l.Emails, em)
		}
	}
	for _, em := range fd.Emails {
		l.Emails = addString(l.Emails, em)
	}
	for _, p := range r.Phones {
		l.Phones = addPhones(l.Phones, structuredPhone(p))
	}
	l.Phones = addPhones(l.Phones, fd.Phones...)

	for plat, u := range r.Social {
		if u = clean(u); u != "" && !isOwnDomain(urlHost(u)) && l.Social[plat] == "" {
			l.Social[plat] = withScheme(u)
		}
	}
	mergeSocial(l.Social, fd.Social)

	for _, k := range sortedKeys(r.Meta) {
		addMeta(l.Meta, k, r.Meta[k]...)
	}
	addMeta(l.Meta, "source_directory", s.Name)
	addMeta(l.Meta, "listing_type", s.ListingType)
	addMeta(l.Meta, "business_name", r.Business)

	l.Country, l.CountryCode = res.resolve(r.Country)
	return l
}

// sourceListings builds the listings of a generic source from a state dir.
func sourceListings(dir string, s *Source, res *countryResolver) ([]Listing, error) {
	recs, err := loadRecords(dir, s)
	if err != nil {
		return nil, err
	}
	out := make([]Listing, 0, len(recs))
	for _, r := range recs {
		out = append(out, recordListing(s, r, res))
	}
	return out, nil
}
