package prodir

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ICF Credentialed Coach Finder. State files (dir):
//
//	icf_countries.json         the site's country list (also the country matcher input)
//	icf_list.jsonl             one search result per line, exactly as the API returned it
//	icf_profiles.jsonl         {"key","url","fields":{label:[values]},"email","phone","website","location"} or {"key","url","missing":true}
//	icf_profiles_errors.jsonl  {"key","url","error"}

const icfPageSize = 1000

var icfContactLabels = map[string]bool{"Web Site": true, "Email Address": true, "Phone": true, "Location": true}

func icfProfileURL(key string) string {
	return ICFBase + "CCFDynamicPage.aspx?webcode=ccfcoachprofileview&coachcstkey=" + key + "&site=icfapp"
}

// ---------- profile page parser ----------

// icfFields is an insertion-ordered label -> values map; the order is the
// column order of the export, so it must survive the JSON round trip.
type icfFields struct {
	labels []string
	vals   map[string][]string
}

func (f *icfFields) add(label, value string) {
	value = clean(value)
	if f.vals == nil {
		f.vals = map[string][]string{}
	}
	if _, ok := f.vals[label]; !ok {
		f.vals[label] = []string{}
		f.labels = append(f.labels, label)
	}
	if value != "" && value != "Unspecified" {
		f.vals[label] = append(f.vals[label], value)
	}
}

func (f icfFields) first(label string) string {
	if v := f.vals[label]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func (f icfFields) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, l := range f.labels {
		if i > 0 {
			buf.WriteByte(',')
		}
		k, err := marshalNoEscape(l)
		if err != nil {
			return nil, err
		}
		v, err := marshalNoEscape(f.vals[l])
		if err != nil {
			return nil, err
		}
		buf.Write(k)
		buf.WriteByte(':')
		buf.Write(v)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (f *icfFields) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return fmt.Errorf("fields: want object")
	}
	*f = icfFields{vals: map[string][]string{}}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		label := t.(string)
		var vals []string
		if err := dec.Decode(&vals); err != nil {
			return err
		}
		if vals == nil {
			vals = []string{}
		}
		if _, dup := f.vals[label]; !dup {
			f.labels = append(f.labels, label)
		}
		f.vals[label] = vals
	}
	return nil
}

// icfParser extracts (label -> values) from the two markups on a coach page:
//
//	contact tab:  <label class="field label">L</label> <x class="field value">V</x>
//	details tab:  <table class="... definition ..."><tr><td>L</td><td><div>V</div>...</td></tr>
type icfParser struct {
	fields icfFields

	// contact-tab state
	inLabel  bool
	labelBuf strings.Builder
	curLabel string
	valTag   string
	valDepth int
	valBuf   strings.Builder
	valHref  string

	// definition-table state
	tableDepth int
	inDef      bool
	cells      *[]icfCell
	cellOpen   bool // a <td> is open (Python: cell_divs is not None)
	cellText   strings.Builder
	cellDivs   []string
	divOpen    bool
	divBuf     strings.Builder
	divDepth   int
}

type icfCell struct {
	text string
	divs []string
}

func (p *icfParser) start(tag string, a map[string]string) {
	cls := classSet(a)
	if p.valTag != "" {
		if tag == p.valTag {
			p.valDepth++
		}
		return
	}
	switch {
	case tag == "label" && cls["field"] && cls["label"]:
		p.inLabel = true
		p.labelBuf.Reset()
	case cls["field"] && cls["value"] && p.curLabel != "":
		p.valTag, p.valDepth = tag, 1
		p.valBuf.Reset()
		p.valHref = ""
		if href := a["href"]; strings.HasPrefix(strings.ToLower(href), "mailto:") {
			p.valHref = href[7:]
		}
	case tag == "table":
		if p.inDef {
			p.tableDepth++
		} else if cls["definition"] {
			p.inDef, p.tableDepth = true, 1
		}
	case p.inDef:
		switch {
		case tag == "tr":
			p.cells = &[]icfCell{}
		case tag == "td" && p.cells != nil:
			p.cellOpen = true
			p.cellText.Reset()
			p.cellDivs = []string{}
		case tag == "div" && p.cellOpen:
			if !p.divOpen {
				p.divOpen, p.divDepth = true, 1
				p.divBuf.Reset()
			} else {
				p.divDepth++
			}
		}
	}
}

func (p *icfParser) end(tag string) {
	if p.valTag != "" {
		if tag == p.valTag {
			p.valDepth--
			if p.valDepth == 0 {
				v := p.valHref
				if v == "" {
					v = p.valBuf.String()
				}
				p.fields.add(p.curLabel, v)
				p.valTag = ""
			}
		}
		return
	}
	if tag == "label" && p.inLabel {
		p.curLabel = clean(p.labelBuf.String())
		p.inLabel = false
		return
	}
	if !p.inDef {
		return
	}
	switch {
	case tag == "table":
		p.tableDepth--
		if p.tableDepth == 0 {
			p.inDef = false
		}
	case tag == "div" && p.divOpen:
		p.divDepth--
		if p.divDepth == 0 {
			p.cellDivs = append(p.cellDivs, clean(p.divBuf.String()))
			p.divOpen = false
		}
	case tag == "td" && p.cellOpen:
		if p.cells != nil {
			*p.cells = append(*p.cells, icfCell{clean(p.cellText.String()), p.cellDivs})
		}
		p.cellOpen = false
	case tag == "tr" && p.cells != nil:
		if c := *p.cells; len(c) >= 2 && c[0].text != "" {
			label := c[0].text
			vals := c[1].divs
			if len(vals) == 0 {
				vals = []string{c[1].text}
			}
			p.fields.add(label, "") // keep the label even when it has no values
			for _, v := range vals {
				p.fields.add(label, v)
			}
		}
		p.cells = nil
	}
}

func (p *icfParser) data(s string) {
	switch {
	case p.valTag != "":
		p.valBuf.WriteString(s)
	case p.inLabel:
		p.labelBuf.WriteString(s)
	case p.cellOpen:
		if p.divOpen {
			p.divBuf.WriteString(s)
		} else {
			p.cellText.WriteString(s)
		}
	}
}

// ICFProfile is one ICF profile state record. Fields is empty for missing ones.
type ICFProfile struct {
	Key      string    `json:"key"`
	URL      string    `json:"url"`
	Missing  bool      `json:"missing"`
	Fields   icfFields `json:"fields"`
	Email    string    `json:"email"`
	Phone    string    `json:"phone"`
	Website  string    `json:"website"`
	Location string    `json:"location"`
}

func (p ICFProfile) MarshalJSON() ([]byte, error) {
	if p.Missing {
		return marshalNoEscape(struct {
			Key     string `json:"key"`
			URL     string `json:"url"`
			Missing bool   `json:"missing"`
		}{p.Key, p.URL, true})
	}
	return marshalNoEscape(struct {
		Key      string    `json:"key"`
		URL      string    `json:"url"`
		Fields   icfFields `json:"fields"`
		Email    string    `json:"email"`
		Phone    string    `json:"phone"`
		Website  string    `json:"website"`
		Location string    `json:"location"`
	}{p.Key, p.URL, p.Fields, p.Email, p.Phone, p.Website, p.Location})
}

// parseICFProfile extracts the profile fields from a coach page.
func parseICFProfile(page []byte) icfFields {
	p := &icfParser{}
	feedHTML(page, p)
	return p.fields
}

// ---------- country matcher ----------

// icfCountry is one entry of the site's country list. Despite the name, `code`
// is the upper-case country NAME as it appears at the end of a location
// ("UNITED STATES", "AUSTRALIA").
type icfCountry struct {
	Code   string `json:"code"`
	Name   string `json:"name"`
	Region string `json:"region"`
}

// countryMatcher finds which country a location string ends with. Longest code
// first, so "SOUTH AFRICA" wins over a shorter suffix.
type countryMatcher struct{ items []icfCountry }

func newCountryMatcher(countries []icfCountry) *countryMatcher {
	items := make([]icfCountry, len(countries))
	for i, c := range countries {
		items[i] = icfCountry{Code: strings.ToUpper(c.Code), Name: c.Name, Region: c.Region}
	}
	sort.SliceStable(items, func(i, j int) bool {
		return utf8.RuneCountInString(items[i].Code) > utf8.RuneCountInString(items[j].Code)
	})
	return &countryMatcher{items}
}

func isAlnum(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsNumber(r) }

// match returns the matched country code (upper-case name) and region, or "" "".
func (m *countryMatcher) match(location string) (code, region string) {
	loc := strings.ToUpper(clean(location))
	for _, c := range m.items {
		if !strings.HasSuffix(loc, c.Code) {
			continue
		}
		rest := loc[:len(loc)-len(c.Code)]
		if rest == "" {
			return c.Code, c.Region
		}
		if r, _ := utf8.DecodeLastRuneInString(rest); !isAlnum(r) {
			return c.Code, c.Region
		}
	}
	return "", ""
}

// ---------- state records ----------

// ICFRecord is one search result (the fields used downstream).
type ICFRecord struct {
	Key                string   `json:"key"`
	FullName           string   `json:"fullName"`
	Headline           string   `json:"headline"`
	Description        string   `json:"description"`
	Credential         string   `json:"credential"`
	Designations       []string `json:"designations"`
	StandardRate       string   `json:"standardRate"`
	Location           string   `json:"location"`
	PhotoURL           string   `json:"photoUrl"`
	HasEnhancedProfile bool     `json:"hasEnhancedProfile"`
}

// icfUniqueList returns the search results deduplicated by key (first wins), in file order.
func icfUniqueList(dir string) ([]ICFRecord, error) {
	path := filepath.Join(dir, "icf_list.jsonl")
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("%s not found; run the 'list' subcommand first", path)
	}
	var out []ICFRecord
	seen := map[string]bool{}
	err := readJSONL(path, func(line []byte) {
		var r ICFRecord
		if json.Unmarshal(line, &r) != nil || r.Key == "" || seen[r.Key] {
			return
		}
		seen[r.Key] = true
		out = append(out, r)
	})
	return out, err
}

func icfProfiles(dir string) (map[string]ICFProfile, error) {
	out := map[string]ICFProfile{}
	err := readJSONL(filepath.Join(dir, "icf_profiles.jsonl"), func(line []byte) {
		var p ICFProfile
		if json.Unmarshal(line, &p) == nil && p.Key != "" {
			out[p.Key] = p
		}
	})
	return out, err
}

func loadICFCountries(dir string) ([]icfCountry, error) {
	path := filepath.Join(dir, "icf_countries.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s not found; run the 'list' subcommand first", path)
	}
	var cs []icfCountry
	if err := json.Unmarshal(b, &cs); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cs, nil
}

// ---------- crawl ----------

func newUUID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func icfSearchBody(skip int) []byte {
	b, _ := json.Marshal(map[string]any{
		"requestId": newUUID(), "continuationToken": "", "skip": skip, "take": icfPageSize,
		"sort": "lastName", "sortDirection": "asc", "keywords": "",
		"filters": map[string]any{
			"keywords": "", "credentials": []any{},
			"services": map[string]any{
				"coachingThemes":  []any{},
				"coachingMethods": map[string]any{"methods": []any{}, "relocate": false},
				"standardRate":    map[string]any{"proBono": false, "nonProfitDiscount": false, "feeRanges": []any{}},
			},
			"experience": map[string]any{
				"haveCoached":          map[string]any{"clientType": "", "organizationalClientTypes": []any{}},
				"coachedOrganizations": map[string]any{"global": false, "nonProfit": false, "industrySector": ""},
				"heldPositions":        []any{},
			},
			"demographics": map[string]any{
				"gender": "", "ageRange": "", "fluentLanguages": []any{},
				"locations": map[string]any{"countries": []any{}, "states": []any{}},
			},
			"additional": map[string]any{"canProvide": []any{}, "designations": []any{}},
		},
	})
	return b
}

type icfSearchPage struct {
	ResultCount int               `json:"resultCount"`
	Results     []json.RawMessage `json:"results"`
}

func (c *client) icfSearch(ctx context.Context, skip int) (*icfSearchPage, error) {
	// The site's jQuery posts a raw JSON string under a form content type; the server expects exactly that.
	resp, err := c.do(ctx, "POST", ICFSearchURL, map[string]string{
		"Accept":       "application/json, text/javascript, */*; q=0.01",
		"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
		"Origin":       "https://apps.coachingfederation.org",
		"Referer":      "https://apps.coachingfederation.org/",
	}, icfSearchBody(skip))
	if err != nil {
		return nil, err
	}
	if resp.status >= 400 {
		return nil, fmt.Errorf("HTTP %d for %s", resp.status, ICFSearchURL)
	}
	var pg icfSearchPage
	if err := json.Unmarshal(resp.body, &pg); err != nil {
		return nil, fmt.Errorf("search skip=%d: not a search response: %w", skip, err)
	}
	return &pg, nil
}

// ICFList fetches the country list and the whole coach search (take=1000 per
// page, sequentially — the server pages by skip) into icf_countries.json and
// icf_list.jsonl. The list is written to a .tmp file and renamed on success, so
// an interrupted run never leaves a truncated list.
func ICFList(ctx context.Context, dir string) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ExitErrors, err
	}
	c := newClient()

	resp, err := c.do(ctx, "POST", ICFBase+"OTI/Services/DataRepository.asmx/GetCountriesStatesRegions", map[string]string{
		"Content-Type": "application/json; charset=UTF-8", "X-Requested-With": "XMLHttpRequest",
	}, []byte("{}"))
	if err != nil {
		return listFailCode(ctx), err
	}
	if resp.status >= 400 {
		return ExitErrors, fmt.Errorf("HTTP %d for countries", resp.status)
	}
	var cresp struct {
		D json.RawMessage `json:"d"`
	}
	if err := json.Unmarshal(resp.body, &cresp); err != nil || len(cresp.D) == 0 {
		return ExitErrors, fmt.Errorf("countries: unexpected response")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, cresp.D); err != nil {
		return ExitErrors, err
	}
	if err := os.WriteFile(filepath.Join(dir, "icf_countries.json"), compact.Bytes(), 0o644); err != nil {
		return ExitErrors, err
	}

	tmpPath := filepath.Join(dir, "icf_list.jsonl.tmp")
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return ExitErrors, err
	}
	written, total, skip := 0, 0, 0
	keys := map[string]bool{}
	for {
		pg, err := c.icfSearch(ctx, skip)
		if err != nil {
			tmp.Close()
			return listFailCode(ctx), err
		}
		total = pg.ResultCount
		for _, raw := range pg.Results {
			var line bytes.Buffer
			if err := json.Compact(&line, raw); err != nil {
				tmp.Close()
				return ExitErrors, err
			}
			line.WriteByte('\n')
			if _, err := tmp.Write(line.Bytes()); err != nil {
				tmp.Close()
				return ExitErrors, err
			}
			var k struct {
				Key string `json:"key"`
			}
			_ = json.Unmarshal(raw, &k)
			keys[k.Key] = true
		}
		written += len(pg.Results)
		fmt.Fprintf(os.Stderr, "skip=%d got=%d resultCount=%d\n", skip, len(pg.Results), total)
		skip += icfPageSize
		if skip >= total {
			break
		}
		if len(pg.Results) == 0 {
			fmt.Fprintln(os.Stderr, "WARNING: empty page before reaching resultCount, stopping")
			break
		}
	}
	if err := tmp.Close(); err != nil {
		return ExitErrors, err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, "icf_list.jsonl")); err != nil {
		return ExitErrors, err
	}
	fmt.Printf("records written : %d\nUNIQUE keys     : %d\nserver resultCount: %d\n", written, len(keys), total)
	return ExitOK, nil
}

// listFailCode is the exit code of a sequential list that died: interrupted
// when the run was stopped, plain failure otherwise.
func listFailCode(ctx context.Context) int {
	if ctx.Err() != nil {
		return ExitInterrupted
	}
	return ExitErrors
}

// ICFProfiles fetches the profile page of every listed key not yet in
// icf_profiles.jsonl. limit caps the profiles attempted (0 = all). A redirect to
// errorpage.aspx or a 404 / 410 is stored as missing.
func ICFProfiles(ctx context.Context, dir string, workers, limit int) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ExitErrors, err
	}
	recs, err := icfUniqueList(dir)
	if err != nil {
		return ExitErrors, err
	}
	have, err := icfProfiles(dir)
	if err != nil {
		return ExitErrors, err
	}
	var todo []string
	for _, r := range recs {
		if _, ok := have[r.Key]; !ok {
			todo = append(todo, r.Key)
		}
	}
	if limit > 0 && len(todo) > limit {
		todo = todo[:limit]
	}
	fmt.Fprintf(os.Stderr, "%d already done, %d to fetch this run\n", len(have), len(todo))
	if len(todo) == 0 {
		return ExitOK, nil
	}
	profW, err := openAppend(filepath.Join(dir, "icf_profiles.jsonl"))
	if err != nil {
		return ExitErrors, err
	}
	defer profW.Close()
	errW, err := openAppend(filepath.Join(dir, "icf_profiles_errors.jsonl"))
	if err != nil {
		return ExitErrors, err
	}
	defer errW.Close()

	c := newClient()
	res := runPool(ctx, todo, workers, "icf-profiles", 200, func(ctx context.Context, key string) error {
		u := icfProfileURL(key)
		fail := func(msg string) error {
			_ = errW.write(struct {
				Key   string `json:"key"`
				URL   string `json:"url"`
				Error string `json:"error"`
			}{key, u, msg})
			return fmt.Errorf("%s: %s", key, msg)
		}
		resp, err := c.do(ctx, "GET", u, map[string]string{"Accept": "text/html,application/xhtml+xml"}, nil)
		if err == errStopped {
			return err
		}
		if err != nil {
			return fail(err.Error())
		}
		if strings.Contains(strings.ToLower(resp.url), "errorpage.aspx") || resp.status == 404 || resp.status == 410 {
			return profW.write(ICFProfile{Key: key, URL: u, Missing: true})
		}
		if resp.status >= 400 {
			return fail(fmt.Sprintf("HTTP %d for %s", resp.status, u))
		}
		fields := parseICFProfile(resp.body)
		if len(fields.labels) == 0 {
			return fail("page parsed with no fields (blocked or markup changed?)")
		}
		return profW.write(ICFProfile{
			Key: key, URL: u, Fields: fields, Email: fields.first("Email Address"),
			Phone: fields.first("Phone"), Website: fields.first("Web Site"), Location: fields.first("Location"),
		})
	})
	fmt.Fprintf(os.Stderr, "finished: %d processed, %d errors\n", res.done+res.errs, res.errs)
	return exitCode(res, ctx), nil
}

// ---------- join + export ----------

type icfEntry struct {
	rec  ICFRecord
	prof *ICFProfile // nil: no profile fetched
}

var (
	icfFixedCols = []string{"key", "full_name", "credential", "designations", "headline", "description",
		"standard_rate", "location", "country", "region", "email", "phone", "website"}
	icfTailCols = []string{"photo_url", "profile_url", "has_enhanced_profile", "profile_missing"}
)

func joinClean(vals []string) string {
	var out []string
	for _, v := range vals {
		if c := clean(v); c != "" {
			out = append(out, c)
		}
	}
	return strings.Join(out, "; ")
}

// location is the profile's location, else the listing's.
func (e icfEntry) location() string {
	if e.prof != nil {
		if l := clean(e.prof.Location); l != "" {
			return l
		}
	}
	return clean(e.rec.Location)
}

func (e icfEntry) fetched() bool { return e.prof != nil && !e.prof.Missing }

// ICFExport joins the list with the profiles and writes the CSV (UTF-8 with
// BOM). out defaults to <dir>/icf_coaches.csv.
func ICFExport(dir, out string) error {
	recs, err := icfUniqueList(dir)
	if err != nil {
		return err
	}
	countries, err := loadICFCountries(dir)
	if err != nil {
		return err
	}
	match := newCountryMatcher(countries)
	profs, err := icfProfiles(dir)
	if err != nil {
		return err
	}
	if out == "" {
		out = filepath.Join(dir, "icf_coaches.csv")
	}

	reserved := map[string]bool{}
	for _, c := range append(append([]string{}, icfFixedCols...), icfTailCols...) {
		reserved[c] = true
	}
	labelCols := map[string]string{} // slug -> column name
	var labelOrder []string          // first-seen order
	for _, r := range recs {
		p, ok := profs[r.Key]
		if !ok {
			continue
		}
		for _, label := range p.Fields.labels {
			if icfContactLabels[label] {
				continue
			}
			s := slug(label)
			if s == "" {
				s = "field"
			}
			if _, ok := labelCols[s]; !ok {
				col := s
				if reserved[s] {
					col = "f_" + s
				}
				labelCols[s] = col
				labelOrder = append(labelOrder, col)
			}
		}
	}
	columns := append(append(append([]string{}, icfFixedCols...), labelOrder...), icfTailCols...)

	w, err := newCSVWriter(out)
	if err != nil {
		return err
	}
	w.row(columns)
	base, _ := url.Parse(ICFBase)
	cells := make([]string, len(columns))
	for _, rec := range recs {
		var e icfEntry
		e.rec = rec
		if p, ok := profs[rec.Key]; ok {
			e.prof = &p
		}
		location := e.location()
		country, region := match.match(location)
		row := map[string]string{
			"key": rec.Key, "full_name": clean(rec.FullName), "credential": clean(rec.Credential),
			"designations": joinClean(rec.Designations), "headline": clean(rec.Headline),
			"description": clean(rec.Description), "standard_rate": clean(rec.StandardRate),
			"location": location, "country": country, "region": region,
			"profile_url":          icfProfileURL(rec.Key),
			"has_enhanced_profile": "false",
		}
		if rec.HasEnhancedProfile {
			row["has_enhanced_profile"] = "true"
		}
		if rec.PhotoURL != "" {
			if ref, err := url.Parse(rec.PhotoURL); err == nil {
				row["photo_url"] = base.ResolveReference(ref).String()
			}
		}
		switch {
		case e.prof != nil && e.prof.Missing:
			row["profile_missing"] = "true"
		case e.fetched():
			row["profile_missing"] = "false"
			row["email"], row["phone"], row["website"] = clean(e.prof.Email), clean(e.prof.Phone), clean(e.prof.Website)
		}
		if e.fetched() {
			merged := map[string][]string{}
			for _, label := range e.prof.Fields.labels {
				if icfContactLabels[label] {
					continue
				}
				s := slug(label)
				if s == "" {
					s = "field"
				}
				merged[labelCols[s]] = append(merged[labelCols[s]], e.prof.Fields.vals[label]...)
			}
			for col, vals := range merged {
				row[col] = joinClean(vals)
			}
		}
		for i, c := range columns {
			cells[i] = row[c]
		}
		w.row(cells)
	}
	if err := w.close(); err != nil {
		return err
	}
	fmt.Printf("rows: %d\n", len(recs))
	fmt.Printf("columns (%d): %v\n", len(columns), columns)
	return nil
}
