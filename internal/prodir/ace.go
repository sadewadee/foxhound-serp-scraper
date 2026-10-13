package prodir

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// ACE "Find an ACE Pro" directory. State files (dir):
//
//	ace_list.jsonl            one listing row per line, plus "_page" = the window start
//	ace_profiles.jsonl        {"id","url","tokens":[[kind,text,[[href,text],...]],...]} or {"id","url","missing":true}
//	ace_profiles_errors.jsonl {"id","url","error"}
//
// Profiles are stored as the raw token stream of the profile <article>;
// interpretation (parseTokens) happens at export / load time so parser fixes
// never require a re-crawl.

const acePage = 25 // rows per listing window

var aceSocialDomains = []string{"facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
	"linkedin.com", "youtube.com", "youtu.be", "tiktok.com", "pinterest.com", "snapchat.com", "threads.net"}

// Link is one <a> inside a profile text token.
type Link struct{ Href, Text string }

// Token is one element of the flattened profile article: kind is "h2" | "h3" |
// "h4" | "hr" | "text". It (un)marshals as [kind, text, [[href, text], ...]].
type Token struct {
	Kind, Text string
	Links      []Link
}

func (t Token) MarshalJSON() ([]byte, error) {
	links := make([][2]string, len(t.Links))
	for i, l := range t.Links {
		links[i] = [2]string{l.Href, l.Text}
	}
	return marshalNoEscape([]any{t.Kind, t.Text, links})
}

func (t *Token) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if len(raw) != 3 {
		return fmt.Errorf("token: want 3 elements, got %d", len(raw))
	}
	var links [][]string
	if err := json.Unmarshal(raw[0], &t.Kind); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &t.Text); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[2], &links); err != nil {
		return err
	}
	t.Links = nil
	for _, l := range links {
		if len(l) != 2 {
			return fmt.Errorf("token: link needs [href, text]")
		}
		t.Links = append(t.Links, Link{l[0], l[1]})
	}
	return nil
}

// ---------- profile tokenizer ----------

// aceTokenizer collects the profile <article> as a flat token list.
type aceTokenizer struct {
	found    bool
	depth    int // nesting depth inside the profile article
	tokens   []Token
	heading  string // h2/h3/h4 while inside one
	buf      strings.Builder
	links    []Link
	inLink   bool
	href     string
	linkText strings.Builder
	skip     int // inside script/style
}

func (p *aceTokenizer) flushText() {
	text := p.buf.String()
	if strings.TrimFunc(text, isPySpace) != "" || len(p.links) > 0 {
		p.tokens = append(p.tokens, Token{"text", text, p.links})
	}
	p.buf.Reset()
	p.links = nil
}

func (p *aceTokenizer) start(tag string, a map[string]string) {
	if p.depth == 0 {
		if tag == "article" && strings.Contains(a["class"], "ace-pro-profile__bio-container") {
			p.depth = 1
			p.found = true
		}
		return
	}
	switch tag {
	case "script", "style":
		p.skip++
	case "article":
		p.depth++
	case "h2", "h3", "h4":
		p.flushText()
		p.heading = tag
	case "hr":
		p.flushText()
		p.tokens = append(p.tokens, Token{Kind: "hr"})
	case "br", "p", "div", "li", "ul":
		p.buf.WriteString("\n")
	case "a":
		p.inLink, p.href = true, a["href"]
		p.linkText.Reset()
	}
}

func (p *aceTokenizer) end(tag string) {
	if p.depth == 0 {
		return
	}
	switch {
	case tag == "script" || tag == "style":
		p.skip = max(p.skip-1, 0)
	case tag == "article":
		p.depth--
		if p.depth == 0 {
			p.flushText()
		}
	case (tag == "h2" || tag == "h3" || tag == "h4") && p.heading == tag:
		p.tokens = append(p.tokens, Token{Kind: tag, Text: clean(p.buf.String())})
		p.buf.Reset()
		p.heading = ""
	case tag == "p" || tag == "div" || tag == "li":
		p.buf.WriteString("\n")
	case tag == "a" && p.inLink:
		if h := strings.TrimFunc(p.href, isPySpace); h != "" {
			p.links = append(p.links, Link{h, clean(p.linkText.String())})
		}
		p.inLink = false
	}
}

func (p *aceTokenizer) data(s string) {
	if p.depth == 0 || p.skip > 0 {
		return
	}
	p.buf.WriteString(s)
	if p.inLink {
		p.linkText.WriteString(s)
	}
}

// tokenizeACE returns the token stream of a profile page. A page without the
// profile article — notably the listing page ACE redirects removed profiles to —
// is an error.
func tokenizeACE(page []byte) ([]Token, error) {
	p := &aceTokenizer{}
	feedHTML(page, p)
	if !p.found {
		return nil, fmt.Errorf("profile block not found")
	}
	if p.tokens == nil {
		p.tokens = []Token{}
	}
	return p.tokens, nil
}

// ---------- token interpretation ----------

type kv struct{ k, v string }

// aceSection is an insertion-ordered label -> value map ("... Specific Information").
type aceSection struct {
	name  string
	items []kv
}

func (s *aceSection) set(label, value string) {
	for i := range s.items {
		if s.items[i].k == label {
			s.items[i].v = value
			return
		}
	}
	s.items = append(s.items, kv{label, value})
}

// ACEProfile is the interpreted profile (Python parse_tokens output).
type ACEProfile struct {
	Address, PostalCode, Phone, Website, About, Credentials, YearsAsPro string
	ProgramsAndRates, YearsInIndustry, AreasOfFocus, Background         string
	SocialLinks, ContactURL                                             string
	Socials                                                             []string
	Sections                                                            []*aceSection
}

func (p *ACEProfile) section(name string) *aceSection {
	for _, s := range p.Sections {
		if s.name == name {
			return s
		}
	}
	s := &aceSection{name: name}
	p.Sections = append(p.Sections, s)
	return s
}

var aceTopLabels = map[string]string{
	"about": "about", "ace credentials": "credentials", "years as an ace pro": "years_as_ace_pro",
	"programs and rates": "programs_and_rates", "years in the industry": "years_in_industry",
	"areas of focus": "areas_of_focus",
}

var (
	aceURLRe       = regexp.MustCompile(`(?i)^(https?://|www\.)`)
	aceCorporateRe = regexp.MustCompile(`/(acefitness|americancouncilonexercise|ace-fitness)\b|acefitness\.org`)
	aceSchemeRe    = regexp.MustCompile(`^[a-z]+://`)
	aceCountryTok  = regexp.MustCompile(`^[A-Z]{2,3}$`)
)

func normValue(s string) string {
	s = clean(s)
	if l := strings.ToLower(s); l == "unspecified" || l == "none" {
		return ""
	}
	return s
}

func linesOf(text string) []string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		if c := clean(l); c != "" {
			out = append(out, c)
		}
	}
	return out
}

func isSocial(href string) bool {
	h := strings.ToLower(href)
	host := strings.SplitN(aceSchemeRe.ReplaceAllString(h, ""), "/", 2)[0]
	for _, d := range aceSocialDomains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

// isPhoneLine is ^\+?[\d\s().\-x]{7,}$ with Python's Unicode \d and \s,
// plus the "at least 7 digits" check.
func isPhoneLine(ln string) bool {
	s := strings.TrimPrefix(ln, "+")
	n, digits := 0, 0
	for _, r := range s {
		n++
		switch {
		case unicode.IsDigit(r):
			digits++
		case isPySpace(r), r == '(', r == ')', r == '.', r == '-', r == 'x':
		default:
			return false
		}
	}
	return n >= 7 && digits >= 7
}

// splitAddress extracts the postal code from "WEST NYACK, NY US 10994" ->
// "10994", using the listing's state / country when given.
func splitAddress(line, country, state string) string {
	tail := line
	if i := strings.LastIndex(line, ","); i >= 0 {
		tail = line[i+1:]
	}
	rest := strings.Fields(tail)
	if st := strings.Fields(strings.ToLower(state)); len(st) > 0 && len(rest) >= len(st) {
		match := true
		for i, t := range st {
			if strings.ToLower(rest[i]) != t {
				match = false
				break
			}
		}
		if match {
			rest = rest[len(st):]
		}
	}
	if country != "" && len(rest) > 0 && strings.EqualFold(rest[0], country) {
		return strings.Join(rest[1:], " ")
	}
	toks := strings.Fields(line) // fallback: rightmost bare 2-3 letter caps token is the country
	for i := len(toks) - 1; i >= 0; i-- {
		if aceCountryTok.MatchString(toks[i]) {
			return strings.Join(toks[i+1:], " ")
		}
	}
	return ""
}

// parseTokens interprets a token stream (Python parse_tokens).
func parseTokens(toks []Token) *ACEProfile {
	out := &ACEProfile{}
	var (
		hasSection, hasLabel   bool
		sect                   *aceSection
		label                  string
		inBackground, seenName bool
		headDone               bool   // the contact block ends at the first <hr>
		phoneFallback          string // odd-format contact line, used only if no line looks like a phone
	)
	addSocial := func(href string) {
		if href == "" || !isSocial(href) || aceCorporateRe.MatchString(strings.ToLower(href)) {
			return
		}
		for _, s := range out.Socials {
			if s == href {
				return
			}
		}
		out.Socials = append(out.Socials, href)
	}

	for _, t := range toks {
		switch t.Kind {
		case "h2":
			if !seenName {
				seenName = true
			} else {
				inBackground = strings.ToLower(clean(t.Text)) == "background"
			}
			hasLabel, hasSection = false, false
			continue
		case "h3":
			hasSection, hasLabel = true, false
			sect = out.section(t.Text)
			continue
		case "h4":
			hasLabel, label = true, t.Text
			continue
		case "hr":
			headDone = true
			continue
		}

		for _, l := range t.Links {
			addSocial(l.Href)
		}
		lines := linesOf(t.Text)

		if !headDone && (!hasLabel || strings.ToLower(label) == "profile") {
			// contact block under "Profile": address / phone / website
			anySocial := false
			for _, l := range t.Links {
				if isSocial(l.Href) {
					anySocial = true
				}
			}
			for _, ln := range lines {
				switch {
				case isPhoneLine(ln):
					if out.Phone == "" {
						out.Phone = ln
					}
				case aceURLRe.MatchString(ln):
					if !anySocial && out.Website == "" {
						out.Website = ln
					}
				case out.Address == "":
					out.Address = ln
				case phoneFallback == "":
					phoneFallback = ln
				}
			}
			for _, l := range t.Links {
				h := strings.ToLower(l.Href)
				if (strings.HasPrefix(h, "http://") || strings.HasPrefix(h, "https://")) && !isSocial(l.Href) && out.Website == "" {
					out.Website = l.Href
				}
			}
			continue
		}

		if !hasLabel {
			if inBackground && !hasSection {
				out.Background = clean(strings.Join(append([]string{out.Background}, lines...), " "))
			}
			continue
		}

		prose := strings.ToLower(label) == "about" && !hasSection // paragraphs, not a list
		sep := "; "
		if prose {
			sep = "  "
		}
		value := normValue(strings.Join(lines, sep))
		if hasSection {
			sect.set(label, value)
			continue
		}
		key, ok := aceTopLabels[strings.ToLower(label)]
		if !ok {
			out.section("Profile").set(label, value)
			continue
		}
		switch key {
		case "programs_and_rates":
			for _, l := range t.Links {
				if strings.HasPrefix(strings.ToLower(l.Text), "contact") && l.Href != "" &&
					!strings.HasPrefix(l.Href, "#") && !strings.HasPrefix(l.Href, "javascript") {
					out.ContactURL = l.Href
				}
			}
			if strings.ToLower(value) == "contact me" {
				value = ""
			}
		case "areas_of_focus":
			if strings.HasPrefix(strings.ToLower(value), "contact me for more information") {
				value = "" // site filler shown when the pro picked no focus areas
			}
		}
		switch key {
		case "about":
			out.About = value
		case "credentials":
			out.Credentials = value
		case "years_as_ace_pro":
			out.YearsAsPro = value
		case "programs_and_rates":
			out.ProgramsAndRates = value
		case "years_in_industry":
			out.YearsInIndustry = value
		case "areas_of_focus":
			out.AreasOfFocus = value
		}
	}

	if out.Phone == "" {
		out.Phone = phoneFallback
	}
	out.PostalCode = splitAddress(out.Address, "", "")
	out.SocialLinks = strings.Join(out.Socials, "; ")
	return out
}

// ---------- state records ----------

// aceListRec is one listing row. Values are decoded with UseNumber so numbers
// survive untouched.
type aceListRec map[string]any

func decodeACEListRec(line []byte) (aceListRec, error) {
	var r aceListRec
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	if err := dec.Decode(&r); err != nil {
		return nil, err
	}
	return r, nil
}

// str is Python's str(v) with None -> "".
func (r aceListRec) str(k string) string {
	switch v := r[k].(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	case bool:
		if v {
			return "True"
		}
		return "False"
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// aceUniqueList returns the listing rows deduplicated by EncryptedCustomerNumber
// (first occurrence wins), in file order.
func aceUniqueList(dir string) (ids []string, recs map[string]aceListRec, err error) {
	recs = map[string]aceListRec{}
	err = readJSONL(filepath.Join(dir, "ace_list.jsonl"), func(line []byte) {
		r, e := decodeACEListRec(line)
		if e != nil {
			return
		}
		id := r.str("EncryptedCustomerNumber")
		if id == "" {
			return
		}
		if _, seen := recs[id]; !seen {
			recs[id] = r
			ids = append(ids, id)
		}
	})
	return
}

type aceProfileRec struct {
	ID      string   `json:"id"`
	URL     string   `json:"url"`
	Tokens  *[]Token `json:"tokens"`
	Missing bool     `json:"missing"`
}

// aceProfiles reads ace_profiles.jsonl; the last record per id wins.
func aceProfiles(dir string) (map[string]aceProfileRec, error) {
	out := map[string]aceProfileRec{}
	err := readJSONL(filepath.Join(dir, "ace_profiles.jsonl"), func(line []byte) {
		var r aceProfileRec
		if json.Unmarshal(line, &r) == nil && r.ID != "" {
			out[r.ID] = r
		}
	})
	return out, err
}

// ---------- crawl ----------

func aceAPIHeaders() map[string]string {
	return map[string]string{
		"X-Requested-With": "XMLHttpRequest",
		"Content-Type":     "application/json",
		"Accept":           "application/json, text/plain, */*",
		"Accept-Language":  "en-US,en;q=0.9",
		"Referer":          ACEBase + "/resources/everyone/find-ace-pro/",
	}
}

var aceHTMLHeaders = map[string]string{
	"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	"Accept-Language": "en-US,en;q=0.9",
}

type aceWindow struct {
	TotalResults json.RawMessage   `json:"TotalResults"`
	Results      []json.RawMessage `json:"Results"`
}

func (c *client) aceFetchWindow(ctx context.Context, lower int) (*aceWindow, int, error) {
	u := fmt.Sprintf("%s/api/FindACEProByName/?lowerLimit=%d&upperLimit=%d&firstName=_&lastName=_",
		ACEBase, lower, lower+acePage-1)
	resp, err := c.do(ctx, "GET", u, aceAPIHeaders(), nil)
	if err != nil {
		return nil, 0, err
	}
	if resp.status != 200 {
		return nil, 0, fmt.Errorf("HTTP %d for %s", resp.status, u)
	}
	var w aceWindow
	if err := json.Unmarshal(resp.body, &w); err != nil {
		return nil, 0, fmt.Errorf("window %d: not a listing response: %w", lower, err)
	}
	f, err := strconv.ParseFloat(strings.Trim(string(w.TotalResults), `" `), 64)
	if err != nil {
		return nil, 0, fmt.Errorf("window %d: bad TotalResults %q", lower, w.TotalResults)
	}
	return &w, int(f), nil
}

// withPage appends "_page": n to a raw JSON object, leaving every other byte
// of the API row as received.
func withPage(raw json.RawMessage, n int) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if len(b) < 2 || b[0] != '{' || b[len(b)-1] != '}' {
		return nil, fmt.Errorf("listing row is not an object")
	}
	sep := ","
	if len(b) == 2 {
		sep = ""
	}
	return append(b[:len(b)-1:len(b)-1], []byte(fmt.Sprintf(`%s"_page":%d}`, sep, n))...), nil
}

func writeWindow(a *appender, w *aceWindow, lower int) error {
	var buf bytes.Buffer
	for _, raw := range w.Results {
		line, err := withPage(raw, lower)
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return a.writeBytes(buf.Bytes())
}

// ACEList fetches the listing in 25-row windows into ace_list.jsonl, resuming
// from the windows already present. limit caps the windows fetched (0 = all).
// It returns the process exit code.
func ACEList(ctx context.Context, dir string, workers, limit int) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ExitErrors, err
	}
	path := filepath.Join(dir, "ace_list.jsonl")
	donePages := map[int]bool{}
	if err := readJSONL(path, func(line []byte) {
		var r struct {
			Page *int `json:"_page"`
		}
		if json.Unmarshal(line, &r) == nil && r.Page != nil {
			donePages[*r.Page] = true
		}
	}); err != nil {
		return ExitErrors, err
	}
	out, err := openAppend(path)
	if err != nil {
		return ExitErrors, err
	}
	defer out.Close()

	c := newClient()
	first, total, err := c.aceFetchWindow(ctx, 1) // also gives TotalResults
	if err != nil {
		return ExitErrors, err
	}
	fetched := 0
	if !donePages[1] {
		if err := writeWindow(out, first, 1); err != nil {
			return ExitErrors, err
		}
		donePages[1] = true
		fetched = 1
	}

	var todo []int
	for lo := 1; lo <= total; lo += acePage {
		if !donePages[lo] {
			todo = append(todo, lo)
		}
	}
	if limit > 0 {
		todo = todo[:min(len(todo), max(limit-fetched, 0))]
	}

	res := runPool(ctx, todo, workers, "ace-list", 50, func(ctx context.Context, lo int) error {
		w, _, err := c.aceFetchWindow(ctx, lo)
		if err != nil {
			return err
		}
		return writeWindow(out, w, lo)
	})
	fetched += res.done

	ids, _, err := aceUniqueList(dir)
	if err != nil {
		return ExitErrors, err
	}
	nrows := 0
	_ = readJSONL(path, func(line []byte) {
		if json.Valid(line) {
			nrows++
		}
	})
	fmt.Printf("windows fetched this run: %d (errors: %d)\n", fetched, res.errs)
	fmt.Printf("records in file: %d\n", nrows)
	fmt.Printf("unique ids in file: %d\n", len(ids))
	fmt.Printf("server TotalResults: %d\n", total)
	return exitCode(res, ctx), nil
}

// ACEProfiles fetches the profile page of every listed id not yet in
// ace_profiles.jsonl. limit caps the profiles attempted (0 = all). Removed
// profiles (404 / 410) are stored as missing; pages without a profile block —
// ACE answers a removed profile with a 302 to the listing page — go to
// ace_profiles_errors.jsonl and count as failures.
func ACEProfiles(ctx context.Context, dir string, workers, limit int) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ExitErrors, err
	}
	ids, recs, err := aceUniqueList(dir)
	if err != nil {
		return ExitErrors, err
	}
	have, err := aceProfiles(dir)
	if err != nil {
		return ExitErrors, err
	}
	var todo []string
	for _, id := range ids {
		if _, ok := have[id]; !ok {
			todo = append(todo, id)
		}
	}
	if limit > 0 && len(todo) > limit {
		todo = todo[:limit]
	}
	profW, err := openAppend(filepath.Join(dir, "ace_profiles.jsonl"))
	if err != nil {
		return ExitErrors, err
	}
	defer profW.Close()
	errW, err := openAppend(filepath.Join(dir, "ace_profiles_errors.jsonl"))
	if err != nil {
		return ExitErrors, err
	}
	defer errW.Close()

	c := newClient()
	type idURL struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	res := runPool(ctx, todo, workers, "ace-profiles", 200, func(ctx context.Context, id string) error {
		u := ACEBase + recs[id].str("ProfileURL")
		fail := func(msg string) error {
			if err := errW.write(struct {
				idURL
				Error string `json:"error"`
			}{idURL{id, u}, msg}); err != nil {
				slog.Error("prodir: write errors file", "error", err)
			}
			return fmt.Errorf("%s: %s", id, msg)
		}
		resp, err := c.do(ctx, "GET", u, aceHTMLHeaders, nil)
		if err == errStopped {
			return err
		}
		if err != nil {
			return fail(err.Error())
		}
		switch {
		case resp.status == 404 || resp.status == 410:
			return profW.write(struct {
				idURL
				Missing bool `json:"missing"`
			}{idURL{id, u}, true})
		case resp.status != 200:
			return fail(fmt.Sprintf("HTTP %d for %s", resp.status, u))
		}
		toks, err := tokenizeACE(resp.body)
		if err != nil {
			return fail(fmt.Sprintf("%v (final url: %s, status %d)", err, resp.url, resp.status))
		}
		return profW.write(struct {
			idURL
			Tokens []Token `json:"tokens"`
		}{idURL{id, u}, toks})
	})
	fmt.Printf("profiles written this run: %d, errors: %d, still pending: %d\n",
		res.done, res.errs, len(todo)-res.done-res.errs)
	fmt.Printf("unique ids in list: %d\n", len(ids))
	return exitCode(res, ctx), nil
}

// ---------- join + export ----------

// aceEntry is one listing row joined with its (optional) interpreted profile.
type aceEntry struct {
	id      string
	list    aceListRec
	prof    *ACEProfile // nil: no profile fetched, or fetched as missing
	missing bool
}

// loadACE joins the listing with the profiles in listing order.
func loadACE(dir string) ([]aceEntry, error) {
	ids, recs, err := aceUniqueList(dir)
	if err != nil {
		return nil, err
	}
	profs, err := aceProfiles(dir)
	if err != nil {
		return nil, err
	}
	entries := make([]aceEntry, 0, len(ids))
	for _, id := range ids {
		e := aceEntry{id: id, list: recs[id]}
		if p, ok := profs[id]; ok {
			e.missing = p.Missing
			if p.Tokens != nil {
				e.prof = parseTokens(*p.Tokens)
			}
		}
		entries = append(entries, e)
	}
	return entries, nil
}

var (
	aceFixedHead = []string{"id", "full_name", "first_name", "middle_name", "last_name", "suffix", "city",
		"state", "country", "postal_code", "phone", "website", "credentials",
		"years_ace_certified", "years_in_industry", "areas_of_focus", "about",
		"background", "programs_and_rates", "social_links", "contact_url", "profile_address"}
	aceFixedTail = []string{"profile_url", "profile_image_url", "profile_missing"}
	slugRe       = regexp.MustCompile(`[^a-z0-9]+`)
)

func slug(s string) string {
	return strings.Trim(slugRe.ReplaceAllString(strings.ToLower(s), "_"), "_")
}

// firstNonEmpty is Python's `a or b` for strings.
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// zeroAsEmpty maps a numeric 0 to "" (falsy in Python's `x or y`).
func zeroAsEmpty(s string) string {
	if s == "0" {
		return ""
	}
	return s
}

// aceRow builds one export row (column -> value, uncleaned).
func aceRow(e aceEntry) map[string]string {
	l := e.list
	p := e.prof
	if p == nil {
		p = &ACEProfile{}
	}
	row := map[string]string{
		"id": e.id, "full_name": l.str("FullName"), "first_name": l.str("FirstName"),
		"middle_name": l.str("MiddleName"), "last_name": l.str("LastName"), "suffix": l.str("Suffix"),
		"city": l.str("City"), "state": l.str("StateCode"), "country": l.str("CountryName"),
		"years_ace_certified": firstNonEmpty(zeroAsEmpty(l.str("YearsACECertified")), p.YearsAsPro), // listing value is always null
		"credentials":         firstNonEmpty(p.Credentials, l.str("CertDisplay")),
		"about":               firstNonEmpty(p.About, clean(l.str("DescriptionForWeb"))),
		"profile_image_url":   l.str("ProfileImageURL"),
		"postal_code":         splitAddress(p.Address, l.str("CountryName"), l.str("StateCode")),
		"phone":               p.Phone, "website": p.Website, "years_in_industry": p.YearsInIndustry,
		"areas_of_focus": p.AreasOfFocus, "background": p.Background,
		"programs_and_rates": p.ProgramsAndRates, "social_links": p.SocialLinks,
		"contact_url": p.ContactURL, "profile_address": p.Address,
	}
	if u := l.str("ProfileURL"); u != "" {
		row["profile_url"] = ACEBase + u
	}
	if e.missing {
		row["profile_missing"] = "true"
	}
	for _, s := range p.Sections {
		for _, it := range s.items {
			row[slug(s.name)+"__"+slug(it.k)] = it.v
		}
	}
	return row
}

// ACEExport joins the listing with the profiles and writes the CSV (UTF-8 with
// BOM). out defaults to <dir>/ace_pros.csv.
func ACEExport(dir, out string) error {
	entries, err := loadACE(dir)
	if err != nil {
		return err
	}
	if out == "" {
		out = filepath.Join(dir, "ace_pros.csv")
	}
	rows := make([]map[string]string, len(entries))
	fixed := map[string]bool{}
	for _, c := range append(append([]string{}, aceFixedHead...), aceFixedTail...) {
		fixed[c] = true
	}
	extra := map[string]bool{}
	for i, e := range entries {
		rows[i] = aceRow(e)
		for k := range rows[i] {
			if !fixed[k] {
				extra[k] = true
			}
		}
	}
	var sectionCols []string
	for k := range extra {
		sectionCols = append(sectionCols, k)
	}
	sort.Strings(sectionCols)
	cols := append(append(append([]string{}, aceFixedHead...), sectionCols...), aceFixedTail...)

	w, err := newCSVWriter(out)
	if err != nil {
		return err
	}
	w.row(cols)
	cells := make([]string, len(cols))
	for _, r := range rows {
		for i, c := range cols {
			cells[i] = clean(r[c])
		}
		w.row(cells)
	}
	if err := w.close(); err != nil {
		return err
	}
	fmt.Printf("wrote %s: %d rows\n", out, len(rows))
	fmt.Println("columns:", strings.Join(cols, ", "))
	return nil
}
