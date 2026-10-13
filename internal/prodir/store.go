package prodir

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"github.com/lib/pq"

	appdb "github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/query"
)

// Listing is one person as written to business_listings. Empty strings mean
// "no value" (stored as NULL, and never allowed to blank an existing value).
type Listing struct {
	Domain, URL, Name, Category, Niche string
	Website, Address, Location, City   string
	Description                        string
	RawCountry, Country, CountryCode   string // Country/CountryCode empty = unresolved
	Social                             map[string]string
	Emails                             []string // lower-cased
	Phones                             []string // primary first
	OwnWebsite                         bool
	Meta                               map[string][]string // business_meta: key -> values
}

// addString appends v unless it is already in list.
func addString(list []string, v string) []string {
	for _, have := range list {
		if have == v {
			return list
		}
	}
	return append(list, v)
}

// addPhones appends the non-empty phones whose digits are not in list yet.
func addPhones(list []string, ps ...string) []string {
	for _, p := range ps {
		if p = clean(p); p != "" {
			list = addPhone(list, p)
		}
	}
	return list
}

// sameSite compares two URLs ignoring scheme, "www." and a trailing slash.
func sameSite(a, b string) bool {
	norm := func(u string) string {
		u = strings.ToLower(aceSchemeRe.ReplaceAllString(strings.ToLower(clean(u)), ""))
		return strings.TrimSuffix(strings.TrimPrefix(u, "www."), "/")
	}
	return norm(a) == norm(b)
}

// mergeSocial fills the platforms of dst that are still empty from src, never
// with the directory's own pages.
func mergeSocial(dst, src map[string]string) {
	for plat, u := range src {
		if dst[plat] == "" && !aceCorporateRe.MatchString(strings.ToLower(u)) {
			dst[plat] = u
		}
	}
}

var aceHTTPRe = regexp.MustCompile(`(?i)^https?://`)

// aceWebsiteField reads the ACE Website value. The site prepends "http://" to
// whatever was typed, so it can hold a handle, a profile page, two sites, an
// email or no site at all. A value that already is one plain URL is the own
// website as is; anything else is scanned like free text. A site only counts
// when nothing but glued junk comes before the first one ("fixture fitness.com"
// is not fitness.com); the first site found is the own website, f carries the
// rest plus any emails and social links.
func aceWebsiteField(v string) (own string, f found) {
	v = clean(v)
	if v == "" {
		return "", found{}
	}
	if !strings.ContainsAny(v, " \t@,;") && validHost(urlHost(v)) && socialPlatform(v) == "" {
		return v, found{}
	}
	rest := aceHTTPRe.ReplaceAllString(v, "")
	if len(rest) > 4 && strings.EqualFold(rest[:5], "www. ") {
		rest = "www." + strings.TrimLeft(rest[5:], " ") // a lone "www. " before the domain
	}
	f = extractContacts(rest)
	first, _, _ := strings.Cut(rest, " ")
	if len(extractContacts(first).Websites) == 0 || strings.HasPrefix(rest, "@") {
		f.Websites = nil
	}
	if len(f.Websites) > 0 {
		own, f.Websites = f.Websites[0], f.Websites[1:]
	}
	return own, f
}

// setWebsites decides the website column: only the structured own site, else
// Website stays the profile URL. Every site found in free text goes to the
// other_websites meta key, except the own site itself: a bio that mentions a
// magazine is not the pro's website.
func (l *Listing) setWebsites(own string, sites []string) {
	own = clean(own)
	if own != "" {
		l.Website, l.OwnWebsite = withScheme(own), true
	}
	for _, w := range sites {
		if own == "" || !sameSite(w, own) {
			addMeta(l.Meta, "other_websites", w)
		}
	}
}

// addMeta appends vals under key: cleaned, empties dropped, duplicates (within
// the key) skipped, source order kept.
func addMeta(m map[string][]string, key string, vals ...string) {
	for _, v := range vals {
		v = clean(v)
		if v == "" {
			continue
		}
		dup := false
		for _, have := range m[key] {
			if have == v {
				dup = true
				break
			}
		}
		if !dup {
			m[key] = append(m[key], v)
		}
	}
}

// splitMeta splits s on every separator in seps.
func splitMeta(s string, seps ...string) []string {
	parts := []string{s}
	for _, sep := range seps {
		var next []string
		for _, p := range parts {
			next = append(next, strings.Split(p, sep)...)
		}
		parts = next
	}
	return parts
}

// aceMetaKeys are the aceRow columns stored as meta besides the section columns.
var aceMetaKeys = []string{"years_ace_certified", "years_in_industry", "areas_of_focus", "programs_and_rates"}

// aceMeta is the business_meta of one ACE entry, built from its export row:
// credential (the row column credentials, split on ";" and ","), the columns of
// aceMetaKeys and every section column (split on ";").
func aceMeta(e aceEntry) map[string][]string {
	row := aceRow(e)
	m := map[string][]string{}
	addMeta(m, "credential", splitMeta(row["credentials"], ";", ",")...)
	fixed := map[string]bool{}
	for _, c := range append(append([]string{}, aceFixedHead...), aceFixedTail...) {
		fixed[c] = true
	}
	keys := append([]string{}, aceMetaKeys...)
	var sections []string
	for k := range row {
		if !fixed[k] {
			sections = append(sections, k)
		}
	}
	sort.Strings(sections)
	for _, k := range append(keys, sections...) {
		addMeta(m, k, splitMeta(row[k], ";")...)
	}
	return m
}

// icfMeta is the business_meta of one ICF entry: the list-level credential,
// designations and standard_rate, plus every non-contact profile label of a
// fetched profile under its export column name (labels sharing it are merged).
func icfMeta(e icfEntry) map[string][]string {
	m := map[string][]string{}
	addMeta(m, "credential", e.rec.Credential)
	addMeta(m, "designations", e.rec.Designations...)
	addMeta(m, "standard_rate", e.rec.StandardRate)
	if e.fetched() {
		for _, label := range e.prof.Fields.labels {
			if icfContactLabels[label] {
				continue
			}
			addMeta(m, icfLabelColumn(label), e.prof.Fields.vals[label]...)
		}
	}
	return m
}

const (
	aceCategory = "acefitness"
	icfCategory = "icfcoaching"
	loadWorkers = 8
)

// ---------- country resolution ----------

// countryResolver maps a raw country value to (canonical name, ISO-2 code).
// Invariant #6: a code is only ever taken from the curated lookup — an ISO-2
// code must be in the whitelist, a name must match a canonical name or alias.
type countryResolver struct {
	names map[string]string // code -> canonical name
	codes map[string]string // normalized name / alias -> code
}

func normName(s string) string { return strings.ToLower(clean(s)) }

func newCountryResolver() *countryResolver {
	r := &countryResolver{names: map[string]string{}, codes: map[string]string{}}
	for _, c := range query.CountryRows() {
		r.names[c.Code] = c.Name
		r.codes[normName(c.Name)] = c.Code
	}
	add := func(m map[string]string) {
		for alias, code := range m {
			if _, ok := r.names[code]; ok {
				r.codes[normName(alias)] = code
			}
		}
	}
	add(query.CountryAliases())
	return r
}

func (r *countryResolver) resolve(raw string) (name, code string) {
	raw = clean(raw)
	if raw == "" {
		return "", ""
	}
	if u := strings.ToUpper(raw); len(u) == 2 {
		if n, ok := r.names[u]; ok {
			return n, u
		}
	}
	if c, ok := r.codes[normName(raw)]; ok {
		return r.names[c], c
	}
	return "", ""
}

// ---------- source -> Listing ----------

// withScheme prefixes http:// to an own website written without a scheme
// ("www.x.com", "x.com/path"); every other writer of the column stores full URLs.
func withScheme(w string) string {
	if l := strings.ToLower(w); strings.HasPrefix(l, "http://") || strings.HasPrefix(l, "https://") {
		return w
	}
	return "http://" + w
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}

// aceNiche is "fitness", or "coaching" for pure Health Coaches.
func aceNiche(credentials string) string {
	if containsFold(credentials, "Health Coach") &&
		!containsFold(credentials, "Personal Trainer") &&
		!containsFold(credentials, "Group Fitness") &&
		!containsFold(credentials, "Medical Exercise") {
		return "coaching"
	}
	return "fitness"
}

// socialPlatform names the social_links key of a profile link.
func socialPlatform(href string) string {
	h := strings.ToLower(href)
	host := strings.SplitN(aceSchemeRe.ReplaceAllString(h, ""), "/", 2)[0]
	for _, p := range []struct{ domain, name string }{
		{"facebook.com", "facebook"}, {"fb.com", "facebook"}, {"instagram.com", "instagram"},
		{"twitter.com", "twitter"}, {"x.com", "twitter"}, {"linkedin.com", "linkedin"},
		{"youtube.com", "youtube"}, {"youtu.be", "youtube"}, {"tiktok.com", "tiktok"},
		{"pinterest.com", "pinterest"}, {"snapchat.com", "snapchat"}, {"threads.net", "threads"},
	} {
		if host == p.domain || strings.HasSuffix(host, "."+p.domain) {
			return p.name
		}
	}
	return ""
}

func aceListings(dir string, res *countryResolver) ([]Listing, error) {
	entries, err := loadACE(dir)
	if err != nil {
		return nil, err
	}
	out := make([]Listing, 0, len(entries))
	for _, e := range entries {
		l := e.list
		p := e.prof
		if p == nil {
			p = &ACEProfile{}
		}
		profileURL := ACEBase + l.str("ProfileURL")
		name := clean(l.str("FullName"))
		city, state := clean(l.str("City")), clean(l.str("StateCode"))
		var loc []string
		for _, s := range []string{city, state} {
			if s != "" {
				loc = append(loc, s)
			}
		}
		social := map[string]string{}
		for _, href := range p.Socials {
			if k := socialPlatform(href); k != "" && social[k] == "" {
				social[k] = href
			}
		}
		x := Listing{
			Domain: strings.ToLower(e.id) + ".pro.acefitness.org", URL: profileURL,
			Name: name, Category: aceCategory,
			Niche:   aceNiche(firstNonEmpty(p.Credentials, l.str("CertDisplay"))),
			Website: profileURL,
			Address: p.Address, Location: strings.Join(loc, ", "), City: city,
			Description: firstNonEmpty(p.About, clean(l.str("DescriptionForWeb"))),
			RawCountry:  clean(l.str("CountryName")), Social: social,
			Meta: aceMeta(e),
		}

		// Everything the pro wrote anywhere in the profile: text and link targets.
		// The Website field is scanned on its own below, not as free text.
		field := clean(p.Website)
		var text strings.Builder
		for _, t := range e.toks {
			for _, ln := range strings.Split(t.Text, "\n") {
				if field == "" || clean(ln) != field {
					text.WriteString(ln + "\n")
				}
			}
			for _, lk := range t.Links {
				if field == "" || clean(lk.Href) != field {
					text.WriteString(lk.Href + "\n")
				}
			}
		}
		text.WriteString(clean(l.str("DescriptionForWeb")))
		fd := extractContacts(text.String())

		x.Emails = fd.Emails
		x.Phones = addPhones(nil, structuredPhone(p.Phone))
		x.Phones = addPhones(x.Phones, fd.Phones...)
		own, wf := aceWebsiteField(p.Website)
		for _, em := range wf.Emails {
			x.Emails = addString(x.Emails, em)
		}
		mergeSocial(social, wf.Social)
		mergeSocial(social, fd.Social)
		x.setWebsites(own, append(wf.Websites, fd.Websites...))
		address := strings.ToLower(clean(p.Address))
		for _, a := range fd.Addresses {
			if !strings.Contains(address, strings.ToLower(a)) { // already in the contact block
				addMeta(x.Meta, "street_address", a)
			}
		}
		x.Country, x.CountryCode = res.resolve(x.RawCountry)
		out = append(out, x)
	}
	return out, nil
}

func icfListings(dir string, res *countryResolver) ([]Listing, error) {
	recs, err := icfUniqueList(dir)
	if err != nil {
		return nil, err
	}
	countries, err := loadICFCountries(dir)
	if err != nil {
		return nil, err
	}
	match := newCountryMatcher(countries)
	profs, err := icfProfiles(dir)
	if err != nil {
		return nil, err
	}
	out := make([]Listing, 0, len(recs))
	for _, rec := range recs {
		e := icfEntry{rec: rec}
		if p, ok := profs[rec.Key]; ok {
			e.prof = &p
		}
		profileURL := icfProfileURL(rec.Key)
		location := e.location()
		matched, _ := match.match(location)

		x := Listing{
			Domain: strings.ToLower(rec.Key) + ".coach.coachingfederation.org", URL: profileURL,
			Name: clean(rec.FullName), Category: icfCategory, Niche: "coaching",
			Website: profileURL, Location: location, City: icfCity(location, matched),
			Description: firstNonEmpty(clean(rec.Description), clean(rec.Headline)),
			RawCountry:  matched, Social: map[string]string{},
			Meta: icfMeta(e),
		}
		var own string
		var fieldText []string
		if e.fetched() {
			x.Phones = splitICFPhone(e.prof.Phone)
			if w := clean(e.prof.Website); socialPlatform(w) != "" {
				if socialProfile(w) { // a profile link typed into Web Site
					x.Social[socialPlatform(w)] = withScheme(w)
				}
			} else {
				own = w
			}
			// The Email field is authoritative: its address stays even at the
			// directory's own domain.
			x.Emails = structuredEmails(e.prof.Email)
			for _, label := range e.prof.Fields.labels {
				fieldText = append(fieldText, e.prof.Fields.vals[label]...)
			}
		}
		for _, em := range extractContacts(strings.Join(fieldText, "\n")).Emails {
			x.Emails = addString(x.Emails, em)
		}

		// The list-level description / headline are free text too.
		fd := extractContacts(rec.Description + "\n" + rec.Headline)
		for _, em := range fd.Emails {
			x.Emails = addString(x.Emails, em)
		}
		x.Phones = addPhones(x.Phones, fd.Phones...)
		mergeSocial(x.Social, fd.Social)
		x.setWebsites(own, fd.Websites)
		x.Country, x.CountryCode = res.resolve(matched)
		out = append(out, x)
	}
	return out, nil
}

// icfCity is the first comma part of the location once the matched country
// suffix is removed ("Colorado Spring, CO UNITED STATES" -> "Colorado Spring").
func icfCity(location, country string) string {
	loc := clean(location)
	if country != "" {
		r := []rune(loc)
		if n := len([]rune(country)); n <= len(r) {
			loc = string(r[:len(r)-n])
		}
	}
	return clean(strings.SplitN(loc, ",", 2)[0])
}

// ---------- dry run ----------

// PrintStats reports what a load would write, without a database.
func PrintStats(w io.Writer, source string, ls []Listing) {
	var email, phone, own, haveCountry, resolved int
	var emailTotal, multiEmail, phoneTotal int
	social := map[string]int{}
	niche := map[string]int{}
	unresolved := map[string]int{}
	for _, l := range ls {
		if len(l.Emails) > 0 {
			email++
		}
		if len(l.Emails) > 1 {
			multiEmail++
		}
		emailTotal += len(l.Emails)
		phoneTotal += len(l.Phones)
		for plat := range l.Social {
			social[plat]++
		}
		if len(l.Phones) > 0 {
			phone++
		}
		if l.OwnWebsite {
			own++
		}
		niche[l.Niche]++
		if l.RawCountry != "" {
			haveCountry++
			if l.CountryCode != "" {
				resolved++
			} else {
				unresolved[l.RawCountry]++
			}
		}
	}
	pct := func(n, d int) string {
		if d == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%.2f%%", 100*float64(n)/float64(d))
	}
	fmt.Fprintf(w, "source: %s (dry run — nothing written)\n", source)
	fmt.Fprintf(w, "records:          %d\n", len(ls))
	fmt.Fprintf(w, "with email:       %d (%s)\n", email, pct(email, len(ls)))
	fmt.Fprintf(w, "emails total:     %d (%d listings with more than one)\n", emailTotal, multiEmail)
	fmt.Fprintf(w, "with phone:       %d (%s)\n", phone, pct(phone, len(ls)))
	fmt.Fprintf(w, "phones total:     %d\n", phoneTotal)
	fmt.Fprintf(w, "with own website: %d (%s)\n", own, pct(own, len(ls)))
	fmt.Fprintf(w, "country value:   %d records; resolved %d (%s), unresolved %d\n",
		haveCountry, resolved, pct(resolved, haveCountry), haveCountry-resolved)
	type kvc struct {
		k string
		n int
	}
	var top []kvc
	for k, n := range unresolved {
		top = append(top, kvc{k, n})
	}
	sort.Slice(top, func(i, j int) bool { return top[i].n > top[j].n || (top[i].n == top[j].n && top[i].k < top[j].k) })
	if len(top) > 20 {
		top = top[:20]
	}
	for _, t := range top {
		fmt.Fprintf(w, "  unresolved %-30q %d\n", t.k, t.n)
	}
	var nk []string
	for k := range niche {
		nk = append(nk, k)
	}
	sort.Strings(nk)
	for _, k := range nk {
		fmt.Fprintf(w, "niche %-10s %d\n", k, niche[k])
	}

	var sk []string
	for k := range social {
		sk = append(sk, k)
	}
	sort.Strings(sk)
	for _, k := range sk {
		fmt.Fprintf(w, "social %-10s %d\n", k, social[k])
	}

	type metaStat struct {
		listings, values int
		distinct         map[string]bool
	}
	meta := map[string]*metaStat{}
	for _, l := range ls {
		for k, vals := range l.Meta {
			if len(vals) == 0 {
				continue
			}
			st := meta[k]
			if st == nil {
				st = &metaStat{distinct: map[string]bool{}}
				meta[k] = st
			}
			st.listings++
			st.values += len(vals)
			for _, v := range vals {
				st.distinct[v] = true
			}
		}
	}
	var mk []string
	for k := range meta {
		mk = append(mk, k)
	}
	sort.Strings(mk)
	for _, k := range mk {
		st := meta[k]
		fmt.Fprintf(w, "meta %-40s listings %d, values %d, distinct %d\n", k, st.listings, st.values, len(st.distinct))
	}
}

// Listings builds the listings of a source ("ace" | "icf") from a state dir.
func Listings(source, dir string) ([]Listing, error) {
	res := newCountryResolver()
	switch source {
	case "ace":
		return aceListings(dir, res)
	case "icf":
		return icfListings(dir, res)
	}
	return nil, fmt.Errorf("unknown source %q", source)
}

// ---------- database ----------

var freeMail = map[string]bool{
	"gmail.com": true, "yahoo.com": true, "hotmail.com": true, "outlook.com": true,
	"aol.com": true, "icloud.com": true, "me.com": true, "mac.com": true,
	"live.com": true, "msn.com": true, "comcast.net": true, "gmx.com": true,
	"protonmail.com": true, "ymail.com": true, "yahoo.co.uk": true,
}

// The EXCLUDED values for nullable columns are NULL when the source has no
// value (NULLIF on insert), so COALESCE(EXCLUDED.x, current) is "a new
// non-empty value wins, an empty one never blanks an existing value". The same
// holds for the NOT-NULL-ish text columns, where an empty-string EXCLUDED value
// is turned into NULL first.
//
// re_enriched_at is set on INSERT only: these synthetic domains have no page
// for the reenrich worker to refetch; without it they stay permanently eligible
// and churn the worker (the 2026-06-01 YogaAlliance churn, see the comment above
// eligibilityQuery in internal/stage/reenrich.go).
//
// niche_source, geo_source and completeness_score stay NULL — NULL marks the
// value as authoritative, as for the yogaalliance rows.
const upsertListingSQL = `
INSERT INTO business_listings
  (domain, url, business_name, contact_name, category, niche_category, off_niche,
   phone, phones, website, address, location, city, country, country_code,
   description, social_links, re_enriched_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, false,
   NULLIF($7::text,''), COALESCE($16::text[], '{}'::text[]),
   $8, NULLIF($9::text,''), NULLIF($10::text,''), NULLIF($11::text,''),
   NULLIF($12::text,''), NULLIF($13::text,'')::char(2),
   NULLIF($14::text,''), $15::jsonb, NOW(), NOW(), NOW())
ON CONFLICT (domain) DO UPDATE SET
  url            = COALESCE(NULLIF(EXCLUDED.url,''), business_listings.url),
  business_name  = COALESCE(NULLIF(EXCLUDED.business_name,''), business_listings.business_name),
  contact_name   = COALESCE(NULLIF(EXCLUDED.contact_name,''), business_listings.contact_name),
  phone          = COALESCE(EXCLUDED.phone, business_listings.phone),
  phones         = CASE WHEN cardinality(EXCLUDED.phones) > 0 THEN EXCLUDED.phones ELSE business_listings.phones END,
  website        = COALESCE(NULLIF(EXCLUDED.website,''), business_listings.website),
  address        = COALESCE(EXCLUDED.address, business_listings.address),
  location       = COALESCE(EXCLUDED.location, business_listings.location),
  city           = COALESCE(EXCLUDED.city, business_listings.city),
  country        = COALESCE(EXCLUDED.country, business_listings.country),
  country_code   = COALESCE(EXCLUDED.country_code, business_listings.country_code),
  description    = COALESCE(EXCLUDED.description, business_listings.description),
  social_links   = CASE WHEN EXCLUDED.social_links <> '{}'::jsonb THEN EXCLUDED.social_links ELSE business_listings.social_links END,
  niche_category = EXCLUDED.niche_category,
  off_niche      = false,
  category       = EXCLUDED.category,
  updated_at     = NOW()
RETURNING id`

// upsert writes one listing (and its email link) in a transaction.
func upsert(ctx context.Context, db *sql.DB, l Listing) error {
	social, _ := json.Marshal(l.Social)
	if l.Social == nil {
		social = []byte("{}")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	phones := l.Phones // primary first
	primary := ""
	if len(phones) > 0 {
		primary = phones[0]
	}

	var bizID int64
	if err := tx.QueryRowContext(ctx, upsertListingSQL,
		l.Domain, l.URL, l.Name, l.Name, l.Category, l.Niche,
		primary, l.Website, l.Address, l.Location, l.City, l.Country, l.CountryCode,
		l.Description, string(social), pq.Array(phones),
	).Scan(&bizID); err != nil {
		return fmt.Errorf("business_listings upsert: %w", err)
	}

	for _, email := range l.Emails {
		at := strings.LastIndex(email, "@")
		if at < 1 {
			continue
		}
		domain := email[at+1:]
		var emailID int64
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO emails (email, domain, local_part, free_email, created_at)
			VALUES ($1,$2,$3,$4,NOW())
			ON CONFLICT (email) DO UPDATE SET domain = EXCLUDED.domain
			RETURNING id`, email, domain, email[:at], freeMail[domain],
		).Scan(&emailID); err != nil {
			return fmt.Errorf("emails upsert: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO business_emails (business_id, email_id, source)
			VALUES ($1,$2,$3) ON CONFLICT (business_id, email_id) DO NOTHING`,
			bizID, emailID, l.Category); err != nil {
			return fmt.Errorf("business_emails link: %w", err)
		}
	}

	if len(l.Meta) > 0 {
		var keys, mk, mv []string
		for k, vals := range l.Meta {
			keys = append(keys, k)
			for _, v := range vals {
				mk, mv = append(mk, k), append(mv, v)
			}
		}
		// Per-key replace: the keys of this record are rewritten, keys it does
		// not carry keep their old rows.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM business_meta WHERE business_id = $1 AND meta_key = ANY($2::text[])`,
			bizID, pq.Array(keys)); err != nil {
			return fmt.Errorf("business_meta delete: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO business_meta (business_id, meta_key, meta_value)
			SELECT $1, k, v FROM unnest($2::text[], $3::text[]) AS t(k, v)
			ON CONFLICT DO NOTHING`,
			bizID, pq.Array(mk), pq.Array(mv)); err != nil {
			return fmt.Errorf("business_meta insert: %w", err)
		}
	}
	return tx.Commit()
}

// Load upserts the listings with loadWorkers concurrent transactions and
// returns the process exit code. Re-running is idempotent.
func Load(ctx context.Context, db *sql.DB, source string, ls []Listing) int {
	if err := appdb.EnsureBusinessMeta(ctx, db); err != nil {
		slog.Error("prodir: business_meta table", "error", err)
		return ExitErrors
	}
	db.SetMaxOpenConns(loadWorkers + 2)
	res := runPool(ctx, ls, loadWorkers, "load-"+source, 5000, func(ctx context.Context, l Listing) error {
		if ctx.Err() != nil {
			return errStopped
		}
		return upsert(ctx, db, l)
	})
	slog.Info("prodir: load finished", "source", source, "upserted", res.done, "failed", res.errs, "records", len(ls))
	return exitCode(res, ctx)
}
