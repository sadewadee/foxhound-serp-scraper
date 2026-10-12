package prodir

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"github.com/sadewadee/serp-scraper/internal/query"
)

// Listing is one person as written to business_listings. Empty strings mean
// "no value" (stored as NULL, and never allowed to blank an existing value).
type Listing struct {
	Domain, URL, Name, Category, Niche      string
	Phone, Website, Address, Location, City string
	Description                             string
	RawCountry, Country, CountryCode        string // Country/CountryCode empty = unresolved
	Social                                  map[string]string
	Email                                   string // lower-cased; empty = none
	OwnWebsite                              bool
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
			Niche: aceNiche(firstNonEmpty(p.Credentials, l.str("CertDisplay"))),
			Phone: p.Phone, Website: profileURL, OwnWebsite: p.Website != "",
			Address: p.Address, Location: strings.Join(loc, ", "), City: city,
			Description: firstNonEmpty(p.About, clean(l.str("DescriptionForWeb"))),
			RawCountry:  clean(l.str("CountryName")), Social: social,
		}
		if p.Website != "" {
			x.Website = withScheme(p.Website)
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
		}
		if e.fetched() {
			x.Phone = clean(e.prof.Phone)
			if w := clean(e.prof.Website); w != "" {
				x.Website, x.OwnWebsite = withScheme(w), true
			}
			if em := strings.ToLower(clean(e.prof.Email)); strings.Contains(em, "@") {
				x.Email = em
			}
		}
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
	niche := map[string]int{}
	unresolved := map[string]int{}
	for _, l := range ls {
		if l.Email != "" {
			email++
		}
		if l.Phone != "" {
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
	fmt.Fprintf(w, "with phone:       %d (%s)\n", phone, pct(phone, len(ls)))
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
   NULLIF($7::text,''), CASE WHEN $7::text = '' THEN '{}'::text[] ELSE ARRAY[$7::text] END,
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

	var bizID int64
	if err := tx.QueryRowContext(ctx, upsertListingSQL,
		l.Domain, l.URL, l.Name, l.Name, l.Category, l.Niche,
		l.Phone, l.Website, l.Address, l.Location, l.City, l.Country, l.CountryCode,
		l.Description, string(social),
	).Scan(&bizID); err != nil {
		return fmt.Errorf("business_listings upsert: %w", err)
	}

	if at := strings.LastIndex(l.Email, "@"); at >= 1 {
		domain := l.Email[at+1:]
		var emailID int64
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO emails (email, domain, local_part, free_email, created_at)
			VALUES ($1,$2,$3,$4,NOW())
			ON CONFLICT (email) DO UPDATE SET domain = EXCLUDED.domain
			RETURNING id`, l.Email, domain, l.Email[:at], freeMail[domain],
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
	return tx.Commit()
}

// Load upserts the listings with loadWorkers concurrent transactions and
// returns the process exit code. Re-running is idempotent.
func Load(ctx context.Context, db *sql.DB, source string, ls []Listing) int {
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
