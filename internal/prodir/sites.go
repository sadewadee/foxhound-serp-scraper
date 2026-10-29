package prodir

// `prodir sites`: hand the external websites of already-loaded directory
// listings to the existing enrich pipeline. The directory profile usually links
// the professional's own site and the email lives there, not on the profile.
// This file only ENQUEUES (a serp_results row per site; the
// trg_serp_results_enqueue trigger turns it into a pending enrichment_jobs row)
// and never fetches anything: the enrich workers do the visiting, and queue
// /contact, /about ... themselves when the page has no email. Results land in
// business_listings under the website's own domain, not under the directory row.
//
// A source's rows are the business_listings with the source's category and a
// domain ending in the synthetic domain suffix `load` writes:
//
//	yogaalliance  category yogaalliance  domain *.ryt|rys.yogaalliance.org
//	ace  category acefitness   domain *.pro.acefitness.org
//	icf  category icfcoaching  domain *.coach.coachingfederation.org
//	slug category in categoryNiche (a record can override the source's own
//	     category, e.g. uesca / repsnz) and domain *.<slug>.<host>
//
// Sites come from business_listings.website plus every business_meta
// other_websites value (website is the profile URL when the record has no own
// site; that host is the directory's own and is skipped like any profile link).

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sort"
	"strings"

	"github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/dedup"
)

// siteBlockedHosts are hosts (and their subdomains) that are never a person's
// own website: link hubs, shorteners, maps, booking / marketplace platforms.
var siteBlockedHosts = []string{
	"linktr.ee", "linkin.bio", "beacons.ai", "bit.ly", "goo.gl", "google.com",
	"maps.app.goo.gl", "yelp.com", "wa.me", "whatsapp.com", "t.me", "calendly.com",
	"mindbodyonline.com", "classpass.com", "eventbrite.com", "meetup.com",
	"amazon.com", "apple.com", "spotify.com",
	"paperbell.com", "bio.site", "lnk.bio", "stan.store", "fb.me", "m.me",
	"acuityscheduling.com", "as.me", "setmore.com", "vagaro.com", "booksy.com", "fresha.com",
}

const (
	sitesBatch       = 500
	sitesHostChunk   = 5000
	sitesReadTimeout = "120s"
	sitesWriteTime   = "30s"
)

// SiteStats are the per-source counters printed by `prodir sites`.
type SiteStats struct {
	Candidates, Invalid, OwnHost, Social, Blocked int
	DupURL, DupHost, AlreadyEnriched              int
	WouldEnqueue, Inserted, Present               int
}

// hostUnder reports whether host is d or a subdomain of d.
func hostUnder(host, d string) bool {
	return host == d || strings.HasSuffix(host, "."+d)
}

// normalizeSiteURL trims raw, adds https:// when there is no scheme, accepts
// only http(s) URLs with a plausible dotted host and no userinfo, and drops the
// fragment. It returns the cleaned URL (an empty path becomes "/") and its host.
func normalizeSiteURL(raw string) (string, string, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || strings.ContainsAny(s, " \t\r\n") {
		return "", "", false
	}
	if strings.HasPrefix(s, "//") {
		s = "https:" + s
	} else if !strings.Contains(s, "://") {
		s = "https://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", "", false
	}
	host := strings.ToLower(u.Hostname())
	if !strings.Contains(host, ".") || !validHost(host) {
		return "", "", false
	}
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), host, true
}

// siteSkipReason is "own", "social", "blocked" or "" (usable). dirHost is the
// source's Source.Host; the shared own-domain list (ACE, ICF) always applies.
func siteSkipReason(rawURL, host, dirHost string) string {
	switch {
	case hostUnder(host, dirHost) || isOwnDomain(host):
		return "own"
	case socialPlatform(rawURL) != "":
		return "social"
	}
	for _, b := range siteBlockedHosts {
		if hostUnder(host, b) {
			return "blocked"
		}
	}
	return ""
}

func isRootURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	return err == nil && (u.Path == "" || u.Path == "/") && u.RawQuery == ""
}

// siteCollector turns raw website strings into one URL per host. claimed is
// shared across sources of one run so a host is enqueued once per run.
type siteCollector struct {
	dirHost string
	st      *SiteStats
	claimed map[string]bool // host key claimed by an earlier source
	seenURL map[string]bool
	idx     map[string]int // host key -> index into urls
	urls    []string
	keys    []string
}

func newSiteCollector(dirHost string, st *SiteStats, claimed map[string]bool) *siteCollector {
	return &siteCollector{dirHost: dirHost, st: st, claimed: claimed,
		seenURL: map[string]bool{}, idx: map[string]int{}}
}

// hostKey ignores a leading "www." so www.x.com and x.com are one host.
func hostKey(host string) string { return strings.TrimPrefix(host, "www.") }

func (c *siteCollector) add(raw string) {
	if strings.TrimSpace(raw) == "" {
		return
	}
	c.st.Candidates++
	u, host, ok := normalizeSiteURL(raw)
	if !ok {
		c.st.Invalid++
		return
	}
	switch siteSkipReason(u, host, c.dirHost) {
	case "own":
		c.st.OwnHost++
		return
	case "social":
		c.st.Social++
		return
	case "blocked":
		c.st.Blocked++
		return
	}
	if c.seenURL[u] {
		c.st.DupURL++
		return
	}
	c.seenURL[u] = true
	key := hostKey(host)
	if c.claimed[key] {
		c.st.DupHost++
		return
	}
	if i, ok := c.idx[key]; ok {
		c.st.DupHost++
		if !isRootURL(c.urls[i]) && isRootURL(u) {
			c.urls[i] = u // prefer the root: enrich adds the contact pages itself
		}
		return
	}
	c.idx[key] = len(c.urls)
	c.urls = append(c.urls, u)
	c.keys = append(c.keys, key)
}

// siteSource describes how one source's rows are selected.
type siteSource struct {
	name, host, domainLike string
	categories             []string
}

func siteSources(sel string) ([]siteSource, error) {
	ace := siteSource{"ace", "acefitness.org", "%.pro.acefitness.org", []string{aceCategory}}
	icf := siteSource{"icf", "coachingfederation.org", "%.coach.coachingfederation.org", []string{icfCategory}}
	// cmd/yogaalliance rows: teachers <id>.ryt.…, schools <id>.rys.…
	ya := siteSource{"yogaalliance", "yogaalliance.org", "%.yogaalliance.org", []string{"yogaalliance"}}
	switch sel {
	case "ace":
		return []siteSource{ace}, nil
	case "yogaalliance":
		return []siteSource{ya}, nil
	case "icf":
		return []siteSource{icf}, nil
	}
	var cats []string
	for c := range categoryNiche {
		cats = append(cats, c)
	}
	sort.Strings(cats)
	gen := func(s *Source) siteSource {
		return siteSource{s.Slug, s.Host, "%." + s.Slug + "." + s.Host, cats}
	}
	if sel == "all" {
		out := []siteSource{ace, icf, ya}
		for _, s := range Sources() {
			out = append(out, gen(s))
		}
		return out, nil
	}
	if s, ok := LookupSource(sel); ok {
		return []siteSource{gen(s)}, nil
	}
	return nil, fmt.Errorf("unknown source %q (ace, icf, yogaalliance, a registered slug or all)", sel)
}

const sitesSelectSQL = `
SELECT COALESCE(bl.website, ''),
       COALESCE(array_agg(m.meta_value) FILTER (WHERE m.meta_value IS NOT NULL), '{}'::text[])
FROM business_listings bl
LEFT JOIN business_meta m ON m.business_id = bl.id AND m.meta_key = 'other_websites'
WHERE bl.category = ANY($1) AND bl.domain LIKE $2
GROUP BY bl.id
ORDER BY bl.id`

// collectSource reads one source's rows (bounded by category + domain suffix)
// and feeds every website / other_websites value to c.
func collectSource(ctx context.Context, db *sql.DB, src siteSource, c *siteCollector) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '"+sitesReadTimeout+"'"); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, sitesSelectSQL, pq.Array(src.categories), src.domainLike)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var website string
		var others []string
		if err := rows.Scan(&website, pq.Array(&others)); err != nil {
			return err
		}
		c.add(website)
		for _, o := range others {
			c.add(o)
		}
	}
	return rows.Err()
}

// dropEnriched removes the URLs whose host already has a business_listings row
// (host, www.host or the host without www.) with at least one business_emails
// row, counting them as AlreadyEnriched.
func dropEnriched(ctx context.Context, db *sql.DB, c *siteCollector) ([]string, error) {
	enriched := map[string]bool{}
	for i := 0; i < len(c.keys); i += sitesHostChunk {
		end := min(i+sitesHostChunk, len(c.keys))
		var doms []string
		for _, k := range c.keys[i:end] {
			doms = append(doms, k, "www."+k)
		}
		rows, err := db.QueryContext(ctx, `
SELECT bl.domain FROM business_listings bl
WHERE bl.domain = ANY($1)
  AND EXISTS (SELECT 1 FROM business_emails be WHERE be.business_id = bl.id)`, pq.Array(doms))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var d string
			if err := rows.Scan(&d); err != nil {
				rows.Close()
				return nil, err
			}
			enriched[hostKey(d)] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	var out []string
	for i, u := range c.urls {
		if enriched[c.keys[i]] {
			c.st.AlreadyEnriched++
			continue
		}
		out = append(out, u)
	}
	return out, nil
}

// enqueueURLs inserts the URLs the way internal/stage/enrich.go does, in short
// transactions, and returns (inserted, already present).
func enqueueURLs(ctx context.Context, db *sql.DB, urls []string) (int, int, error) {
	var ins, present int
	for i := 0; i < len(urls); i += sitesBatch {
		batch := urls[i:min(i+sitesBatch, len(urls))]
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return ins, present, err
		}
		if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '"+sitesWriteTime+"'"); err != nil {
			tx.Rollback()
			return ins, present, err
		}
		bi := 0
		for _, u := range batch {
			res, err := tx.ExecContext(ctx,
				`INSERT INTO serp_results (url, url_hash, domain, source_query_id) VALUES ($1, $2, $3, NULL)
				 ON CONFLICT (url_hash) DO NOTHING`,
				u, dedup.HashURL(u), dedup.ExtractDomain(u))
			if err != nil {
				tx.Rollback()
				return ins, present, err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				bi++
			}
		}
		if err := tx.Commit(); err != nil {
			return ins, present, err
		}
		ins += bi
		present += len(batch) - bi
	}
	return ins, present, nil
}

// Sites enqueues the external websites of the selected source's listings
// (ace, icf, a generic slug or "all") into the enrich pipeline. dryRun does no
// writes; limit > 0 caps the URLs enqueued in total. Per-source counts go to w.
func Sites(ctx context.Context, db *sql.DB, sel string, dryRun bool, limit int, w io.Writer) error {
	srcs, err := siteSources(sel)
	if err != nil {
		return err
	}
	claimed := map[string]bool{}
	remaining := limit
	for _, src := range srcs {
		if limit > 0 && remaining <= 0 {
			slog.Info("prodir sites: limit reached, skipping remaining sources", "next", src.name)
			break
		}
		st := &SiteStats{}
		c := newSiteCollector(src.host, st, claimed)
		if err := collectSource(ctx, db, src, c); err != nil {
			return fmt.Errorf("%s: read: %w", src.name, err)
		}
		urls, err := dropEnriched(ctx, db, c)
		if err != nil {
			return fmt.Errorf("%s: enriched check: %w", src.name, err)
		}
		if limit > 0 && len(urls) > remaining {
			urls = urls[:remaining]
		}
		for _, k := range c.keys {
			claimed[k] = true
		}
		st.WouldEnqueue = len(urls)
		if !dryRun {
			if st.Inserted, st.Present, err = enqueueURLs(ctx, db, urls); err != nil {
				return fmt.Errorf("%s: enqueue: %w", src.name, err)
			}
		}
		remaining -= len(urls)
		slog.Info("prodir sites", "source", src.name, "dry_run", dryRun, "candidates", st.Candidates,
			"would_enqueue", st.WouldEnqueue, "inserted", st.Inserted, "present", st.Present)
		printSiteStats(w, src.name, st, dryRun)
	}
	return nil
}

func printSiteStats(w io.Writer, name string, st *SiteStats, dryRun bool) {
	fmt.Fprintf(w, "%s: candidates=%d invalid=%d own_host=%d social=%d blocked=%d dup_url=%d dup_host=%d already_enriched=%d would_enqueue=%d",
		name, st.Candidates, st.Invalid, st.OwnHost, st.Social, st.Blocked, st.DupURL, st.DupHost, st.AlreadyEnriched, st.WouldEnqueue)
	if !dryRun {
		fmt.Fprintf(w, " inserted=%d already_present=%d", st.Inserted, st.Present)
	}
	fmt.Fprintln(w)
}
