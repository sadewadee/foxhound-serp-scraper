package prodir

// CorePower Yoga studios, https://www.corepoweryoga.com/yoga-studios
// (~246 studios, listing_type studio; a corporate chain, so the contacts are
// the studios' own generic inboxes).
//
// Enumeration: sitemap.xml (an index) -> sitemap-0.xml; every <loc> of the
// form /yoga-studios/<state>/<market>/<studio> is a studio page (the bare
// /yoga-studios page and the market landing pages are not). List needs only
// those two requests and emits the page URL with a name from the slug. Detail
// fetches the studio page (~275 KB) and reads the studio record that the
// Next.js page embeds in script#__NEXT_DATA__ (props.pageProps.studio.fields).
//
// Contact fields: phone and distributionEmail (the studio's generic inbox; the
// studio manager's personal email is deliberately not taken), the address
// (addressLine1 / city / state / zipCode). Country: "United States", or Canada
// for a Canadian province code; the data has no country field. No website (the
// studio page is on the directory's own domain), and the Instagram / Twitter
// fields are the chain's own accounts, so they are not stored. Studios with
// closure entries and not marked reopened are skipped.
//
// Known gaps: the phone can be the chain's shared number rather than a
// studio line.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	corepowerBase    = "https://www.corepoweryoga.com"
	corepowerSitemap = corepowerBase + "/sitemap.xml"
)

func init() {
	register(&Source{
		Slug: "corepower", Name: "CorePower Yoga studios", Host: "corepoweryoga.com",
		Category: "hot_yoga_teacher", ListingType: "studio", MinInterval: time.Second,
		List: corepowerList, Detail: corepowerDetail,
	})
}

var (
	corepowerLocRe  = regexp.MustCompile(`<loc>\s*([^<\s]+)\s*</loc>`)
	corepowerPathRe = regexp.MustCompile(`^https?://[^/]+/yoga-studios/([a-z0-9-]+/[a-z0-9-]+/[a-z0-9-]+)/?$`)
	corepowerNextRe = regexp.MustCompile(`(?s)<script id="__NEXT_DATA__"[^>]*>(.*?)</script>`)

	// Canadian province / territory codes; any other state code is a US one.
	corepowerCanada = map[string]bool{"AB": true, "BC": true, "MB": true, "NB": true, "NL": true, "NS": true,
		"NT": true, "NU": true, "ON": true, "PE": true, "QC": true, "SK": true, "YT": true}
)

func corepowerGet(ctx context.Context, f Fetcher, u string) ([]byte, error) {
	p, err := f.Get(ctx, u, nil)
	if err != nil {
		return nil, err
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	return p.Body, nil
}

func corepowerList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	idx, err := corepowerGet(ctx, f, corepowerSitemap)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	// the index lists the sitemaps; a studio URL found anywhere is taken too
	for _, m := range corepowerLocRe.FindAllStringSubmatch(string(idx), -1) {
		locs := []string{m[1]}
		if strings.HasSuffix(m[1], ".xml") {
			body, err := corepowerGet(ctx, f, m[1])
			if err != nil {
				return err
			}
			locs = locs[:0]
			for _, l := range corepowerLocRe.FindAllStringSubmatch(string(body), -1) {
				locs = append(locs, l[1])
			}
		}
		for _, loc := range locs {
			sm := corepowerPathRe.FindStringSubmatch(loc)
			if sm == nil || seen[sm[1]] {
				continue
			}
			seen[sm[1]] = true
			parts := strings.Split(sm[1], "/")
			if err := emit(Record{
				ID:   strings.ReplaceAll(sm[1], "/", "-"),
				URL:  corepowerBase + "/yoga-studios/" + sm[1],
				Name: "CorePower Yoga " + strings.ToUpper(parts[0]) + " " + parts[2],
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

type corepowerStudio struct {
	Title             string `json:"title"`
	Phone             string `json:"phone"`
	DistributionEmail string `json:"distributionEmail"`
	Closed            []any  `json:"closed"`
	Reopened          bool   `json:"studioReopened"`
	StudioType        string `json:"studioType"`
	Address           struct {
		Fields struct {
			Line1 string `json:"addressLine1"`
			Line2 string `json:"addressLine2"`
			City  string `json:"city"`
			State string `json:"state"`
			Zip   string `json:"zipCode"`
		} `json:"fields"`
	} `json:"address"`
}

func corepowerDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	body, err := corepowerGet(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	m := corepowerNextRe.FindSubmatch(body)
	if m == nil {
		return Record{}, fmt.Errorf("no __NEXT_DATA__ on %s (layout changed?)", r.URL)
	}
	var nd struct {
		Props struct {
			PageProps struct {
				Studio struct {
					Fields corepowerStudio `json:"fields"`
				} `json:"studio"`
			} `json:"pageProps"`
		} `json:"props"`
	}
	if err := json.Unmarshal(m[1], &nd); err != nil {
		return Record{}, fmt.Errorf("__NEXT_DATA__ of %s: %w", r.URL, err)
	}
	s := nd.Props.PageProps.Studio.Fields
	if s.Title == "" {
		return Record{}, fmt.Errorf("%w: no studio record on %s", ErrNotFound, r.URL)
	}
	// a closed studio (closure entries and not reopened) is not a lead
	if len(s.Closed) > 0 && !s.Reopened {
		return Record{}, fmt.Errorf("%w: studio closed: %s", ErrNotFound, r.URL)
	}
	a := s.Address.Fields
	d := Record{
		Name: "CorePower Yoga " + s.Title, Address: strings.Join(corepowerNonEmpty(a.Line1, a.Line2), ", "),
		City: a.City, Region: a.State, PostalCode: a.Zip, Country: "United States", Meta: map[string][]string{},
	}
	if corepowerCanada[strings.ToUpper(a.State)] {
		d.Country = "Canada"
	}
	if s.Phone != "" {
		d.Phones = []string{s.Phone}
	}
	if s.DistributionEmail != "" {
		d.Emails = []string{s.DistributionEmail}
	}
	addMeta(d.Meta, "studio_type", s.StudioType)
	return d, nil
}

func corepowerNonEmpty(vs ...string) []string {
	var out []string
	for _, v := range vs {
		if v = clean(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
