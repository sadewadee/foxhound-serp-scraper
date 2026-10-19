package prodir

// PGA of America coach finder, https://www.pga.com/coaches-sitemap.xml (~6,400
// golf coaches, individuals).
//
// List reads the coaches sitemap (one request), every <loc> being
// https://www.pga.com/coach/{customSlug}; the slug is the stable id. Each
// profile embeds its data as props.pageProps.data of the __NEXT_DATA__ script:
// name, city, state, zip, bio, booking link, certification level, member type,
// social links, skills, a booking link (kept in meta: it is no own site) and the facilities the coach works at. There is no
// personal email or phone: the phone and website are the FIRST facility's own
// (club / range), which is the coach's workplace and the only business contact
// the directory has, so the facility name goes to Business. The facility's
// street address is the record address. The directory is American; the profile
// has no country, so Country is "United States".

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	pgaAmericaBase    = "https://www.pga.com"
	pgaAmericaCountry = "United States" // the profile carries none
)

var (
	pgaAmericaLocRe  = regexp.MustCompile(`<loc>\s*(https://www\.pga\.com/coach/([^<\s/]+))\s*</loc>`)
	pgaAmericaDataRe = regexp.MustCompile(`(?s)<script id="__NEXT_DATA__"[^>]*>(.*?)</script>`)
)

func init() {
	register(&Source{
		Slug: "pga-america", Name: "PGA of America coach finder", Host: "pga.com",
		Category: "sports_golf", ListingType: "individual", MinInterval: time.Second,
		List: pgaAmericaList, Detail: pgaAmericaDetail,
	})
}

func pgaAmericaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, pgaAmericaBase+"/coaches-sitemap.xml", nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, m := range pgaAmericaLocRe.FindAllSubmatch(p.Body, -1) {
		id := string(m[2])
		if seen[id] {
			continue
		}
		seen[id] = true
		if err := emit(Record{ID: id, URL: string(m[1]), Country: pgaAmericaCountry}); err != nil {
			return err
		}
	}
	if len(seen) == 0 {
		return fmt.Errorf("no coach URLs in the sitemap (layout changed?)")
	}
	return nil
}

type pgaAmericaCoach struct {
	DisplayName string `json:"displayName"`
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	City        string `json:"city"`
	State       string `json:"state"`
	Zip         string `json:"zipCode"`
	Bio         string `json:"bio"`
	BookingURL  string `json:"bookingUrl"`
	Status      string `json:"status"`
	Level       string `json:"certificationLevel"`
	MemberType  string `json:"memberType"`
	Social      []struct {
		Reference string `json:"reference"`
	} `json:"socialJsonData"`
	Facilities []struct {
		Name    string `json:"facilityName"`
		Street  string `json:"street"`
		City    string `json:"city"`
		State   string `json:"state"`
		Zip     string `json:"zipCode"`
		Phone   string `json:"phoneNumber"`
		Website string `json:"website"`
	} `json:"facilities"`
	Skills []struct {
		Name string `json:"skillName"`
	} `json:"skills"`
}

func pgaAmericaDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	p, err := f.Get(ctx, r.URL, nil)
	if err != nil {
		return Record{}, err
	}
	if err := p.Err(); err != nil {
		return Record{}, err
	}
	m := pgaAmericaDataRe.FindSubmatch(p.Body)
	if m == nil {
		return Record{}, fmt.Errorf("no __NEXT_DATA__ on %s (layout changed?)", p.URL)
	}
	var page struct {
		Props struct {
			PageProps struct {
				Data *pgaAmericaCoach `json:"data"`
			} `json:"pageProps"`
		} `json:"props"`
	}
	if err := json.Unmarshal(m[1], &page); err != nil {
		return Record{}, fmt.Errorf("%s: %w", p.URL, err)
	}
	c := page.Props.PageProps.Data
	if c == nil {
		return Record{}, fmt.Errorf("%w: no coach data on %s", ErrNotFound, p.URL)
	}
	d := Record{
		Name: firstNonEmpty(clean(c.DisplayName), clean(c.FirstName+" "+c.LastName)),
		City: c.City, Region: c.State, PostalCode: c.Zip, Country: pgaAmericaCountry,
		Bio: c.Bio, Meta: map[string][]string{},
	}
	if len(c.Facilities) > 0 {
		fac := c.Facilities[0]
		d.Business = clean(fac.Name)
		d.Address = clean(fac.Street)
		d.City, d.Region, d.PostalCode = firstNonEmpty(clean(fac.City), d.City), firstNonEmpty(clean(fac.State), d.Region), firstNonEmpty(clean(fac.Zip), d.PostalCode)
		if ph := structuredPhone(fac.Phone); ph != "" {
			d.Phones = []string{ph}
		}
		if w := strings.TrimSpace(fac.Website); w != "" {
			d.Websites = []string{w}
		}
		for _, fac := range c.Facilities {
			addMeta(d.Meta, "facilities", fac.Name)
		}
	}
	// the type field is an abbreviation ("YT"): the platform comes from the URL
	for _, s := range c.Social {
		if u := strings.TrimSpace(s.Reference); socialPlatform(u) != "" {
			if d.Social == nil {
				d.Social = map[string]string{}
			}
			d.Social[socialPlatform(u)] = u
		}
	}
	addMeta(d.Meta, "booking_url", c.BookingURL)
	addMeta(d.Meta, "credential", c.Level)
	addMeta(d.Meta, "member_type", c.MemberType)
	addMeta(d.Meta, "status", c.Status)
	for _, s := range c.Skills {
		addMeta(d.Meta, "specialties", s.Name)
	}
	return d, nil
}
