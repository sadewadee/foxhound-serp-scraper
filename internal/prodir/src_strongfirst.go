package prodir

// StrongFirst certified instructors, https://www.strongfirst.com/instructors/search/
// (~2,300 kettlebell / barbell / bodyweight strength instructors, individuals).
//
// The search page builds its table from one unauthenticated JSON array
// (instructor-search-data.php, ~570 KB, no pagination) of {id, fullName,
// level, location, url, extendedSearch}. The id is the Salesforce account id
// (18 chars, lower-cased for the record id). The country is the second path
// segment of the profile URL (/instructors/{country-slug}/...); the free-text
// location is only used for its first part (the town) when it has several.
// The profile adds the structured location (City / Zip code / State /
// Country), the certification list, the bio and, for about one instructor in
// five, a website. There is no email or phone: contact is a relay form that
// never reveals the address.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const strongfirstBase = "https://www.strongfirst.com"

const strongfirstListURL = strongfirstBase + "/wordpress/wp-content/themes/sf2016/functions/instructor-search-data.php?s="

func init() {
	register(&Source{
		Slug: "strongfirst", Name: "StrongFirst certified instructors", Host: "www.strongfirst.com",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: strongfirstList, Detail: strongfirstDetail,
	})
}

type strongfirstRow struct {
	ID       string `json:"id"`
	FullName string `json:"fullName"`
	Level    string `json:"level"`
	Location string `json:"location"`
	URL      string `json:"url"`
	Extended string `json:"extendedSearch"`
}

func strongfirstList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, strongfirstListURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	var rows []strongfirstRow
	if err := json.Unmarshal(p.Body, &rows); err != nil {
		return fmt.Errorf("instructor list: %w", err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		id := strings.ToLower(clean(row.ID))
		// a row without id or name is not an entry; ids are unique but a repeat is dropped
		if id == "" || clean(row.FullName) == "" || seen[id] {
			continue
		}
		seen[id] = true
		r := Record{
			ID: id, URL: absURL(strongfirstBase, row.URL), Name: clean(row.FullName),
			Country: strongfirstSlugCountry(row.URL), Meta: map[string][]string{},
		}
		if parts := strings.Split(row.Location, ","); len(parts) > 1 {
			r.City = clean(parts[0])
		}
		addMeta(r.Meta, "level", splitMeta(row.Level, ",")...)
		for _, tok := range strings.Fields(row.Extended) {
			switch tok {
			case "newStudents":
				addMeta(r.Meta, "availability", "accepting new students")
			case "online":
				addMeta(r.Meta, "availability", "online training")
			}
		}
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// strongfirstSlugCountry is the country of "/instructors/{country-slug}/...".
func strongfirstSlugCountry(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 3 || parts[0] != "instructors" {
		return ""
	}
	return strongfirstCountry(strings.ReplaceAll(parts[1], "-", " "))
}

// strongfirstCountry rewrites the ISO-style "Korea, Republic of" (the only name
// the site uses that the country resolver has no alias for).
func strongfirstCountry(name string) string {
	if name = clean(name); strings.EqualFold(name, "korea, republic of") {
		return "Republic of Korea"
	}
	return name
}

var (
	sfCityRe    = regexp.MustCompile(`City:\s*([^<]*)<br`)
	sfZipRe     = regexp.MustCompile(`Zip code:\s*([^<]*)<br`)
	sfStateRe   = regexp.MustCompile(`State:\s*([^<]*)<br`)
	sfCountryRe = regexp.MustCompile(`Country:\s*([^<]*)<br`)
	sfWebsiteRe = regexp.MustCompile(`Website:\s*<a [^>]*href="([^"]+)"`)
)

func strongfirstDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	bio := doc.Find("#instructor-bio")
	if bio.Length() == 0 {
		return Record{}, fmt.Errorf("no instructor bio on %s (layout changed?)", base)
	}
	d := Record{Meta: map[string][]string{}}
	var paras []string
	bio.Find("p").Each(func(_ int, p *goquery.Selection) { paras = append(paras, clean(p.Text())) })
	d.Bio = clean(strings.Join(paras, " "))

	info := doc.Find("div.instructor-info").First()
	info.Find("a").Each(func(_ int, a *goquery.Selection) { addMeta(d.Meta, "certification", clean(a.Text())) })
	loc, _ := info.Html()
	field := func(re *regexp.Regexp) string {
		if m := re.FindStringSubmatch(loc); m != nil {
			return clean(m[1])
		}
		return ""
	}
	d.City, d.PostalCode, d.Region, d.Country = field(sfCityRe), field(sfZipRe), field(sfStateRe), strongfirstCountry(field(sfCountryRe))

	if contact, err := doc.Find("div.instructor-contact").First().Html(); err == nil {
		if m := sfWebsiteRe.FindStringSubmatch(contact); m != nil {
			if w := absURL(base, m[1]); strings.HasPrefix(w, "http") {
				d.Websites = []string{w}
			}
		}
	}
	return d, nil
}
