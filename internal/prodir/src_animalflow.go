package prodir

// Animal Flow instructor directory, https://animalflow.com/instructor-directory/
// (~1,300 instructors, individuals).
//
// The directory is server-rendered WordPress: ?country=all, then
// /instructor-directory/page/N/?country=all, 20 cards per page; the "[ 1308
// instructors found (Page 1 of 66) ]" note gives the page count. Cards link to
// /profile?user={id} (sparse ids, so they must come from the list). Cards with
// an empty name are deleted / placeholder instructors and are skipped. The list
// has name, company, a free-text location line, certifications and languages;
// the profile adds email (Cloudflare-obfuscated), phone, website, services and
// a bio. Location is free text ("City, ST, USA"); its last comma-separated part
// is the country when the resolver recognises it ("USA", "Canada"), otherwise
// the country stays unknown. No per-instructor social links.

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const animalflowBase = "https://animalflow.com"

var animalflowPagesRe = regexp.MustCompile(`Page \d+ of (\d+)`)

func init() {
	register(&Source{
		Slug: "animalflow", Name: "Animal Flow instructor directory", Host: "animalflow.com",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: animalflowList, Detail: animalflowDetail,
	})
}

var animalflowCountries = sync.OnceValue(newCountryResolver)

// animalflowCountry is the last comma-separated part of a location when it is a
// recognised country, else "".
func animalflowCountry(location string) string {
	parts := strings.Split(location, ",")
	if last := clean(parts[len(parts)-1]); len(parts) > 1 && last != "" {
		if _, code := animalflowCountries().resolve(last); code != "" {
			return last
		}
	}
	return ""
}

func animalflowList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	total := 0 // unknown until page 1 says so
	for page := 1; (total == 0 && page <= 500) || page <= total; page++ {
		u := fmt.Sprintf("%s/instructor-directory/page/%d/?country=all", animalflowBase, page)
		if page == 1 {
			u = animalflowBase + "/instructor-directory/?country=all"
		}
		doc, base, err := getDoc(ctx, f, u)
		if err != nil {
			return err
		}
		if page == 1 {
			if m := animalflowPagesRe.FindStringSubmatch(doc.Find("p.dirFoundNote").Text()); m != nil {
				total, _ = strconv.Atoi(m[1])
			}
		}
		cards := doc.Find("div.dirUserResult")
		if cards.Length() == 0 {
			return nil
		}
		var emitErr error
		cards.EachWithBreak(func(_ int, card *goquery.Selection) bool {
			info := card.Find("div.dirSearchResultInfo")
			href := info.Find("a").First().AttrOr("href", "")
			name := clean(info.Find("span.dirSearchResultName").Text())
			id := animalflowID(href)
			if id == "" || name == "" { // placeholder card without a name
				return true
			}
			r := Record{
				ID: id, URL: absURL(base, href), Name: name,
				Business: clean(info.Find("div.dirSearchResultCompany").Text()),
				Meta:     map[string][]string{},
			}
			// the location is the bare text node between the company and the certs
			loc := info.Clone()
			loc.Children().Remove()
			r.Address = clean(loc.Text())
			r.Country = animalflowCountry(r.Address)
			info.Find("ul.dirSearchResultCerts li").Each(func(_ int, li *goquery.Selection) {
				addMeta(r.Meta, "certification", li.Text())
			})
			info.Find("ul.dirSearchResultLanguages li").Each(func(_ int, li *goquery.Selection) {
				addMeta(r.Meta, "languages", li.Text())
			})
			emitErr = emit(r)
			return emitErr == nil
		})
		if emitErr != nil {
			return emitErr
		}
	}
	return nil
}

// animalflowID is the numeric user id of a /profile?user=N link.
func animalflowID(href string) string {
	_, id, ok := strings.Cut(href, "user=")
	if !ok {
		return ""
	}
	for i, c := range id {
		if c < '0' || c > '9' {
			id = id[:i]
			break
		}
	}
	return id
}

func animalflowDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	block := doc.Find("div.dirUserProfileBlock").First()
	if block.Length() == 0 {
		return Record{}, fmt.Errorf("no profile block on %s (layout changed?)", base)
	}
	d := Record{Meta: map[string][]string{}}
	// a removed instructor renders an empty shell with HTTP 200
	if clean(block.Find("div.dirProfileName h2").Text()) == "" {
		return Record{}, fmt.Errorf("%w: empty profile for %s", ErrNotFound, base)
	}
	d.Business = clean(block.Find("div.dirProfileCompany").Text())
	if addr := clean(block.Find("div.dirProfileAddress").Text()); addr != "" {
		d.Address = addr
		d.Country = animalflowCountry(addr)
	}
	d.Emails = emailsIn(block.Find("div.dirEmail"))
	if ph := structuredPhone(block.Find("div.dirPhone").Text()); ph != "" {
		d.Phones = []string{ph}
	}
	if w := clean(block.Find("div.dirWebsite a").First().AttrOr("href", "")); w != "" {
		d.Websites = []string{w}
	}
	block.Find("div.dirLanguageSingle").Each(func(_ int, s *goquery.Selection) { addMeta(d.Meta, "languages", s.Text()) })
	block.Find("div.dirCertSingle").Each(func(_ int, s *goquery.Selection) { addMeta(d.Meta, "certification", s.Text()) })
	block.Find("div.dirServicesSingle").Each(func(_ int, s *goquery.Selection) { addMeta(d.Meta, "services", s.Text()) })
	bio := block.Find("div.dirProfileBio").Clone()
	bio.Find("h2").First().Remove()
	d.Bio = clean(bio.Text())
	return d, nil
}
