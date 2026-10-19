package prodir

// Precision Nutrition Certified Coach Directory,
// https://www.precisionnutrition.com/certified-coach-directory
// (~18.8k opted-in coaches in ~100 countries, individuals).
//
// The server renders the results from query params: ?tab=country&country=NAME&pg=N
// (20 cards per page, pg 1-based). Every page embeds fpdGlobalVars.allFitPros,
// a JSON array with the country of every coach, so the country list comes from
// the first request, then each country is paged until the "A - B of N results"
// counter reaches N (a pg past the end returns the LAST page again, never a 404).
// The cards carry everything: there is no profile page and the name is not
// linked. Email is Cloudflare-obfuscated (emailsIn decodes it); phone, own
// website and business name are optional. Location is one free-text line
// ("street, city, region, postal, country") and goes to Address; Country is the
// country the page was listed under. Pages are ~1.6 MB raw (~110 KB gzip).
// No social links, bio or rates. There is no stable id (data-id is mostly 0),
// so ID is a hash of name + email-or-phone + location.

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const pnURL = "https://www.precisionnutrition.com/certified-coach-directory"

var (
	pnAllRe   = regexp.MustCompile(`"allFitPros":("(?:[^"\\]|\\.)*")`)
	pnCountRe = regexp.MustCompile(`(?:(\d+)\s*[–-]\s*(\d+)\s+of\s+)?(\d+)\s+results?`)
)

func init() {
	register(&Source{
		Slug: "precisionnutrition", Name: "Precision Nutrition certified coach directory", Host: "precisionnutrition.com",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: pnList,
	})
}

// pnCountries is the distinct countries of the embedded allFitPros JSON, in
// order of first appearance.
func pnCountries(body []byte) ([]string, error) {
	m := pnAllRe.FindSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("allFitPros not found (layout changed?)")
	}
	var raw string
	if err := json.Unmarshal(m[1], &raw); err != nil {
		return nil, err
	}
	var pros []struct {
		Country string `json:"country"`
	}
	if err := json.Unmarshal([]byte(raw), &pros); err != nil {
		return nil, err
	}
	var out []string
	seen := map[string]bool{}
	for _, p := range pros {
		if c := clean(p.Country); c != "" && !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	return out, nil
}

func pnList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, pnURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	countries, err := pnCountries(p.Body)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, country := range countries {
		for page := 1; page <= 2000; page++ {
			u := fmt.Sprintf("%s?tab=country&country=%s&pg=%d", pnURL, url.QueryEscape(country), page)
			doc, _, err := getDoc(ctx, f, u)
			if err != nil {
				return err
			}
			cards := doc.Find("div.pn-fpd__1")
			var emitErr error
			cards.EachWithBreak(func(_ int, card *goquery.Selection) bool {
				r, ok := pnCard(card, country)
				if !ok || seen[r.ID] {
					return true
				}
				seen[r.ID] = true
				emitErr = emit(r)
				return emitErr == nil
			})
			if emitErr != nil {
				return emitErr
			}
			// "1 – 20 of 11531 results" (or "4 results" when it all fits one page):
			// done once the end index reaches the total.
			m := pnCountRe.FindStringSubmatch(doc.Find("#js-count").Text())
			if cards.Length() == 0 || m == nil || m[2] == "" {
				break
			}
			end, _ := strconv.Atoi(m[2])
			total, _ := strconv.Atoi(m[3])
			if end >= total {
				break
			}
		}
	}
	return nil
}

// pnCard parses one coach card; ok is false for a card without a name.
func pnCard(card *goquery.Selection, country string) (Record, bool) {
	nameSel := card.Find("h5.pn-fpd__1__name").Clone()
	nameSel.Find("a, br").Remove()
	r := Record{Name: clean(nameSel.Text()), Country: country, Meta: map[string][]string{}}
	if r.Name == "" {
		return r, false
	}
	addMeta(r.Meta, "level", card.Find("a.pn-fpd__1__tag").Text())
	addMeta(r.Meta, "specialties", card.Find("h6.pn-fpd__1__spec").Text())
	r.Emails = emailsIn(card)
	card.Find("span.pn-fpd__1__label").Each(func(_ int, l *goquery.Selection) {
		field := l.Parent()
		val := clean(strings.TrimSpace(strings.Replace(field.Text(), l.Text(), "", 1)))
		switch strings.ToLower(strings.TrimSuffix(clean(l.Text()), ":")) {
		case "business name":
			r.Business = val
		case "location":
			r.Address = clean(field.Find("a").First().Text())
			if r.Address == "" {
				r.Address = val
			}
		case "mobile", "phone":
			if ph := structuredPhone(val); ph != "" {
				r.Phones = addPhones(r.Phones, ph)
			}
		case "website":
			if w := clean(field.Find("a").First().AttrOr("href", "")); w != "" {
				r.Websites = addString(r.Websites, w)
			}
		}
	})
	key := r.Name + "|" + firstNonEmpty(strings.Join(r.Emails, ","), strings.Join(r.Phones, ",")) + "|" + r.Address
	sum := sha1.Sum([]byte(key))
	r.ID = hex.EncodeToString(sum[:8])
	return r, true
}
