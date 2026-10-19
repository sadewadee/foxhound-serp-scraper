package prodir

// Iyengar Yoga (UK) teachers directory, https://iyengaryoga.org.uk/teachers-directory/
// (~845 certified teachers, individuals).
//
// The listing is a FacetWP grid that the server also renders for a plain GET:
// /teachers-directory/?_paged=N (50 cards per page; the page carries
// "total_pages" in its FWP_JSON). Profiles are /teacher/{slug}-{id}/ and add
// the contact details (email and phone are plain text, the website a link).
// The directory has no country field (a few teachers are outside the UK), so
// Country stays empty rather than guessing.

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const iyengarBase = "https://iyengaryoga.org.uk"

var iyengarPagesRe = regexp.MustCompile(`"total_pages":(\d+)`)

func init() {
	register(&Source{
		Slug: "iyengaruk", Name: "Iyengar Yoga UK teachers directory", Host: "iyengaryoga.org.uk",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: iyengarList, Detail: iyengarDetail,
	})
}

// labelValue is the text of a "<li><span>Label:</span>value</li>" row without its label.
func labelValue(li *goquery.Selection) string {
	c := li.Clone()
	c.Find("span").First().Remove()
	return clean(c.Text())
}

// labelOf is the label of such a row, lower-cased and without the colon.
func labelOf(li *goquery.Selection) string {
	return strings.ToLower(strings.TrimSpace(strings.TrimSuffix(clean(li.Find("span").First().Text()), ":")))
}

func iyengarList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	total := 0 // unknown until page 1 says so; then pages run until one is empty
	for page := 1; (total == 0 && page <= 500) || page <= total; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s/teachers-directory/?_paged=%d", iyengarBase, page), nil)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		if page == 1 {
			if m := iyengarPagesRe.FindSubmatch(p.Body); m != nil {
				total, _ = strconv.Atoi(string(m[1]))
			}
		}
		doc, err := docFromBody(p.Body)
		if err != nil {
			return err
		}
		recs := iyengarCards(doc, p.URL)
		if len(recs) == 0 {
			return nil
		}
		for _, r := range recs {
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// iyengarCards parses the teacher cards of one listing page.
func iyengarCards(doc *goquery.Document, base string) []Record {
	var out []Record
	doc.Find("div.post").Each(func(_ int, card *goquery.Selection) {
		href := absURL(base, card.Find("a.read-more").AttrOr("href", ""))
		if !strings.Contains(href, "/teacher/") {
			return
		}
		r := Record{
			ID: path.Base(strings.TrimRight(href, "/")), URL: href,
			Name: clean(card.Find(".post-title h3").Text()),
			Bio:  clean(card.Find(".teacher-bio").Text()),
			Meta: map[string][]string{},
		}
		card.Find(".post-details li, .post-list li").Each(func(_ int, li *goquery.Selection) {
			switch labelOf(li) {
			case "level":
				addMeta(r.Meta, "level", labelValue(li))
			case "location":
				r.City = labelValue(li)
			}
		})
		out = append(out, r)
	})
	return out
}

func iyengarDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	if doc.Find("ul.listing-details").Length() == 0 {
		return Record{}, fmt.Errorf("no listing details on %s (layout changed?)", base)
	}
	d := Record{Meta: map[string][]string{}}
	bio := doc.Find(".listing-description").First().Clone()
	bio.Find("h4").Remove()
	d.Bio = clean(bio.Text())
	doc.Find("ul.listing-details li").Each(func(_ int, li *goquery.Selection) {
		val := labelValue(li)
		switch labelOf(li) {
		case "current level":
			addMeta(d.Meta, "level", val)
		case "contact email":
			d.Emails = emailsIn(li)
			for _, e := range structuredEmails(val) {
				d.Emails = addString(d.Emails, e)
			}
		case "contact number":
			d.Phones = []string{val}
		case "website booking":
			d.Websites = []string{firstNonEmpty(absURL(base, li.Find("a").AttrOr("href", "")), val)}
		case "location":
			// a free-form line ("Flat 4"), not the town of the list card
			if !strings.EqualFold(val, r.City) {
				d.Address = val
			}
		}
	})
	return d, nil
}
