package prodir

// Iyengar Yoga Association of New Zealand (find a teacher),
// https://iyengar-yoga.org.nz/ (~132 teachers, individuals).
//
// The finder loads HTML cards through a public admin-ajax action: POST
// /wp-admin/admin-ajax.php with "action=get_teacher&page=N" answers
// {counter, next_page, html}; counter is the number of cards in the page (32)
// and is 0 past the end (a short page is NOT the end: the last one holds 4).
// A card gives name, level, region, the city (data-city), a plain mailto email
// and an optional website. The profile adds nothing (no phone), so there is no
// Detail. Studios are a separate tab (not listed). The association is the New
// Zealand one: Country is "New Zealand".

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const iyengarNZURL = "https://iyengar-yoga.org.nz/wp-admin/admin-ajax.php"

func init() {
	register(&Source{
		Slug: "iyengar-nz", Name: "Iyengar Yoga Association of New Zealand teachers", Host: "iyengar-yoga.org.nz",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: iyengarNZList,
	})
}

func iyengarNZList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"content-type": "application/x-www-form-urlencoded", "x-requested-with": "XMLHttpRequest"}
	seen := map[string]bool{}
	for page := 1; page <= 200; page++ {
		p, err := f.Post(ctx, iyengarNZURL, hdr, []byte(fmt.Sprintf("action=get_teacher&page=%d", page)))
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Counter int    `json:"counter"`
			HTML    string `json:"html"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		if resp.Counter == 0 {
			return nil
		}
		doc, err := docFromBody([]byte(resp.HTML))
		if err != nil {
			return err
		}
		for _, r := range iyengarNZCards(doc) {
			if seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// iyengarNZCards parses the teacher cards of one page of the ajax html.
func iyengarNZCards(doc *goquery.Document) []Record {
	var out []Record
	doc.Find("div.teacher-card").Each(func(_ int, card *goquery.Selection) {
		href := strings.TrimSpace(card.Find("a.full-coverage-link").AttrOr("href", ""))
		name := clean(card.Find(".name h4 a").Text())
		id := yogaSlugID(path.Base(strings.TrimRight(href, "/"))) // the /author/{slug}/ profile link
		if id == "" || name == "" {                               // no profile link or name: not an entry
			return
		}
		r := Record{
			ID: id, URL: href, Name: name, City: clean(card.AttrOr("data-city", "")),
			Region: clean(card.Find(".region").Text()), Country: "New Zealand",
			Emails: emailsIn(card.Find(".email")), Meta: map[string][]string{},
		}
		addMeta(r.Meta, "level", card.Find(".level").Text())
		// an empty website cell still holds a link to a bare "https://"
		if w := strings.TrimSpace(card.Find(".website a").AttrOr("href", "")); validHost(urlHost(w)) {
			r.Websites = []string{w}
		}
		out = append(out, r)
	})
	return out
}
