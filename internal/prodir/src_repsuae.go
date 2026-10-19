package prodir

// REPs UAE, the UAE Register of Exercise Professionals,
// https://www.repsuae.com/find-trainer (~2,500 registrants, individuals).
//
// Plain GET pagination: /find-trainer?page=N, 12 cards per page (~208 pages),
// ended by the "Showing A-B of T" counter. There is no profile page and the
// "Contact Trainer" link is a reCAPTCHA message form, so the cards are all there
// is: name, REPs number (the stable ID), category badges, nationality, emirate
// and expiry date. NO email, phone or website, so these are name/emirate seeds.
// The directory is UAE-only and cards carry no country, so Country is "United
// Arab Emirates"; nationality is a person attribute and goes to Meta only.

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const repsUAEList = "https://www.repsuae.com/find-trainer"

var (
	repsUAECountRe = regexp.MustCompile(`Showing\s+([\d,]+)\s*[–-]\s*([\d,]+)\s+of\s+([\d,]+)`)
	repsUAEEmirate = map[string]bool{"abu dhabi": true, "dubai": true, "sharjah": true, "ajman": true,
		"umm al quwain": true, "ras al khaimah": true, "fujairah": true}
)

func init() {
	register(&Source{
		Slug: "repsuae", Name: "REPs UAE register of exercise professionals", Host: "repsuae.com",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: repsUAEListAll,
	})
}

func repsUAEListAll(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[string]bool{}
	for page := 1; page <= 1000; page++ {
		doc, _, err := getDoc(ctx, f, fmt.Sprintf("%s?page=%d", repsUAEList, page))
		if err != nil {
			return err
		}
		cards := doc.Find("div.trainer-card")
		var emitErr error
		cards.EachWithBreak(func(_ int, card *goquery.Selection) bool {
			r := repsUAECard(card)
			if r.ID == "" || r.Name == "" || seen[r.ID] { // no REPs number / name: not a real entry
				return true
			}
			seen[r.ID] = true
			emitErr = emit(r)
			return emitErr == nil
		})
		if emitErr != nil {
			return emitErr
		}
		m := repsUAECountRe.FindStringSubmatch(doc.Text())
		if cards.Length() == 0 || m == nil {
			return nil
		}
		end, _ := strconv.Atoi(strings.ReplaceAll(m[2], ",", ""))
		total, _ := strconv.Atoi(strings.ReplaceAll(m[3], ",", ""))
		if end >= total {
			return nil
		}
	}
	return nil
}

func repsUAECard(card *goquery.Selection) Record {
	r := Record{
		ID:      clean(card.Find(".trainer-card__reps").First().Text()),
		Name:    clean(card.Find(".trainer-card__name").First().Text()),
		Country: "United Arab Emirates",
		Meta:    map[string][]string{},
	}
	addMeta(r.Meta, "reps_number", r.ID)
	card.Find(".category-badge").Each(func(_ int, b *goquery.Selection) {
		addMeta(r.Meta, "category", b.Text())
		addMeta(r.Meta, "role", b.AttrOr("title", ""))
	})
	// two meta rows, nationality then emirate; either may be missing, so the
	// emirate is recognised by name rather than by position
	card.Find(".trainer-card__meta-row").Each(func(_ int, row *goquery.Selection) {
		v := clean(row.Text())
		if repsUAEEmirate[strings.ToLower(v)] {
			r.Region = v
		} else {
			addMeta(r.Meta, "nationality", v)
		}
	})
	addMeta(r.Meta, "expires", card.Find(".trainer-card__expiry span").First().Text())
	return r
}
