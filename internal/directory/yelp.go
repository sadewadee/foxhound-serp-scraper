package directory

import (
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// YelpExtractor extracts business listings from Yelp pages.
type YelpExtractor struct{}

func (e *YelpExtractor) Name() string { return "yelp" }

func (e *YelpExtractor) Match(domain string) bool {
	return domain == "yelp.com" || strings.HasSuffix(domain, ".yelp.com")
}

func (e *YelpExtractor) Extract(body []byte) []Listing {
	var listings []Listing
	resp := &foxhound.Response{Body: body}

	// JSON-LD — Yelp embeds LocalBusiness structured data.
	jsonlds, _ := parse.ExtractJSONLD(resp)
	for _, ld := range jsonlds {
		listing := extractFromJSONLD(ld, "yelp")
		if listing.Name != "" {
			listings = append(listings, listing)
		}
	}

	// HTML fallback — Yelp search result cards.
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return listings
	}

	seen := make(map[string]bool)
	doc.Each("[data-testid='serp-ia-card'], .regular-search-result, li[class*='border-color']", func(_ int, s *goquery.Selection) {
		name := strings.TrimSpace(s.Find("a[href*='/biz/'] span, h3 a, .css-1m051bw").First().Text())
		if name == "" || seen[name] {
			return
		}
		seen[name] = true

		href, _ := s.Find("a[href*='/biz/']").First().Attr("href")
		if href != "" && !strings.HasPrefix(href, "http") {
			href = "https://www.yelp.com" + href
		}

		address := strings.TrimSpace(s.Find("[class*='secondaryAttributes'] address, .secondary-attributes address").First().Text())
		category := strings.TrimSpace(s.Find("[class*='category'], .category-str-list a").First().Text())
		rating := strings.TrimSpace(s.Find("[aria-label*='rating'], .rating-large").First().AttrOr("aria-label", ""))
		phone := strings.TrimSpace(s.Find("[class*='phone'], .biz-phone").First().Text())

		listings = append(listings, Listing{
			Name:     name,
			URL:      href,
			Phone:    phone,
			Address:  address,
			Category: category,
			Rating:   rating,
			Source:   "yelp",
		})
	})

	return listings
}
