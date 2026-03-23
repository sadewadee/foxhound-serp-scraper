package directory

import (
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// YellowPagesExtractor extracts business listings from Yellow Pages.
type YellowPagesExtractor struct{}

func (e *YellowPagesExtractor) Name() string { return "yellowpages" }

func (e *YellowPagesExtractor) Match(domain string) bool {
	return domain == "yellowpages.com" || strings.HasSuffix(domain, ".yellowpages.com")
}

func (e *YellowPagesExtractor) Extract(body []byte) []Listing {
	var listings []Listing
	resp := &foxhound.Response{Body: body}

	doc, err := parse.NewDocument(resp)
	if err != nil {
		return nil
	}

	// Yellow Pages result cards.
	doc.Each(".result, .search-results .srp-listing, .v-card", func(_ int, s *goquery.Selection) {
		name := strings.TrimSpace(s.Find(".business-name a, .n a, h2 a").First().Text())
		if name == "" {
			return
		}

		href, _ := s.Find(".business-name a, .n a, h2 a").First().Attr("href")
		if href != "" && !strings.HasPrefix(href, "http") {
			href = "https://www.yellowpages.com" + href
		}

		// Yellow Pages shows website link separately.
		website, _ := s.Find("a.track-visit-website, a[href*='website']").First().Attr("href")

		phone := strings.TrimSpace(s.Find(".phone, .phones, [class*='phone']").First().Text())
		address := strings.TrimSpace(s.Find(".adr, .street-address, [class*='address']").First().Text())
		category := strings.TrimSpace(s.Find(".categories a, [class*='category']").First().Text())

		url := website
		if url == "" {
			url = href
		}

		listings = append(listings, Listing{
			Name:     name,
			URL:      url,
			Phone:    phone,
			Address:  address,
			Category: category,
			Source:   "yellowpages",
		})
	})

	return listings
}
