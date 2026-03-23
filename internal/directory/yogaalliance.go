package directory

import (
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// YogaAllianceExtractor extracts teacher/studio listings from Yoga Alliance.
type YogaAllianceExtractor struct{}

func (e *YogaAllianceExtractor) Name() string { return "yogaalliance" }

func (e *YogaAllianceExtractor) Match(domain string) bool {
	return domain == "yogaalliance.org" || strings.HasSuffix(domain, ".yogaalliance.org")
}

func (e *YogaAllianceExtractor) Extract(body []byte) []Listing {
	var listings []Listing
	resp := &foxhound.Response{Body: body}

	doc, err := parse.NewDocument(resp)
	if err != nil {
		return nil
	}

	// Yoga Alliance directory cards.
	selectors := []string{
		".directory-card",
		".search-result-card",
		"[class*='result-item']",
		".card",
	}

	seen := make(map[string]bool)
	for _, sel := range selectors {
		doc.Each(sel, func(_ int, s *goquery.Selection) {
			name := strings.TrimSpace(s.Find("h3, h4, .name, [class*='name']").First().Text())
			if name == "" || seen[name] {
				return
			}
			seen[name] = true

			href, _ := s.Find("a[href]").First().Attr("href")
			if href != "" && !strings.HasPrefix(href, "http") {
				href = "https://www.yogaalliance.org" + href
			}

			address := strings.TrimSpace(s.Find("[class*='location'], [class*='address']").First().Text())
			category := strings.TrimSpace(s.Find("[class*='designation'], [class*='style']").First().Text())

			// Website link if available.
			website, _ := s.Find("a[href*='http']:not([href*='yogaalliance'])").First().Attr("href")
			url := website
			if url == "" {
				url = href
			}

			listings = append(listings, Listing{
				Name:     name,
				URL:      url,
				Address:  address,
				Category: category,
				Source:   "yogaalliance",
			})
		})
	}

	return listings
}
