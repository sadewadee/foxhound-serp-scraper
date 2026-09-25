package directory

import (
	"net/url"
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

	seen := make(map[string]bool)

	// Yellow Pages result cards.
	// Prefer .search-results .srp-listing to target organic results and avoid matching
	// nested elements (.result contains .srp-listing which contains .v-card).
	cards := doc.Find(".search-results .srp-listing")
	if cards.Length() == 0 {
		cards = doc.Find(".srp-listing")
	}
	if cards.Length() == 0 {
		cards = doc.Find(".result")
	}

	cards.Each(func(_ int, s *goquery.Selection) {
		name := strings.TrimSpace(s.Find(".business-name a, .business-name, .n a, h2 a").First().Text())
		if name == "" {
			return
		}

		// Yellow Pages shows website link separately.
		website, _ := s.Find("a.track-visit-website, a[href*='website']").First().Attr("href")
		website = strings.TrimSpace(website)
		if website != "" {
			website = unwrapYellowPagesURL(website)
		}
		if isYellowPagesDomain(website) {
			website = ""
		}

		phone := strings.TrimSpace(s.Find(".phone, .phones, [class*='phone']").First().Text())
		address := strings.TrimSpace(s.Find(".adr, .street-address, [class*='address']").First().Text())
		category := strings.TrimSpace(s.Find(".categories a, [class*='category']").First().Text())

		// Deduplicate: key by external website if present, otherwise by name + "|" + phone
		dedupKey := strings.ToLower(name) + "|" + strings.ToLower(phone)
		if website != "" {
			dedupKey = "url:" + strings.ToLower(website)
		}
		if seen[dedupKey] {
			return
		}
		seen[dedupKey] = true

		listings = append(listings, Listing{
			Name:     name,
			URL:      website, // Business website URL only (never internal yellowpages.com detail page)
			Phone:    phone,
			Address:  address,
			Category: category,
			Source:   "yellowpages",
		})
	})

	return listings
}

// isYellowPagesDomain returns true if the URL belongs to yellowpages.com or a subdomain.
func isYellowPagesDomain(rawURL string) bool {
	if rawURL == "" {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host == "yellowpages.com" || strings.HasSuffix(host, ".yellowpages.com")
}

// unwrapYellowPagesURL unwraps external destination URLs from YellowPages redirect/tracking links.
func unwrapYellowPagesURL(rawURL string) string {
	if !isYellowPagesDomain(rawURL) {
		return rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	for _, qKey := range []string{"target", "url", "dest", "destination", "u"} {
		if dest := u.Query().Get(qKey); dest != "" {
			if strings.HasPrefix(dest, "http://") || strings.HasPrefix(dest, "https://") {
				if !isYellowPagesDomain(dest) {
					return dest
				}
			}
		}
	}
	return rawURL
}
