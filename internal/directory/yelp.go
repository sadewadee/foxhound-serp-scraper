package directory

import (
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// YelpExtractor extracts business listings from Yelp pages.
//
// NOTE (correctness): Yelp serves us a DataDome 403 on every fetch method as
// of 2026-09-25, so no real Yelp page has ever been parsed by this code. The
// HTML selectors below are modeled on Yelp's documented structure and are
// covered only by synthetic fixtures. Only Listing.URLs that point at the
// business's OWN website are ever queued for enrichment — never a yelp.com
// URL — mirroring the YellowPages pattern from #52.
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
			listing.URL = businessWebsite(listing.URL, "yelp.com")
			listings = append(listings, listing)
		}
	}

	// HTML fallback — Yelp search result cards.
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return listings
	}

	seen := make(map[string]bool)
	// (goquery splits nested anchors, so card titles are read as headings
	// rather than links — an <a> inside another <a> is invalid HTML and real
	// browsers split it the same way.)
	doc.Each("[data-testid='serp-ia-card'], .regular-search-result, li[class*='border-color']", func(_ int, s *goquery.Selection) {
		name := strings.TrimSpace(s.Find("h3, h2, a[href*='/biz/'] span, .css-1m051bw").First().Text())
		if name == "" || seen[name] {
			return
		}
		seen[name] = true

		// Prefer the business's own website. Yelp cards link the business name
		// to /biz/<slug> and, when present, a Website link through biz_redir.
		website := ""
		if href, ok := s.Find("a[href*='biz_redir']").First().Attr("href"); ok {
			website = UnwrapRedirectURL(absURL(href, "https://www.yelp.com"), "url")
		}
		if website == "" {
			if href, ok := s.Find("a[href^='http']:not([href*='yelp.com'])").First().Attr("href"); ok {
				website = href
			}
		}

		address := strings.TrimSpace(s.Find("[class*='secondaryAttributes'] address, .secondary-attributes address").First().Text())
		category := strings.TrimSpace(s.Find("[class*='category'], .category-str-list a").First().Text())
		rating := strings.TrimSpace(s.Find("[aria-label*='rating'], .rating-large").First().AttrOr("aria-label", ""))
		phone := strings.TrimSpace(s.Find("[class*='phone'], .biz-phone").First().Text())

		listings = append(listings, Listing{
			Name:     name,
			URL:      businessWebsite(website, "yelp.com"),
			Phone:    phone,
			Address:  address,
			Category: category,
			Rating:   rating,
			Source:   "yelp",
		})
	})

	return listings
}

// businessWebsite keeps only URLs that point at the business itself. An empty
// input, a URL on the directory's own domain, or an unparsable URL all become
// "" so the caller can never queue a directory URL for enrichment.
func businessWebsite(raw string, directoryDomain string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if host == directoryDomain || strings.HasSuffix(host, "."+directoryDomain) {
		return ""
	}
	return raw
}

// absURL resolves a possibly-relative href against a base scheme+host.
func absURL(href, base string) string {
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href
	}
	if !strings.HasPrefix(href, "/") {
		href = "/" + href
	}
	return strings.TrimSuffix(base, "/") + href
}
