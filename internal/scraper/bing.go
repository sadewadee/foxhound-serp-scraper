//go:build playwright

package scraper

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// bingDomains are Bing/Microsoft domains to filter from results.
var bingDomains = []string{
	"bing.com",
	"microsoft.com",
	"msn.com",
	"live.com",
}

// BingEngine implements SearchEngine for Bing Search.
type BingEngine struct{}

func (b *BingEngine) Name() string { return "bing" }

func (b *BingEngine) BuildURL(query string, page, perPage int, gl, hl string) string {
	q := url.QueryEscape(query)
	// Bing uses 1-based "first" param: page 0 → first=1, page 1 → first=11, etc.
	first := page*10 + 1
	return fmt.Sprintf(
		"https://www.bing.com/search?q=%s&first=%d&cc=%s&setlang=%s",
		q, first, url.QueryEscape(gl), url.QueryEscape(hl),
	)
}

func (b *BingEngine) ParseResults(body []byte) ([]string, error) {
	resp := &foxhound.Response{Body: body}
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return nil, fmt.Errorf("bing: parsing HTML: %w", err)
	}

	var urls []string
	seen := make(map[string]bool)

	doc.Each("li.b_algo", func(_ int, s *goquery.Selection) {
		// First anchor inside each organic result.
		link := s.Find("a[href]").First()
		href, exists := link.Attr("href")
		if !exists || href == "" {
			return
		}
		if !strings.HasPrefix(href, "http") {
			return
		}
		if isBingDomain(href) {
			return
		}
		if isExcludedDomain(href) && !isDirectoryDomain(href) {
			return
		}
		if !seen[href] {
			seen[href] = true
			urls = append(urls, href)
		}
	})

	return urls, nil
}

func (b *BingEngine) FetchSteps() []foxhound.JobStep {
	return nil
}

func (b *BingEngine) IsCaptchaPage(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "turing") || strings.Contains(lower, "/captcha")
}

func (b *BingEngine) ExcludedDomains() []string {
	return bingDomains
}

func (b *BingEngine) MaxPages() int { return 5 }

func (b *BingEngine) NeedsBrowser() bool { return false }

// isBingDomain returns true if the URL belongs to a Bing/Microsoft property.
func isBingDomain(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, bd := range bingDomains {
		if host == bd || strings.HasSuffix(host, "."+bd) {
			return true
		}
	}
	return false
}
