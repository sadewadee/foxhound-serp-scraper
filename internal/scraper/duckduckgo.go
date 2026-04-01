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

// ddgDomains are DuckDuckGo domains to filter from results.
var ddgDomains = []string{
	"duckduckgo.com",
}

// DuckDuckGoEngine implements SearchEngine for DuckDuckGo HTML search.
type DuckDuckGoEngine struct{}

func (d *DuckDuckGoEngine) Name() string { return "duckduckgo" }

func (d *DuckDuckGoEngine) BuildURL(query string, page, perPage int, gl, hl string) string {
	q := url.QueryEscape(query)
	// DDG HTML endpoint uses kl param for region: "{country}-{lang}" e.g. "us-en".
	kl := fmt.Sprintf("%s-%s", url.QueryEscape(gl), url.QueryEscape(hl))
	return fmt.Sprintf(
		"https://html.duckduckgo.com/html/?q=%s&kl=%s",
		q, kl,
	)
}

func (d *DuckDuckGoEngine) ParseResults(body []byte) ([]string, error) {
	resp := &foxhound.Response{Body: body}
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return nil, fmt.Errorf("ddg: parsing HTML: %w", err)
	}

	var urls []string
	seen := make(map[string]bool)

	doc.Each(".web-result .result__a[href]", func(_ int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists || href == "" {
			return
		}

		// DDG wraps all URLs in a redirect:
		// //duckduckgo.com/l/?kh=-1&uddg=ENCODED_URL
		// Extract the real URL from the "uddg" query parameter.
		realURL := extractDDGRedirect(href)
		if realURL == "" {
			return
		}
		if !strings.HasPrefix(realURL, "http") {
			return
		}
		if isDDGDomain(realURL) {
			return
		}
		if isExcludedDomain(realURL) && !isDirectoryDomain(realURL) {
			return
		}
		if !seen[realURL] {
			seen[realURL] = true
			urls = append(urls, realURL)
		}
	})

	return urls, nil
}

func (d *DuckDuckGoEngine) FetchSteps() []foxhound.JobStep {
	return nil
}

// IsCaptchaPage checks for DDG's anomaly detection page.
func (d *DuckDuckGoEngine) IsCaptchaPage(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "anomaly-modal") || strings.Contains(lower, "anomaly_modal")
}

func (d *DuckDuckGoEngine) ExcludedDomains() []string {
	return ddgDomains
}

func (d *DuckDuckGoEngine) MaxPages() int { return 1 }

func (d *DuckDuckGoEngine) NeedsBrowser() bool { return false }

// extractDDGRedirect parses the real URL from a DDG redirect link.
// DDG wraps results as: //duckduckgo.com/l/?kh=-1&uddg=ENCODED_URL
func extractDDGRedirect(href string) string {
	// Normalize protocol-relative URLs.
	if strings.HasPrefix(href, "//") {
		href = "https:" + href
	}

	u, err := url.Parse(href)
	if err != nil {
		return ""
	}

	// If this is a DDG redirect, extract the uddg param.
	uddg := u.Query().Get("uddg")
	if uddg != "" {
		decoded, err := url.QueryUnescape(uddg)
		if err != nil {
			return uddg // Return as-is if unescape fails.
		}
		return decoded
	}

	// Not a redirect — return original if it looks like a real URL.
	if strings.HasPrefix(href, "http") {
		return href
	}
	return ""
}

// isDDGDomain returns true if the URL belongs to DuckDuckGo.
func isDDGDomain(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, dd := range ddgDomains {
		if host == dd || strings.HasSuffix(host, "."+dd) {
			return true
		}
	}
	return false
}
