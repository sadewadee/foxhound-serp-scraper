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

// googleDomains are domains to filter out from SERP results.
var googleDomains = []string{
	"google.com", "google.co.id", "google.co.uk", "google.co.jp",
	"google.com.au", "google.de", "google.fr", "google.es",
	"gstatic.com", "googleapis.com", "googleusercontent.com",
	"youtube.com", "youtu.be",
	"accounts.google", "support.google", "policies.google",
	"maps.google", "translate.google",
}

// BuildSERPURL constructs a Google Search URL.
func BuildSERPURL(query string, page, resultsPerPage int) string {
	q := url.QueryEscape(query)
	start := page * resultsPerPage
	return fmt.Sprintf(
		"https://www.google.com/search?q=%s&num=%d&start=%d&hl=en",
		q, resultsPerPage, start,
	)
}

// ParseSERPResults extracts organic result URLs from a Google SERP HTML page.
func ParseSERPResults(body []byte) ([]string, error) {
	resp := &foxhound.Response{Body: body}
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return nil, fmt.Errorf("serp: parsing HTML: %w", err)
	}

	var urls []string
	seen := make(map[string]bool)

	// Multiple CSS selectors for Google SERP organic results.
	selectors := []string{
		"div.yuRUbf > div > span > a[href]",
		"a[jsname='UWckNb'][href]",
		"div.g a[href]",
		"div[data-sokoban-container] a[href]",
	}

	for _, sel := range selectors {
		doc.Each(sel, func(_ int, s *goquery.Selection) {
			href, exists := s.Attr("href")
			if !exists || href == "" {
				return
			}
			// Clean the URL.
			href = cleanSERPURL(href)
			if href == "" {
				return
			}
			// Filter Google domains.
			if isGoogleDomain(href) {
				return
			}
			if !seen[href] {
				seen[href] = true
				urls = append(urls, href)
			}
		})
	}

	return urls, nil
}

// cleanSERPURL extracts the actual URL from Google's redirect wrapper.
func cleanSERPURL(href string) string {
	// Skip non-http links.
	if !strings.HasPrefix(href, "http") {
		// Check for Google redirect: /url?q=...
		if strings.HasPrefix(href, "/url?") {
			u, err := url.Parse(href)
			if err != nil {
				return ""
			}
			return u.Query().Get("q")
		}
		return ""
	}

	// Handle Google redirect URLs: https://www.google.com/url?q=...
	u, err := url.Parse(href)
	if err != nil {
		return href
	}
	if strings.Contains(u.Host, "google.") && u.Path == "/url" {
		if q := u.Query().Get("q"); q != "" {
			return q
		}
	}
	return href
}

// isGoogleDomain returns true if the URL belongs to a Google property.
func isGoogleDomain(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, gd := range googleDomains {
		if host == gd || strings.HasSuffix(host, "."+gd) {
			return true
		}
	}
	return false
}
