//go:build playwright

package scraper

import (
	"fmt"
	"net/url"
	"strings"
	"time"

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

// socialDomains are social media and content platforms to exclude from SERP results.
// These waste crawl budget — no contact emails on social profiles.
var socialDomains = []string{
	// Social media
	"facebook.com", "instagram.com", "twitter.com", "x.com",
	"linkedin.com", "tiktok.com", "pinterest.com", "snapchat.com",
	"reddit.com", "tumblr.com", "threads.net",
	// Video
	"vimeo.com", "dailymotion.com",
	// Review / listing (no direct contact) — yelp, tripadvisor handled as directories
	"trustpilot.com", "foursquare.com", "zomato.com",
	// Aggregators / directories (low email yield)
	"wikipedia.org", "wikimedia.org",
	"amazon.com", "ebay.com", "alibaba.com",
	"apple.com", "play.google.com",
	// Link shorteners / aggregators
	"linktr.ee", "linkin.bio", "bit.ly", "t.co",
	"medium.com", "blogspot.com", "wordpress.com",
}

// GoogleEngine implements SearchEngine for Google Search.
type GoogleEngine struct{}

func (g *GoogleEngine) Name() string { return "google" }

func (g *GoogleEngine) BuildURL(query string, page, perPage int, gl, hl string) string {
	q := url.QueryEscape(query)
	start := page * perPage
	return fmt.Sprintf(
		"https://www.google.com/search?q=%s&num=%d&start=%d&gl=%s&hl=%s",
		q, perPage, start, url.QueryEscape(gl), url.QueryEscape(hl),
	)
}

func (g *GoogleEngine) ParseResults(body []byte) ([]string, error) {
	return ParseSERPResults(body)
}

func (g *GoogleEngine) FetchSteps() []foxhound.JobStep {
	return []foxhound.JobStep{
		// Click Google consent "Accept all" button if present.
		{Action: foxhound.JobStepClick, Selector: "button#L2AGLb", Optional: true},
		{Action: foxhound.JobStepClick, Selector: "button[jsname='higCR']", Optional: true},
		// Wait for results — optional so we still get content even if captcha blocks.
		{Action: foxhound.JobStepWait, Selector: "div#search", Duration: 10 * time.Second, Optional: true},
	}
}

func (g *GoogleEngine) IsCaptchaPage(body []byte) bool {
	return IsCaptchaPage(body)
}

func (g *GoogleEngine) ExcludedDomains() []string {
	return googleDomains
}

func (g *GoogleEngine) MaxPages() int { return 3 }

func (g *GoogleEngine) NeedsBrowser() bool { return true }

// BuildSERPURL constructs a Google Search URL.
// Retained for backward compatibility with existing callers.
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
			// Filter social media — but keep directory sites (yelp, classpass, etc.)
			// They'll be handled by the directory extractor.
			if isExcludedDomain(href) && !isDirectoryDomain(href) {
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

// isDirectoryDomain returns true if the URL is a known business directory
// that should be kept for listing extraction.
func isDirectoryDomain(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	dirDomains := []string{
		"classpass.com", "yelp.com", "yellowpages.com",
		"yogaalliance.org", "tripadvisor.com",
	}
	for _, d := range dirDomains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

// IsCaptchaPage checks if the HTML body contains Google captcha/block indicators.
func IsCaptchaPage(body []byte) bool {
	lower := strings.ToLower(string(body))
	indicators := []string{
		"captcha",
		"unusual traffic",
		"not a robot",
		"recaptcha",
		"/sorry/",
		"detected unusual traffic",
		"systems have detected",
	}
	for _, ind := range indicators {
		if strings.Contains(lower, ind) {
			return true
		}
	}
	return false
}

// isExcludedDomain returns true if the URL is a social media or low-yield platform.
func isExcludedDomain(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, sd := range socialDomains {
		if host == sd || strings.HasSuffix(host, "."+sd) {
			return true
		}
	}
	return false
}
