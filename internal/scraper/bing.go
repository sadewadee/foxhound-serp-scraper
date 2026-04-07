//go:build playwright

package scraper

import (
	"encoding/base64"
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

	// Bing organic results: anchor inside h2 inside li.b_algo.
	// Picking the first a[href] picks tracking/sitelinks; h2 a is the title link.
	// Bing wraps real URLs in /ck/a?...&u=a1<base64>&ntb=1 — decode to get target.
	doc.Each("li.b_algo h2 a", func(_ int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists || href == "" {
			return
		}
		// Unwrap Bing tracking redirect.
		if strings.Contains(href, "bing.com/ck/a") {
			if real := unwrapBingRedirect(href); real != "" {
				href = real
			}
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

// unwrapBingRedirect decodes the actual URL from a Bing /ck/a? tracking link.
// Format: ...&u=a1<base64-url-of-target>&ntb=1
// The "a1" prefix marks the encoding scheme; remainder is base64url of the URL.
func unwrapBingRedirect(href string) string {
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}
	uParam := u.Query().Get("u")
	if uParam == "" {
		return ""
	}
	if !strings.HasPrefix(uParam, "a1") {
		return ""
	}
	encoded := uParam[2:]
	// Bing uses URL-safe base64 without padding.
	if pad := len(encoded) % 4; pad != 0 {
		encoded += strings.Repeat("=", 4-pad)
	}
	decoded, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		// Try standard base64.
		decoded, err = base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return ""
		}
	}
	return string(decoded)
}

func (b *BingEngine) FetchSteps() []foxhound.JobStep {
	return nil // stealth HTTP — no browser steps needed
}

func (b *BingEngine) IsCaptchaPage(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "turing") ||
		strings.Contains(lower, "/captcha") ||
		strings.Contains(lower, "class=\"captcha") ||
		strings.Contains(lower, "captcha_header")
}

func (b *BingEngine) ExcludedDomains() []string {
	return bingDomains
}

func (b *BingEngine) MaxPages() int { return 5 }

// NeedsBrowser returns false — Bing works via stealth HTTP with proxy.
// Browser fingerprint triggers Cloudflare Turnstile; stealth avoids it.
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
