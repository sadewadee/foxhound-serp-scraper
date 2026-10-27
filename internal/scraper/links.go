package scraper

import (
	"net/url"
	"sort"
	"strings"
	"unicode"

	"github.com/PuerkitoBio/goquery"
)

// contactLinkKeywords are matched as whole words against a link's path and
// anchor text. Order is priority: contact-style links rank before about/team
// ones, so the cap in DiscoverContactLinks keeps the likeliest email carriers.
var contactLinkKeywords = []string{
	"contact", "kontakt", "contacto", "contatti", "get in touch", "reach us",
	"impressum", "about", "team",
}

// guessedContactPaths are the paths the enrichment_jobs trigger already queues
// for every email-less homepage; discovery must not re-queue them.
var guessedContactPaths = map[string]bool{
	"/contact": true, "/contact-us": true, "/about": true, "/about-us": true,
}

var assetExts = map[string]bool{
	".pdf": true, ".jpg": true, ".jpeg": true, ".png": true, ".gif": true,
	".svg": true, ".webp": true, ".ico": true, ".css": true, ".js": true,
	".zip": true, ".doc": true, ".docx": true, ".xls": true, ".xlsx": true,
	".mp3": true, ".mp4": true,
}

// maxAnchorTextLen keeps whole paragraphs wrapped in an <a> from matching.
const maxAnchorTextLen = 40

// DiscoverContactLinks returns up to max same-site links from a fetched page
// whose path or anchor text looks like a contact/about page. pageURL is the
// page the HTML came from (used to resolve relative hrefs). Order is keyword
// priority, then document order.
func DiscoverContactLinks(pageURL string, body []byte, max int) []string {
	base, err := url.Parse(pageURL)
	if err != nil || base.Host == "" || max <= 0 {
		return nil
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(body)))
	if err != nil {
		return nil
	}

	type cand struct {
		url  string
		rank int
	}
	var cands []cand
	seen := map[string]bool{}
	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		href = strings.TrimSpace(href)
		if href == "" || strings.HasPrefix(href, "#") {
			return
		}
		ref, err := url.Parse(href)
		if err != nil {
			return
		}
		u := base.ResolveReference(ref)
		if (u.Scheme != "http" && u.Scheme != "https") || !sameSite(base.Hostname(), u.Hostname()) {
			return
		}
		u.Fragment = ""
		path := strings.ToLower(strings.TrimRight(u.Path, "/"))
		if path == "" || guessedContactPaths[path] || assetExts[pathExt(path)] {
			return
		}
		text := strings.TrimSpace(s.Text())
		rank := keywordRank(path)
		if len(text) <= maxAnchorTextLen {
			if r := keywordRank(strings.ToLower(text)); r >= 0 && (rank < 0 || r < rank) {
				rank = r
			}
		}
		if rank < 0 || seen[u.String()] {
			return
		}
		seen[u.String()] = true
		cands = append(cands, cand{u.String(), rank})
	})

	sort.SliceStable(cands, func(i, j int) bool { return cands[i].rank < cands[j].rank })
	var out []string
	for _, c := range cands {
		if len(out) == max {
			break
		}
		out = append(out, c.url)
	}
	return out
}

// keywordRank returns the index of the highest-priority keyword found as a
// whole-word sequence in s, or -1.
func keywordRank(s string) int {
	words := strings.FieldsFunc(s, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	padded := " " + strings.Join(words, " ") + " "
	for i, kw := range contactLinkKeywords {
		if strings.Contains(padded, " "+kw+" ") {
			return i
		}
	}
	return -1
}

func sameSite(a, b string) bool {
	return strings.TrimPrefix(strings.ToLower(a), "www.") == strings.TrimPrefix(strings.ToLower(b), "www.")
}

func pathExt(p string) string {
	i := strings.LastIndex(p, ".")
	if i < 0 || strings.Contains(p[i:], "/") {
		return ""
	}
	return p[i:]
}
