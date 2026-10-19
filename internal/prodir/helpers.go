package prodir

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// Helpers shared by the HTML sources (src_*.go). They are package-private:
// sources live in package prodir.

// docFromBody parses an HTML response body.
func docFromBody(body []byte) (*goquery.Document, error) {
	return goquery.NewDocumentFromReader(bytes.NewReader(body))
}

// getDoc GETs rawURL (default headers), checks the status with Page.Err and
// parses the body. It also returns the final URL, the base for absURL.
func getDoc(ctx context.Context, f Fetcher, rawURL string) (*goquery.Document, string, error) {
	p, err := f.Get(ctx, rawURL, nil)
	if err != nil {
		return nil, "", err
	}
	if err := p.Err(); err != nil {
		return nil, "", err
	}
	doc, err := docFromBody(p.Body)
	return doc, p.URL, err
}

// absURL resolves ref against the page URL base. An empty ref stays empty and
// an unparsable one is returned unchanged.
func absURL(base, ref string) string {
	if ref = strings.TrimSpace(ref); ref == "" {
		return ""
	}
	r, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	b, err := url.Parse(base)
	if err != nil {
		return ref
	}
	return b.ResolveReference(r).String()
}

// decodeCFEmail decodes a Cloudflare email-protection payload (the hex of a
// data-cfemail attribute, or the part after "#" in /cdn-cgi/l/email-protection#):
// the first byte is the XOR key of all the others.
func decodeCFEmail(h string) (string, bool) {
	b, err := hex.DecodeString(strings.TrimSpace(h))
	if err != nil || len(b) < 2 {
		return "", false
	}
	key := b[0]
	out := make([]byte, len(b)-1)
	for i, c := range b[1:] {
		out[i] = c ^ key
	}
	return string(out), true
}

const cfEmailPath = "/cdn-cgi/l/email-protection#"

// emailsIn returns the structured emails of sel and its descendants: mailto:
// links, Cloudflare-protected addresses (data-cfemail attribute or
// email-protection link), validated and lower-cased, first-seen order. Plain
// text is not scanned: pass an element's text to structuredEmails for that.
func emailsIn(sel *goquery.Selection) []string {
	const q = "a[href], [data-cfemail]"
	var raw []string
	sel.Find(q).AddBackFiltered(q).Each(func(_ int, s *goquery.Selection) {
		if h, ok := s.Attr("data-cfemail"); ok {
			if e, ok := decodeCFEmail(h); ok {
				raw = append(raw, e)
			}
		}
		href, _ := s.Attr("href")
		switch low := strings.ToLower(href); {
		case strings.HasPrefix(low, "mailto:"):
			addr, _, _ := strings.Cut(href[len("mailto:"):], "?")
			if u, err := url.PathUnescape(addr); err == nil {
				addr = u
			}
			raw = append(raw, strings.Split(addr, ",")...)
		case strings.Contains(low, cfEmailPath):
			h := href[strings.Index(low, cfEmailPath)+len(cfEmailPath):]
			if e, ok := decodeCFEmail(h); ok {
				raw = append(raw, e)
			}
		}
	})
	var out []string
	for _, r := range raw {
		for _, e := range structuredEmails(r) {
			out = addString(out, e)
		}
	}
	return out
}
