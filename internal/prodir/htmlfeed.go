package prodir

import (
	"bytes"
	"html"
	"strings"

	xhtml "golang.org/x/net/html"
)

// htmlHandler receives the same event stream Python's html.parser.HTMLParser
// (convert_charrefs=True) produces; both scrapers' parsers were written against
// that stream and are ported event-for-event.
type htmlHandler interface {
	start(tag string, attrs map[string]string)
	end(tag string)
	data(s string)
}

// feedHTML tokenizes src and replays it into h. Self-closing tags emit start
// then end, text is entity-decoded (but left raw inside <script>/<style>, and
// "\r\n" is kept as-is), attribute values are decoded, the last duplicate
// attribute wins.
func feedHTML(src []byte, h htmlHandler) {
	z := xhtml.NewTokenizer(bytes.NewReader(src))
	rawText := false // inside <script>/<style>
	for {
		tt := z.Next()
		switch tt {
		case xhtml.ErrorToken:
			return
		case xhtml.TextToken:
			s := string(z.Raw())
			if !rawText {
				s = html.UnescapeString(s)
			}
			h.data(s)
		case xhtml.StartTagToken, xhtml.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			tag := string(name)
			attrs := map[string]string{}
			for hasAttr {
				var k, v []byte
				k, v, hasAttr = z.TagAttr()
				attrs[string(k)] = string(v)
			}
			h.start(tag, attrs)
			if tt == xhtml.SelfClosingTagToken {
				h.end(tag)
			} else {
				rawText = tag == "script" || tag == "style"
			}
		case xhtml.EndTagToken:
			name, _ := z.TagName()
			rawText = false
			h.end(string(name))
		}
	}
}

// classSet splits a class attribute into its tokens.
func classSet(attrs map[string]string) map[string]bool {
	set := map[string]bool{}
	for _, c := range strings.Fields(attrs["class"]) {
		set[c] = true
	}
	return set
}
