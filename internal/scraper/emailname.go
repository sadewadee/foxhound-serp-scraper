package scraper

// Per-email person names. A name is recorded for an email ONLY when the page
// itself ties that name to that email (JSON-LD Person with the same email, the
// text of its mailto: anchor, or text sitting right next to the address).
// No evidence -> no name. The listing-level ContactName is deliberately not
// used here: it is often an author, agency or org name.

import (
	"bytes"
	"encoding/json"
	"html"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/text/unicode/norm"
)

// Sources stored in business_emails.person_name_source.
const (
	NameSourceJSONLD   = "jsonld_person"
	NameSourceMailto   = "mailto_text"
	NameSourceAdjacent = "adjacent_text"
)

// EmailName is the person name a page ties to one email.
type EmailName struct {
	Name   string `json:"name"`
	Source string `json:"source"`
}

// maxNameEmails bounds the per-page work on pages that dump huge email lists.
const maxNameEmails = 50

// nonNameWords is the single list of role / business / generic words. It is
// used both to reject name candidates (any token in it) and to detect role
// accounts (any local-part segment in it). Lowercase, ASCII-folded.
var nonNameWords = setOfWords(
	"info", "information", "contact", "contacts", "contactus", "office", "team", "studio", "yoga", "pilates",
	"spa", "hotel", "gmbh", "ltd", "llc", "inc", "corp", "limited", "ag", "sarl", "bv", "pty",
	"email", "emails", "mail", "kontakt", "booking", "bookings", "book", "reception",
	"admin", "administrator", "support", "sales", "marketing", "hello", "hi", "hey", "welcome",
	"enquiries", "enquiry", "inquiries", "inquiry", "service", "services", "customer", "help",
	"general", "manager", "management", "director", "owner", "staff", "training", "fitness",
	"wellness", "health", "center", "centre", "club", "school", "academy", "institute", "gym",
	"retreat", "resort", "group", "company", "shop", "store", "online", "web", "webmaster", "website",
	"newsletter", "press", "media", "hr", "jobs", "careers", "events", "event", "feedback", "privacy",
	"legal", "billing", "accounts", "finance", "orders", "order", "postmaster", "noreply", "no-reply",
	"us", "we", "our", "call", "write", "send", "click", "here", "touch", "reach", "massage", "therapy",
	"clinic", "studios", "classes", "class", "teacher", "teachers", "instructor", "coach", "coaching",
)

func setOfWords(w ...string) map[string]bool {
	m := make(map[string]bool, len(w))
	for _, s := range w {
		m[s] = true
	}
	return m
}

var (
	nameSplitRe   = regexp.MustCompile(`\s[-–—]\s|[–—•·|:;,/()\[\]<>]`)
	nameTrimSet   = " \t\r\n,:;|<>([{}])-–—•·/\"'“”‘’"
	localSplitRe  = regexp.MustCompile(`[._\-+]+`)
	blockTagRe    = regexp.MustCompile(`(?i)<\s*(?:br|/p|/div|/li|/tr|/td|/th|/h[1-6]|/section|/article|/address)\b[^>]*>`)
	scriptStyleRe = regexp.MustCompile(`(?is)<(script|style|noscript)\b.*?</(script|style|noscript)\s*>`)
	anyTagRe      = regexp.MustCompile(`<[^>]*>`)
)

// ExtractEmailNames returns, for each email that has page evidence, the person
// name tied to it. Keys are lowercase emails. businessName (may be empty) is
// rejected as a name.
func ExtractEmailNames(body []byte, emails []string, businessName string) map[string]EmailName {
	out := map[string]EmailName{}
	if len(emails) > maxNameEmails {
		emails = emails[:maxNameEmails]
	}
	want := map[string]bool{}
	for _, e := range emails {
		e = strings.ToLower(strings.TrimSpace(e))
		if e != "" && !isRoleLocal(localPart(e)) {
			want[e] = true
		}
	}
	if len(want) == 0 {
		return out
	}
	set := func(email, name, source string) {
		if _, done := out[email]; !done {
			out[email] = EmailName{Name: name, Source: source}
		}
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err == nil {
		// a. JSON-LD Person carrying this exact email.
		doc.Find(`script[type="application/ld+json"]`).Each(func(_ int, s *goquery.Selection) {
			var v any
			if json.Unmarshal([]byte(s.Text()), &v) != nil {
				return
			}
			walkJSONLD(v, "", func(m map[string]any, parentKey string) {
				email := cleanMailto(strVal(m["email"]))
				name := strVal(m["name"])
				if !want[email] || !isPersonNode(m, parentKey) || validName(name, businessName) == "" {
					return
				}
				set(email, validName(name, businessName), NameSourceJSONLD)
			})
		})

		// b. Text of the mailto: anchor, when it is a name.
		doc.Find(`a[href]`).Each(func(_ int, s *goquery.Selection) {
			href, _ := s.Attr("href")
			if len(href) < 7 || !strings.EqualFold(href[:7], "mailto:") {
				return
			}
			email := cleanMailto(href)
			text := strings.Join(strings.Fields(s.Text()), " ")
			if !want[email] || strings.Contains(text, "@") {
				return
			}
			if n := validName(text, businessName); n != "" && localMatchesName(localPart(email), n) {
				set(email, n, NameSourceMailto)
			}
		})
	}

	// c. Text adjacent to the address on the same line.
	text := pageLines(body)
	for email := range want {
		if _, done := out[email]; done {
			continue
		}
		for _, line := range text {
			if n := adjacentName(line, email, businessName); n != "" {
				out[email] = EmailName{Name: n, Source: NameSourceAdjacent}
				break
			}
		}
	}
	return out
}

// EmailNamesJSON marshals names for the given (final, filtered) emails as the
// raw_email_names JSONB value, or returns nil (SQL NULL) when there are none.
func EmailNamesJSON(names map[string]EmailName, emails []string) []byte {
	kept := map[string]EmailName{}
	for _, e := range emails {
		if n, ok := names[strings.ToLower(e)]; ok {
			kept[strings.ToLower(e)] = n
		}
	}
	if len(kept) == 0 {
		return nil
	}
	b, err := json.Marshal(kept)
	if err != nil {
		return nil
	}
	return b
}

func localPart(email string) string {
	if i := strings.IndexByte(email, '@'); i >= 0 {
		return email[:i]
	}
	return email
}

// isRoleLocal reports a role account: any local-part segment is a role word.
func isRoleLocal(local string) bool {
	if nonNameWords[foldLetters(local)] {
		return true
	}
	for _, seg := range localSplitRe.Split(local, -1) {
		if nonNameWords[foldLetters(seg)] {
			return true
		}
	}
	return false
}

func cleanMailto(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 7 && strings.EqualFold(s[:7], "mailto:") {
		s = s[7:]
	}
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[:i]
	}
	if u, err := url.QueryUnescape(s); err == nil {
		s = u
	}
	return strings.ToLower(strings.TrimSpace(s))
}

func strVal(v any) string {
	s, _ := v.(string)
	return s
}

func walkJSONLD(v any, parentKey string, fn func(map[string]any, string)) {
	switch t := v.(type) {
	case map[string]any:
		fn(t, parentKey)
		for k, c := range t {
			walkJSONLD(c, k, fn)
		}
	case []any:
		for _, c := range t {
			walkJSONLD(c, parentKey, fn)
		}
	}
}

var personKeys = setOfWords("employee", "employees", "founder", "founders", "member", "members", "contactpoint")

// isPersonNode: a JSON-LD node typed Person, or nested as employee / founder /
// member / contactPoint.
func isPersonNode(m map[string]any, parentKey string) bool {
	switch t := m["@type"].(type) {
	case string:
		if strings.HasSuffix(t, "Person") {
			return true
		}
	case []any:
		for _, v := range t {
			if strings.HasSuffix(strVal(v), "Person") {
				return true
			}
		}
	}
	return personKeys[strings.ToLower(parentKey)]
}

// pageLines turns the page into text lines: scripts/styles dropped, block
// boundaries become newlines, remaining tags vanish (so inline markup around a
// name/address does not split them), entities decoded.
func pageLines(body []byte) []string {
	s := scriptStyleRe.ReplaceAllString(string(body), " ")
	s = blockTagRe.ReplaceAllString(s, "\n")
	s = anyTagRe.ReplaceAllString(s, "")
	s = strings.ReplaceAll(html.UnescapeString(s), " ", " ")
	return strings.Split(s, "\n")
}

func isEmailChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '%' || c == '+' || c == '-'
}

// adjacentName finds email in line and validates the text right before it.
func adjacentName(line, email, businessName string) string {
	lower := strings.ToLower(line)
	if len(lower) != len(line) || !strings.Contains(lower, email) {
		return ""
	}
	for from := 0; ; {
		i := strings.Index(lower[from:], email)
		if i < 0 {
			return ""
		}
		i += from
		from = i + len(email)
		if i > 0 && isEmailChar(lower[i-1]) {
			continue
		}
		if from < len(lower) && (lower[from] == '-' || lower[from] >= 'a' && lower[from] <= 'z' || lower[from] >= '0' && lower[from] <= '9') {
			continue
		}
		prefix := strings.TrimRight(line[:i], nameTrimSet)
		segs := nameSplitRe.Split(prefix, -1)
		cand := strings.Trim(segs[len(segs)-1], nameTrimSet)
		if n := validName(cand, businessName); n != "" && localMatchesName(localPart(email), n) {
			return n
		}
	}
}

// validName returns the cleaned name when s has the shape of a person name
// (2-4 capitalized letter-only tokens, no role/business word, not the business
// name), else "".
func validName(s, businessName string) string {
	s = strings.Join(strings.Fields(strings.Trim(s, nameTrimSet)), " ")
	if s == "" || utf8.RuneCountInString(s) > 60 {
		return ""
	}
	toks := strings.Fields(s)
	if len(toks) < 2 || len(toks) > 4 {
		return ""
	}
	for _, t := range toks {
		first, _ := utf8.DecodeRuneInString(t)
		if !unicode.IsUpper(first) {
			return ""
		}
		for _, r := range t {
			if !unicode.IsLetter(r) && r != '\'' && r != '’' && r != '-' {
				return ""
			}
		}
		if nonNameWords[foldLetters(t)] {
			return ""
		}
	}
	if businessName != "" && foldLetters(s) == foldLetters(businessName) {
		return ""
	}
	return s
}

// localMatchesName: some name token (>= 3 letters) appears in the local part,
// or the local part is initial+surname / surname+initial (j.doe, jdoe, doej).
func localMatchesName(local, name string) bool {
	l := foldLetters(local)
	toks := strings.Fields(name)
	folded := make([]string, len(toks))
	for i, t := range toks {
		folded[i] = foldLetters(t)
		if len(folded[i]) >= 3 && strings.Contains(l, folded[i]) {
			return true
		}
	}
	for _, f := range folded[:1] {
		for _, sur := range folded[1:] {
			if f == "" || sur == "" {
				continue
			}
			if l == f[:1]+sur || l == sur+f[:1] {
				return true
			}
		}
	}
	for _, f := range folded[1:] {
		if f != "" && (l == f[:1]+folded[0] || l == folded[0]+f[:1]) {
			return true
		}
	}
	return false
}

// foldLetters lowercases s, strips diacritics and keeps only ASCII a-z.
func foldLetters(s string) string {
	var b strings.Builder
	for _, r := range norm.NFD.String(strings.ToLower(s)) {
		if r >= 'a' && r <= 'z' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
