package prodir

import (
	"regexp"
	"strings"
	"unicode"
)

// Contact details hidden in free text (bios, descriptions, misused fields).
// The directories have no structured field for most of them, so load scans the
// text it already has; nothing here touches the network.

// found is what extractContacts finds in a text.
type found struct {
	Emails    []string          // lower-cased, first-seen order
	Phones    []string          // as written, de-duplicated by digits
	Social    map[string]string // platform -> URL with scheme, first one wins
	Websites  []string          // URLs with scheme
	Addresses []string          // street addresses
}

var (
	// An optional "http://" / "www." in front of the address is swallowed, so
	// "http://someone@gmail.com" yields the address and no leftover URL.
	extEmailRe  = regexp.MustCompile(`(?i)(?:https?://)?(?:www\.)?[a-z0-9][a-z0-9._%+\-]*@[a-z0-9\-]+(?:\.[a-z0-9\-]+)*\.[a-z]{2,}`)
	extAssetTLD = map[string]bool{"png": true, "jpg": true, "jpeg": true, "gif": true, "webp": true, "svg": true}

	// scheme URL, "www." + a real domain, or a bare domain with a known TLD
	extURLRe = regexp.MustCompile(`(?i)\bhttps?://[^\s<>"'()\[\]]+|\bwww\.[a-z0-9][a-z0-9\-]*(?:\.[a-z0-9\-]+)+[^\s<>"'()\[\]]*|` +
		`\b[a-z0-9][a-z0-9\-]*(?:\.[a-z0-9\-]+)*\.(?:com|net|org|fit|us|ca|co\.uk|info|biz|me|io|health|coach|life|studio|fitness)\b(?:/[^\s<>"'()\[\]]*)?`)

	// platform word, up to ~25 characters on the same line, then @handle; the
	// handle belongs to the last platform word before the @
	extHandleRe   = regexp.MustCompile(`(?i)\b(?:instagram|insta|ig|tik ?tok|twitter|facebook|fb|youtube)\b[^@\n]{0,25}@([a-z0-9_][a-z0-9_.]*)`)
	extPlatformRe = regexp.MustCompile(`(?i)\b(?:instagram|insta|ig|tik ?tok|twitter|facebook|fb|youtube)\b`)
	// what follows a handle that is really a page name, and a platform word right after it
	extPageNameRe      = regexp.MustCompile(`^ +& +`)
	extPlatformStartRe = regexp.MustCompile(`(?i)^(?:instagram|insta|ig|tik ?tok|twitter|facebook|fb|youtube)\b`)

	// Phone candidates: "(555) 010-0199" / "(555)010-0199", "+<digits>" with few
	// separators, or a leading group followed by 2-5 separated groups
	// ("555.010.0142"). A separator is one space or dot, or a dash with optional
	// spaces; ". " after digits is a sentence end, never a separator.
	extPhoneSep = `(?: ?- ?|[ .])`
	extPhoneRe  = regexp.MustCompile(`\(\d{1,4}\)[ .\-]?\d{1,5}(?:` + extPhoneSep + `\d{1,5}){1,4}` +
		`|\+\d(?:` + extPhoneSep + `?\(?\d+\)?){1,5}` +
		`|\d{1,5}(?:` + extPhoneSep + `\(?\d{1,5}\)?){2,5}`)

	extStreet = `(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Highway|Hwy|Parkway|Pkwy|Place|Pl|Circle|Cir)`
	// house number, 1-4 capitalised words, suffix, optional unit, optional ", City, ST 12345"
	extAddressRe = regexp.MustCompile(`\b\d{1,6}[ \t]+(?:[NSEW]\.?[ \t]+)?(?:[A-Z0-9][\w.'&\-]*[ \t]+){1,4}?` + extStreet + `\b\.?` +
		`(?:,?[ \t]*(?:Suite|Ste|Unit|Apt|#)\.?[ \t]*[\w\-]+)?` +
		`(?:,[ \t]*[A-Z][A-Za-z.]*(?:[ \t]+[A-Z][A-Za-z.]*)*,?[ \t]+[A-Z][A-Za-z]?\.?[ \t]+\d{5}(?:-\d{4})?)?`)
)

// extOwnDomains are the directories themselves: links to them are not the pro's.
var extOwnDomains = []string{"acefitness.org", "coachingfederation.org"}

func isOwnDomain(host string) bool {
	for _, d := range extOwnDomains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

// urlHost is the lower-cased host of a URL-ish string (no scheme, userinfo, port).
func urlHost(u string) string {
	h := strings.ToLower(aceSchemeRe.ReplaceAllString(strings.ToLower(u), ""))
	if i := strings.IndexAny(h, "/?#"); i >= 0 {
		h = h[:i]
	}
	if i := strings.LastIndex(h, "@"); i >= 0 {
		h = h[i+1:]
	}
	if i := strings.Index(h, ":"); i >= 0 {
		h = h[:i]
	}
	return h
}

// validHost needs hostname characters, a dot and an alphabetic TLD.
func validHost(h string) bool {
	for _, r := range h {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '.' || r == '_') {
			return false
		}
	}
	if strings.HasPrefix(h, "www.") && !strings.Contains(h[4:], ".") {
		return false // "www.Name": a www. host needs a real domain behind it
	}
	i := strings.LastIndex(h, ".")
	if i < 1 || len(h)-i-1 < 2 {
		return false
	}
	for _, r := range h[i+1:] {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

// sentenceBoundary reports a scheme-less, www-less domain whose TLD is
// capitalised while the host is not all upper-case: two sentences glued together
// ("Ms.Fit", "dancer.Fitness"), not a site. "FIXTUREMOM.COM" is a site.
func sentenceBoundary(u string) bool {
	h := u
	if i := strings.IndexAny(h, "/?#"); i >= 0 {
		h = h[:i]
	}
	tld := h[strings.LastIndex(h, ".")+1:]
	return tld != "" && unicode.IsUpper([]rune(tld)[0]) && h != strings.ToUpper(h)
}

// socialProfile reports whether a social URL points at a profile: it needs a
// path ("linkedin.com/" alone is no profile), and YouTube video links
// ("youtube.com/watch?v=...", "youtu.be/...") are no channel.
func socialProfile(u string) bool {
	rest := aceSchemeRe.ReplaceAllString(strings.ToLower(u), "")
	host, path, _ := strings.Cut(rest, "/")
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	path = strings.Trim(path, "/")
	if path == "" || host == "youtu.be" || strings.HasSuffix(host, ".youtu.be") {
		return false
	}
	return !(strings.HasSuffix(host, "youtube.com") && path == "watch")
}

// digitsOf keeps the ASCII digits of s.
func digitsOf(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
}

// cutEmails returns text with every email replaced by a space, and the emails
// (lower-cased, first-seen order). Addresses at the directories' own domains and
// asset-looking ones ("logo@2x.png") are dropped unless keepAll: the structured
// Email value of a profile keeps whatever address it holds.
func cutEmails(text string, keepAll bool) (string, []string) {
	var emails []string
	seen := map[string]bool{}
	text = extEmailRe.ReplaceAllStringFunc(text, func(m string) string {
		at := strings.LastIndex(m, "@")
		local, domain := strings.ToLower(m[:at]), strings.ToLower(m[at+1:])
		local = strings.TrimPrefix(strings.TrimPrefix(local, "https://"), "http://")
		local = strings.TrimPrefix(local, "www.")
		if !keepAll && (isOwnDomain(domain) || extAssetTLD[domain[strings.LastIndex(domain, ".")+1:]]) {
			return " "
		}
		if addr := local + "@" + domain; !seen[addr] {
			seen[addr] = true
			emails = append(emails, addr)
		}
		return " "
	})
	return text, emails
}

// structuredEmails are the addresses in a structured Email value.
func structuredEmails(v string) []string {
	_, emails := cutEmails(v, true)
	return emails
}

// structuredPhone is a structured phone value, or "" when it has fewer than 7
// digits ("n/a", "-", "+44").
func structuredPhone(v string) string {
	if v = clean(v); len(digitsOf(v)) < 7 {
		return ""
	}
	return v
}

// extractContacts finds emails, phones, social links, websites and street
// addresses in free text.
func extractContacts(text string) found {
	f := found{Social: map[string]string{}}
	text = strings.ReplaceAll(text, " ", " ")

	// Emails first and cut out, so an email domain is never a website and
	// name@gmail.com is never a handle.
	text, f.Emails = cutEmails(text, false)

	// Handles next, cut out too ("@fixture.coach" is not a ".coach" domain);
	// they only fill platforms no URL claimed.
	handles := map[string]string{}
	var cut strings.Builder
	last := 0
	for _, loc := range extHandleRe.FindAllStringSubmatchIndex(text, -1) {
		cut.WriteString(text[last:loc[0]])
		cut.WriteByte(' ')
		last = loc[1]
		m := text[loc[0]:loc[1]]
		h := strings.TrimRight(text[loc[2]:loc[3]], ".")
		// "FB@Fixture & Mind Studio": a name with spaces, not a handle (unless
		// a platform word follows the "&": "IG @a & Twitter @b").
		if after := extPageNameRe.FindString(text[loc[1]:]); after != "" && !extPlatformStartRe.MatchString(text[loc[1]+len(after):]) {
			continue
		}
		words := extPlatformRe.FindAllString(m[:strings.Index(m, "@")], -1)
		var p, u string
		switch strings.ToLower(strings.ReplaceAll(words[len(words)-1], " ", "")) {
		case "instagram", "insta", "ig":
			p, u = "instagram", "https://www.instagram.com/"+h
		case "tiktok":
			p, u = "tiktok", "https://www.tiktok.com/@"+h
		case "twitter":
			p, u = "twitter", "https://twitter.com/"+h
		case "facebook", "fb":
			p, u = "facebook", "https://www.facebook.com/"+h
		case "youtube":
			p, u = "youtube", "https://www.youtube.com/@"+h
		}
		if p != "" && h != "" && handles[p] == "" {
			handles[p] = u
		}
	}
	cut.WriteString(text[last:])
	text = cut.String()

	// URLs, cut out as well so their digits are not read as phones.
	var addURL func(m string)
	addURL = func(m string) {
		u := strings.TrimRight(m, ".,;:!?")
		if strings.HasPrefix(aceSchemeRe.ReplaceAllString(strings.ToLower(u), ""), "@") {
			return // "http://@fixture.handle": a handle, not a site
		}
		host := urlHost(u)
		if !validHost(host) {
			// "http://www,site.com", "http://?www.site.com": look for the real site inside
			if rest := aceSchemeRe.ReplaceAllString(strings.TrimLeft(m, " "), ""); rest != m {
				for _, in := range extURLRe.FindAllString(rest, -1) {
					addURL(in)
				}
			}
			return
		}
		if !aceURLRe.MatchString(u) && sentenceBoundary(u) {
			return // "improvement.Health": a sentence boundary without its space
		}
		if isOwnDomain(host) {
			return
		}
		if p := socialPlatform(u); p != "" {
			if socialProfile(u) && f.Social[p] == "" {
				f.Social[p] = withScheme(u)
			}
			return
		}
		w := withScheme(u)
		for _, have := range f.Websites {
			if strings.EqualFold(have, w) {
				return
			}
		}
		f.Websites = append(f.Websites, w)
	}
	text = extURLRe.ReplaceAllStringFunc(text, func(m string) string {
		addURL(m)
		return " "
	})

	for p, u := range handles {
		if f.Social[p] == "" {
			f.Social[p] = u
		}
	}

	for _, loc := range extPhoneRe.FindAllStringIndex(text, -1) {
		if ph := phoneAt(text, loc[0], loc[1]); ph != "" {
			f.Phones = addPhone(f.Phones, ph)
		}
	}

	for _, a := range extAddressRe.FindAllString(text, -1) {
		f.Addresses = addString(f.Addresses, clean(a))
	}
	// "5801 Fixture Road" next to "5801 Fixture Road, Exampleville, TX 75093":
	// keep the longer one.
	kept := f.Addresses[:0:0]
	for _, a := range f.Addresses {
		covered := false
		for _, b := range f.Addresses {
			covered = covered || len(b) > len(a) && strings.HasPrefix(b, strings.TrimRight(a, ".,;"))
		}
		if !covered {
			kept = append(kept, a)
		}
	}
	f.Addresses = kept
	return f
}

// phoneAt validates a phone candidate text[start:end]: 10-15 digits, not glued
// to a longer alphanumeric token. A leading year or 5-digit ZIP ("since 2005
// 555 010 0199") is dropped when the rest is still a phone.
func phoneAt(text string, start, end int) string {
	isAlnum := func(b byte) bool { return b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }
	if start > 0 && isAlnum(text[start-1]) || end < len(text) && isAlnum(text[end]) {
		return ""
	}
	c := text[start:end]
	if i := strings.IndexAny(c, " .-"); i == 5 || i == 4 && (strings.HasPrefix(c, "19") || strings.HasPrefix(c, "20")) {
		if rest := strings.TrimLeft(c[i:], " .-"); len(digitsOf(rest)) >= 10 {
			c = rest
		}
	}
	if n := len(digitsOf(c)); n < 10 || n > 15 {
		return ""
	}
	if strings.Count(c, ")") > strings.Count(c, "(") { // "(555-010-0199)": the parentheses are not part of it
		c = strings.TrimRight(c, ")")
	}
	return clean(c)
}

// addPhone appends p unless a phone with the same digits is already there.
func addPhone(list []string, p string) []string {
	d := digitsOf(p)
	for _, have := range list {
		if digitsOf(have) == d {
			return list
		}
	}
	return append(list, p)
}

var (
	icfPhoneSepRe   = regexp.MustCompile(`(?i)\s*(?:;|,|/|or)\s*`)
	icfPhoneParenRe = regexp.MustCompile(`^(.*?)\s*\(([^()]*)\)$`)
)

// splitICFPhone turns the structured ICF Phone value into its numbers, primary
// first. ";", ",", "/" and "or" separate numbers only when every part has at
// least 7 digits ("0711/1234567" stays whole); a trailing parenthesised group
// with at least 7 digits is another number ("555.010.0100 (5550100100)").
func splitICFPhone(v string) []string {
	v = clean(v)
	if v == "" {
		return nil
	}
	parts := []string{v}
	if split := icfPhoneSepRe.Split(v, -1); len(split) > 1 {
		ok := true
		for _, p := range split {
			ok = ok && len(digitsOf(p)) >= 7
		}
		if ok {
			parts = split
		}
	}
	var out []string
	for _, p := range parts {
		if m := icfPhoneParenRe.FindStringSubmatch(p); m != nil && len(digitsOf(m[1])) >= 7 && len(digitsOf(m[2])) >= 7 {
			out = addPhone(out, clean(m[1]))
			out = addPhone(out, clean(m[2]))
		} else if c := clean(p); len(digitsOf(c)) >= 7 {
			out = addPhone(out, c)
		}
	}
	return out
}
