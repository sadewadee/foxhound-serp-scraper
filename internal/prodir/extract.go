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

	extURLRe = regexp.MustCompile(`(?i)\bhttps?://[^\s<>"'()\[\]]+|\bwww\.[^\s<>"'()\[\]]+|` +
		`\b[a-z0-9][a-z0-9\-]*(?:\.[a-z0-9\-]+)*\.(?:com|net|org|fit|us|ca|co\.uk|info|biz|me|io|health|coach|life|studio|fitness)\b(?:/[^\s<>"'()\[\]]*)?`)

	// platform word, up to ~25 characters on the same line, then @handle
	extHandleRe = regexp.MustCompile(`(?i)\b(instagram|insta|ig|tik ?tok|twitter|facebook|fb|youtube)\b[^@\n]{0,25}@([a-z0-9_][a-z0-9_.]*)`)

	// leading group, then 2-5 separated groups: "(555) 010-0199", "+1 555 010 0100"
	extPhoneRe = regexp.MustCompile(`\+?\(?\d{1,4}\)?(?:[ .\-]{1,2}\(?\d{1,5}\)?){2,5}`)

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

// validHost needs a dot and an alphabetic TLD.
func validHost(h string) bool {
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

// digitsOf keeps the ASCII digits of s.
func digitsOf(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
}

// extractContacts finds emails, phones, social links, websites and street
// addresses in free text.
func extractContacts(text string) found {
	f := found{Social: map[string]string{}}
	text = strings.ReplaceAll(text, " ", " ")

	// Emails first and cut out, so an email domain is never a website and
	// name@gmail.com is never a handle.
	seenEmail := map[string]bool{}
	text = extEmailRe.ReplaceAllStringFunc(text, func(m string) string {
		at := strings.LastIndex(m, "@")
		local, domain := strings.ToLower(m[:at]), strings.ToLower(m[at+1:])
		local = strings.TrimPrefix(strings.TrimPrefix(local, "https://"), "http://")
		local = strings.TrimPrefix(local, "www.")
		if isOwnDomain(domain) || extAssetTLD[domain[strings.LastIndex(domain, ".")+1:]] {
			return " "
		}
		if addr := local + "@" + domain; !seenEmail[addr] {
			seenEmail[addr] = true
			f.Emails = append(f.Emails, addr)
		}
		return " "
	})

	// Handles next, cut out too ("@fixture.coach" is not a ".coach" domain);
	// they only fill platforms no URL claimed.
	handles := map[string]string{}
	text = extHandleRe.ReplaceAllStringFunc(text, func(m string) string {
		sub := extHandleRe.FindStringSubmatch(m)
		h := strings.TrimRight(sub[2], ".")
		var p, u string
		switch strings.ToLower(strings.ReplaceAll(sub[1], " ", "")) {
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
		return " "
	})

	// URLs, cut out as well so their digits are not read as phones.
	text = extURLRe.ReplaceAllStringFunc(text, func(m string) string {
		u := strings.TrimRight(m, ".,;:!?")
		host := urlHost(u)
		if !validHost(host) || isOwnDomain(host) {
			return " "
		}
		if p := socialPlatform(u); p != "" {
			if f.Social[p] == "" {
				f.Social[p] = withScheme(u)
			}
			return " "
		}
		w := withScheme(u)
		for _, have := range f.Websites {
			if strings.EqualFold(have, w) {
				return " "
			}
		}
		f.Websites = append(f.Websites, w)
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
		a = clean(a)
		dup := false
		for _, have := range f.Addresses {
			dup = dup || have == a
		}
		if !dup {
			f.Addresses = append(f.Addresses, a)
		}
	}
	return f
}

// phoneAt validates a phone candidate text[start:end]: 10-15 digits, not glued
// to a longer alphanumeric token. A leading year ("since 2005 555 010 0199")
// is dropped when the rest is still a phone.
func phoneAt(text string, start, end int) string {
	isAlnum := func(b byte) bool { return b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }
	if start > 0 && isAlnum(text[start-1]) || end < len(text) && isAlnum(text[end]) {
		return ""
	}
	c := text[start:end]
	if i := strings.IndexAny(c, " .-"); i == 4 && (strings.HasPrefix(c, "19") || strings.HasPrefix(c, "20")) {
		if rest := strings.TrimLeft(c[i:], " .-"); len(digitsOf(rest)) >= 10 {
			c = rest
		}
	}
	if n := len(digitsOf(c)); n < 10 || n > 15 {
		return ""
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
		} else if c := clean(p); c != "" {
			out = addPhone(out, c)
		}
	}
	return out
}
