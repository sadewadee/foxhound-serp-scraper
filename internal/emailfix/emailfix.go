// Package emailfix repairs email addresses whose page text got glued on during
// extraction: a phone number in front of the local part
// ("+1-212-555-7575info@studio.com") or website text after the TLD
// ("info@inivie.comview"). It is pure logic (no DB, no network) so the extractor
// and the one-off repair command share one definition.
package emailfix

import (
	"regexp"
	"strings"

	"golang.org/x/net/publicsuffix"
)

var cleanRe = regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)

// minPhoneDigits is the digit count a leading run needs to count as a phone number.
const minPhoneDigits = 7

// roleWords are local parts that, right after a digit run, mark the run as glued
// phone text even when the run has no separator and the domain is free-mail.
var roleWords = []string{
	"info", "hello", "hi", "contact", "office", "booking", "book", "reservation",
	"reception", "enquir", "inquir", "admin", "support", "sales", "mail", "email",
	"team", "studio", "manager", "marketing", "marcom", "events", "membership",
	"service", "care", "help", "welcome",
}

// freeMail lists exact free-mail domains; freeMailFamilies lists provider
// prefixes that come with many country TLDs (yahoo.co.uk, outlook.fr, ...).
var freeMail = map[string]bool{
	"gmail.com": true, "googlemail.com": true, "icloud.com": true, "me.com": true,
	"mac.com": true, "qq.com": true, "vip.qq.com": true, "foxmail.com": true,
	"163.com": true, "126.com": true, "139.com": true, "sina.com": true,
	"sina.cn": true, "sohu.com": true, "yeah.net": true, "naver.com": true,
	"daum.net": true, "hanmail.net": true, "mail.ru": true, "aol.com": true,
	"msn.com": true, "proton.me": true, "protonmail.com": true, "web.de": true,
	"rediffmail.com": true,
}

var freeMailFamilies = []string{"yahoo.", "hotmail.", "outlook.", "live.", "yandex.", "gmx."}

func isFreeMail(domain string) bool {
	if freeMail[domain] {
		return true
	}
	for _, p := range freeMailFamilies {
		if strings.HasPrefix(domain, p) {
			return true
		}
	}
	return false
}

func isPhoneRune(r byte) bool {
	return r == '+' || (r >= '0' && r <= '9') || strings.IndexByte(" ().-", r) >= 0
}

func hasRoleWordPrefix(s string) bool {
	for _, w := range roleWords {
		if strings.HasPrefix(s, w) {
			return true
		}
	}
	return false
}

// stripPhonePrefix applies R1 to a lowercase local part.
func stripPhonePrefix(local, domain string) string {
	i, digits, marked := 0, 0, false
	for i < len(local) && isPhoneRune(local[i]) {
		if c := local[i]; c >= '0' && c <= '9' {
			digits++
		} else {
			marked = true // '+' or a separator
		}
		i++
	}
	// The run must be followed by a letter and leave a usable local part.
	if digits < minPhoneDigits || i == len(local) || local[i] < 'a' || local[i] > 'z' || len(local)-i < 2 {
		return local
	}
	rest := local[i:]
	if marked || hasRoleWordPrefix(rest) || !isFreeMail(domain) {
		return rest
	}
	return local // bare digits + free-mail: likely a real number-based address
}

// cutGluedTLD applies R2: trim the last domain label back to its longest prefix
// that is an ICANN TLD.
func cutGluedTLD(domain string) string {
	dot := strings.LastIndex(domain, ".")
	if dot < 0 {
		return domain
	}
	label := domain[dot+1:]
	if _, icann := publicsuffix.PublicSuffix(label); icann {
		return domain
	}
	for n := len(label) - 1; n >= 2; n-- {
		if _, icann := publicsuffix.PublicSuffix(label[:n]); icann {
			return domain[:dot+1] + label[:n]
		}
	}
	return domain
}

// Repair strips glued phone text from the local part and glued text from the TLD.
// fixed is lowercase; changed reports whether a repair (beyond case) altered the
// address; ok is false when the result is still not a clean address.
func Repair(email string) (fixed string, changed bool, ok bool) {
	orig := strings.ToLower(strings.TrimSpace(email))
	at := strings.LastIndex(orig, "@")
	if at < 0 {
		return orig, false, false
	}
	domain := cutGluedTLD(orig[at+1:])
	local := stripPhonePrefix(orig[:at], domain)
	fixed = local + "@" + domain
	changed = fixed != orig
	if len(local) < 2 || !cleanRe.MatchString(fixed) {
		return fixed, changed, false
	}
	_, icann := publicsuffix.PublicSuffix(domain[strings.LastIndex(domain, ".")+1:])
	return fixed, changed, icann
}
