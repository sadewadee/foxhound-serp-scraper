// Package emailfix repairs email addresses whose page text got glued on during
// extraction: a phone number in front of the local part
// ("+1-212-555-7575info@studio.com"), website text after the TLD
// ("info@inivie.comview"), a label or place name in front of a role word
// ("emailinfo@", "rostockinfo@"), or text between two domains
// ("gmail.comfacebookfacebook.com"). It is pure logic (no DB, no network) so the
// extractor and the one-off repair command share one definition.
//
// The phone and TLD rules are context-free. The label, prefix and mid-domain
// rules guess at real words, so they only fire when a Context vouches for the
// result (the address exists on its own, or the domain is the listing's own).
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

// Context is what the caller knows about the address being repaired. The zero
// value knows nothing, which disables every rule that needs vouching for.
type Context struct {
	// ListingDomains are the domains of the listings the address is linked to.
	ListingDomains []string
	// Exists reports whether an address exists on its own.
	Exists func(email string) bool
	// ExistsLinked reports whether an address exists and is linked to the same
	// listing as the one being repaired.
	ExistsLinked func(email string) bool
}

func (c Context) exists(e string) bool { return c.Exists != nil && c.Exists(e) }

func (c Context) linked(e string) bool { return c.ExistsLinked != nil && c.ExistsLinked(e) }

// ownDomain reports whether domain is (a subdomain of) one of the listing domains.
func (c Context) ownDomain(domain string) bool {
	for _, ld := range c.ListingDomains {
		ld = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(ld)), "www.")
		if ld != "" && (domain == ld || strings.HasSuffix(domain, "."+ld)) {
			return true
		}
	}
	return false
}

// gmailOK enforces Gmail's 6-30 character local part (dots ignored) so a repair
// never shortens a real Gmail address into something Gmail cannot issue.
func gmailOK(local, domain string) bool {
	if domain != "gmail.com" && domain != "googlemail.com" {
		return true
	}
	n := len(strings.ReplaceAll(local, ".", ""))
	return n >= 6 && n <= 30
}

// prefixRoles are the role local parts the label and place-glue rules may
// expose, longest first so "bookings" wins over "mail"-style tails.
var prefixRoles = []string{
	"reservations", "enquiries", "bookings", "booking", "contact", "kontakt",
	"support", "admin", "hello", "office", "info", "mail",
}

// protectedLeads are words that form real compounds with a role word
// ("boxoffice", "customerservice") and must not be stripped.
var protectedLeads = map[string]bool{
	"box": true, "front": true, "back": true, "post": true, "head": true,
	"home": true, "main": true, "customer": true, "client": true, "guest": true,
	"member": true, "student": true, "studierenden": true, "patient": true,
	"tourist": true, "ticket": true, "tickets": true, "press": true,
	"media": true, "general": true, "public": true,
}

func isRoleLocal(s string) bool {
	for _, r := range prefixRoles {
		if s == r {
			return true
		}
	}
	return false
}

func allLetters(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 'a' || s[i] > 'z' {
			return false
		}
	}
	return s != ""
}

// acceptStripped decides whether a shortened local part may replace the
// original. Free-mail shortenings need an existing address linked to the same
// listing; elsewhere the caller's own trust or an existing address is enough.
func acceptStripped(local, domain string, c Context, trusted bool) bool {
	if !gmailOK(local, domain) {
		return false
	}
	cand := local + "@" + domain
	if isFreeMail(domain) {
		return c.linked(cand)
	}
	return trusted || c.exists(cand)
}

// glueLabels are the literal labels that get glued in front of an address.
var glueLabels = []string{"e-mail", "email", "mail"}

// stripLabel removes a literal "email"/"e-mail"/"mail" label, so "emails.anwar369"
// becomes "s.anwar369" (the "s" belongs to the address).
func stripLabel(local, domain string, c Context) (string, bool) {
	for _, l := range glueLabels {
		if !strings.HasPrefix(local, l) {
			continue
		}
		r := strings.TrimLeft(local[len(l):], ".-_:")
		if len(r) >= 2 && acceptStripped(r, domain, c, isRoleLocal(r)) {
			return r, true
		}
	}
	return local, false
}

// stripPlaceGlue removes a place or word glued in front of a role word
// ("rostockinfo" -> "info").
func stripPlaceGlue(local, domain string, c Context) (string, bool) {
	for _, role := range prefixRoles {
		if !strings.HasSuffix(local, role) || len(local)-len(role) < 3 {
			continue
		}
		lead := local[:len(local)-len(role)]
		if !allLetters(lead) || protectedLeads[lead] || isRoleLocal(lead) {
			continue
		}
		if acceptStripped(role, domain, c, c.ownDomain(domain)) {
			return role, true
		}
	}
	return local, false
}

// cutMidDomain handles text between two domains ("gmail.comfacebookfacebook.com"):
// it cuts a non-final label back to an ICANN suffix prefix, at the first label
// where the result is vouched for (the listing's domain, a free-mail provider, or
// an address that exists on its own). Labels that are themselves a suffix
// ("mail.company.com") are left alone.
func cutMidDomain(local, domain string, c Context) string {
	labels := strings.Split(domain, ".")
	for j := 1; j < len(labels)-1; j++ {
		label := labels[j]
		if _, icann := publicsuffix.PublicSuffix(label); icann {
			continue
		}
		for n := len(label) - 1; n >= 2; n-- {
			if _, icann := publicsuffix.PublicSuffix(label[:n]); !icann {
				continue
			}
			d := strings.Join(labels[:j], ".") + "." + label[:n]
			if c.ownDomain(d) || isFreeMail(d) || c.exists(local+"@"+d) {
				return d
			}
		}
	}
	return domain
}

// Repair is RepairCtx without context: only the phone and TLD rules can fire.
func Repair(email string) (fixed string, changed bool, ok bool) {
	fixed, rules, ok := RepairCtx(email, Context{})
	return fixed, len(rules) > 0, ok
}

// RepairCtx repairs a glued address. fixed is lowercase; rules names every rule
// that changed it (phone, tld, middomain, label, prefix); ok is false when the
// result is still not a clean address.
func RepairCtx(email string, c Context) (fixed string, rules []string, ok bool) {
	orig := strings.ToLower(strings.TrimSpace(email))
	at := strings.LastIndex(orig, "@")
	if at < 0 {
		return orig, nil, false
	}
	local, domain := orig[:at], orig[at+1:]

	if d := cutGluedTLD(domain); d != domain {
		domain, rules = d, append(rules, "tld")
	} else if d := cutMidDomain(local, domain, c); d != domain {
		domain, rules = d, append(rules, "middomain")
	}

	if l := stripPhonePrefix(local, domain); l != local && gmailOK(l, domain) {
		local, rules = l, append(rules, "phone")
	} else if l, hit := stripLabel(local, domain, c); hit {
		local, rules = l, append(rules, "label")
	} else if l, hit := stripPlaceGlue(local, domain, c); hit {
		local, rules = l, append(rules, "prefix")
	}

	fixed = local + "@" + domain
	if len(local) < 2 || !cleanRe.MatchString(fixed) {
		return fixed, rules, false
	}
	_, icann := publicsuffix.PublicSuffix(domain[strings.LastIndex(domain, ".")+1:])
	return fixed, rules, icann
}
