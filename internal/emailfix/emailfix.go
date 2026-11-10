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
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"golang.org/x/net/publicsuffix"
	"golang.org/x/text/unicode/norm"
)

// cleanRe is the CRM (Zod) address check without its lookaheads: RE2 has none,
// so the leading-dot and ".." rules are checked separately in RepairCtx.
var cleanRe = regexp.MustCompile(`^[a-z0-9_'+\-.]*[a-z0-9_+\-]@([a-z0-9][a-z0-9\-]*\.)+[a-z]{2,}$`)

var pctRe = regexp.MustCompile(`%[0-9a-f]{2}`)

// invisibleRunes are zero-width or soft-hyphen characters pasted into addresses.
var invisibleRunes = strings.NewReplacer("\u200b", "", "\u200c", "", "\u200d", "", "\u2060", "", "\ufeff", "", "\u00ad", "")

// prefixWords are mail-label words that can precede an address in page text.
var prefixWords = map[string]bool{
	"mailto": true, "matilto": true, "malito": true, "emailto": true,
	"email": true, "e-mail": true, "e-mailto": true, "mail": true, "tel": true,
}

// placeholders are local parts that only appear in form examples.
var placeholders = map[string]bool{
	"nachname": true, "nazwisko": true, "vorname": true, "yourname": true,
	"name": true, "email": true, "username": true,
}

func isAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

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

// IsTLD reports whether label is an ICANN top-level domain. It probes "x."+label
// because wildcard-only TLDs (np, mm, bd, kh, ...) are not reported as ICANN when
// asked as a bare label.
func IsTLD(label string) bool {
	_, icann := publicsuffix.PublicSuffix("x." + label)
	return icann
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
	// A space in the label is separate text ("co uk"), not glue: leave it to the
	// trailing-text rule and the final check.
	if IsTLD(label) || strings.ContainsAny(label, " \t") {
		return domain
	}
	for n := len(label) - 1; n >= 2; n-- {
		// A cut leaving 1-2 letters is a typo (".con" -> ".co", ".archive" ->
		// ".archi"), not glue, unless the cut is com/net/org (".comm", ".comn").
		// Cuts at punctuation are fine.
		rest := label[n:]
		typo := len(rest) < 3 && rest[0] >= 'a' && rest[0] <= 'z' && label[:n] != "com" && label[:n] != "net" && label[:n] != "org"
		if !IsTLD(label[:n]) {
			continue
		}
		if typo {
			break // a longer TLD fits with a 1-2 letter leftover: guessing a shorter one would be wrong
		}
		return domain[:dot+1] + label[:n]
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
	"tech": true, "technical": true, "web": true, "pro": true, "request": true,
	"more": true, "get": true, "ask": true, "online": true, "free": true,
	"news": true, "system": true, "site": true, "sales": true, "service": true,
	"order": true, "product": true,
}

// ownDomainRoles are the only roles trusted on the listing's own domain without
// a standalone twin; compounds like "techsupport" or "ptymail" are too common
// for support/admin/mail/office.
var ownDomainRoles = map[string]bool{"info": true, "contact": true, "kontakt": true, "hello": true}

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
		if acceptStripped(role, domain, c, ownDomainRoles[role] && c.ownDomain(domain)) {
			return role, true
		}
	}
	return local, false
}

// midFreeMail are the free-mail domains a mid-domain cut may land on.
var midFreeMail = map[string]bool{
	"gmail.com": true, "yahoo.com": true, "hotmail.com": true, "outlook.com": true,
	"live.com": true, "aol.com": true, "ymail.com": true, "icloud.com": true,
	"web.de": true, "gmx.de": true, "gmx.net": true, "gmx.com": true, "qq.com": true,
	"163.com": true, "naver.com": true, "hanmail.net": true, "mail.ru": true,
}

// glueLead matches a local part that opens with a glued phone or code run and
// then continues; group 1 is the continuation.
var glueLead = regexp.MustCompile(`^[a-z]{0,4}\d{3,}[-._]?([a-z].*)$`)

// webHostLocal matches a web or mail host left inside the local part: a "www."
// label, or a provider domain that starts the local part or follows a non-alnum
// character. Such a local part is text from the neighbouring domain, never a mailbox.
// domainGlue matches a TLD or a short label glued to a provider name in a domain
// ("msn.comyahoo.com.sa", "yahoo.com.hkhotmail.com"). "live", "ymail" are left out.
var domainGlue = regexp.MustCompile(`(^|\.)(com|net|org|edu|gov|[a-z]{2})(gmail|googlemail|yahoo|hotmail|outlook|msn|aol|icloud|gmx)(\.|$)`)

var webHostLocal = regexp.MustCompile(`www\.|(^|[^a-z0-9])(gmail|googlemail|yahoo|ymail|hotmail|outlook|live|msn|aol|icloud|gmx)\.(com|net|de|fr|it|es|co)`)

// glueVouched reports whether stripped, the local part after a glued run, is
// confirmed by the cut-off tail: one of its labels (not "www") or the tail itself.
// Without that evidence the start of the address is unknown.
func glueVouched(stripped, tail, domain string) bool {
	if len(stripped) < 2 || !gmailOK(stripped, domain) {
		return false
	}
	if stripped == tail || stripped == strings.TrimPrefix(tail, "www.") {
		return true
	}
	for _, l := range strings.Split(tail, ".") {
		if l != "www" && l == stripped {
			return true
		}
	}
	return false
}

// cutMidDomain handles text between two domains ("gmail.comfacebookfacebook.com"):
// it cuts a non-final label back to an ICANN TLD prefix, at the first label where
// the result is vouched for (a free-mail provider, the listing's domain, or an
// address that exists on its own). Labels that are themselves a TLD
// ("mail.company.com") are left alone. A cut onto a 2-letter ccTLD also needs a
// glue signal in the leftover text ("www", or the first label repeated), so
// "live.mercer.edu" is not read as "live.me".
func cutMidDomain(local, domain string, c Context) string {
	labels := strings.Split(domain, ".")
	for j := 1; j < len(labels)-1; j++ {
		label := labels[j]
		if IsTLD(label) {
			continue
		}
		for n := len(label) - 1; n >= 2; n-- {
			if !IsTLD(label[:n]) {
				continue
			}
			d := strings.Join(labels[:j], ".") + "." + label[:n]
			rest := label[n:] + "." + strings.Join(labels[j+1:], ".")
			signal := strings.Contains(rest, "www") || strings.Contains(rest, labels[0])
			// A leftover that starts with "www." or repeats the first label is glue
			// on its own, so the cut needs no vouching.
			glue := strings.HasPrefix(rest, "www.") || strings.HasPrefix(rest, labels[0]+".")
			if (n >= 3 || signal) && (glue || midFreeMail[d] || c.ownDomain(d) || c.exists(local+"@"+d)) {
				return d
			}
		}
	}
	return domain
}

// strictCut are separators that must not follow a letter or digit: glued onto
// text they mean the local part was continuing, so the address is ambiguous.
const strictCut = ",<>;"

// Whole-string repairs for normalizeSyntax and the list splitter for CleanList.
var (
	atSpaceRe = regexp.MustCompile(`\s*(?:\(@\)|\[@\]|@)\s*`)
	htmlTagRe = regexp.MustCompile(`(?i)</?[a-z][^<>@]*>`)
	// separatorEscRe and listSepRe mark where a field holding several addresses
	// is split: encoded separators, and ":", '">' and "&cc=" style joins.
	separatorEscRe = regexp.MustCompile(`(?i)%(?:20|09|0a|0d|2c|3b)`)
	listSepRe      = regexp.MustCompile(`(?i):|["']>|&b?cc=|\?b?cc=`)
	multiSep       = regexp.MustCompile(`[\s,;]+`)
)

// syntaxRoles are the role locals the syntax step trusts for leading-dot and
// percent-escape repairs. prefixRoles stays as it is for stripPlaceGlue.
var syntaxRoles = func() map[string]bool {
	m := map[string]bool{
		"biuro": true, "hola": true, "contato": true, "contacto": true,
		"sales": true, "help": true, "enquiries": true,
	}
	for _, r := range prefixRoles {
		m[r] = true
	}
	return m
}()

func isAlnumRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}

// labelWord reduces a label token such as "#email" or "email.:" to its word.
func labelWord(tok string) string {
	return strings.TrimRight(strings.TrimLeftFunc(tok, func(r rune) bool { return !isAlnumRune(r) }), ":.")
}

// trimWrap strips wrapper characters from both ends, repeatedly, so "{{x@y.com}}"
// and "[(x@y.com)]" both come out bare.
func trimWrap(s string) string {
	for s != "" && (strings.IndexByte("[({", s[0]) >= 0 || strings.IndexByte("])}", s[len(s)-1]) >= 0) {
		if strings.IndexByte("[({", s[0]) >= 0 {
			s = s[1:]
		} else {
			s = s[:len(s)-1]
		}
	}
	return s
}

// collapseDoubled turns a doubled address or doubled "@" part back into one:
// "info@fixture.deinfo@fixture.de", "x@gmail@gmail.com", "info@info@fixture.de".
func collapseDoubled(s string) string {
	if n := len(s); n > 0 && n%2 == 0 {
		h := s[:n/2]
		if h == s[n/2:] && strings.Count(h, "@") == 1 && cleanRe.MatchString(h) {
			return h
		}
	}
	parts := strings.Split(s, "@")
	if len(parts) != 3 {
		return s
	}
	l, a, d := parts[0], parts[1], parts[2]
	switch {
	case a == d, strings.HasPrefix(d, a+"."), l == a:
		return l + "@" + d
	}
	return s
}

// lastLabel returns the text after the last dot of s.
func lastLabel(s string) string {
	return s[strings.LastIndexByte(s, '.')+1:]
}

// joinDomainSpaces removes whitespace inside a domain after "-", and after "."
// unless the label before that dot is a TLD ("pec.rupar. puglia.it" joins,
// "fixture.com. phone" keeps its space).
func joinDomainSpaces(d string) string {
	var b strings.Builder
	for i := 0; i < len(d); i++ {
		b.WriteByte(d[i])
		if d[i] != '-' && d[i] != '.' {
			continue
		}
		j := i + 1
		for j < len(d) && (d[j] == ' ' || d[j] == '\t') {
			j++
		}
		if j == i+1 || j == len(d) {
			continue
		}
		if l := lastLabel(d[:i]); d[i] == '.' && l != "" && IsTLD(l) {
			continue
		}
		i = j - 1
	}
	return b.String()
}

// trimTrailingText cuts a domain at the first whitespace left after joining,
// but only when the text before it ends in a TLD ("fixture.com. phone"). When the
// next token would extend the public suffix ("company.co" + "uk" is "co.uk"),
// the address is ambiguous: the domain is returned unchanged so the final check
// rejects it.
func trimTrailingText(d string) string {
	j := strings.IndexAny(d, " \t")
	if j < 0 {
		return d
	}
	head := strings.TrimRight(d[:j], ".")
	if l := lastLabel(head); l == "" || !IsTLD(l) {
		return d
	}
	rest := strings.TrimLeft(d[j:], " \t")
	k := 0
	for k < len(rest) && (isAlnum(rest[k]) || rest[k] == '-') {
		k++
	}
	if k > 0 && spansSuffix(head, rest[:k]) {
		return d
	}
	return head
}

// spansSuffix reports whether head+"."+next has a longer public suffix than next
// alone, as in "company.co" + "uk" (suffix "co.uk").
func spansSuffix(head, next string) bool {
	suffix, _ := publicsuffix.PublicSuffix(head + "." + next)
	return len(suffix) > len(next)
}

// noLetterOrDigit reports whether s holds no letter and no digit, so a token like
// "✉", "-" or "|" is a separator and "anna-" is not.
func noLetterOrDigit(s string) bool {
	return strings.IndexFunc(s, func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }) < 0
}

// cutEvidence reports whether a result after a domain cut is trusted: the address
// exists on its own, the local part is a label or the domain of the cut-off tail,
// or it is a role word on a domain that is not free-mail. Anything else may be
// text from either side of the glue, so it is rejected.
func cutEvidence(local, fixed, tail, domain string, c Context) bool {
	existsOK := c.exists(fixed) && (!isFreeMail(domain) || len(strings.ReplaceAll(local, ".", "")) >= 8)
	if existsOK || glueVouched(local, tail, domain) {
		return true
	}
	return syntaxRoles[local] && !isFreeMail(domain)
}

// tidyParts applies the whole-string repairs of normalizeSyntax: spacing around
// "@", HTML tags, bracket wrappers, a leading "@", doubled parts and spacing
// inside the domain.
func tidyParts(s string) string {
	s = atSpaceRe.ReplaceAllString(s, "@")
	s = strings.TrimSpace(htmlTagRe.ReplaceAllString(s, " "))
	s = trimWrap(s)
	if strings.HasPrefix(s, "@") && strings.Count(s, "@") == 2 {
		s = s[1:]
	}
	s = collapseDoubled(s)
	if strings.Count(s, "@") == 1 {
		i := strings.IndexByte(s, '@')
		s = s[:i+1] + trimTrailingText(joinDomainSpaces(s[i+1:]))
	}
	return s
}

// spaceRunRe matches two or more spaces in a row.
var spaceRunRe = regexp.MustCompile(" {2,}")

// normalizeSyntax strips wrappers, prefixes and encoding from a raw address
// without any context. It returns the cleaned lowercase address, whether it
// changed, and ok=false for shapes whose real address cannot be recovered.
func normalizeSyntax(raw string) (string, bool, bool) {
	orig := strings.ToLower(strings.TrimSpace(raw))
	s := invisibleRunes.Replace(orig)
	// A %XX escape in the local part is trusted only when a wrapper is stripped.
	escLocal := false
	if at := strings.LastIndex(s, "@"); at >= 0 {
		escLocal = pctRe.MatchString(s[:at])
	}
	for i := 0; i < 3 && pctRe.MatchString(s); i++ {
		d, err := url.PathUnescape(s)
		if err != nil {
			break
		}
		s = d
	}
	s = tidyParts(invisibleRunes.Replace(norm.NFKC.String(s)))

	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s, s != orig, true
	}
	if strings.Count(s, "@") != 1 {
		return orig, false, false
	}
	local, domain := s[:at], s[at+1:]
	// Runs of spaces in the local part count as one space. Other control
	// characters are left for the cut loop below.
	local = spaceRunRe.ReplaceAllString(local, " ")

	// Find the last prefix or wrapper separator in the local part.
	wrapped, cut := false, -1
	for i := 0; i < len(local); i++ {
		c := local[i]
		isCut := false
		switch {
		case c < ' ' || c == 0x7f || c == ':' || c == '/' || c == '"':
			isCut = true
		case strings.IndexByte(strictCut, c) >= 0:
			isCut = true
		case c == '\'':
			isCut = i == 0
		case c == ' ':
			// A space cuts only after a token with no letter or digit, so
			// "studio berlin - info" cuts and "anna- maria" does not.
			tok := local[strings.LastIndexByte(local[:i], ' ')+1 : i]
			isCut = prefixWords[local[:i]] || (i > 0 && tok != "" && noLetterOrDigit(tok))
		}
		if !isCut {
			continue
		}
		tok := local[strings.LastIndexByte(local[:i], ' ')+1 : i]
		if i > 0 && local[i-1] == '.' && !(c == ':' && prefixWords[labelWord(tok)]) {
			return orig, false, false // the local part was continuing
		}
		if i > 0 && strings.IndexByte(strictCut, c) >= 0 && isAlnum(local[i-1]) {
			return orig, false, false
		}
		cut = i
	}
	if cut >= 0 {
		local, wrapped = local[cut+1:], true
	}
	local = strings.TrimSpace(local)
	// The last whitespace decides: "via email x" keeps x, "mary beth smith x" is
	// ambiguous and stays as it is (rejected later by the final check).
	if i := strings.LastIndexByte(local, ' '); i > 0 {
		tok := local[strings.LastIndexByte(local[:i], ' ')+1 : i]
		if prefixWords[labelWord(tok)] || (tok != "" && noLetterOrDigit(tok)) {
			local, wrapped = strings.TrimSpace(local[i+1:]), true
		}
	}
	for stripped := true; stripped; {
		stripped = false
		for _, p := range []string{"u003c", "u003e", "u0026"} {
			if strings.HasPrefix(local, p) {
				local, wrapped, stripped = local[len(p):], true, true
			}
		}
	}
	if wrapped && (strings.HasPrefix(local, "mailto") || strings.HasPrefix(local, "email") || strings.HasPrefix(local, "e-mail")) {
		return orig, false, false
	}
	if strings.HasPrefix(local, "protect.") {
		return orig, false, false // Cloudflare email-protection artifact
	}
	if escLocal && !wrapped && !syntaxRoles[local] {
		return orig, false, false
	}
	if strings.HasPrefix(local, ".") {
		core := strings.TrimLeft(local, ".")
		if !syntaxRoles[core] {
			return orig, false, false
		}
		local = core
	}
	if local == "" || strings.HasSuffix(local, ".") || strings.Contains(local, "..") {
		return orig, false, false
	}

	if trimmed := strings.Trim(domain, "."); trimmed != domain {
		if _, err := publicsuffix.EffectiveTLDPlusOne(trimmed); err != nil {
			return orig, false, false // only a public suffix left
		}
		domain = trimmed
	}
	if strings.Contains(domain, "..") {
		return orig, false, false
	}

	all := local + "@" + domain
	for i := 0; i < len(all); i++ {
		if c := all[i]; c >= 0x80 || strings.IndexByte("%&*!=", c) >= 0 {
			return orig, false, false
		}
	}
	if placeholders[local] || strings.Contains(local, "xxx") || strings.Contains(domain, "xxx") {
		return orig, false, false
	}
	if all != orig && (domain == "gmail.com" || domain == "googlemail.com") && !gmailOK(local, domain) {
		return orig, false, false
	}
	return all, all != orig, true
}

// splitList breaks a field holding several addresses into candidate pieces.
func splitList(e string) []string {
	s := separatorEscRe.ReplaceAllString(e, " ")
	s = htmlTagRe.ReplaceAllString(s, " ")
	s = listSepRe.ReplaceAllString(s, " ")
	return multiSep.Split(s, -1)
}

// CleanList repairs each address with Repair, drops the ones that are not ok,
// and dedupes the rest keeping order. An input holding several addresses is
// split first (see splitList); "@@" is never split and is rejected.
func CleanList(emails []string) []string {
	var out []string
	seen := make(map[string]bool, len(emails))
	add := func(e string) {
		fixed, _, ok := Repair(e)
		if ok && !seen[fixed] {
			seen[fixed] = true
			out = append(out, fixed)
		}
	}
	for _, e := range emails {
		if strings.Count(e, "@") > 1 && !strings.Contains(e, "@@") {
			for _, piece := range splitList(e) {
				add(piece)
			}
			continue
		}
		add(e)
	}
	return out
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
	s, synChanged, synOK := normalizeSyntax(email)
	if !synOK {
		return orig, []string{"syntax"}, false
	}
	if synChanged {
		rules = append(rules, "syntax")
	}
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s, rules, false
	}
	local, domain := s[:at], s[at+1:]

	fullDomain := domain
	if d := cutGluedTLD(domain); d != domain {
		domain, rules = d, append(rules, "tld")
	} else if d := cutMidDomain(local, domain, c); d != domain {
		domain, rules = d, append(rules, "middomain")
	}

	// cut is true when a tld or middomain rule trimmed the domain. Then the left
	// boundary is trusted only with evidence (see cutEvidence).
	tail := fullDomain[len(domain):]
	cut := domain != fullDomain && !noLetterOrDigit(tail)

	if l := stripPhonePrefix(local, domain); l != local && gmailOK(l, domain) {
		local, rules = l, append(rules, "phone")
		if isFreeMail(domain) && !syntaxRoles[local] && len(strings.ReplaceAll(local, ".", "")) < 8 {
			// On free-mail a short personal remainder is likely another person's
			// mailbox. A role word ("info") is generic and stays as before.
			return local + "@" + domain, rules, false
		}
	} else if m := glueLead.FindStringSubmatch(local); cut && m != nil && glueVouched(m[1], tail, domain) {
		// A glued run before the local part is stripped only when the tail vouches
		// for the rest.
		local, rules = m[1], append(rules, "phone")
	} else if l, hit := stripLabel(local, domain, c); hit {
		local, rules = l, append(rules, "label")
	} else if l, hit := stripPlaceGlue(local, domain, c); hit {
		local, rules = l, append(rules, "prefix")
	}

	fixed = local + "@" + domain
	// Checked on the final local part, after every rule, and before the evidence
	// check: an existing row cannot vouch for a host embedded in the local part.
	if webHostLocal.MatchString(local) || domainGlue.MatchString(domain) {
		return fixed, rules, false
	}
	if cut && !cutEvidence(local, fixed, tail, domain, c) {
		return fixed, rules, false
	}
	// A CRM-invalid result that no rule fixed is still flagged, so repair-emails
	// marks the row malformed instead of leaving it exportable. This runs before
	// the short-local return so "c@-.at" is rejected too.
	if strings.HasPrefix(local, ".") || strings.Contains(fixed, "..") || !cleanRe.MatchString(fixed) {
		if !slices.Contains(rules, "syntax") {
			rules = append(rules, "syntax")
		}
		return fixed, rules, false
	}
	if len(local) < 2 {
		return fixed, rules, false
	}
	return fixed, rules, IsTLD(domain[strings.LastIndex(domain, ".")+1:])
}
