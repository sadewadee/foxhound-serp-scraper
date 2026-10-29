package prodir

// Which website email (if any) a directory row gets. A faithful Go port of the
// CRM loader's pick (trainer-directories/crmload/pick_web_email.py choose() /
// judge()) and of the junk filters of prep.py that run before it (the MX check
// stays in crmload). Rules: the directory page's own email always wins (rows
// with one are never touched), a row gets at most ONE website email, and it is
// the person's own address; other staff, departments and vendor/template
// addresses never are.

import (
	"regexp"
	"sort"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

var pickFree = setOf("gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com", "msn.com", "yahoo.com", "icloud.com", "me.com",
	"mac.com", "aol.com", "comcast.net", "verizon.net", "shaw.ca", "pm.me", "proton.me", "protonmail.com", "mail.com", "gmx.com",
	"gmx.de", "web.de", "att.net", "earthlink.net", "cox.net", "sbcglobal.net", "bellsouth.net", "rogers.com", "sympatico.ca", "ymail.com")

// prepFree is prep.py's own free-mail list (used for the vendor-domain rule).
var prepFree = setOf("gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com", "msn.com", "yahoo.com",
	"icloud.com", "me.com", "mac.com", "aol.com", "comcast.net", "verizon.net", "shaw.ca", "pm.me",
	"proton.me", "protonmail.com", "mail.com", "gmx.com", "gmx.de", "web.de", "naver.com", "att.net", "earthlink.net", "cox.net", "sbcglobal.net", "bellsouth.net", "rogers.com", "sympatico.ca")

var freeLabels = setOf("yahoo", "hotmail", "outlook", "live", "gmx", "ymail")

var genericLocals = []string{"info", "hello", "contact", "hi", "hey", "contactus", "office", "studio", "team", "inquiries", "inquiry", "enquiries",
	"enquiry", "booking", "bookings", "book", "connect", "welcome", "mail", "email", "emails", "ahlan", "hola", "coach",
	"coaching", "training", "fitness", "yoga", "pilates", "health", "wellness", "support", "help", "bewell"}

var (
	placeholderRe = regexp.MustCompile(`^(janed|johnd|janedoe|johndoe|anymail\d*|mymail|abc|test|example|yourname|name|email)@|@(xyz|example|mailservice)\.`)
	rejectEmails  = setOf("mariamickschl78@gmail.com") // another person's name on the profile's site
	nameTitles    = setOf("dr", "mr", "mrs", "ms", "miss", "phd", "msc", "jr", "sr")
	nonLetterRe   = regexp.MustCompile(`[^a-z]+`)

	// prep.py junk filters
	junkEmailRe = regexp.MustCompile(`(?i)^(u00[0-9a-f]{2}|%[0-9a-f]{2}|x2[0-9a-f])|^(unsubscribe|no-?reply|donotreply|do-not-reply|postmaster|abuse|mailer-daemon|bounces?)@|\.(png|jpe?g|gif|webp|svg)$|@(example|domain|email|yourdomain|sentry|wixpress)\.`)
	emailOKRe   = regexp.MustCompile(`^[a-z0-9._%+'-]+@[a-z0-9.-]+\.[a-z]{2,24}$`)
)

// vendorDomains are template / platform addresses prep.py always drops.
var vendorDomains = setOf("jouwweb.nl", "latofonts.com", "webador.com", "google.com", "squarespace.com", "wix.com", "godaddy.com",
	"techaro.lol", "shorturl.at", "netflix.com", "cam.tv", "arketa.com", "innovate.co.zw")

const (
	maxHostShare   = 3 // a host listed by more directory rows is an employer/platform, not "own"
	maxSiteEmails  = 3 // above this keep only emails on the site's own domain
	vendorMinSites = 3 // a non-free email domain on this many unrelated sites is a vendor
)

func setOf(v ...string) map[string]bool {
	m := make(map[string]bool, len(v))
	for _, s := range v {
		m[s] = true
	}
	return m
}

func isFreeMail(d string) bool {
	return pickFree[d] || freeLabels[strings.SplitN(d, ".", 2)[0]]
}

func isFreeMailPrep(d string) bool {
	return prepFree[d] || freeLabels[strings.SplitN(d, ".", 2)[0]]
}

func bareHost(d string) string { return strings.TrimPrefix(strings.ToLower(d), "www.") }

func lettersOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// asciiLower is python's unicodedata NFKD + encode('ascii','ignore') + lower().
func asciiLower(s string) string {
	var b strings.Builder
	for _, r := range norm.NFKD.String(s) {
		if r < unicode.MaxASCII {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// nameWords are the lower-cased ASCII words of a name, titles dropped.
func nameWords(first, last string) []string {
	var out []string
	for _, t := range nonLetterRe.Split(asciiLower(first+" "+last), -1) {
		if t != "" && !nameTitles[t] {
			out = append(out, t)
		}
	}
	return out
}

// webCand is an email found on a site (the bare host it was found on).
type webCand struct{ email, site string }

// judgeEmail ranks an email for this person: 0 own name in the address,
// 1 generic on own/personal domain, 2 nickname on a personal domain or the
// site's own brand; -1 = not theirs.
func judgeEmail(email, site, first, last string) int {
	at := strings.IndexByte(email, '@')
	lp, dom := email[:at], email[at+1:]
	l, label := lettersOnly(lp), lettersOnly(strings.SplitN(dom, ".", 2)[0])
	words := nameWords(first, last)
	var toks []string
	var initials strings.Builder
	for _, t := range words {
		if len(t) >= 3 {
			toks = append(toks, t)
		}
		initials.WriteByte(t[0])
	}
	ini := initials.String()
	named := false
	for _, t := range toks {
		if strings.Contains(l, t) || (len(t) >= 4 && strings.Contains(l, t[:4])) || (len(l) >= 3 && strings.Contains(t, l)) {
			named = true
		}
	}
	if !named && len(l) >= 2 && len(l) <= 3 && ini != "" && (l == ini || l == string(ini[0])+string(ini[len(ini)-1])) {
		named = true
	}
	if named {
		return 0
	}
	personal := false
	for _, t := range toks {
		if (len(t) >= 4 || t == toks[len(toks)-1]) && strings.Contains(label, t) {
			personal = true
		}
	}
	isGeneric := false
	for _, g := range genericLocals {
		isGeneric = isGeneric || g == l
	}
	if isGeneric && (bareHost(dom) == site || personal) {
		return 1
	}
	if personal && len(words) > 0 && l != "" && l[:1] == words[0][:1] {
		return 2
	}
	siteLabel := lettersOnly(strings.SplitN(site, ".", 2)[0])
	if len(l) >= 5 && strings.Contains(siteLabel, l) && float64(len(l)) >= 0.6*float64(len(siteLabel)) {
		return 2 // the address is the site's own brand
	}
	return -1
}

// chooseWebEmail returns the one email that is the person's own, or ok=false.
func chooseWebEmail(cands []webCand, first, last string) (webCand, bool) {
	var cs []webCand
	for _, c := range cands {
		if !rejectEmails[c.email] && !placeholderRe.MatchString(c.email) {
			cs = append(cs, c)
		}
	}
	type ranked struct {
		c            webCand
		judge, free  int
		generic, len int
	}
	rs := make([]ranked, len(cs))
	for i, c := range cs {
		l := lettersOnly(c.email[:strings.IndexByte(c.email, '@')])
		r := ranked{c: c, judge: judgeEmail(c.email, c.site, first, last), generic: 99, len: len(c.email)}
		if isFreeMail(c.email[strings.IndexByte(c.email, '@')+1:]) {
			r.free = 1
		}
		for i, g := range genericLocals {
			if g == l {
				r.generic = i
				break
			}
		}
		rs[i] = r
	}
	sort.SliceStable(rs, func(a, b int) bool {
		x, y := rs[a], rs[b]
		xn, yn := x.judge < 0, y.judge < 0
		switch {
		case xn != yn:
			return !xn
		case x.judge != y.judge:
			return x.judge < y.judge
		case x.free != y.free:
			return x.free < y.free
		case x.generic != y.generic:
			return x.generic < y.generic
		case x.len != y.len:
			return x.len < y.len
		}
		return x.c.email < y.c.email
	})
	if len(rs) > 0 && rs[0].judge >= 0 {
		return rs[0].c, true
	}
	if len(cs) == 1 && isFreeMail(cs[0].email[strings.IndexByte(cs[0].email, '@')+1:]) {
		return cs[0], true // the coach's own free-mail address, the only one on the site
	}
	var words []string
	for _, t := range nonLetterRe.Split(asciiLower(first+" "+last), -1) {
		if len(t) >= 4 && !nameTitles[t] {
			words = append(words, t)
		}
	}
	var personal []webCand
	for _, c := range cs {
		label := lettersOnly(strings.SplitN(c.email[strings.IndexByte(c.email, '@')+1:], ".", 2)[0])
		for _, t := range words {
			if strings.Contains(label, t) {
				personal = append(personal, c)
				break
			}
		}
	}
	if len(personal) == 1 {
		return personal[0], true // the only address on a domain named after the person
	}
	return webCand{}, false
}

// siteEmailSet turns the emails scraped for each host (site key -> addresses)
// into the ones prep.py keeps: junk, vendor-domain and template/credit-line
// addresses dropped; hosts listed by more than maxHostShare directory rows
// skipped (share = rows per host); glued artifacts and over-full sites trimmed.
func siteEmailSet(raw map[string][]string, share map[string]int) map[string][]string {
	site := map[string][]string{}
	for k, es := range raw {
		for _, e := range es {
			if emailOKRe.MatchString(e) && !junkEmailRe.MatchString(e) {
				site[k] = append(site[k], e)
			}
		}
	}
	domSites := map[string]map[string]bool{}
	addrSites, addrProfiles := map[string]map[string]bool{}, map[string]int{}
	for k, es := range site {
		for _, e := range es {
			d := e[strings.IndexByte(e, '@')+1:]
			if bareHost(d) != bareHost(k) {
				if domSites[d] == nil {
					domSites[d] = map[string]bool{}
				}
				domSites[d][k] = true
			}
			if addrSites[e] == nil {
				addrSites[e] = map[string]bool{}
			}
			if !addrSites[e][k] {
				addrSites[e][k] = true
				addrProfiles[e] += share[k]
			}
		}
	}
	vendor := map[string]bool{}
	for d := range vendorDomains {
		vendor[d] = true
	}
	for d, s := range domSites {
		if len(s) >= vendorMinSites && !isFreeMailPrep(d) {
			vendor[d] = true
		}
	}
	out := map[string][]string{}
	for k, es := range site {
		if share[k] > maxHostShare {
			continue
		}
		set := map[string]bool{}
		for _, e := range es {
			if vendor[e[strings.IndexByte(e, '@')+1:]] || (len(addrSites[e]) >= 3 && addrProfiles[e] >= 3) {
				continue
			}
			set[e] = true
		}
		keep := map[string]bool{}
		for e := range set { // glued artifacts: "emailhello@x.com" next to "hello@x.com"
			glued := false
			for o := range set {
				glued = glued || (e != o && strings.HasSuffix(e, o))
			}
			if !glued {
				keep[e] = true
			}
		}
		if len(keep) > maxSiteEmails {
			own := map[string]bool{}
			for e := range keep {
				if bareHost(e[strings.IndexByte(e, '@')+1:]) == bareHost(k) {
					own[e] = true
				}
			}
			keep = own
			if len(keep) > maxSiteEmails {
				continue
			}
		}
		for e := range keep {
			out[k] = append(out[k], e)
		}
		sort.Strings(out[k])
	}
	return out
}
