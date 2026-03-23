//go:build playwright

package scraper

import (
	"fmt"
	"net"
	"regexp"
	"strings"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// Social media regex patterns.
var (
	whatsappRe  = regexp.MustCompile(`(?:wa\.me|api\.whatsapp\.com/send\?phone=)[/=]*(\+?\d{10,15})`)
	instagramRe = regexp.MustCompile(`https?://(?:www\.)?instagram\.com/([a-zA-Z0-9._]+)/?`)
	facebookRe  = regexp.MustCompile(`https?://(?:www\.)?facebook\.com/([a-zA-Z0-9._\-]+)/?`)
	twitterRe   = regexp.MustCompile(`https?://(?:www\.)?(?:twitter\.com|x\.com)/([a-zA-Z0-9._]+)/?`)
	linkedinRe  = regexp.MustCompile(`https?://(?:www\.)?linkedin\.com/(?:company|in)/([a-zA-Z0-9._\-]+)/?`)
)

// extraBlocklist extends foxhound's built-in spam domain list.
var extraBlocklist = []string{
	"example.com", "domain.com", "email.com", "test.com",
	"yoursite.com", "youremail.com", "yourdomain.com",
	"company.com", "placeholder.com", "mail.com",
	"reddit.com", "error-tracking",
	"noreply@", "no-reply@",
}

// socialNoise are tracking/widget handles to filter out.
var socialNoise = []string{
	"share", "intent", "sharer", "login", "signup",
	"oauth", "dialog", "widgets", "platform", "plugins",
	"tr", "flx", "hashtag", "search",
}

// ContactData holds extracted contact and business information from a page.
type ContactData struct {
	// Contact info (from foxhound parse).
	Emails []string
	Phones []string

	// Social media.
	WhatsApp  string
	Instagram string
	Facebook  string
	Twitter   string
	LinkedIn  string

	// Address (from HTML selectors + JSON-LD).
	Address string

	// Business info (from JSON-LD / OpenGraph / meta tags).
	BusinessName     string
	BusinessCategory string
	Description      string
	Location         string
	OpeningHours     string
	Rating           string
	PageTitle        string
}

// ExtractContacts extracts contact + business info from HTML body.
// Delegates email/phone extraction entirely to foxhound (cfemail, mailto, tel, regex).
// Adds JSON-LD, OpenGraph, and meta tag extraction for business fields.
func ExtractContacts(body []byte) *ContactData {
	cd := &ContactData{}
	resp := &foxhound.Response{Body: body}

	// ── Emails — foxhound handles cfemail, mailto, plaintext regex, dedup ──
	// Post-filter: reject garbage that foxhound's regex picks up from raw HTML.
	for _, e := range parse.ExtractEmails(resp) {
		e = strings.ToLower(strings.TrimSpace(e))
		if e == "" || isExtraBlocklisted(e) || !isCleanEmail(e) {
			continue
		}
		cd.Emails = append(cd.Emails, e)
	}

	// ── Phones — foxhound handles tel: links, plaintext regex, validation ──
	for _, p := range parse.ExtractPhones(resp) {
		cleaned := normalizePhone(p)
		if len(cleaned) >= 10 && len(cleaned) <= 15 {
			cd.Phones = append(cd.Phones, cleaned)
		}
	}

	bodyStr := string(body)

	// ── WhatsApp ──
	if m := whatsappRe.FindStringSubmatch(bodyStr); len(m) > 1 {
		cd.WhatsApp = m[1]
	}

	// ── Social media ──
	if m := instagramRe.FindStringSubmatch(bodyStr); len(m) > 1 && !isSocialNoise(m[1]) {
		cd.Instagram = "https://instagram.com/" + m[1]
	}
	if m := facebookRe.FindStringSubmatch(bodyStr); len(m) > 1 && !isSocialNoise(m[1]) {
		cd.Facebook = "https://facebook.com/" + m[1]
	}
	if m := twitterRe.FindStringSubmatch(bodyStr); len(m) > 1 && !isSocialNoise(m[1]) {
		cd.Twitter = "https://twitter.com/" + m[1]
	}
	if m := linkedinRe.FindStringSubmatch(bodyStr); len(m) > 1 && !isSocialNoise(m[1]) {
		cd.LinkedIn = "https://linkedin.com/company/" + m[1]
	}

	// ── Structured data: JSON-LD ──
	extractJSONLD(resp, cd)

	// ── OpenGraph + Meta tags ──
	extractMetadata(resp, cd)

	// ── HTML address fallback (if JSON-LD didn't provide one) ──
	if cd.Address == "" {
		extractHTMLAddress(resp, cd)
	}

	// ── Page title fallback ──
	if cd.PageTitle == "" {
		if doc, err := parse.NewDocument(resp); err == nil {
			cd.PageTitle = strings.TrimSpace(doc.Text("title"))
		}
	}

	return cd
}

// extractJSONLD parses JSON-LD structured data for business information.
// Handles LocalBusiness, Organization, Restaurant, GymFitness, etc.
func extractJSONLD(resp *foxhound.Response, cd *ContactData) {
	jsonlds, err := parse.ExtractJSONLD(resp)
	if err != nil || len(jsonlds) == 0 {
		return
	}

	for _, ld := range jsonlds {
		ldType, _ := ld["@type"].(string)

		// Extract business name.
		if cd.BusinessName == "" {
			if name, ok := ld["name"].(string); ok && name != "" {
				cd.BusinessName = name
			}
		}

		// Extract business category from @type.
		if cd.BusinessCategory == "" && ldType != "" {
			// Skip generic types.
			switch ldType {
			case "WebSite", "WebPage", "BreadcrumbList", "ItemList",
				"SearchAction", "ReadAction", "ImageObject":
			default:
				cd.BusinessCategory = ldType
			}
		}

		// Extract description.
		if cd.Description == "" {
			if desc, ok := ld["description"].(string); ok && desc != "" {
				cd.Description = truncate(desc, 500)
			}
		}

		// Extract address from JSON-LD.
		if cd.Address == "" {
			cd.Address = extractLDAddress(ld)
		}

		// Extract location (geo coordinates or locality).
		if cd.Location == "" {
			cd.Location = extractLDLocation(ld)
		}

		// Extract opening hours.
		if cd.OpeningHours == "" {
			if hours, ok := ld["openingHours"].(string); ok {
				cd.OpeningHours = hours
			} else if hoursArr, ok := ld["openingHours"].([]any); ok {
				var parts []string
				for _, h := range hoursArr {
					if s, ok := h.(string); ok {
						parts = append(parts, s)
					}
				}
				cd.OpeningHours = strings.Join(parts, ", ")
			}
		}

		// Extract rating.
		if cd.Rating == "" {
			if rating, ok := ld["aggregateRating"].(map[string]any); ok {
				rVal, _ := rating["ratingValue"]
				rCount, _ := rating["reviewCount"]
				if rVal != nil {
					cd.Rating = fmt.Sprintf("%v", rVal)
					if rCount != nil {
						cd.Rating += fmt.Sprintf(" (%v reviews)", rCount)
					}
				}
			}
		}

		// Extract email/phone from JSON-LD if not already found.
		if len(cd.Emails) == 0 {
			if email, ok := ld["email"].(string); ok && email != "" {
				email = strings.ToLower(strings.TrimSpace(email))
				if !isExtraBlocklisted(email) {
					cd.Emails = append(cd.Emails, email)
				}
			}
		}
		if len(cd.Phones) == 0 {
			if phone, ok := ld["telephone"].(string); ok && phone != "" {
				cleaned := normalizePhone(phone)
				if len(cleaned) >= 10 {
					cd.Phones = append(cd.Phones, cleaned)
				}
			}
		}
	}
}

// extractLDAddress extracts a formatted address from JSON-LD address field.
func extractLDAddress(ld map[string]any) string {
	addr, ok := ld["address"].(map[string]any)
	if !ok {
		// Try as string.
		if s, ok := ld["address"].(string); ok && len(s) > 10 {
			return s
		}
		return ""
	}

	var parts []string
	for _, key := range []string{"streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"} {
		if v, ok := addr[key].(string); ok && v != "" {
			parts = append(parts, v)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ", ")
}

// extractLDLocation extracts location info (geo or locality) from JSON-LD.
func extractLDLocation(ld map[string]any) string {
	// Try geo coordinates.
	if geo, ok := ld["geo"].(map[string]any); ok {
		lat, _ := geo["latitude"]
		lng, _ := geo["longitude"]
		if lat != nil && lng != nil {
			return fmt.Sprintf("%v, %v", lat, lng)
		}
	}

	// Try address locality.
	if addr, ok := ld["address"].(map[string]any); ok {
		var parts []string
		if loc, ok := addr["addressLocality"].(string); ok && loc != "" {
			parts = append(parts, loc)
		}
		if reg, ok := addr["addressRegion"].(string); ok && reg != "" {
			parts = append(parts, reg)
		}
		if country, ok := addr["addressCountry"].(string); ok && country != "" {
			parts = append(parts, country)
		}
		if len(parts) > 0 {
			return strings.Join(parts, ", ")
		}
	}
	return ""
}

// extractMetadata extracts business info from OpenGraph and meta tags.
func extractMetadata(resp *foxhound.Response, cd *ContactData) {
	// OpenGraph.
	og := parse.ExtractOpenGraph(resp)
	if cd.BusinessName == "" {
		if title, ok := og["og:title"]; ok {
			cd.BusinessName = title
		} else if title, ok := og["og:site_name"]; ok {
			cd.BusinessName = title
		}
	}
	if cd.Description == "" {
		if desc, ok := og["og:description"]; ok {
			cd.Description = truncate(desc, 500)
		}
	}

	// Meta tags.
	meta := parse.ExtractMeta(resp)
	if cd.Description == "" {
		if desc, ok := meta["description"]; ok {
			cd.Description = truncate(desc, 500)
		}
	}
	if cd.BusinessCategory == "" {
		if kw, ok := meta["keywords"]; ok {
			cd.BusinessCategory = truncate(kw, 200)
		}
	}
}

// extractHTMLAddress tries common HTML selectors for address.
func extractHTMLAddress(resp *foxhound.Response, cd *ContactData) {
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return
	}
	for _, sel := range []string{
		"address", "[itemprop='address']", "[class*='address']",
		"[itemtype*='PostalAddress']",
	} {
		addr := strings.TrimSpace(doc.Text(sel))
		if addr != "" && len(addr) > 10 && len(addr) < 300 {
			cd.Address = addr
			return
		}
	}
}

// ValidateMX checks if the email domain has MX records.
func ValidateMX(email string) bool {
	parts := strings.SplitN(email, "@", 2)
	if len(parts) != 2 {
		return false
	}
	mx, err := net.LookupMX(parts[1])
	return err == nil && len(mx) > 0
}

func normalizePhone(p string) string {
	var sb strings.Builder
	for i, r := range p {
		if r >= '0' && r <= '9' {
			sb.WriteRune(r)
		} else if r == '+' && i == 0 {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// isExtraBlocklisted checks against our extended blocklist (on top of foxhound's built-in).
func isExtraBlocklisted(email string) bool {
	lower := strings.ToLower(email)
	for _, blocked := range extraBlocklist {
		if strings.Contains(lower, blocked) {
			return true
		}
	}
	return false
}

func isSocialNoise(handle string) bool {
	lower := strings.ToLower(handle)
	for _, n := range socialNoise {
		if lower == n {
			return true
		}
	}
	return false
}

// isCleanEmail rejects garbage emails that pass basic regex but are not real.
// Catches: concatenated text (phone+email+button), fake TLDs, tracking IDs.
func isCleanEmail(email string) bool {
	atIdx := strings.LastIndex(email, "@")
	if atIdx < 1 {
		return false
	}

	local := email[:atIdx]
	domain := email[atIdx+1:]

	// Reject if local part has digits immediately followed by a known email prefix
	// Pattern: "9346082info@" or "812-3868-7387marcom@" — phone concatenated with email.
	if len(local) > 20 {
		return false // Local parts > 20 chars are almost always garbage.
	}

	// Reject if domain has extra text appended after TLD.
	// Pattern: "inivie.comview", "balifitness.asiaphone", "ttbeach.clubmenu"
	// Real domains end with a valid TLD, nothing after it.
	dotIdx := strings.LastIndex(domain, ".")
	if dotIdx < 0 {
		return false
	}
	tld := domain[dotIdx+1:]
	if !isValidTLD(tld) {
		return false
	}

	// Reject hex tracking IDs (32+ hex chars in local part).
	hexCount := 0
	for _, r := range local {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			hexCount++
		}
	}
	if hexCount > 20 && float64(hexCount)/float64(len(local)) > 0.8 {
		return false
	}

	return true
}

// validTLDs is a set of common real TLDs. This is not exhaustive but catches
// the most common fake TLDs from social media content.
var validTLDs = map[string]bool{
	// Generic
	"com": true, "net": true, "org": true, "info": true, "biz": true,
	"io": true, "co": true, "me": true, "tv": true, "cc": true,
	"xyz": true, "app": true, "dev": true, "tech": true, "site": true,
	"online": true, "store": true, "shop": true, "club": true, "pro": true,
	"studio": true, "agency": true, "design": true, "media": true,
	"digital": true, "global": true, "world": true, "life": true,
	"fitness": true, "health": true, "travel": true, "hotel": true,
	"space": true, "today": true, "email": true, "link": true,
	"work": true, "asia": true, "rent": true, "page": true,

	// Country codes (common ones)
	"id": true, "uk": true, "au": true, "de": true, "fr": true,
	"jp": true, "kr": true, "sg": true, "my": true, "th": true,
	"ph": true, "vn": true, "in": true, "cn": true, "tw": true,
	"hk": true, "nz": true, "ca": true, "us": true, "br": true,
	"mx": true, "es": true, "it": true, "nl": true, "be": true,
	"ch": true, "at": true, "pl": true, "se": true, "no": true,
	"dk": true, "fi": true, "pt": true, "ru": true, "ua": true,
	"za": true, "ae": true, "sa": true, "il": true, "tr": true,
	"ie": true, "cz": true, "ro": true, "hu": true, "gr": true,

	// Second-level country (treated as TLD after last dot)
	// e.g. info@foo.co.id → TLD is "id", works.
	// But for co.uk addresses the last dot gives "uk", also works.

	// Education & gov
	"edu": true, "gov": true, "mil": true, "ac": true,
}

func isValidTLD(tld string) bool {
	return validTLDs[strings.ToLower(tld)]
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
