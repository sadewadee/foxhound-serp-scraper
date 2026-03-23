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
	for _, e := range parse.ExtractEmails(resp) {
		e = strings.ToLower(strings.TrimSpace(e))
		if e != "" && !isExtraBlocklisted(e) {
			cd.Emails = append(cd.Emails, e)
		}
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

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
