//go:build playwright

package scraper

import (
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"

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
	tiktokRe    = regexp.MustCompile(`https?://(?:www\.)?tiktok\.com/@([a-zA-Z0-9._]+)/?`)
	youtubeRe   = regexp.MustCompile(`https?://(?:www\.)?youtube\.com/(?:c/|channel/|user/|@)([a-zA-Z0-9._\-]+)/?`)
	telegramRe  = regexp.MustCompile(`https?://(?:www\.)?t(?:elegram)?\.me/([a-zA-Z0-9_]+)/?`)
)

// extraBlockedDomains are placeholder/spam domains to reject (exact match on domain part).
var extraBlockedDomains = map[string]bool{
	"example.com": true, "domain.com": true, "email.com": true, "test.com": true,
	"yoursite.com": true, "youremail.com": true, "yourdomain.com": true,
	"company.com": true, "placeholder.com": true,
	"reddit.com": true,
}

// extraBlockedPrefixes are email prefixes to reject (prefix match).
var extraBlockedPrefixes = []string{
	"noreply@", "no-reply@", "error-tracking",
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
	TikTok    string
	YouTube   string
	Telegram  string

	// Address (from HTML selectors + JSON-LD).
	Address string

	// Geo (split from JSON-LD addressLocality / addressCountry — easier targeting
	// than parsing the flat Address string at query time).
	City    string
	Country string // ISO 3166-1 alpha-2 when derivable, raw otherwise

	// Contact person name (from JSON-LD author/founder, meta author, vCard).
	ContactName string

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
	if m := tiktokRe.FindStringSubmatch(bodyStr); len(m) > 1 && !isSocialNoise(m[1]) {
		cd.TikTok = "https://tiktok.com/@" + m[1]
	}
	if m := youtubeRe.FindStringSubmatch(bodyStr); len(m) > 1 && !isSocialNoise(m[1]) {
		// Preserve original path form: /channel/UCxxx and /user/xxx are not
		// valid /@handle URLs. Prepending @ to a UCid produced unreachable
		// URLs in production data. Reconstruct the matched URL instead.
		full := whichYouTubePath(m[0], m[1])
		cd.YouTube = full
	}
	if m := telegramRe.FindStringSubmatch(bodyStr); len(m) > 1 && !isSocialNoise(m[1]) {
		cd.Telegram = m[1] // username only, t.me/<username> is the canonical form
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

		// Extract contact person name from JSON-LD (author, founder, employee, contactPoint).
		if cd.ContactName == "" {
			for _, field := range []string{"author", "founder", "employee", "director", "contactPoint"} {
				if cd.ContactName != "" {
					break
				}
				switch v := ld[field].(type) {
				case map[string]any:
					if name, ok := v["name"].(string); ok && name != "" {
						cd.ContactName = name
					}
				case []any:
					if len(v) > 0 {
						if person, ok := v[0].(map[string]any); ok {
							if name, ok := person["name"].(string); ok && name != "" {
								cd.ContactName = name
							}
						}
					}
				case string:
					if v != "" {
						cd.ContactName = v
					}
				}
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

		// Extract structured city/country from JSON-LD address — keeps targeting
		// queries simple instead of regex-parsing the flat Address blob later.
		if cd.City == "" || cd.Country == "" {
			city, country := extractLDCityCountry(ld)
			if cd.City == "" {
				cd.City = city
			}
			if cd.Country == "" {
				cd.Country = country
			}
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

// iso3166Alpha2 is the canonical set of ISO 3166-1 alpha-2 country codes.
// Used to validate "country" extractions — without this, US/AU/CA/BR state
// codes (CA, NY, TX, NSW, ON, SP) would land in the country column and
// pollute global targeting queries.
var iso3166Alpha2 = map[string]bool{
	"AD": true, "AE": true, "AF": true, "AG": true, "AI": true, "AL": true, "AM": true, "AO": true,
	"AQ": true, "AR": true, "AS": true, "AT": true, "AU": true, "AW": true, "AX": true, "AZ": true,
	"BA": true, "BB": true, "BD": true, "BE": true, "BF": true, "BG": true, "BH": true, "BI": true,
	"BJ": true, "BL": true, "BM": true, "BN": true, "BO": true, "BQ": true, "BR": true, "BS": true,
	"BT": true, "BV": true, "BW": true, "BY": true, "BZ": true, "CA": true, "CC": true, "CD": true,
	"CF": true, "CG": true, "CH": true, "CI": true, "CK": true, "CL": true, "CM": true, "CN": true,
	"CO": true, "CR": true, "CU": true, "CV": true, "CW": true, "CX": true, "CY": true, "CZ": true,
	"DE": true, "DJ": true, "DK": true, "DM": true, "DO": true, "DZ": true, "EC": true, "EE": true,
	"EG": true, "EH": true, "ER": true, "ES": true, "ET": true, "FI": true, "FJ": true, "FK": true,
	"FM": true, "FO": true, "FR": true, "GA": true, "GB": true, "GD": true, "GE": true, "GF": true,
	"GG": true, "GH": true, "GI": true, "GL": true, "GM": true, "GN": true, "GP": true, "GQ": true,
	"GR": true, "GS": true, "GT": true, "GU": true, "GW": true, "GY": true, "HK": true, "HM": true,
	"HN": true, "HR": true, "HT": true, "HU": true, "ID": true, "IE": true, "IL": true, "IM": true,
	"IN": true, "IO": true, "IQ": true, "IR": true, "IS": true, "IT": true, "JE": true, "JM": true,
	"JO": true, "JP": true, "KE": true, "KG": true, "KH": true, "KI": true, "KM": true, "KN": true,
	"KP": true, "KR": true, "KW": true, "KY": true, "KZ": true, "LA": true, "LB": true, "LC": true,
	"LI": true, "LK": true, "LR": true, "LS": true, "LT": true, "LU": true, "LV": true, "LY": true,
	"MA": true, "MC": true, "MD": true, "ME": true, "MF": true, "MG": true, "MH": true, "MK": true,
	"ML": true, "MM": true, "MN": true, "MO": true, "MP": true, "MQ": true, "MR": true, "MS": true,
	"MT": true, "MU": true, "MV": true, "MW": true, "MX": true, "MY": true, "MZ": true, "NA": true,
	"NC": true, "NE": true, "NF": true, "NG": true, "NI": true, "NL": true, "NO": true, "NP": true,
	"NR": true, "NU": true, "NZ": true, "OM": true, "PA": true, "PE": true, "PF": true, "PG": true,
	"PH": true, "PK": true, "PL": true, "PM": true, "PN": true, "PR": true, "PS": true, "PT": true,
	"PW": true, "PY": true, "QA": true, "RE": true, "RO": true, "RS": true, "RU": true, "RW": true,
	"SA": true, "SB": true, "SC": true, "SD": true, "SE": true, "SG": true, "SH": true, "SI": true,
	"SJ": true, "SK": true, "SL": true, "SM": true, "SN": true, "SO": true, "SR": true, "SS": true,
	"ST": true, "SV": true, "SX": true, "SY": true, "SZ": true, "TC": true, "TD": true, "TF": true,
	"TG": true, "TH": true, "TJ": true, "TK": true, "TL": true, "TM": true, "TN": true, "TO": true,
	"TR": true, "TT": true, "TV": true, "TW": true, "TZ": true, "UA": true, "UG": true, "UM": true,
	"US": true, "UY": true, "UZ": true, "VA": true, "VC": true, "VE": true, "VG": true, "VI": true,
	"VN": true, "VU": true, "WF": true, "WS": true, "YE": true, "YT": true, "ZA": true, "ZM": true,
	"ZW": true,
}

// countryNameToISO maps full country names (including common variants) to
// their ISO 3166-1 alpha-2 code. Extend as new sources surface.
var countryNameToISO = map[string]string{
	"united states": "US", "united states of america": "US", "usa": "US", "u.s.a.": "US", "u.s.": "US", "america": "US",
	"united kingdom": "GB", "uk": "GB", "great britain": "GB", "england": "GB", "scotland": "GB", "wales": "GB",
	"indonesia": "ID", "republic of indonesia": "ID",
	"australia": "AU", "canada": "CA", "germany": "DE", "deutschland": "DE",
	"france": "FR", "spain": "ES", "españa": "ES", "italy": "IT", "italia": "IT",
	"netherlands": "NL", "nederland": "NL", "holland": "NL",
	"singapore": "SG", "malaysia": "MY", "thailand": "TH",
	"japan": "JP", "korea": "KR", "south korea": "KR", "republic of korea": "KR",
	"china": "CN", "people's republic of china": "CN", "vietnam": "VN", "viet nam": "VN",
	"philippines": "PH", "india": "IN",
	"brazil": "BR", "brasil": "BR", "mexico": "MX", "méxico": "MX",
	"new zealand": "NZ", "switzerland": "CH",
	"belgium": "BE", "sweden": "SE", "norway": "NO", "denmark": "DK", "finland": "FI",
	"poland": "PL", "turkey": "TR", "türkiye": "TR",
	"united arab emirates": "AE", "uae": "AE",
	"saudi arabia": "SA", "south africa": "ZA",
}

// normalizeCountry converts a raw country string into ISO 3166-1 alpha-2
// when possible. Returns "" for unrecognized inputs (better empty than wrong).
// Rejects 2-letter inputs that look like US/AU/CA/BR state codes commonly
// confused with country codes ("CA"=California vs Canada, "NY", "WA",
// "NSW", "ON", "SP" — all have caused real false positives in scraped data).
func normalizeCountry(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	if s == "" {
		return ""
	}
	// Full name lookup first (more reliable than 2-letter heuristics).
	if iso, ok := countryNameToISO[s]; ok {
		return iso
	}
	upper := strings.ToUpper(s)
	switch len(upper) {
	case 2:
		// Reject state codes that are also valid ISO codes (CA=Canada vs
		// California, NC=North Carolina vs New Caledonia, etc.). These are
		// almost always state context in scraped addresses; the false-
		// positive cost is much higher than missing a few real Canada/NC
		// entries (those will arrive via full-name path or JSON-LD).
		stateBlocklist := map[string]bool{
			"CA": true, "NY": true, "TX": true, "WA": true, "OR": true,
			"FL": true, "IL": true, "MA": true, "PA": true, "AZ": true,
			"CO": true, "NV": true, "NJ": true, "GA": true, "NC": true,
			"VA": true, "MI": true, "OH": true, "IN": true, "MN": true,
			"WI": true, "MO": true, "MD": true, "TN": true, "SC": true,
			"AL": true, "KY": true, "LA": true, "OK": true, "CT": true,
			"UT": true, "NM": true, "NH": true, "VT": true, "ME": true,
			"RI": true, "DE": true, "HI": true, "AK": true, "ID": true,
			"MT": true, "ND": true, "SD": true, "NE": true, "KS": true,
			"WV": true, "AR": true, "MS": true, "IA": true,
		}
		if stateBlocklist[upper] {
			return ""
		}
		if iso3166Alpha2[upper] {
			return upper
		}
	case 3:
		// alpha-3 → alpha-2 mapping is incomplete here; pass through if
		// recognized as ISO-3 family. Most scrapers will provide alpha-2
		// or full names anyway.
		if upper == "USA" {
			return "US"
		}
		if upper == "GBR" {
			return "GB"
		}
		if upper == "IDN" {
			return "ID"
		}
	}
	return ""
}

// extractLDCityCountry returns (city, country) from a JSON-LD address node.
// Country is normalized to ISO 3166-1 alpha-2 via normalizeCountry — values
// that don't resolve to a real country code are dropped to keep the column
// queryable instead of polluted with state codes / partial names.
func extractLDCityCountry(ld map[string]any) (string, string) {
	addr, ok := ld["address"].(map[string]any)
	if !ok {
		return "", ""
	}
	city, _ := addr["addressLocality"].(string)
	city = strings.TrimSpace(city)

	var rawCountry string
	switch v := addr["addressCountry"].(type) {
	case string:
		rawCountry = v
	case map[string]any:
		if name, ok := v["name"].(string); ok {
			rawCountry = name
		}
	}
	return city, normalizeCountry(rawCountry)
}

// whichYouTubePath reconstructs a canonical YouTube URL from the matched
// regex form. Channel/user paths preserve their structure; @-handles and
// /c/ vanity URLs collapse to the canonical /@handle form.
func whichYouTubePath(matched, handle string) string {
	switch {
	case strings.Contains(matched, "/channel/"):
		return "https://youtube.com/channel/" + handle
	case strings.Contains(matched, "/user/"):
		return "https://youtube.com/user/" + handle
	default:
		return "https://youtube.com/@" + handle
	}
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

	// Meta author → contact name.
	if cd.ContactName == "" {
		if author, ok := meta["author"]; ok && author != "" {
			cd.ContactName = author
		}
	}

	// vCard / hCard fallback for contact name.
	if cd.ContactName == "" {
		doc, err := parse.NewDocument(resp)
		if err == nil {
			for _, sel := range []string{".vcard .fn", ".h-card .p-name", "[itemprop='name'][itemtype*='Person']"} {
				doc.Each(sel, func(_ int, s *goquery.Selection) {
					if cd.ContactName == "" {
						name := strings.TrimSpace(s.Text())
						if name != "" && len(name) < 100 {
							cd.ContactName = name
						}
					}
				})
				if cd.ContactName != "" {
					break
				}
			}
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
// Uses exact domain match (not substring) to avoid killing gmail.com/hotmail.com.
func isExtraBlocklisted(email string) bool {
	lower := strings.ToLower(email)

	// Check prefix blocklist (substring match on full email is OK for prefixes).
	for _, prefix := range extraBlockedPrefixes {
		if strings.Contains(lower, prefix) {
			return true
		}
	}

	// Check domain blocklist (exact match on domain part only).
	atIdx := strings.LastIndex(lower, "@")
	if atIdx < 0 {
		return false
	}
	domain := lower[atIdx+1:]
	return extraBlockedDomains[domain]
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
	if len(local) > 40 {
		return false // Local parts > 40 chars are almost always garbage.
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
	"fitness": true, "health": true, "travel": true, "hotel": true, "yoga": true,
	"gym": true, "beauty": true, "salon": true, "restaurant": true, "cafe": true,
	"space": true, "today": true, "email": true, "link": true,
	"work": true, "asia": true, "rent": true, "page": true,
	// Wellness/business gTLDs
	"services": true, "center": true, "coach": true, "training": true,
	"consulting": true, "solutions": true, "academy": true, "education": true,
	"care": true, "clinic": true, "dental": true, "vet": true,
	"events": true, "marketing": true, "photography": true, "wedding": true,
	"group": true, "team": true, "zone": true, "network": true, "systems": true,
	"expert": true, "guru": true, "tips": true, "bio": true, "eco": true,
	"foundation": true, "institute": true, "international": true,
	"partners": true, "ventures": true, "holdings": true, "limited": true,
	"enterprises": true, "management": true, "properties": true,
	"fun": true, "live": true, "news": true, "social": true, "run": true,
	"one": true, "top": true, "icu": true, "vip": true, "mobi": true,
	"name": true, "ly": true, "gg": true, "to": true, "is": true,

	// Country codes
	"id": true, "uk": true, "au": true, "de": true, "fr": true,
	"jp": true, "kr": true, "sg": true, "my": true, "th": true,
	"ph": true, "vn": true, "in": true, "cn": true, "tw": true,
	"hk": true, "nz": true, "ca": true, "us": true, "br": true,
	"mx": true, "es": true, "it": true, "nl": true, "be": true,
	"ch": true, "at": true, "pl": true, "se": true, "no": true,
	"dk": true, "fi": true, "pt": true, "ru": true, "ua": true,
	"za": true, "ae": true, "sa": true, "il": true, "tr": true,
	"ie": true, "cz": true, "ro": true, "hu": true, "gr": true,
	// Africa & Middle East
	"ke": true, "tz": true, "ug": true, "ng": true, "gh": true,
	"eg": true, "ma": true, "et": true, "qa": true, "kw": true,
	"om": true, "bh": true, "jo": true, "lb": true,
	// Americas
	"ar": true, "cl": true, "pe": true, "ec": true,
	"ve": true, "cr": true, "pa": true, "do": true, "gt": true,
	"pr": true, "tt": true, "jm": true,
	// Europe (additional)
	"sk": true, "bg": true, "lt": true, "lv": true, "ee": true,
	"si": true, "hr": true, "rs": true, "lu": true, "mt": true,
	"cy": true,
	// South/SE Asia (additional)
	"pk": true, "bd": true, "lk": true, "np": true, "mm": true,
	"kh": true, "la": true,

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
