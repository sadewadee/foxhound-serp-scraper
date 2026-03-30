//go:build playwright

package scraper

import (
	"strings"
)

// FilterPhones removes invalid phone numbers from the extracted list.
// Rules:
// 1. Must start with + or 0 (international or local format)
// 2. 10-15 digits only (ITU-T E.164)
// 3. Reject unix timestamps (10 digits starting with 1 or 2, range 1000000000-2999999999)
// 4. Deduplicate by digits-only form
func FilterPhones(phones []string) []string {
	seen := make(map[string]bool)
	var result []string

	for _, p := range phones {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		// Extract digits and leading +.
		hasPlus := strings.HasPrefix(p, "+")
		digits := extractDigits(p)

		if len(digits) < 10 || len(digits) > 15 {
			continue
		}

		// Must start with + or 0.
		if !hasPlus && !strings.HasPrefix(digits, "0") {
			continue
		}

		// Reject unix timestamps (10 digits starting with 1-2).
		if len(digits) == 10 && (digits[0] == '1' || digits[0] == '2') && !hasPlus {
			continue
		}

		// Dedup by digits.
		key := digits
		if hasPlus {
			key = "+" + digits
		}
		if seen[key] {
			continue
		}
		seen[key] = true

		// Normalize: keep original format but ensure clean.
		if hasPlus {
			result = append(result, "+"+digits)
		} else {
			result = append(result, digits)
		}
	}
	return result
}

// FilterEmails removes invalid/placeholder emails from the extracted list.
// Rules:
// 1. Reject placeholder local parts (your, enter, example, test, noreply, etc.)
// 2. Reject placeholder domains (example.com, test.com, yourdomain.com, etc.)
// 3. Reject if local part > 64 chars (RFC 5321)
// 4. Deduplicate case-insensitive
func FilterEmails(emails []string) []string {
	seen := make(map[string]bool)
	var result []string

	for _, e := range emails {
		e = strings.TrimSpace(strings.ToLower(e))
		if e == "" {
			continue
		}

		parts := strings.SplitN(e, "@", 2)
		if len(parts) != 2 {
			continue
		}
		local, domain := parts[0], parts[1]

		// Local part max 64 chars.
		if len(local) > 64 {
			continue
		}

		// Reject placeholder local parts.
		if isPlaceholderLocal(local) {
			continue
		}

		// Reject placeholder domains.
		if isPlaceholderDomain(domain) {
			continue
		}

		// Dedup.
		if seen[e] {
			continue
		}
		seen[e] = true
		result = append(result, e)
	}
	return result
}

func extractDigits(s string) string {
	var b strings.Builder
	for _, c := range s {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	return b.String()
}

var placeholderLocals = []string{
	"your", "enter", "email", "test", "example", "sample",
	"noreply", "no-reply", "no_reply", "donotreply",
	"user", "username", "name", "info@info",
	"admin@admin", "mail@mail", "contact@contact",
}

func isPlaceholderLocal(local string) bool {
	for _, p := range placeholderLocals {
		if local == p || strings.Contains(local, "your") || strings.Contains(local, "enter") {
			return true
		}
	}
	// Reject "enteryour", "youremail", "testemail", etc.
	for _, keyword := range []string{"youremail", "enteryour", "testemail", "sampleemail", "myemail"} {
		if strings.Contains(local, keyword) {
			return true
		}
	}
	return false
}

var placeholderDomains = []string{
	"example.com", "example.org", "example.net",
	"test.com", "test.org",
	"domain.com", "yourdomain.com", "mydomain.com",
	"email.com",
	"addresshere.com", "sampleemail.com",
	"placeholder.com", "fakeemail.com",
	"company.com", "yourcompany.com",
	"website.com", "yourwebsite.com",
}

func isPlaceholderDomain(domain string) bool {
	for _, pd := range placeholderDomains {
		if domain == pd {
			return true
		}
	}
	return false
}
