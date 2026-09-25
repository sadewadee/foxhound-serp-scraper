package directory

import (
	"fmt"
	"strings"
)

// jsonLDTypeString normalizes a JSON-LD "@type" value to a single display
// string. Schema.org allows "@type" to be either a plain string or an array
// of strings (a node can belong to multiple types at once, e.g.
// ["LocalBusiness","HealthClub"]) — the previous `ld["@type"].(string)`
// assertion silently returned "" for the array form. Returns the first
// non-empty entry — good enough for Listing.Category, a single display
// field, not a classifier input.
func jsonLDTypeString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []any:
		for _, item := range t {
			if s, ok := item.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// extractFromJSONLD creates a Listing from a JSON-LD object.
// Works for LocalBusiness, Restaurant, GymFitness, Organization, etc.
func extractFromJSONLD(ld map[string]any, source string) Listing {
	l := Listing{Source: source}

	if name, ok := ld["name"].(string); ok {
		l.Name = name
	}

	if url, ok := ld["url"].(string); ok {
		l.URL = url
	}

	if phone, ok := ld["telephone"].(string); ok {
		l.Phone = phone
	}

	if email, ok := ld["email"].(string); ok {
		l.Email = email
	}

	if typ := jsonLDTypeString(ld["@type"]); typ != "" {
		l.Category = typ
	}

	// Address.
	if addr, ok := ld["address"].(map[string]any); ok {
		var parts []string
		for _, key := range []string{"streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"} {
			if v, ok := addr[key].(string); ok && v != "" {
				parts = append(parts, v)
			}
		}
		l.Address = strings.Join(parts, ", ")
	} else if addr, ok := ld["address"].(string); ok {
		l.Address = addr
	}

	// Rating.
	if rating, ok := ld["aggregateRating"].(map[string]any); ok {
		if v := rating["ratingValue"]; v != nil {
			l.Rating = fmt.Sprintf("%v", v)
		}
	}

	return l
}
