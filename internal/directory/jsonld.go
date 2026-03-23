package directory

import (
	"fmt"
	"strings"
)

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

	if typ, ok := ld["@type"].(string); ok {
		l.Category = typ
	}

	// Address.
	if addr, ok := ld["address"].(map[string]any); ok {
		var parts []string
		for _, key := range []string{"streetAddress", "addressLocality", "addressRegion", "postalCode"} {
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
