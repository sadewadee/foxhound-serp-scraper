//go:build playwright

package query

import (
	"fmt"
	"strings"
)

// Niches for wellness/fitness industry — business names.
var Niches = []string{
	"yoga studio", "bikram yoga", "hot yoga", "vinyasa yoga",
	"pilates studio", "pilates class",
	"gym", "fitness center", "health club", "crossfit",
	"wellness center", "wellness spa",
	"healing center", "holistic healing",
	"meditation center", "meditation class",
	"spa", "massage", "day spa",
	"personal trainer", "fitness trainer",
	"dance studio", "barre studio",
}

// PersonalNiches target individual practitioners (more likely to have gmail/yahoo).
var PersonalNiches = []string{
	"yoga instructor", "yoga teacher", "yoga therapist",
	"pilates instructor", "pilates teacher",
	"personal trainer", "fitness coach", "fitness instructor",
	"wellness coach", "wellness practitioner", "wellness consultant",
	"health coach", "life coach", "nutrition coach",
	"meditation teacher", "meditation guide",
	"massage therapist", "sports massage therapist",
	"reiki practitioner", "reiki healer",
	"dance instructor", "zumba instructor",
	"crossfit coach", "strength coach",
	"physical therapist", "physiotherapist",
	"acupuncturist", "chiropractor",
	"naturopath", "ayurvedic practitioner",
}

// WellnessTemplates for business search query variations.
var WellnessTemplates = []string{
	"%s %s",           // "yoga studio bali"
	"%s in %s",        // "yoga studio in bali"
	"%s near %s",      // "yoga studio near bali"
	"best %s in %s",   // "best yoga studio in bali"
	"%s %s contact",   // "yoga studio bali contact"
	"%s %s email",     // "yoga studio bali email"
}

// PersonalTemplates target individuals — higher gmail/yahoo yield.
var PersonalTemplates = []string{
	"%s %s",                       // "yoga instructor bali"
	"%s in %s",                    // "yoga instructor in bali"
	"%s %s email",                 // "yoga instructor bali email"
	"%s %s contact",               // "yoga instructor bali contact"
	"%s %s \"@gmail.com\"",        // "yoga instructor bali" "@gmail.com"
	"%s %s \"@yahoo.com\"",        // "yoga instructor bali" "@yahoo.com"
	"hire %s in %s",               // "hire yoga instructor in bali"
	"freelance %s %s",             // "freelance personal trainer bali"
	"%s %s instagram",             // "yoga instructor bali instagram" (bio has email)
}

// Cities organized by country.
var Cities = map[string][]string{
	// Indonesia
	"Indonesia": {
		"Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
		"Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
		"Denpasar", "Bali", "Ubud", "Canggu", "Seminyak",
		"Yogyakarta", "Malang", "Solo", "Bogor", "Balikpapan",
		"Manado", "Lombok", "Batam",
	},
	// United States
	"United States": {
		"New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
		"San Antonio", "San Diego", "Dallas", "San Jose", "Austin",
		"San Francisco", "Seattle", "Denver", "Portland", "Miami",
		"Atlanta", "Boston", "Nashville", "Las Vegas", "Minneapolis",
		"Scottsdale", "Boulder", "Santa Monica", "Sedona", "Asheville",
		"Honolulu", "Maui", "Oahu",
		"Brooklyn", "Venice Beach", "Silver Lake", "West Hollywood",
		"Williamsburg", "Park Slope",
	},
	// Australia
	"Australia": {
		"Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
		"Gold Coast", "Canberra", "Hobart", "Darwin", "Newcastle",
		"Byron Bay", "Bondi", "Manly", "Surry Hills", "Fitzroy",
	},
	// United Kingdom
	"United Kingdom": {
		"London", "Manchester", "Birmingham", "Leeds", "Liverpool",
		"Bristol", "Edinburgh", "Glasgow", "Brighton", "Oxford",
		"Cambridge", "Bath", "Nottingham", "Sheffield", "Cardiff",
	},
	// Germany
	"Germany": {
		"Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne",
		"Stuttgart", "Düsseldorf", "Leipzig", "Dresden", "Hannover",
	},
	// France
	"France": {
		"Paris", "Lyon", "Marseille", "Toulouse", "Nice",
		"Bordeaux", "Strasbourg", "Nantes", "Montpellier", "Lille",
	},
	// Spain
	"Spain": {
		"Madrid", "Barcelona", "Valencia", "Seville", "Malaga",
		"Bilbao", "Ibiza", "Palma de Mallorca", "Granada", "Alicante",
	},
	// Netherlands
	"Netherlands": {
		"Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven",
		"Groningen", "Haarlem", "Leiden",
	},
	// Italy
	"Italy": {
		"Rome", "Milan", "Florence", "Naples", "Turin",
		"Venice", "Bologna", "Genoa", "Verona", "Palermo",
	},
	// Portugal
	"Portugal": {
		"Lisbon", "Porto", "Faro", "Braga", "Coimbra",
		"Cascais", "Lagos", "Ericeira",
	},
	// Sweden
	"Sweden": {
		"Stockholm", "Gothenburg", "Malmö", "Uppsala", "Lund",
	},
	// Thailand
	"Thailand": {
		"Bangkok", "Chiang Mai", "Phuket", "Koh Samui", "Pattaya",
		"Krabi", "Hua Hin", "Pai", "Koh Phangan",
	},
	// Vietnam
	"Vietnam": {
		"Ho Chi Minh City", "Hanoi", "Da Nang", "Hoi An", "Nha Trang",
	},
	// Philippines
	"Philippines": {
		"Manila", "Cebu", "Davao", "Makati", "Quezon City",
		"Siargao", "Palawan", "Boracay",
	},
	// Malaysia
	"Malaysia": {
		"Kuala Lumpur", "Penang", "Johor Bahru", "Kota Kinabalu",
		"Langkawi", "Malacca", "Ipoh",
	},
	// Singapore
	"Singapore": {
		"Singapore",
	},
}

// GenerateKeywords produces all {niche} x {city} x {template} combinations.
// Returns a deduplicated, lowercased keyword list.
func GenerateKeywords(niches, cities []string, templates []string) []string {
	if len(templates) == 0 {
		templates = WellnessTemplates
	}

	seen := make(map[string]bool)
	var keywords []string

	for _, niche := range niches {
		for _, city := range cities {
			for _, tmpl := range templates {
				kw := strings.ToLower(fmt.Sprintf(tmpl, niche, city))
				if !seen[kw] {
					seen[kw] = true
					keywords = append(keywords, kw)
				}
			}
		}
	}
	return keywords
}

// GenerateAllKeywords generates keywords for ALL cities in ALL countries.
// Combines business templates + personal templates for maximum coverage.
func GenerateAllKeywords() []string {
	var allCities []string
	for _, c := range Cities {
		allCities = append(allCities, c...)
	}
	// Business queries (yoga studio bali, etc).
	business := GenerateKeywords(Niches, allCities, WellnessTemplates)
	// Personal queries (yoga instructor bali @gmail.com, etc).
	personal := GenerateKeywords(PersonalNiches, allCities, PersonalTemplates)
	return dedupStrings(append(business, personal...))
}

// GenerateKeywordsForCountry generates keywords for a specific country.
func GenerateKeywordsForCountry(country string) []string {
	cities, ok := Cities[country]
	if !ok {
		return nil
	}
	business := GenerateKeywords(Niches, cities, WellnessTemplates)
	personal := GenerateKeywords(PersonalNiches, cities, PersonalTemplates)
	return dedupStrings(append(business, personal...))
}

// dedupStrings removes duplicate strings (case-insensitive).
func dedupStrings(items []string) []string {
	seen := make(map[string]bool, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		key := strings.ToLower(item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}
	return result
}

// CountryList returns all available country names.
func CountryList() []string {
	countries := make([]string, 0, len(Cities))
	for c := range Cities {
		countries = append(countries, c)
	}
	return countries
}
