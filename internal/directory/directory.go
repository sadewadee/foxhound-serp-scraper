package directory

import (
	"strings"

	"github.com/sadewadee/serp-scraper/internal/dedup"
)

// Listing represents a single business listing extracted from a directory page.
type Listing struct {
	Name     string
	URL      string // Business website URL (for further enrichment)
	Phone    string
	Email    string
	Address  string
	Category string
	Rating   string
	Source   string // e.g. "classpass", "yelp", "yellowpages"
}

// Extractor extracts business listings from a directory page.
type Extractor interface {
	// Match returns true if this extractor handles the given domain.
	Match(domain string) bool
	// Extract parses the HTML body and returns business listings.
	Extract(body []byte) []Listing
	// Name returns the directory name.
	Name() string
}

// registry of all known directory extractors.
var extractors []Extractor

func init() {
	extractors = []Extractor{
		&ClassPassExtractor{},
		&YelpExtractor{},
		&YellowPagesExtractor{},
		&YogaAllianceExtractor{},
		&TripAdvisorExtractor{},
	}
}

// IsDirectory returns true if the URL belongs to a known business directory.
func IsDirectory(rawURL string) bool {
	domain := dedup.ExtractDomain(rawURL)
	for _, ext := range extractors {
		if ext.Match(domain) {
			return true
		}
	}
	return false
}

// ExtractListings extracts business listings from a directory page.
// Returns nil if the domain is not a known directory.
func ExtractListings(rawURL string, body []byte) []Listing {
	domain := dedup.ExtractDomain(rawURL)
	for _, ext := range extractors {
		if ext.Match(domain) {
			return ext.Extract(body)
		}
	}
	return nil
}

// directoryDomains are domains that should be scraped as directories
// instead of being excluded from SERP results.
var directoryDomains = []string{
	"classpass.com",
	"yelp.com",
	"yellowpages.com",
	"yogaalliance.org",
	"tripadvisor.com",
}

// IsDirectoryDomain checks if a domain is a known directory.
func IsDirectoryDomain(domain string) bool {
	domain = strings.ToLower(domain)
	for _, d := range directoryDomains {
		if domain == d || strings.HasSuffix(domain, "."+d) {
			return true
		}
	}
	return false
}
