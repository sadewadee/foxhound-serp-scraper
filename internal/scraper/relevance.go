package scraper

import (
	"net/url"
	"strings"
	"unicode"
)

// SERPResult represents an individual organic search result with URL, title,
// and snippet extracted from the search engine result page.
type SERPResult struct {
	URL     string
	Title   string
	Snippet string
}

// stopwords contains common English stopwords and generic intent words to
// ignore when extracting tokens from search queries.
var stopwords = map[string]bool{
	// Intent & noise words
	"contact":   true,
	"contacts":  true,
	"email":     true,
	"emails":    true,
	"instagram": true,
	"facebook":  true,
	"review":    true,
	"reviews":   true,
	"best":      true,
	"rated":     true,
	"near":      true,
	"me":        true,
	"classes":   true,
	// Short prepositions / articles / conjunctions
	"the":  true,
	"and":  true,
	"for":  true,
	"with": true,
	"from": true,
	"in":   true,
	"a":    true,
	"of":   true,
	// Temporal terms that cause Bing calendar / dictionary SERP poisoning
	"day":  true,
	"days": true,
}

// QueryTokens extracts meaningful search tokens from a raw search query.
// It normalizes to lowercase, strips quoted operator fragments (e.g. "@gmail.com"),
// drops queries with "@" or search operators like "site:"/"inurl:",
// filters out stopwords and generic intent words, and only keeps tokens with length >= 3.
func QueryTokens(query string) []string {
	rawFields := strings.Fields(query)
	var tokens []string
	seen := make(map[string]bool)

	for _, field := range rawFields {
		clean := strings.Trim(field, "\"'")
		if strings.Contains(field, "@") || strings.Contains(clean, "@") {
			continue
		}
		cleanLower := strings.ToLower(clean)
		if strings.HasPrefix(cleanLower, "site:") || strings.HasPrefix(cleanLower, "inurl:") {
			continue
		}

		words := splitWords(cleanLower)
		for _, w := range words {
			if len(w) < 3 {
				continue
			}
			if stopwords[w] {
				continue
			}
			if !seen[w] {
				seen[w] = true
				tokens = append(tokens, w)
			}
		}
	}

	return tokens
}

// IsRelevant checks whether a SERP result is relevant to the given query tokens.
// A result is considered relevant if any token or its >=5-char prefix (stems:
// e.g. chiropract, physio, massag) appears in the lowercase title, snippet,
// URL host, or URL path (with path split on non-alphanumerics).
func IsRelevant(r SERPResult, tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}

	u, err := url.Parse(r.URL)
	var host, path string
	if err == nil {
		host = strings.ToLower(u.Hostname())
		path = strings.ToLower(u.Path)
	} else {
		host = strings.ToLower(r.URL)
	}

	title := strings.ToLower(r.Title)
	snippet := strings.ToLower(r.Snippet)

	words := splitWords(title + " " + snippet + " " + path)

	for _, token := range tokens {
		// 1. Host match: direct substring check or stem prefix check for concatenated hosts (e.g. honolulumedspa.com)
		if strings.Contains(host, token) {
			return true
		}
		if len(token) >= 5 {
			for prefixLen := 5; prefixLen <= len(token); prefixLen++ {
				if strings.Contains(host, token[:prefixLen]) {
					return true
				}
			}
		}

		// 2. Word match against title, snippet, and path words
		for _, w := range words {
			if w == token {
				return true
			}
			// Plural / singular match (e.g. spas <-> spa, gyms <-> gym)
			if strings.HasSuffix(w, "s") && w[:len(w)-1] == token {
				return true
			}
			if strings.HasSuffix(token, "s") && token[:len(token)-1] == w {
				return true
			}
			// Stem / common prefix match with length >= 5 (e.g. chiropractor <-> chiropractic, physiotherapy <-> physio)
			if len(token) >= 5 || len(w) >= 5 {
				if commonPrefixLen(token, w) >= 5 {
					return true
				}
			}
		}
	}

	return false
}

// FilterRelevant filters a slice of SERP results against a query string.
// It returns the kept relevant results and the ratio of relevant to total results.
// If results or query tokens are empty, it returns nil and 0.0.
func FilterRelevant(results []SERPResult, query string) ([]SERPResult, float64) {
	if len(results) == 0 {
		return nil, 0.0
	}
	tokens := QueryTokens(query)
	if len(tokens) == 0 {
		return nil, 0.0
	}

	var kept []SERPResult
	for _, r := range results {
		if IsRelevant(r, tokens) {
			kept = append(kept, r)
		}
	}

	ratio := float64(len(kept)) / float64(len(results))
	return kept, ratio
}

// splitWords splits an input string into lowercase alphanumeric words.
func splitWords(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

// commonPrefixLen calculates the length of the common prefix between two strings.
func commonPrefixLen(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}
