package prodir

// Generic directory sources. ACE and ICF are bespoke modules (~800 lines each:
// their own state shapes, tokenizers and exports). Every other professional
// directory (Pilates / yoga / fitness / CrossFit / swimming / sports coach
// finders) has the same three needs, so a new directory is ONE small file that
// registers a Source: how to list its entries and, optionally, how to enrich
// one entry from its detail page. Everything else (rate limiting, retries,
// resumable state files, the circuit breaker, CSV export, the Record ->
// business_listings mapping and the loader) is shared; see generic.go.
//
// Adding a source (src_<slug>.go, package prodir):
//
//  1. Pick Slug, Name, Host (the directory's own host), Category (a key of
//     categoryNiche), ListingType and MinInterval.
//  2. Write List: walk the directory and call emit once per entry with a
//     Record (ID and Name are the minimum; ID must be stable and unique).
//  3. If the list lacks contact data, write Detail: fetch the profile page
//     and return a Record carrying only what the page adds.
//  4. register(&Source{...}) from init() of that file; nothing else to edit.
//  5. Add a synthetic fixture under testdata/<slug>_*_SYNTHETIC.* and a test
//     that runs List/Detail against it through a fake Fetcher.
//
// Helpers for HTML sources live in helpers.go (docFromBody, getDoc, absURL,
// emailsIn, decodeCFEmail).

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"
)

// Record is one directory entry as stored in the state files. Only ID is
// required; every other field is optional and an empty one means "unknown".
// Emails, Phones and Websites hold STRUCTURED values only (the directory's own
// email / phone / website fields); contact details hiding in Bio are found at
// load time by extractContacts.
type Record struct {
	ID         string `json:"id"`                    // stable and unique within the source
	URL        string `json:"url,omitempty"`         // public profile / detail page
	Name       string `json:"name,omitempty"`        // the person for individuals, otherwise the business
	Business   string `json:"business,omitempty"`    // studio / business name of an individual
	Address    string `json:"address,omitempty"`     // street address
	City       string `json:"city,omitempty"`        //
	Region     string `json:"region,omitempty"`      // state / province / county
	PostalCode string `json:"postal_code,omitempty"` //
	Country    string `json:"country,omitempty"`     // name or ISO-2 code; resolved against the curated whitelist at load

	Emails   []string          `json:"emails,omitempty"`
	Phones   []string          `json:"phones,omitempty"`
	Websites []string          `json:"websites,omitempty"` // Websites[0] is the entry's own site
	Social   map[string]string `json:"social,omitempty"`   // platform (see socialPlatform) -> URL
	Bio      string            `json:"bio,omitempty"`

	Category string              `json:"category,omitempty"` // per-record override of Source.Category
	Meta     map[string][]string `json:"meta,omitempty"`     // site-native labels: credential, level, styles, specialties, ...
	Missing  bool                `json:"missing,omitempty"`  // detail page is gone (404/410); the list data is all there is
}

// Page is a fetched response: HTTP status, final URL (after redirects) and body.
type Page struct {
	Status int
	URL    string
	Body   []byte
}

// ErrNotFound is returned (wrapped) by Page.Err and getDoc for HTTP 404/410.
// A Detail func that returns it makes the profiles stage store the entry as
// Missing, so it is never fetched again.
var ErrNotFound = errors.New("not found")

// Err is nil for 200, ErrNotFound (wrapped) for 404/410 and an error for any
// other status.
func (p Page) Err() error {
	switch {
	case p.Status == 200:
		return nil
	case p.Status == 404 || p.Status == 410:
		return fmt.Errorf("%w: HTTP %d for %s", ErrNotFound, p.Status, p.URL)
	}
	return fmt.Errorf("HTTP %d for %s", p.Status, p.URL)
}

// Fetcher is what List and Detail use to reach the network. Every call waits
// for the source's MinInterval (shared by all workers) and uses the shared
// retry policy (5 attempts, exponential backoff on 403 / 429 / 5xx and
// connection errors); any other status, 404 included, comes back as a Page
// for the caller to judge. A cancelled run makes calls fail with an error that
// ends the stage quietly; return it unchanged.
type Fetcher interface {
	Get(ctx context.Context, url string, hdr map[string]string) (Page, error)
	Post(ctx context.Context, url string, hdr map[string]string, body []byte) (Page, error)
}

// Source describes one directory.
type Source struct {
	Slug        string        // CLI name and state-file prefix: lower-case, digits, hyphens
	Name        string        // human name, stored as meta source_directory
	Host        string        // the directory's own host (no scheme); synthetic domains end in it, and links to it are never an entry's website or social profile
	Category    string        // default category: a key of categoryNiche
	ListingType string        // individual | studio | gym | club | school | center
	MinInterval time.Duration // minimum gap between two requests to this source, shared by all workers (0 = DefaultMinInterval)

	// List walks the directory and calls emit once per entry. emit validates
	// the record (ID, category) and persists it; it returns errListLimit once
	// the -limit is reached, which List must return as is.
	List func(ctx context.Context, f Fetcher, emit func(Record) error) error

	// Detail fetches the entry's detail page. It may return the whole record
	// or only the fields the page adds: non-empty fields override the list
	// record, Meta merges per key. Return an error wrapping ErrNotFound (see
	// Page.Err) when the page is gone. nil means the list carries everything.
	Detail func(ctx context.Context, f Fetcher, r Record) (Record, error)
}

// DefaultMinInterval is the request gap of a Source that leaves MinInterval 0.
const DefaultMinInterval = time.Second

// categoryNiche is the one canonical Category -> niche_category map. An empty
// niche (sports without a niche bucket yet) is stored as NULL.
var categoryNiche = map[string]string{
	"pilates_instructor":  "pilates",
	"yoga_teacher":        "yoga",
	"hot_yoga_teacher":    "yoga",
	"fitness_trainer":     "fitness",
	"crossfit_trainer":    "fitness",
	"swimming_coach":      "fitness",
	"sports_martial_arts": "fitness",
	"sports_tennis":       "",
	"sports_golf":         "",
	"sports_pickleball":   "",
	"sports_running":      "",
	"sports_triathlon":    "",
	"sports_cycling":      "",
	"sports_equestrian":   "",
}

var listingTypes = map[string]bool{
	"individual": true, "studio": true, "gym": true, "club": true, "school": true, "center": true,
}

var (
	sourceSlugRe = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

	// slugs that would shadow a CLI word or a bespoke source
	reservedSlugs = map[string]bool{"ace": true, "icf": true, "sources": true}

	registry = map[string]*Source{}
)

// validate reports why s cannot be registered.
func (s *Source) validate() error {
	switch {
	case !sourceSlugRe.MatchString(s.Slug) || reservedSlugs[s.Slug]:
		return fmt.Errorf("invalid slug %q", s.Slug)
	case registry[s.Slug] != nil:
		return fmt.Errorf("duplicate slug %q", s.Slug)
	case s.Name == "":
		return fmt.Errorf("source %s: missing Name", s.Slug)
	case s.Host == "" || s.Host != urlHost(s.Host) || !validHost(s.Host):
		return fmt.Errorf("source %s: Host %q must be a bare host like example.org", s.Slug, s.Host)
	case !listingTypes[s.ListingType]:
		return fmt.Errorf("source %s: unknown ListingType %q", s.Slug, s.ListingType)
	case s.List == nil:
		return fmt.Errorf("source %s: missing List", s.Slug)
	}
	if _, ok := categoryNiche[s.Category]; !ok {
		return fmt.Errorf("source %s: unknown Category %q", s.Slug, s.Category)
	}
	return nil
}

// register adds a source; call it from the init() of the source's own file.
// It panics on an invalid or duplicate source (a programming error that must
// not survive the first run). The source's Host joins the own-domain filter of
// extract.go: links back to a directory are never taken as a person's website
// or social profile.
func register(s *Source) {
	if err := s.validate(); err != nil {
		panic("prodir: register: " + err.Error())
	}
	registry[s.Slug] = s
	extOwnDomains = append(extOwnDomains, s.Host)
}

// LookupSource finds a registered generic source by slug.
func LookupSource(slug string) (*Source, bool) {
	s, ok := registry[slug]
	return s, ok
}

// Sources lists the registered generic sources sorted by slug.
func Sources() []*Source {
	out := make([]*Source, 0, len(registry))
	for _, s := range registry {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out
}

// Niche is the niche_category of a category ("" = NULL).
func Niche(category string) string { return categoryNiche[category] }
