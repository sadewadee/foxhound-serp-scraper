package prodir

// Balanced Body Studio Finder, https://www.pilates.com/studio-finder
// (1,149 studios in the dataset, of which the Gatsby static query serves the
// first 1,000; listing_type studio: these are Pilates studios that own Balanced
// Body equipment, not individual instructors).
//
// The finder is a Gatsby page whose studio dataset is a static-query JSON file,
// /page-data/sq/d/<hash>.json, under data.allContentfulStudios.nodes[0].studios.
// The hash changes with every site rebuild, so it is discovered on each run:
// /page-data/studio-finder/page-data.json lists its staticQueryHashes and the
// list requests them in turn until one holds allContentfulStudios (1 request
// for the page data + 1..N small ones, ~5 typical). No detail pages.
//
// Per studio, all structured: businessName (Name), contactName (Meta
// contact_name; the person behind the studio), email, telephoneBusiness,
// websiteUrl, the address parts (streetAddress1/2, city, state, postalCode,
// country) and filters.equipment (a JSON object of equipment type -> models,
// flattened to Meta equipment). Country casing is mixed ("UNITED STATES") and
// ~20 distinct names are native-language ("Italia", "Schweiz"): the common
// ones are mapped to ISO-2 here, the shared resolver handles case and aliases,
// the rest (~3% of rows, e.g. Guam, "U.S. Virgin Islands") stays unresolved.
//
// Skipped: studios with publish == false (unpublished drafts) or approved ==
// false; approved == null counts as approved.
// Known gaps: 149 studios fall outside the 1,000 cap of the static query and
// cannot be reached (there is no paging parameter on a static file); the
// separate educator / instructor search is not backed by any public dataset.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const balancedbodyBase = "https://www.pilates.com"

func init() {
	register(&Source{
		Slug: "balancedbody", Name: "Balanced Body studio finder", Host: "pilates.com",
		Category: "pilates_instructor", ListingType: "studio", MinInterval: time.Second,
		List: balancedbodyList,
	})
}

type balancedbodyStudio struct {
	Sys struct {
		ID string `json:"id"`
	} `json:"sys"`
	BusinessName string `json:"businessName"`
	ContactName  string `json:"contactName"`
	Email        string `json:"email"`
	Street1      string `json:"streetAddress1"`
	Street2      string `json:"streetAddress2"`
	City         string `json:"city"`
	State        string `json:"state"`
	PostalCode   string `json:"postalCode"`
	Country      string `json:"country"`
	Phone        string `json:"telephoneBusiness"`
	Website      string `json:"websiteUrl"`
	Publish      bool   `json:"publish"`
	Approved     *bool  `json:"approved"`
	Filters      struct {
		Equipment string `json:"equipment"` // a JSON object serialised as a string
	} `json:"filters"`
}

func balancedbodyList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	studios, err := balancedbodyStudios(ctx, f)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, s := range studios {
		r, ok := s.record()
		if !ok || seen[r.ID] {
			continue
		}
		seen[r.ID] = true
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// balancedbodyStudios finds the static query that holds the studios.
func balancedbodyStudios(ctx context.Context, f Fetcher) ([]balancedbodyStudio, error) {
	p, err := f.Get(ctx, balancedbodyBase+"/page-data/studio-finder/page-data.json", nil)
	if err != nil {
		return nil, err
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	var pd struct {
		Hashes []json.RawMessage `json:"staticQueryHashes"` // strings (numbers tolerated)
	}
	if err := json.Unmarshal(p.Body, &pd); err != nil {
		return nil, fmt.Errorf("studio-finder page-data: %w", err)
	}
	for _, raw := range pd.Hashes {
		hash := strings.Trim(string(raw), `"`)
		if hash == "" {
			continue
		}
		q, err := f.Get(ctx, balancedbodyBase+"/page-data/sq/d/"+hash+".json", nil)
		if err != nil {
			return nil, err
		}
		if q.Status == 404 { // a hash listed by the page but gone: not the one
			continue
		}
		if err := q.Err(); err != nil {
			return nil, err
		}
		if !bytes.Contains(q.Body, []byte(`"allContentfulStudios"`)) {
			continue
		}
		var resp struct {
			Data struct {
				All struct {
					Nodes []struct {
						Studios []balancedbodyStudio `json:"studios"`
					} `json:"nodes"`
				} `json:"allContentfulStudios"`
			} `json:"data"`
		}
		if err := json.Unmarshal(q.Body, &resp); err != nil {
			return nil, fmt.Errorf("static query %s: %w", hash, err)
		}
		var out []balancedbodyStudio
		for _, n := range resp.Data.All.Nodes {
			out = append(out, n.Studios...)
		}
		return out, nil
	}
	return nil, fmt.Errorf("no static query with allContentfulStudios among %d hashes (layout changed?)", len(pd.Hashes))
}

func (s balancedbodyStudio) record() (Record, bool) {
	id, name := clean(s.Sys.ID), clean(s.BusinessName)
	// unpublished drafts and explicitly unapproved rows are not live studios
	if id == "" || name == "" || !s.Publish || (s.Approved != nil && !*s.Approved) {
		return Record{}, false
	}
	r := Record{
		ID: id, Name: name,
		Address:    strings.Join(balancedbodyNonEmpty(s.Street1, s.Street2), ", "),
		City:       clean(s.City),
		Region:     clean(s.State),
		PostalCode: clean(s.PostalCode),
		Country:    balancedbodyCountry(s.Country),
		Meta:       map[string][]string{},
	}
	r.Emails = structuredEmails(s.Email)
	if p := clean(s.Phone); p != "" {
		r.Phones = []string{p}
	}
	if w := clean(s.Website); w != "" {
		r.Websites = []string{withScheme(w)}
	}
	addMeta(r.Meta, "contact_name", s.ContactName)
	// {"Reformers":["Studio Reformer"],"Chairs":[],...}
	var eq map[string][]string
	if json.Unmarshal([]byte(s.Filters.Equipment), &eq) == nil {
		keys := make([]string, 0, len(eq))
		for k := range eq {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			addMeta(r.Meta, "equipment", eq[k]...)
		}
	}
	return r, true
}

// balancedbodyNonEmpty returns the cleaned non-empty values.
func balancedbodyNonEmpty(vals ...string) []string {
	var out []string
	for _, v := range vals {
		if v = clean(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// balancedbodyNative maps the native-language country names studios typed
// (the site's country field is free text) to ISO-2 codes the resolver knows.
var balancedbodyNative = map[string]string{
	"belgique": "BE", "deutschland": "DE", "españa": "ES", "espanha": "ES", "estados unidos": "US",
	"hrvatska": "HR", "italia": "IT", "nederland": "NL", "polska": "PL", "românia": "RO", "schweiz": "CH",
	"slovensko": "SK", "suomi": "FI", "türkiye": "TR", "österreich": "AT", "ελλάδα": "GR", "россия": "RU",
	"ישראל": "IL", "美国": "US", "美國": "US",
}

// balancedbodyCountry is the cleaned country text, or the ISO-2 code of a
// known native-language spelling.
func balancedbodyCountry(s string) string {
	s = clean(s)
	if code, ok := balancedbodyNative[strings.ToLower(s)]; ok {
		return code
	}
	return s
}
