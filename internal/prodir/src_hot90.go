package prodir

// Hot 90 Finder directory, https://hot90finder.notion.site/ (~59 California
// hot yoga studios, listing_type studio).
//
// A public Notion site. The directory is a Notion database that Notion's own
// web client reads anonymously through POST /api/v3/queryCollection (no cookies
// or login). One request returns every row (limit 1000; 59 rows, hasMore false;
// a hasMore answer fails the stage rather than silently truncating). The
// collection / view / space ids are those of the public directory page. Column
// names are resolved from the collection schema of each response, because
// Notion's property keys (">XwH", "cK;R", ...) are specific to the collection.
//
// Contact fields: the Website and Instagram columns (all rows), the street
// address, city, state and postal code from the Place column (a geocoded address
// string; the country is its last part, "United States" when absent). There is
// no phone and no email: the studio website is the seed for enrich. The region
// group (Southern California, Bay Area, ...) and the verification status go to
// Meta. The class schedule column is not stored.
//
// Host is the directory's own notion.site subdomain, never notion.site itself.
// Rows that are not live pages, or have no name, or are marked closed, are
// skipped.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
)

const (
	hot90Base       = "https://hot90finder.notion.site"
	hot90Collection = "337a1465-b62d-8072-8575-000b9c45a5ce"
	hot90Space      = "281ba7fd-0f92-46cd-8995-2b78199089c6"
	hot90View       = "337a1465-b62d-801b-8511-000c223dcc38"
)

func init() {
	register(&Source{
		Slug: "hot90", Name: "Hot 90 Finder directory", Host: "hot90finder.notion.site",
		Category: "hot_yoga_teacher", ListingType: "studio", MinInterval: time.Second,
		List: hot90List,
	})
}

// hot90Block is a Notion block; the API wraps it once or twice in "value".
type hot90Block struct {
	Value json.RawMessage `json:"value"`
}

type hot90Page struct {
	Alive      *bool                        `json:"alive"`
	Properties map[string][]json.RawMessage `json:"properties"`
}

type hot90Response struct {
	Result struct {
		Reducer struct {
			Group struct {
				BlockIDs []string `json:"blockIds"`
				HasMore  bool     `json:"hasMore"`
			} `json:"collection_group_results"`
		} `json:"reducerResults"`
	} `json:"result"`
	RecordMap struct {
		Block      map[string]hot90Block `json:"block"`
		Collection map[string]hot90Block `json:"collection"`
	} `json:"recordMap"`
}

// unwrap returns the innermost {"value": ...} payload of a record.
func (b hot90Block) unwrap() json.RawMessage {
	raw := b.Value
	for i := 0; i < 3; i++ {
		var in struct {
			Value json.RawMessage `json:"value"`
			Type  string          `json:"type"`
		}
		if json.Unmarshal(raw, &in) != nil || len(in.Value) == 0 || in.Type != "" {
			break
		}
		raw = in.Value
	}
	return raw
}

// hot90Cell is one property cell: [text, [[format, arg], ...]].
func hot90Text(cells []json.RawMessage) string {
	var sb strings.Builder
	for _, c := range cells {
		var el []json.RawMessage
		if json.Unmarshal(c, &el) != nil || len(el) == 0 {
			continue
		}
		var s string
		if json.Unmarshal(el[0], &s) == nil {
			sb.WriteString(s)
		}
	}
	return clean(sb.String())
}

// hot90Place is the address string of a Place cell ("" when none).
func hot90Place(cells []json.RawMessage) string {
	for _, c := range cells {
		var el []json.RawMessage
		if json.Unmarshal(c, &el) != nil || len(el) < 2 {
			continue
		}
		var fmts [][]json.RawMessage
		if json.Unmarshal(el[1], &fmts) != nil {
			continue
		}
		for _, f := range fmts {
			var kind string
			if len(f) == 2 && json.Unmarshal(f[0], &kind) == nil && kind == "plc" {
				var pl struct {
					Name    string `json:"name"`
					Address string `json:"address"`
				}
				if json.Unmarshal(f[1], &pl) == nil {
					return clean(pl.Address)
				}
			}
		}
	}
	return ""
}

var (
	hot90StateZipRe = regexp.MustCompile(`^([A-Z]{2})\s+(\d{5})(?:-\d{4})?$`)
	hot90StatusRe   = regexp.MustCompile(`(?i)closed`)
)

func hot90List(ctx context.Context, f Fetcher, emit func(Record) error) error {
	body, _ := json.Marshal(map[string]any{
		"source":         map[string]any{"type": "collection", "id": hot90Collection, "spaceId": hot90Space},
		"collectionView": map[string]any{"id": hot90View, "spaceId": hot90Space},
		"loader": map[string]any{
			"type":         "reducer",
			"reducers":     map[string]any{"collection_group_results": map[string]any{"type": "results", "limit": 1000}},
			"sort":         []any{},
			"searchQuery":  "",
			"userTimeZone": "UTC",
		},
	})
	p, err := f.Post(ctx, hot90Base+"/api/v3/queryCollection", map[string]string{"Content-Type": "application/json"}, body)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	var resp hot90Response
	if err := json.Unmarshal(p.Body, &resp); err != nil {
		return fmt.Errorf("hot90: %w", err)
	}
	if resp.Result.Reducer.Group.HasMore {
		return fmt.Errorf("hot90: queryCollection has more than 1000 rows, raise the limit")
	}

	// schema: property key by lower-cased column name
	key := map[string]string{}
	for _, c := range resp.RecordMap.Collection {
		var col struct {
			Schema map[string]struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"schema"`
		}
		if json.Unmarshal(c.unwrap(), &col) != nil {
			continue
		}
		for k, s := range col.Schema {
			key[strings.ToLower(s.Name)] = k
			if s.Type == "title" {
				key["title"] = k
			}
		}
	}
	if key["title"] == "" || key["website"] == "" {
		return fmt.Errorf("hot90: collection schema has no title / Website column (layout changed?)")
	}

	for _, id := range resp.Result.Reducer.Group.BlockIDs {
		b, ok := resp.RecordMap.Block[id]
		if !ok {
			continue
		}
		var pg hot90Page
		if json.Unmarshal(b.unwrap(), &pg) != nil || (pg.Alive != nil && !*pg.Alive) {
			continue
		}
		get := func(col string) string { return hot90Text(pg.Properties[key[col]]) }
		name := get("title")
		status := get("status")
		// skip rows without a name and studios marked closed
		if name == "" || hot90StatusRe.MatchString(status) {
			continue
		}
		r := Record{
			ID: strings.ToLower(id), URL: hot90Base + "/" + strings.ReplaceAll(id, "-", ""), Name: name,
			Country: "United States", Meta: map[string][]string{},
		}
		if w := hot90Clean(get("website")); w != "" {
			r.Websites = []string{w}
		}
		if ig := hot90Clean(get("instagram")); ig != "" {
			r.Social = map[string]string{"instagram": ig}
		}
		r.City, _, _ = strings.Cut(get("city"), ",")
		hot90Address(&r, hot90Place(pg.Properties[key["place"]]), name)
		addMeta(r.Meta, "region_group", get("region"))
		addMeta(r.Meta, "status", strings.TrimLeftFunc(status, func(c rune) bool { return !unicode.IsLetter(c) && !unicode.IsDigit(c) }))
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// hot90Clean drops the tracking query and fragment of a pasted link.
func hot90Clean(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return strings.TrimSpace(raw)
	}
	u.RawQuery, u.Fragment = "", ""
	return u.String()
}

// hot90Address splits a Place address "Name, street, City, ST 12345, Country"
// into the record. Without the "ST 12345" part nothing but the whole string is
// kept as the street address.
func hot90Address(r *Record, addr, name string) {
	if addr == "" {
		return
	}
	parts := strings.Split(addr, ", ")
	if len(parts) > 1 && parts[0] == name {
		parts = parts[1:]
	}
	if n := len(parts); n > 1 && !hot90StateZipRe.MatchString(parts[n-1]) {
		r.Country = parts[n-1] // the geocoded country, e.g. "United States"
		parts = parts[:n-1]
	}
	n := len(parts)
	if n >= 3 {
		if m := hot90StateZipRe.FindStringSubmatch(parts[n-1]); m != nil {
			r.Region, r.PostalCode = m[1], m[2]
			r.City = parts[n-2]
			r.Address = strings.Join(parts[:n-2], ", ")
			return
		}
	}
	r.Address = strings.Join(parts, ", ")
}
