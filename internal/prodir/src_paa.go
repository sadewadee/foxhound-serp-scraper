package prodir

// Pilates Association Australia (PAA) member and studio directory,
// https://member.pilates.org.au/MemberSearch and /StudioSearch
// (~475 opted-in members, individuals, plus 36 studios; the directory shows
// only members who chose to be listed).
//
// The two pages host an iMIS "Search Buddy" widget that reads the iMIS query
// API anonymously: GET /api/iqa?QueryName=<query>&Limit=500 with the page's
// hidden __RequestVerificationToken sent as the RequestVerificationToken
// header (and the session cookie the first GET sets; the shared client keeps a
// cookie jar). The answer is an unpaged PagedResult: Items.$values[] each with
// Properties.$values[] = [{Name, Value}], HasNext / NextOffset when it would
// page (it did not at 475 rows, so the list follows NextOffset only as a
// safety net). Requests: 1 page GET for token + cookie, 1 IQA call per query
// (members, studios) = 3. No detail pages.
//
// Members (IDs "m-<ID>"): MemberName (Name), Company (Business), MemberEmail,
// MemberMobile + StudioPhone + StudioMobile (Phones), Website (~37%), the
// address parts (FullAddress as Address, City, Zip, Country) and Meta
// member_type (MemberTypeDetail), practitioner_level (PractitionerLevel) and
// group_instructor_category (GroupInstructorCategory). Studios (IDs "s-<ID>"):
// Company (Name), Email, Mobile + Phone, Website, address parts, Meta
// member_type. The Description column holds only a generic membership label,
// so it is not used as Bio.
//
// Country: records carry "Australia" (473 of 475 members); the rest default to
// Australia, the association being national. The query names are the site's
// own configuration and are fixed here. Known gaps: studio-owner members
// (MemberType STDIO) are not separated from instructors (the framework has one
// listing type per source); Source ListingType is individual.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	paaBase       = "https://member.pilates.org.au"
	paaMembersQ   = "$/PseudoCode/Search-Buddy/Search Buddy - Online Directory Members"
	paaStudiosQ   = "$/PseudoCode/Search-Buddy/Search Buddy - Online Directory Studios"
	paaDefaultCty = "Australia"
)

func init() {
	register(&Source{
		Slug: "paa", Name: "Pilates Association Australia directory", Host: "member.pilates.org.au",
		Category: "pilates_instructor", ListingType: "individual", MinInterval: time.Second,
		List: paaList,
	})
}

// paaIQAURL is the query API URL of one page of results.
func paaIQAURL(query string, offset int) string {
	v := url.Values{"QueryName": {query}, "Limit": {"500"}}
	if offset > 0 {
		v.Set("Offset", strconv.Itoa(offset))
	}
	return paaBase + "/api/iqa?" + strings.ReplaceAll(v.Encode(), "+", "%20")
}

type paaResult struct {
	Items struct {
		Values []struct {
			Properties struct {
				Values []struct {
					Name  string `json:"Name"`
					Value any    `json:"Value"`
				} `json:"$values"`
			} `json:"Properties"`
		} `json:"$values"`
	} `json:"Items"`
	HasNext    bool `json:"HasNext"`
	NextOffset int  `json:"NextOffset"`
}

func paaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, _, err := getDoc(ctx, f, paaBase+"/MemberSearch")
	if err != nil {
		return err
	}
	tok := doc.Find("input#__RequestVerificationToken, input[name=__RequestVerificationToken]").First().AttrOr("value", "")
	if tok == "" {
		return fmt.Errorf("no __RequestVerificationToken on the member search page (layout changed?)")
	}
	hdr := map[string]string{"RequestVerificationToken": tok, "Accept": "application/json"}

	seen := map[string]bool{}
	for _, q := range []struct {
		name   string
		studio bool
	}{{paaMembersQ, false}, {paaStudiosQ, true}} {
		offset := 0
		for page := 0; page < 100; page++ {
			p, err := f.Get(ctx, paaIQAURL(q.name, offset), hdr)
			if err != nil {
				return err
			}
			if err := p.Err(); err != nil {
				return err
			}
			var res paaResult
			if err := json.Unmarshal(p.Body, &res); err != nil {
				return fmt.Errorf("iqa %q: %w", q.name, err)
			}
			for _, it := range res.Items.Values {
				props := map[string]string{}
				for _, pr := range it.Properties.Values {
					props[pr.Name] = paaString(pr.Value)
				}
				r, ok := paaRecord(props, q.studio)
				if !ok || seen[r.ID] {
					continue
				}
				seen[r.ID] = true
				if err := emit(r); err != nil {
					return err
				}
			}
			if !res.HasNext || res.NextOffset <= offset {
				break
			}
			offset = res.NextOffset
		}
	}
	return nil
}

// paaString renders a property value: strings as is, numbers without exponent,
// null as empty.
func paaString(v any) string {
	switch x := v.(type) {
	case string:
		return clean(x)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return ""
}

func paaRecord(p map[string]string, studio bool) (Record, bool) {
	r := Record{Meta: map[string][]string{}}
	if studio {
		r.ID, r.Name = p["ID"], p["Company"]
		r.Emails = structuredEmails(p["Email"])
		r.Phones = paaNonBlank(p["Mobile"], p["Phone"])
		if r.ID != "" {
			r.ID = "s-" + r.ID
		}
	} else {
		r.ID = p["ID"]
		r.Name = firstNonEmpty(p["MemberName"], clean(p["FirstName"]+" "+p["LastName"]))
		r.Business = p["Company"]
		r.Emails = structuredEmails(p["MemberEmail"])
		r.Phones = paaNonBlank(p["MemberMobile"], p["StudioPhone"], p["StudioMobile"])
		if r.ID != "" {
			r.ID = "m-" + r.ID
		}
		addMeta(r.Meta, "practitioner_level", p["PractitionerLevel"])
		for _, c := range strings.Split(p["GroupInstructorCategory"], ",") {
			addMeta(r.Meta, "group_instructor_category", c)
		}
	}
	// a row without an id or a name is not an entry
	if r.ID == "" || r.Name == "" {
		return Record{}, false
	}
	if w := p["Website"]; w != "" {
		r.Websites = []string{withScheme(w)}
	}
	r.Address, r.City, r.PostalCode = p["FullAddress"], p["City"], p["Zip"]
	r.Country = firstNonEmpty(p["Country"], paaDefaultCty)
	addMeta(r.Meta, "member_type", firstNonEmpty(p["MemberTypeDetail"], p["MemberType"]))
	return r, true
}

// paaNonBlank returns the non-empty values (already cleaned strings).
func paaNonBlank(vals ...string) []string {
	var out []string
	for _, v := range vals {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
