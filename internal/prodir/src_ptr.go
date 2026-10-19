package prodir

// PTR (Professional Tennis Registry) "Find a Pro", https://ptrtennis.org/find-a-pro
// (~15.8k certified tennis coaches, individuals).
//
// The page is a single-page app backed by an open JSON API,
// /api/proxy/pros?pageSize=100&page=N (1-based, ~159 pages; use the bare host,
// www does not answer): {"items":[...],"total":N,"page":N,"pageSize":N}. Every
// row already carries everything (no detail request): name, a plain email on
// every row, a phone on ~10%, facility, city / state / zip / ISO-2 country, a
// structured address, bio, hourly lesson price and the certifications. There
// is no website or social link.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const ptrURL = "https://ptrtennis.org/api/proxy/pros"

const ptrPageSize = 100

func init() {
	register(&Source{
		Slug: "ptr", Name: "PTR Find a Pro", Host: "ptrtennis.org",
		Category: "sports_tennis", ListingType: "individual", MinInterval: time.Second,
		List: ptrList,
	})
}

// ptrStr is a JSON value the API may send as a string, a number or null
// (a certification level, for one, comes as either): its text, "" for null.
type ptrStr string

func (s *ptrStr) UnmarshalJSON(b []byte) error {
	switch {
	case string(b) == "null":
		*s = ""
	case len(b) > 0 && b[0] == '"':
		var v string
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*s = ptrStr(v)
	default:
		*s = ptrStr(b)
	}
	return nil
}

type ptrPro struct {
	ID        ptrStr `json:"id"`
	FirstName ptrStr `json:"firstName"`
	LastName  ptrStr `json:"lastName"`
	Email     ptrStr `json:"email"`
	Phone     ptrStr `json:"phone"`
	Country   ptrStr `json:"country"`
	State     ptrStr `json:"state"`
	City      ptrStr `json:"city"`
	Zip       ptrStr `json:"zip"`
	Status    ptrStr `json:"status"`
	Bio       ptrStr `json:"bio"`
	Facility  ptrStr `json:"facility"`
	Member    ptrStr `json:"memberNumber"`
	Price     any    `json:"lessonPriceHourly"` // a number, or null
	Offers    struct {
		Private bool `json:"privateLessons"`
		Group   bool `json:"groupLessons"`
		Clinics bool `json:"clinics"`
	} `json:"offers"`
	Addresses []struct {
		Line1 ptrStr `json:"line1"`
		Line2 ptrStr `json:"line2"`
	} `json:"addresses"`
	Certifications []struct {
		Name     ptrStr `json:"certificationName"`
		Category ptrStr `json:"category"`
		Level    ptrStr `json:"level"`
	} `json:"certifications"`
}

func ptrList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"Accept": "application/json"}
	seen := map[string]bool{}
	for page := 1; page <= 1000; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s?pageSize=%d&page=%d", ptrURL, ptrPageSize, page), hdr)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Items    []ptrPro `json:"items"`
			Total    int      `json:"total"`
			PageSize int      `json:"pageSize"` // the size the server actually used
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		for _, it := range resp.Items {
			id := clean(string(it.ID))
			name := clean(string(it.FirstName) + " " + string(it.LastName))
			// a row without id or name is not a coach; an inactive member is not listed any more
			if id == "" || name == "" || seen[id] || (it.Status != "" && !strings.EqualFold(string(it.Status), "active")) {
				continue
			}
			seen[id] = true
			if err := emit(it.record(id, name)); err != nil {
				return err
			}
		}
		size := resp.PageSize
		if size <= 0 {
			size = ptrPageSize
		}
		if len(resp.Items) == 0 || page*size >= resp.Total {
			return nil
		}
	}
	return nil
}

func (it ptrPro) record(id, name string) Record {
	r := Record{
		ID: id, Name: name, Business: string(it.Facility), City: string(it.City), Region: string(it.State),
		PostalCode: string(it.Zip), Country: string(it.Country), Bio: string(it.Bio), Meta: map[string][]string{},
	}
	if e := strings.TrimSpace(string(it.Email)); e != "" {
		r.Emails = []string{e}
	}
	if ph := strings.TrimSpace(string(it.Phone)); ph != "" {
		r.Phones = []string{ph}
	}
	if len(it.Addresses) > 0 {
		r.Address = clean(string(it.Addresses[0].Line1) + " " + string(it.Addresses[0].Line2))
	}
	for _, c := range it.Certifications {
		addMeta(r.Meta, "credential", string(c.Name))
		addMeta(r.Meta, "certification_category", string(c.Category))
		addMeta(r.Meta, "level", string(c.Level))
	}
	addMeta(r.Meta, "member_number", string(it.Member))
	if n, ok := it.Price.(float64); ok && n > 0 {
		addMeta(r.Meta, "lesson_price_hourly", strconv.FormatFloat(n, 'f', -1, 64))
	}
	if it.Offers.Private {
		addMeta(r.Meta, "offers", "private lessons")
	}
	if it.Offers.Group {
		addMeta(r.Meta, "offers", "group lessons")
	}
	if it.Offers.Clinics {
		addMeta(r.Meta, "offers", "clinics")
	}
	return r
}
