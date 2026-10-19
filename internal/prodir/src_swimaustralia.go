package prodir

// SWIM Coaches & Teachers Australia "Find a Swim School",
// https://swim.org.au/find-a-swim-school/ (~486 swim schools, pools and clubs,
// organisations).
//
// The page embeds a MetaLocator widget; its iframe document
// (code.metalocator.com/index.php?option=com_locator&view=directory&...&Itemid=2807&tmpl=component&framed=1&limitstart=N&limit=500)
// carries every location as "var location_data =[{...},...];" in the HTML, and
// limit=500 returns all of them in one request (the default is 100 per page, so
// the walk continues with limitstart while a page is full). The row has name,
// address, city, state, postalcode, country, phone, email and link (the website,
// often without a scheme); the id is the numeric location id. Pools and clubs are
// mixed in with the schools ("Aquatic Centre", "Club"), they are kept.
//
// Host is the association's own swim.org.au, not the third-party metalocator.com
// widget host. Country is the row's own; a row without one is Australia only when
// its state is an Australian state (the directory is Australia's).
//
// KNOWN GAPS: no social links, bio or credentials (the description is empty).
// code.metalocator.com is a third-party SaaS: the 1 s gap is kept.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const swimAustraliaURL = "https://code.metalocator.com/index.php?option=com_locator&view=directory&layout=combined_bootstrap&Itemid=2807&tmpl=component&framed=1&limit=%d&limitstart=%d"

const swimAustraliaPage = 500

func init() {
	register(&Source{
		Slug: "swimaustralia", Name: "SWIM Coaches & Teachers Australia swim school finder", Host: "swim.org.au",
		Category: "swimming_coach", ListingType: "school", MinInterval: time.Second,
		List: swimAustraliaList,
	})
}

// swimAustraliaStates are the Australian state names and codes (lower case).
var swimAustraliaStates = map[string]bool{
	"nsw": true, "vic": true, "qld": true, "sa": true, "wa": true, "nt": true, "tas": true, "act": true,
	"new south wales": true, "victoria": true, "queensland": true, "south australia": true,
	"western australia": true, "northern territory": true, "tasmania": true, "australian capital territory": true,
}

type swimAustraliaLocation struct {
	ID         json.Number `json:"id"`
	Name       string      `json:"name"`
	Address    string      `json:"address"`
	City       string      `json:"city"`
	State      string      `json:"state"`
	PostalCode string      `json:"postalcode"`
	Country    string      `json:"country"`
	Phone      string      `json:"phone"`
	Email      string      `json:"email"`
	Link       string      `json:"link"`
}

func swimAustraliaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"Referer": "https://swim.org.au/"}
	seen := map[string]bool{}
	for start, page := 0, 0; page < 20; page++ {
		p, err := f.Get(ctx, fmt.Sprintf(swimAustraliaURL, swimAustraliaPage, start), hdr)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		// the locations are the JSON array right after "var location_data ="
		_, after, ok := bytes.Cut(p.Body, []byte("var location_data ="))
		if !ok {
			return fmt.Errorf("no location_data at limitstart=%d (layout changed?)", start)
		}
		var rows []swimAustraliaLocation
		if err := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(after))).Decode(&rows); err != nil {
			return fmt.Errorf("location_data: %w", err)
		}
		for _, l := range rows {
			id := l.ID.String()
			if id == "" || seen[id] || clean(l.Name) == "" { // not a real entry, or a repeat
				continue
			}
			seen[id] = true
			r := Record{
				ID: id, Name: clean(l.Name), Address: clean(l.Address), City: clean(l.City), Region: clean(l.State),
				PostalCode: clean(l.PostalCode), Country: clean(l.Country),
			}
			if r.Country == "" && swimAustraliaStates[strings.ToLower(r.Region)] {
				r.Country = "Australia"
			}
			if e := clean(l.Email); e != "" {
				r.Emails = []string{e}
			}
			if ph := clean(l.Phone); ph != "" {
				r.Phones = []string{ph}
			}
			if w := clean(l.Link); w != "" {
				r.Websites = []string{w}
			}
			if err := emit(r); err != nil {
				return err
			}
		}
		if len(rows) < swimAustraliaPage {
			return nil
		}
		start += len(rows)
	}
	return nil
}
