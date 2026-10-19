package prodir

// AUSTSWIM Swim School Finder, https://austswim.com.au/australian-swim-school-finder
// (~223 swim schools, organisations).
//
// The finder page calls a HubSpot-fronted JSON API:
// POST https://webapi.austswim.com.au/api/search_swim_center with
// {"search_val":"","country":"","state":"","after":N,"limit":200}; an empty
// filter is everything, limit is capped at 200 and the response carries
// "paging":{"next":{"after":"200"}} until the last page. Each result is a
// HubSpot company (id = the record id) whose properties hold the business name,
// address, city, state, zip, business_email, phone and website (no scheme),
// the recognition level and the directory flag. There is no profile page.
//
// A school whose allow_directory_search is an explicit No is not in the public
// directory and is skipped (a missing value is public: most rows carry none).
// Country is Australia for the Australian state codes; the finder files New
// Zealand and overseas schools under the state "INT" / "OVS", which names no
// country, so those rows keep an empty country (never guessed from a city).
//
// KNOWN GAPS: no social links, bio or individual teacher names (there is no
// public teacher finder).

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const austswimURL = "https://webapi.austswim.com.au/api/search_swim_center"

func init() {
	register(&Source{
		Slug: "austswim", Name: "AUSTSWIM swim school finder", Host: "austswim.com.au",
		Category: "swimming_coach", ListingType: "school", MinInterval: time.Second,
		List: austswimList,
	})
}

// austswimStates are the Australian states and territories of the finder.
var austswimStates = map[string]bool{"NSW": true, "VIC": true, "QLD": true, "SA": true, "WA": true, "NT": true, "TAS": true, "ACT": true}

type austswimSchool struct {
	ID         json.Number `json:"id"`
	Archived   bool        `json:"archived"`
	Properties struct {
		Name        string `json:"name"`
		Address     string `json:"address"`
		Address2    string `json:"address2"`
		City        string `json:"city"`
		State       string `json:"state"`
		Zip         string `json:"zip"`
		Email       string `json:"business_email"`
		Phone       string `json:"phone"`
		Website     string `json:"website"`
		Recognition string `json:"swim_school_recognition_level"`
		Type        string `json:"type"`
		InDirectory string `json:"allow_directory_search"`
	} `json:"properties"`
}

func austswimList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"Content-Type": "application/json", "Accept": "application/json", "Origin": "https://austswim.com.au"}
	seen := map[string]bool{}
	for after, page := 0, 0; page < 50; page++ {
		body, _ := json.Marshal(map[string]any{"search_val": "", "country": "", "state": "", "after": after, "limit": 200})
		p, err := f.Post(ctx, austswimURL, hdr, body)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Results []austswimSchool `json:"results"`
			Paging  *struct {
				Next struct {
					After json.Number `json:"after"`
				} `json:"next"`
			} `json:"paging"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("after=%d: %w", after, err)
		}
		for _, s := range resp.Results {
			id := s.ID.String()
			pr := s.Properties
			switch in := strings.ToLower(clean(pr.InDirectory)); {
			case id == "" || seen[id] || s.Archived || clean(pr.Name) == "": // not a real entry, or a repeat
				continue
			case in == "no" || in == "false" || in == "0": // not in the public directory
				continue
			}
			seen[id] = true
			r := Record{
				ID: id, Name: clean(pr.Name), City: clean(pr.City), PostalCode: clean(pr.Zip),
				Address: strings.Trim(clean(pr.Address)+", "+clean(pr.Address2), ", "),
				Meta:    map[string][]string{},
			}
			if st := strings.ToUpper(clean(pr.State)); austswimStates[st] {
				r.Region, r.Country = st, "Australia"
			}
			if e := clean(pr.Email); e != "" {
				r.Emails = []string{e}
			}
			if ph := clean(pr.Phone); ph != "" {
				r.Phones = []string{ph}
			}
			if w := clean(pr.Website); len(w) > 3 { // "-" is the form's placeholder
				r.Websites = []string{w}
			}
			addMeta(r.Meta, "recognition_level", pr.Recognition)
			addMeta(r.Meta, "type", pr.Type)
			if err := emit(r); err != nil {
				return err
			}
		}
		// the last page has no paging key; a repeated or non-advancing cursor ends the walk too
		if len(resp.Results) == 0 || resp.Paging == nil {
			return nil
		}
		next, err := strconv.Atoi(resp.Paging.Next.After.String())
		if err != nil || next <= after {
			return nil
		}
		after = next
	}
	return nil
}
