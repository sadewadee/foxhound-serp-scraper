package prodir

// Swimming Canada "Find a Club", https://www.swimming.ca/find-a-club/
// (~366 clubs and provincial bodies, organisations).
//
// The page loads one JSONP file, /club-list.php?callback=load_clubs, that
// lists every club: load_clubs([{...}, ...]) (~70 KB, no paging). A row has
// name, address (one string with street and postal code, the province is not
// separate), website, phone (key present, always empty) and coordinates.
// There is no id: it is the slugified name plus a short hash of the lower-cased
// name; a repeated name gets a numeric suffix. No profile page.
//
// KNOWN GAPS: no email, no phone and no social links (the website is often a
// TeamUnify / gomotion page). Provincial bodies ("Swim <Province>") are mixed
// in with the clubs. Canada is the only country.

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const swimmingCanadaURL = "https://www.swimming.ca/club-list.php?callback=load_clubs"

func init() {
	register(&Source{
		Slug: "swimmingcanada", Name: "Swimming Canada Find a Club", Host: "swimming.ca",
		Category: "swimming_coach", ListingType: "club", MinInterval: time.Second,
		List: swimmingCanadaList,
	})
}

type swimmingCanadaClub struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Website string `json:"website"`
	Phone   string `json:"phone"`
}

// swimmingCanadaJSONP is the JSON between the first "(" and the last ")" of a JSONP response.
func swimmingCanadaJSONP(b []byte) ([]byte, error) {
	i, j := bytes.IndexByte(b, '('), bytes.LastIndexByte(b, ')')
	if i < 0 || j < i {
		return nil, fmt.Errorf("not a JSONP response")
	}
	return b[i+1 : j], nil
}

func swimmingCanadaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, swimmingCanadaURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	body, err := swimmingCanadaJSONP(p.Body)
	if err != nil {
		return err
	}
	var clubs []swimmingCanadaClub
	if err := json.Unmarshal(body, &clubs); err != nil {
		return err
	}
	used := map[string]int{}
	for _, c := range clubs {
		name := clean(c.Name)
		if name == "" { // not a real entry
			continue
		}
		sum := sha1.Sum([]byte(strings.ToLower(name)))
		id := strings.ReplaceAll(slug(name), "_", "-")
		if len(id) > 40 {
			id = strings.Trim(id[:40], "-")
		}
		id += "-" + hex.EncodeToString(sum[:])[:6]
		if used[id]++; used[id] > 1 {
			id += "-" + strconv.Itoa(used[id])
		}
		r := Record{ID: id, Name: name, Address: clean(c.Address), Country: "Canada"}
		if w := clean(c.Website); w != "" {
			r.Websites = []string{w}
		}
		if ph := clean(c.Phone); ph != "" {
			r.Phones = []string{ph}
		}
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}
