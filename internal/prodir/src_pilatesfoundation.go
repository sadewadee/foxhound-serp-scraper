package prodir

// Pilates Foundation (UK) "Find a teacher", https://www.pilatesfoundation.com/map-teachers.php
// (individual teachers; unknown total, likely ~600-1,000).
//
// The finder is a postcode/town radius search: GET map-teachers.php?address=
// <postcode or town> answers with the page HTML carrying `PF_INITIAL = {...}`,
// a JSON object with the teachers within a FIXED 30 miles of the query point
// (results[], sorted by distance; one response holds them all, 154 around
// central London, no cap seen). The radius cannot be changed and there is no
// wildcard, so List walks a fixed bounded grid of pfGrid query points (~150:
// one central postcode district per UK postcode area, plus the Channel
// Islands and extra points in the north, the Highlands, Wales and the
// south-west where areas are larger than the radius) and dedupes on the teacher
// id. Circles of 30 miles around the district centres overlap and cover every
// populated area; a teacher living in a remote gap between two circles is the
// known coverage gap. No detail pages: the listing carries everything.
//
// Per teacher (results[]): title (Name), business_name (Business, when it
// differs), postcode, email (an <a href="mailto:"> fragment), numbers (one or
// more phone numbers, comma-separated, ~50%), website (an <a href> fragment,
// ~45%) and profile_link (an <a href> fragment, only the ~7% with a viewable
// profile; becomes URL). There is no street address, city, credential or bio.
//
// Country: the directory is the UK association's and the grid covers the UK,
// Jersey, Guernsey and Northern Ireland, so records default to "United
// Kingdom". A query the geocoder cannot place answers without PF_INITIAL or
// with no center; such a point is skipped, but if NO point answers with
// PF_INITIAL the layout has changed and List fails.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const pfSearchURL = "https://www.pilatesfoundation.com/map-teachers.php"

func init() {
	register(&Source{
		Slug: "pilatesfoundation", Name: "Pilates Foundation UK teacher finder", Host: "pilatesfoundation.com",
		Category: "pilates_instructor", ListingType: "individual", MinInterval: time.Second,
		List: pfList,
	})
}

// pfGrid are the query points: a central district of every UK postcode area
// plus fill-in points where an area is wider than the 30-mile radius.
var pfGrid = []string{
	// postcode areas
	"AB10", "AL1", "B1", "BA1", "BB1", "BD1", "BH1", "BL1", "BN1", "BR1", "BS1", "BT1", "CA1", "CB1", "CF10",
	"CH1", "CM1", "CO1", "CR0", "CT1", "CV1", "CW1", "DA1", "DD1", "DE1", "DG1", "DH1", "DL1", "DN1", "DT1",
	"DY1", "E1", "EC1A", "EH1", "EN1", "EX1", "FK1", "FY1", "G1", "GL1", "GU1", "HA1", "HD1", "HG1", "HP1",
	"HR1", "HS1", "HU1", "HX1", "IG1", "IP1", "IV1", "KA1", "KT1", "KW1", "KY1", "L1", "LA1", "LD1", "LE1",
	"LL11", "LN1", "LS1", "LU1", "M1", "ME1", "MK1", "ML1", "N1", "NE1", "NG1", "NN1", "NP10", "NR1", "NW1",
	"OL1", "OX1", "PA1", "PE1", "PH1", "PL1", "PO1", "PR1", "RG1", "RH1", "RM1", "S1", "SA1", "SE1", "SG1",
	"SK1", "SL1", "SM1", "SN1", "SO14", "SP1", "SR1", "SS1", "ST1", "SW1A", "SY1", "TA1", "TD1", "TF1", "TN1",
	"TQ1", "TR1", "TS1", "TW1", "UB1", "W1", "WA1", "WC1", "WD1", "WF1", "WN1", "WR1", "WS1", "WV1", "YO1",
	"ZE1",
	// Channel Islands: the geocoder places only full postcodes there (it places
	// nothing for the Isle of Man)
	"GY1 1WG", "JE2 4UH",
	// fill-in points where a postcode area is wider than the radius
	"TR18", "PL19", "EX31", "EX39", "TQ12", "TA24", "DT6", "SY23", "SA31", "SA61", "LL57", "LL30", "LL53",
	"LD3", "NP7", "LA9", "CA11", "NE46", "NE66", "TD15", "DG9", "KA7", "PA34", "PH33", "IV26", "IV51", "IV30",
	"AB42", "DD10", "KW14", "KW15", "YO21", "YO11", "NR32", "PE30", "LN11", "DN15", "BT48", "BT74", "BT92",
}

func pfList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[string]bool{}
	answered := false
	for _, point := range pfGrid {
		p, err := f.Get(ctx, pfSearchURL+"?address="+url.QueryEscape(point), nil)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return err
		}
		res, ok, err := pfInitial(p.Body)
		if err != nil {
			return fmt.Errorf("query %q: %w", point, err)
		}
		if !ok {
			continue
		}
		answered = true
		for _, t := range res {
			r, ok := t.record()
			if !ok || seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	if !answered {
		return fmt.Errorf("no query point answered with PF_INITIAL (layout changed?)")
	}
	return nil
}

type pfTeacher struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	BusinessName string `json:"business_name"`
	Postcode     string `json:"postcode"`
	Website      string `json:"website"` // <a href="...">text</a>
	Email        string `json:"email"`   // <a href="mailto:...">text</a>
	Numbers      string `json:"numbers"` // "07700 900001," or several, comma-separated
	Active       string `json:"active"`
	ProfileLink  string `json:"profile_link"` // <a href="...">text</a>, only for viewable profiles
}

// pfInitial decodes the PF_INITIAL object of a results page. ok is false when
// the page has none or the geocoder found no place for the query.
func pfInitial(body []byte) (results []pfTeacher, ok bool, err error) {
	i := bytes.Index(body, []byte("PF_INITIAL"))
	if i < 0 {
		return nil, false, nil
	}
	j := bytes.IndexByte(body[i:], '{')
	if j < 0 {
		return nil, false, nil
	}
	var v struct {
		Center  json.RawMessage `json:"center"`
		Results []pfTeacher     `json:"results"`
	}
	if err := json.NewDecoder(bytes.NewReader(body[i+j:])).Decode(&v); err != nil {
		return nil, false, fmt.Errorf("PF_INITIAL: %w", err)
	}
	if s := strings.TrimSpace(string(v.Center)); s == "" || s == "null" {
		return nil, false, nil
	}
	return v.Results, true, nil
}

// pfHref is the href of the first link of an HTML fragment, else its text.
func pfHref(fragment string) string {
	if strings.TrimSpace(fragment) == "" {
		return ""
	}
	doc, err := docFromBody([]byte(fragment))
	if err != nil {
		return ""
	}
	if h := clean(doc.Find("a[href]").First().AttrOr("href", "")); h != "" {
		return h
	}
	return clean(doc.Text())
}

func (t pfTeacher) record() (Record, bool) {
	id, name := clean(t.ID), clean(t.Title)
	// "active" is "1" for live teachers; an explicit "0" is a lapsed one
	if id == "" || name == "" || t.Active == "0" {
		return Record{}, false
	}
	r := Record{ID: id, Name: name, PostalCode: clean(t.Postcode), Country: "United Kingdom"}
	if b := clean(t.BusinessName); b != "" && !strings.EqualFold(b, name) {
		r.Business = b
	}
	if strings.TrimSpace(t.Email) != "" {
		if doc, err := docFromBody([]byte(t.Email)); err == nil {
			r.Emails = emailsIn(doc.Selection)
			for _, e := range structuredEmails(doc.Text()) {
				r.Emails = addString(r.Emails, e)
			}
		}
	}
	for _, n := range strings.Split(t.Numbers, ",") {
		if n = clean(n); n != "" {
			r.Phones = append(r.Phones, n)
		}
	}
	if w := pfHref(t.Website); w != "" {
		r.Websites = []string{withScheme(w)}
	}
	if u := pfHref(t.ProfileLink); isOwnDomain(urlHost(u)) {
		r.URL = u // only the directory's own profile page is a profile URL
	}
	return r, true
}
