package prodir

// Merrithew (STOTT PILATES) "Find an Instructor", https://www.merrithew.com/
// (~9,000 instructors, individuals).
//
// The site's finder calls an open JSON API on port 5000 (no key, no cookies):
// GET https://www.merrithew.com:5000/api/instructors/find?...&skip=S&take=1000&countryCodes[]=XX
// with Accept: application/vnd.api.v3+json. It refuses a query with neither a
// name nor a country, so the walk iterates the ISO-2 country codes: the four
// big countries (US, CA, GB, AU) one query each, the rest ten codes per query
// (several countryCodes[] are allowed), each paged by skip until skip >=
// totalCount. The row carries everything the site shows, so there is no detail
// stage: Email and BusinessPhone (often empty) are the directory's own fields,
// Country is the full English name, Province the region. No website, no street.
//
// Every instructor stays pilates_instructor (Merrithew is the STOTT PILATES
// brand); the programmes (Certifications: STOTT PILATES Matwork, Total Barre,
// ZEN-GA, Halo ...) go to Meta "certifications".

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const merrithewAPI = "https://www.merrithew.com:5000/api/instructors/find"

// merrithewBig are queried one by one (thousands of instructors each).
var merrithewBig = []string{"US", "CA", "GB", "AU"}

// merrithewISO is every ISO 3166-1 alpha-2 code; the API answers an unknown
// code with totalCount 0.
const merrithewISO = "AD AE AF AG AI AL AM AO AQ AR AS AT AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ " +
	"CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR " +
	"GA GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP " +
	"KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ " +
	"NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW " +
	"SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ " +
	"UA UG UM UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW"

func init() {
	register(&Source{
		Slug: "merrithew", Name: "Merrithew STOTT PILATES instructor finder", Host: "merrithew.com",
		Category: "pilates_instructor", ListingType: "individual", MinInterval: time.Second,
		List: merrithewList,
	})
}

type merrithewRow struct {
	ID             int      `json:"Id"`
	FirstName      string   `json:"FirstName"`
	LastName       string   `json:"LastName"`
	Email          string   `json:"Email"`
	BusinessPhone  string   `json:"BusinessPhone"`
	Country        string   `json:"Country"`
	City           string   `json:"City"`
	Province       string   `json:"Province"`
	IsCurrentSp    bool     `json:"IsCurrentSp"`
	Certifications []string `json:"Certifications"`
}

func merrithewQueries() [][]string {
	big := map[string]bool{}
	var qs [][]string
	for _, c := range merrithewBig {
		big[c] = true
		qs = append(qs, []string{c})
	}
	var group []string
	for _, c := range strings.Fields(merrithewISO) {
		if big[c] {
			continue
		}
		if group = append(group, c); len(group) == 10 {
			qs = append(qs, group)
			group = nil
		}
	}
	if len(group) > 0 {
		qs = append(qs, group)
	}
	return qs
}

func merrithewList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	const take = 1000
	hdr := map[string]string{"Accept": "application/vnd.api.v3+json"}
	seen := map[int]bool{}
	for _, codes := range merrithewQueries() {
		for skip := 0; ; {
			v := url.Values{"name": {""}, "province": {""}, "city": {""}, "certification": {"0"},
				"skip": {strconv.Itoa(skip)}, "take": {strconv.Itoa(take)}}
			for _, c := range codes {
				v.Add("countryCodes[]", c)
			}
			p, err := f.Get(ctx, merrithewAPI+"?"+v.Encode(), hdr)
			if err != nil {
				return err
			}
			if err := p.Err(); err != nil {
				return err
			}
			var resp struct {
				TotalCount int            `json:"totalCount"`
				Data       []merrithewRow `json:"data"`
			}
			if err := json.Unmarshal(p.Body, &resp); err != nil {
				return fmt.Errorf("%v skip %d: %w", codes, skip, err)
			}
			for _, row := range resp.Data {
				r, ok := row.record()
				if !ok || seen[row.ID] {
					continue
				}
				seen[row.ID] = true
				if err := emit(r); err != nil {
					return err
				}
			}
			skip += take
			if len(resp.Data) == 0 || skip >= resp.TotalCount {
				break
			}
		}
	}
	return nil
}

// record maps a row; a row without a name or an id is not an entry.
func (m merrithewRow) record() (Record, bool) {
	name := clean(m.FirstName + " " + m.LastName)
	if m.ID == 0 || name == "" {
		return Record{}, false
	}
	r := Record{
		ID: strconv.Itoa(m.ID), Name: name, City: m.City, Region: m.Province, Country: m.Country,
		Meta: map[string][]string{},
	}
	if e := structuredEmails(m.Email); len(e) > 0 {
		r.Emails = e
	}
	if p := structuredPhone(m.BusinessPhone); p != "" {
		r.Phones = []string{p}
	}
	addMeta(r.Meta, "certifications", m.Certifications...)
	if m.IsCurrentSp {
		addMeta(r.Meta, "current_specialist", "yes")
	}
	return r, true
}
