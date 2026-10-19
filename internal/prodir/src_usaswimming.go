package prodir

// USA Swimming "Find a Team", https://www.usaswimming.org/home/find-a-team
// (~1,930 clubs and ~250 learn-to-swim providers, organisations).
//
// The page's map widget POSTs to club-api.usaswimming.org/swims/ClubFacilityMap/search
// and gets the WHOLE dataset back as one JSON array (no geo filter, no paging):
// one call per finderType, "USA Swimming Club" (~2.5 MB) and "Learn To Swim"
// (~260 KB). A row is one club/facility pair, so a club that trains at several
// pools appears several times with the same orgUnitKey; rows are folded into
// ONE record per orgUnitKey (id "club-<key>" / "lts-<key>"): address and
// website of the first row, emails / phones / contact names / facilities of all
// rows. There is no profile page: the list carries everything.
//
// Contacts: contactEmailAddress and contactPhoneNumber are the club's contact
// (often the head coach or registrar), websiteAddress is frequently a
// SportsEngine / TeamUnify page. No social links and no bio. The US is the
// only country. ListingType is club; the finderType is kept in meta
// finder_type (Learn To Swim providers are schools, not clubs).

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const usaswimmingURL = "https://club-api.usaswimming.org/swims/ClubFacilityMap/search"

func init() {
	register(&Source{
		Slug: "usaswimming", Name: "USA Swimming Find a Team", Host: "usaswimming.org",
		Category: "swimming_coach", ListingType: "club", MinInterval: time.Second,
		List: usaswimmingList,
	})
}

// usaswimmingFinders are the two finderType values the widget queries.
var usaswimmingFinders = []struct{ finder, idPrefix, label string }{
	{"USA Swimming Club", "club", "usa_swimming_club"},
	{"Learn To Swim", "lts", "learn_to_swim"},
}

type usaswimmingRow struct {
	OrgUnitKey          int    `json:"orgUnitKey"`
	FacilityName        string `json:"facilityName"`
	ContactName         string `json:"contactName"`
	ContactEmail        string `json:"contactEmailAddress"`
	ContactPhone        string `json:"contactPhoneNumber"`
	Website             string `json:"websiteAddress"`
	Curriculum          string `json:"curriculumName"`
	OfferAdultLTS       bool   `json:"offerAdultLearnToSwim"`
	OfferSpecialNeeds   bool   `json:"offerSpecialNeeds"`
	ExcellenceLevel     string `json:"clubExcellenceLevel"`
	SafeSportRecognized bool   `json:"safeSportRecognition"`
	ClubSize            int    `json:"clubSize"`
	ClubName            string `json:"clubName"`
	Address1            string `json:"address1"`
	Address2            string `json:"address2"`
	City                string `json:"city"`
	State               string `json:"stateCode"`
	PostalCode          string `json:"postalCode"`
}

func usaswimmingList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{
		"Content-Type": "application/json", "Accept": "application/json",
		"Origin": "https://www.usaswimming.org", "Referer": "https://www.usaswimming.org/",
	}
	for _, fd := range usaswimmingFinders {
		// the widget sends all the ClubExcellence flags: an empty set may filter rows out
		body, _ := json.Marshal(map[string]any{
			"finderType":           fd.finder,
			"clubExcellenceLevel0": "NA", "clubExcellenceLevel1": "Bronze",
			"clubExcellenceLevel2": "Silver", "clubExcellenceLevel3": "Gold",
			"isSafeSportRecognized": 0,
		})
		p, err := f.Post(ctx, usaswimmingURL, hdr, body)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return fmt.Errorf("%s: %w", fd.finder, err)
		}
		var rows []usaswimmingRow
		if err := json.Unmarshal(p.Body, &rows); err != nil {
			return fmt.Errorf("%s: %w", fd.finder, err)
		}
		for _, r := range usaswimmingFold(rows, fd.idPrefix, fd.label) {
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// usaswimmingFold folds the club/facility rows into one record per orgUnitKey,
// in first-seen order. A row without a club name or key is not a real entry
// and is skipped.
func usaswimmingFold(rows []usaswimmingRow, idPrefix, finder string) []Record {
	var out []Record
	at := map[int]int{} // orgUnitKey -> index in out
	for _, r := range rows {
		name := clean(r.ClubName)
		if name == "" || r.OrgUnitKey == 0 {
			continue
		}
		i, seen := at[r.OrgUnitKey]
		if !seen {
			i = len(out)
			at[r.OrgUnitKey] = i
			rec := Record{
				ID: idPrefix + "-" + strconv.Itoa(r.OrgUnitKey), Name: name,
				Address: strings.Trim(clean(r.Address1)+", "+clean(r.Address2), ", "),
				City:    clean(r.City), Region: clean(r.State), PostalCode: clean(r.PostalCode),
				Country: "United States", Meta: map[string][]string{},
			}
			addMeta(rec.Meta, "finder_type", finder)
			if lvl := clean(r.ExcellenceLevel); !strings.EqualFold(lvl, "NA") {
				addMeta(rec.Meta, "club_excellence_level", lvl)
			}
			if r.ClubSize > 0 {
				addMeta(rec.Meta, "club_size", strconv.Itoa(r.ClubSize))
			}
			out = append(out, rec)
		}
		rec := &out[i]
		if v := strings.ToLower(clean(r.ContactEmail)); v != "" {
			rec.Emails = addString(rec.Emails, v)
		}
		if v := clean(r.ContactPhone); v != "" {
			rec.Phones = addString(rec.Phones, v)
		}
		if v := clean(r.Website); v != "" {
			rec.Websites = addString(rec.Websites, v)
		}
		addMeta(rec.Meta, "contact_name", r.ContactName)
		addMeta(rec.Meta, "facility", r.FacilityName)
		if c := clean(r.Curriculum); !strings.EqualFold(c, "NA") { // the club rows carry "NA"
			addMeta(rec.Meta, "curriculum", c)
		}
		if r.SafeSportRecognized {
			rec.Meta["safesport_recognized"] = []string{"yes"}
		}
		if r.OfferAdultLTS {
			rec.Meta["adult_learn_to_swim"] = []string{"yes"}
		}
		if r.OfferSpecialNeeds {
			rec.Meta["special_needs"] = []string{"yes"}
		}
	}
	return out
}
