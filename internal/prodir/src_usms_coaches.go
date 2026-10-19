package prodir

// U.S. Masters Swimming certified coaches, https://www.usms.org/coach/certified
// (~1,700 coaches, individuals).
//
// One form POST to /coach/certified/jqs/certifiedlist.php returns an HTML
// fragment whose jqGrid call embeds EVERY certification row as JSON
// ("datastr":[...], ~1 MB, no paging). A coach has one row per certification
// level (~2.4 rows each), so rows fold into one record per SwimmerID (a 5-char
// code, lower-cased for the id): credential is the highest level, meta
// certification_levels lists all of them. The name is "Last, First M" and is
// reordered; the club name is the business, the LMSC (regional committee) the
// region. Rows with a masked name ("?, ??") are skipped.
//
// KNOWN GAP: the directory has NO email, phone, website or address. The records
// only identify certified coaches (meta club_code is the club's USMS
// abbreviation, which names the club listed by usms-clubs). No profile page.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const usmsCoachesURL = "https://www.usms.org/coach/certified/jqs/certifiedlist.php"

func init() {
	register(&Source{
		Slug: "usms-coaches", Name: "U.S. Masters Swimming certified coaches", Host: "usms.org",
		Category: "swimming_coach", ListingType: "individual", MinInterval: time.Second,
		List: usmsCoachesList,
	})
}

type usmsCoachRow struct {
	SwimmerID     string `json:"SwimmerID"`
	SwimmerName   string `json:"SwimmerName"`
	CertLevel     string `json:"CertLevel"`
	DateCompleted string `json:"DateCompleted"`
	LMSCName      string `json:"LMSCName"`
	ClubAbbr      string `json:"ClubAbbr"`
	ClubName      string `json:"ClubName"`
}

// usmsLevelRank orders the levels; an unknown label ranks lowest.
var usmsLevelRank = map[string]int{"Grandfathered": 1, "Level 1": 2, "Level 2": 3, "Level 3": 4, "Level 4": 5}

func usmsCoachesList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"Content-Type": "application/x-www-form-urlencoded", "Referer": "https://www.usms.org/coach/certified"}
	p, err := f.Post(ctx, usmsCoachesURL, hdr, []byte("CourseType=CoachCert&LmscID=&ClubID=&WogID="))
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	// the rows are the JSON array right after "datastr":
	_, after, ok := bytes.Cut(p.Body, []byte(`"datastr":`))
	if !ok {
		return fmt.Errorf("no datastr in the response (layout changed?)")
	}
	var rows []usmsCoachRow
	if err := json.NewDecoder(bytes.NewReader(after)).Decode(&rows); err != nil {
		return fmt.Errorf("datastr: %w", err)
	}
	for _, r := range usmsCoachesFold(rows) {
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// usmsCoachName turns "Last, First M" into "First M Last".
func usmsCoachName(n string) string {
	last, first, ok := strings.Cut(n, ",")
	if !ok {
		return clean(n)
	}
	return clean(clean(first) + " " + clean(last))
}

// usmsCoachesFold folds the certification rows into one record per SwimmerID.
func usmsCoachesFold(rows []usmsCoachRow) []Record {
	var out []Record
	at := map[string]int{}
	best := map[string]int{} // id -> rank of the credential shown
	for _, r := range rows {
		id := strings.ToLower(clean(r.SwimmerID))
		// a masked name ("?, ??") is not a real entry
		if id == "" || strings.Contains(r.SwimmerName, "?") || clean(r.SwimmerName) == "" {
			continue
		}
		i, seen := at[id]
		if !seen {
			i = len(out)
			at[id] = i
			out = append(out, Record{
				ID: id, Name: usmsCoachName(r.SwimmerName), Business: clean(r.ClubName), Region: clean(r.LMSCName),
				Country: "United States", Meta: map[string][]string{},
			})
			addMeta(out[i].Meta, "club_code", r.ClubAbbr)
		}
		rec := &out[i]
		addMeta(rec.Meta, "certification_levels", r.CertLevel)
		if lvl := clean(r.CertLevel); lvl != "" {
			if rk := usmsLevelRank[lvl]; rk > best[id] || rec.Meta["credential"] == nil {
				best[id] = rk
				rec.Meta["credential"] = []string{lvl}
			}
		}
		date, _, _ := strings.Cut(clean(r.DateCompleted), "T")
		if cur := rec.Meta["certified_since"]; date != "" && (len(cur) == 0 || date < cur[0]) {
			rec.Meta["certified_since"] = []string{date}
		}
	}
	return out
}
