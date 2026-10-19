package prodir

// Kundalini Research Institute, Aquarian Trainer Academy directory,
// https://www.kundaliniresearchinstitute.org/ (~477 trainers, mostly
// individuals, a few schools or organisations).
//
// The directory is a Zoho Creator app whose public report answers a plain
// POST with JSON rows (HTML cells inside): report-perma-json/All_Trainer, given
// the report's public "privatelink" key. The key sits in the Trainers page,
// which the Home permalink links to, so List derives it: Home -> Trainers
// page -> key (a rotated Home permalink is the one constant to update).
// Rows give name, city, state, country, email (plain), an optional website and
// the trainer levels. There is no phone, address or bio.
//
// Paging quirk: the endpoint returns up to toIDX rows from fromIDX, not
// toIDX-fromIDX+1 (a 51..100 request answers 100 rows). List therefore moves
// fromIDX on by the rows it received, dedupes by row ID and stops on isLastPage
// (about 4 requests for the whole directory).
//
// Host is the institute's own site: the directory's zohopublic.com host is a
// shared platform domain.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	kriApp    = "https://creatorapp.zohopublic.com/kriportal/atadirectory"
	kriHome   = kriApp + "/page-perma/Home/UghmBMD6m2gFBfdaks05eeG1BVpBRpgvymManbvTUDRpSb1vH94SJeJwACsMtw91PC85bYjWsadknxZrVPebjtgYEbKbMX4uyAJW"
	kriReport = kriApp + "/report-perma-json/All_Trainer"
	kriCol    = "4820155000000" // prefix of the report's column ids
	kriStep   = 50
)

// column ids (kriCol + suffix)
const (
	kriName       = kriCol + "062173"
	kriCity       = kriCol + "062181"
	kriState      = kriCol + "062257"
	kriCountry    = kriCol + "064464"
	kriWebsite    = kriCol + "063057"
	kriRole       = kriCol + "184024"
	kriMentoring  = kriCol + "063029"
	kriLevelTwo   = kriCol + "184095"
	kriStages     = kriCol + "184147"
	kriEmailPlain = "zc_Email_unformatted"
)

var (
	kriTrainersRe = regexp.MustCompile(`page-perma\\*/Trainers1\\*/([A-Za-z0-9]+)`)
	kriKeyRe      = regexp.MustCompile(`compLinkName&quot;:&quot;All_Trainer&quot;(?s:.*?)privatelink&quot;:&quot;([A-Za-z0-9]+)&quot;`)
)

func init() {
	register(&Source{
		Slug: "kri", Name: "KRI Aquarian Trainer Academy directory", Host: "kundaliniresearchinstitute.org",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: kriList,
	})
}

// kriKey finds the report's privatelink through the Home and Trainers pages.
func kriKey(ctx context.Context, f Fetcher) (string, error) {
	home, err := f.Get(ctx, kriHome, nil)
	if err != nil {
		return "", err
	}
	if err := home.Err(); err != nil {
		return "", err
	}
	m := kriTrainersRe.FindSubmatch(home.Body)
	if m == nil {
		return "", errors.New("no Trainers page link on the Home page (layout changed?)")
	}
	tr, err := f.Get(ctx, kriApp+"/page-perma/Trainers1/"+string(m[1]), nil)
	if err != nil {
		return "", err
	}
	if err := tr.Err(); err != nil {
		return "", err
	}
	k := kriKeyRe.FindSubmatch(tr.Body)
	if k == nil {
		return "", errors.New("no All_Trainer privatelink on the Trainers page (layout changed?)")
	}
	return string(k[1]), nil
}

func kriList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	key, err := kriKey(ctx, f)
	if err != nil {
		return err
	}
	hdr := map[string]string{"x-requested-with": "XMLHttpRequest", "content-type": "application/x-www-form-urlencoded"}
	seen := map[string]bool{}
	total := 0
	for from := 1; from < 100000; {
		form := url.Values{
			"zc_Search": {"true"}, "zc_LoadIn": {"html"}, "zc_Footer": {"false"}, "privatelink": {key},
			"fromIDX": {strconv.Itoa(from)}, "toIDX": {strconv.Itoa(from + kriStep - 1)},
		}
		p, err := f.Post(ctx, kriReport, hdr, []byte(form.Encode()))
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Model struct {
				Data []struct {
					Data []map[string]any `json:"data"`
				} `json:"DATAJSONARRAY"`
				View struct {
					TotalRecords int  `json:"totalRecords"`
					IsLastPage   bool `json:"isLastPage"`
				} `json:"VIEWPROPMAP"`
			} `json:"MODEL"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("fromIDX %d: %w", from, err)
		}
		total = resp.Model.View.TotalRecords
		var rows []map[string]any
		if len(resp.Model.Data) > 0 {
			rows = resp.Model.Data[0].Data
		}
		fresh := 0
		for _, row := range rows {
			r, ok := kriRecord(row)
			if !ok || seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			fresh++
			if err := emit(r); err != nil {
				return err
			}
		}
		from += len(rows)
		if len(rows) == 0 || fresh == 0 || resp.Model.View.IsLastPage {
			break
		}
	}
	if total > 0 && len(seen) != total {
		slog.Warn("kri: record count differs from totalRecords", "records", len(seen), "total", total)
	}
	return nil
}

// kriCell is a column of a row as plain text (entities decoded).
func kriCell(row map[string]any, col string) string {
	s, _ := row[col].(string)
	return clean(html.UnescapeString(s))
}

func kriRecord(row map[string]any) (Record, bool) {
	id, _ := row["ID"].(string)
	name := kriCell(row, kriName)
	if id == "" || name == "" { // no id or name: not an entry
		return Record{}, false
	}
	r := Record{
		ID: id, Name: name, City: kriCell(row, kriCity), Region: kriCell(row, kriState),
		Country: kriCell(row, kriCountry), Meta: map[string][]string{},
	}
	if e := kriCell(row, kriEmailPlain); e != "" {
		r.Emails = []string{e}
	}
	if cell, _ := row[kriWebsite].(string); cell != "" {
		if doc, err := docFromBody([]byte(cell)); err == nil {
			if href := strings.TrimSpace(doc.Find("a[href]").First().AttrOr("href", "")); href != "" {
				r.Websites = []string{href}
			}
		}
	}
	addMeta(r.Meta, "role", kriCell(row, kriRole))
	addMeta(r.Meta, "mentoring_pathway", kriCell(row, kriMentoring))
	raw, _ := row[kriLevelTwo].(string)
	addMeta(r.Meta, "level_two", yogaSplitList(html.UnescapeString(raw), "<br>")...)
	addMeta(r.Meta, "twentyone_stages", kriCell(row, kriStages))
	return r, true
}
