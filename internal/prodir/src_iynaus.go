package prodir

// Iyengar Yoga National Association of the United States (Find a Teacher),
// https://www.iynaus.org/ (~903 teachers, individuals).
//
// The finder is an ArcGIS Experience over a public FeatureServer layer
// (IYNAUS_FATM_public_view); one query returns every record (maxRecordCount
// 2000; List still pages on exceededTransferLimit). Each row gives the
// teacher, studio, address, phone and website the teacher chose to publish,
// the CIYT level and the region. There is no email, social or bio. The
// directory is the US association's: a row without a country is a US one
// ("United States"), the rest name their own country.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const iynausLayer = "https://services8.arcgis.com/7SDRW0aZF9jiUqUD/arcgis/rest/services/IYNAUS_FATM_public_view/FeatureServer/0/query"

func init() {
	register(&Source{
		Slug: "iynaus", Name: "Iyengar Yoga National Association of the United States", Host: "iynaus.org",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: iynausList,
	})
}

type iynausRow struct {
	ObjectID   int64  `json:"OBJECTID"`
	Region     string `json:"Current_Region"`
	Level      string `json:"CIYT_Level"`
	Studio     string `json:"Studio_Name"`
	Name       string `json:"Full_Name"`
	Languages  string `json:"Languages"`
	Online     string `json:"Online"`
	Specialty  string `json:"Specialty"`
	Address1   string `json:"Public_Listing_Address_1"`
	Address2   string `json:"Public_Listing_Address_2"`
	City       string `json:"Public_Listing_City"`
	State      string `json:"Public_Listing_State_Province"`
	PostalCode string `json:"Public_Listing_Postal_Code"`
	Country    string `json:"Public_Listing_Country"`
	Phone      string `json:"Public_Listing_Phone"`
	Website    string `json:"Public_Listing_Website"`
}

func iynausList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	const pageSize = 2000
	for offset := 0; offset < 100000; {
		u := fmt.Sprintf("%s?where=1%%3D1&outFields=*&returnGeometry=false&orderByFields=OBJECTID&resultOffset=%d&resultRecordCount=%d&f=json", iynausLayer, offset, pageSize)
		p, err := f.Get(ctx, u, nil)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Features []struct {
				Attributes iynausRow `json:"attributes"`
			} `json:"features"`
			Exceeded bool `json:"exceededTransferLimit"`
			Error    *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("offset %d: %w", offset, err)
		}
		if resp.Error != nil { // ArcGIS reports query errors with HTTP 200
			return fmt.Errorf("offset %d: %s", offset, resp.Error.Message)
		}
		for _, ft := range resp.Features {
			if r, ok := ft.Attributes.record(); ok {
				if err := emit(r); err != nil {
					return err
				}
			}
		}
		if !resp.Exceeded || len(resp.Features) == 0 {
			return nil
		}
		offset += len(resp.Features)
	}
	return nil
}

func (a iynausRow) record() (Record, bool) {
	if a.ObjectID == 0 || clean(a.Name) == "" { // no name: not an entry
		return Record{}, false
	}
	r := Record{
		ID: fmt.Sprint(a.ObjectID), Name: clean(a.Name), Business: clean(a.Studio),
		Address: strings.Join(yogaSplitList(a.Address1+"\n"+a.Address2, "\n"), ", "),
		City:    clean(a.City), Region: clean(a.State), PostalCode: clean(a.PostalCode),
		Country: firstNonEmpty(clean(a.Country), "United States"),
		Meta:    map[string][]string{},
	}
	if w := strings.TrimSpace(a.Website); w != "" {
		r.Websites = []string{w}
	}
	if p := strings.TrimSpace(a.Phone); p != "" {
		r.Phones = []string{p}
	}
	addMeta(r.Meta, "level", a.Level)
	addMeta(r.Meta, "region", a.Region)
	addMeta(r.Meta, "specialties", yogaSplitList(a.Specialty, ",", ";")...)
	addMeta(r.Meta, "languages", yogaSplitList(a.Languages, ",", ";")...)
	addMeta(r.Meta, "online", a.Online)
	return r, true
}
