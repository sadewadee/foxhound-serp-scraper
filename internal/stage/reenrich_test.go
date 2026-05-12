//go:build playwright

package stage

import (
	"database/sql"
	"testing"
)

// TestOfflineParseDecision covers the pure decision logic of applyOfflineParse:
// what country/city values to write, and whether the network fetch can be
// skipped. SQL ops + slog are covered by integration tests in staging.
func TestOfflineParseDecision(t *testing.T) {
	cases := []struct {
		name           string
		row            reenrichRow
		parsedCountry  string
		parsedCity     string
		wantNewCountry sql.NullString
		wantNewCity    sql.NullString
		wantSkipFetch  bool
	}{
		{
			name: "country and city both NULL, parser found both, row has email — skip fetch",
			row: reenrichRow{
				Address:    sql.NullString{String: "521 Oak Grove Rd, Flat Rock, NC, United States", Valid: true},
				Country:    sql.NullString{Valid: false},
				City:       sql.NullString{Valid: false},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "US",
			parsedCity:     "Flat Rock",
			wantNewCountry: sql.NullString{String: "US", Valid: true},
			wantNewCity:    sql.NullString{String: "Flat Rock", Valid: true},
			wantSkipFetch:  true,
		},
		{
			name: "country NULL, city NULL, no contact data — fill but DON'T skip",
			row: reenrichRow{
				Address:    sql.NullString{String: "1 Main St, San Antonio, TX", Valid: true},
				EmailCount: 0,
				PhoneCount: 0,
			},
			parsedCountry:  "",
			parsedCity:     "San Antonio",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{String: "San Antonio", Valid: true},
			wantSkipFetch:  false,
		},
		{
			name: "row has phone, country already set, city NULL, parser fills city — skip fetch",
			row: reenrichRow{
				Address:    sql.NullString{String: "521 Oak Grove Rd, Flat Rock, NC, US", Valid: true},
				Country:    sql.NullString{String: "US", Valid: true},
				City:       sql.NullString{Valid: false},
				EmailCount: 0,
				PhoneCount: 2,
			},
			parsedCountry:  "US",
			parsedCity:     "Flat Rock",
			wantNewCountry: sql.NullString{Valid: false}, // already set, don't overwrite
			wantNewCity:    sql.NullString{String: "Flat Rock", Valid: true},
			wantSkipFetch:  true,
		},
		{
			name: "everything already filled, parser also found same — write nothing, skip fetch",
			row: reenrichRow{
				Address:    sql.NullString{String: "521 Oak Grove Rd, Flat Rock, NC, US", Valid: true},
				Country:    sql.NullString{String: "US", Valid: true},
				City:       sql.NullString{String: "Flat Rock", Valid: true},
				EmailCount: 5,
				PhoneCount: 0,
			},
			parsedCountry:  "US",
			parsedCity:     "Flat Rock",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  true,
		},
		{
			name: "parser found nothing, row already complete — skip fetch (still valid)",
			row: reenrichRow{
				Address:    sql.NullString{String: "Compagnonsplein 1", Valid: true},
				Country:    sql.NullString{String: "NL", Valid: true},
				City:       sql.NullString{String: "Amsterdam", Valid: true},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "",
			parsedCity:     "",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  true,
		},
		{
			name: "country present, city NULL, parser nothing, has contact — DON'T skip (city missing)",
			row: reenrichRow{
				Address:    sql.NullString{String: "Compagnonsplein 1", Valid: true},
				Country:    sql.NullString{String: "NL", Valid: true},
				City:       sql.NullString{Valid: false},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "",
			parsedCity:     "",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  false,
		},
		{
			name: "row totally empty, parser fills country only — fill country, no skip",
			row: reenrichRow{
				Address:    sql.NullString{String: "Frankfurt, Germany", Valid: true},
				EmailCount: 0,
				PhoneCount: 0,
			},
			parsedCountry:  "DE",
			parsedCity:     "",
			wantNewCountry: sql.NullString{String: "DE", Valid: true},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  false,
		},
		{
			name: "Country empty-string sql.Valid=true treated as missing",
			row: reenrichRow{
				Address:    sql.NullString{String: "1 Main St, Boston, MA, USA", Valid: true},
				Country:    sql.NullString{String: "", Valid: true}, // edge: stored empty
				City:       sql.NullString{String: "", Valid: true},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "US",
			parsedCity:     "Boston",
			wantNewCountry: sql.NullString{String: "US", Valid: true},
			wantNewCity:    sql.NullString{String: "Boston", Valid: true},
			wantSkipFetch:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCountry, gotCity, gotSkip := offlineParseDecision(tc.row, tc.parsedCountry, tc.parsedCity)
			if gotCountry != tc.wantNewCountry {
				t.Errorf("newCountry = %+v; want %+v", gotCountry, tc.wantNewCountry)
			}
			if gotCity != tc.wantNewCity {
				t.Errorf("newCity = %+v; want %+v", gotCity, tc.wantNewCity)
			}
			if gotSkip != tc.wantSkipFetch {
				t.Errorf("skipFetch = %v; want %v", gotSkip, tc.wantSkipFetch)
			}
		})
	}
}
