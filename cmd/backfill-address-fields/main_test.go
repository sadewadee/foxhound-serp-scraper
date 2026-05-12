//go:build playwright

package main

import (
	"database/sql"
	"testing"
)

// TestComputeUpdate covers the pure decision logic — what country/city to
// write back for a given row. SQL ops (fetchBatch, bulkUpdate) are exercised
// by manual dry-run against staging or production read-only.
func TestComputeUpdate(t *testing.T) {
	cases := []struct {
		name         string
		row          backfillRow
		wantCountry  sql.NullString
		wantCity     sql.NullString
		wantWriteAny bool // helper assertion: any non-empty result
	}{
		{
			name: "both fields fillable from address",
			row: backfillRow{
				ID:      1,
				Address: "521 Oak Grove Rd, Flat Rock, NC, United States",
				Country: sql.NullString{Valid: false},
				City:    sql.NullString{Valid: false},
			},
			wantCountry:  sql.NullString{String: "US", Valid: true},
			wantCity:     sql.NullString{String: "Flat Rock", Valid: true},
			wantWriteAny: true,
		},
		{
			name: "country already set, only city fillable",
			row: backfillRow{
				ID:      2,
				Address: "521 Oak Grove Rd, Flat Rock, NC, US",
				Country: sql.NullString{String: "US", Valid: true},
				City:    sql.NullString{Valid: false},
			},
			wantCountry:  sql.NullString{Valid: false},
			wantCity:     sql.NullString{String: "Flat Rock", Valid: true},
			wantWriteAny: true,
		},
		{
			name: "both already set — no write needed",
			row: backfillRow{
				ID:      3,
				Address: "521 Oak Grove Rd, Flat Rock, NC, US",
				Country: sql.NullString{String: "US", Valid: true},
				City:    sql.NullString{String: "Flat Rock", Valid: true},
			},
			wantCountry:  sql.NullString{Valid: false},
			wantCity:     sql.NullString{Valid: false},
			wantWriteAny: false,
		},
		{
			name: "address has no extractable signal",
			row: backfillRow{
				ID:      4,
				Address: "Compagnonsplein 1",
				Country: sql.NullString{Valid: false},
				City:    sql.NullString{Valid: false},
			},
			wantCountry:  sql.NullString{Valid: false},
			wantCity:     sql.NullString{Valid: false},
			wantWriteAny: false,
		},
		{
			name: "address resolves country only (non-US format)",
			row: backfillRow{
				ID:      5,
				Address: "Frankfurt, Germany",
				Country: sql.NullString{Valid: false},
				City:    sql.NullString{Valid: false},
			},
			wantCountry:  sql.NullString{String: "DE", Valid: true},
			wantCity:     sql.NullString{Valid: false},
			wantWriteAny: true,
		},
		{
			name: "country is sql.NullString with empty Valid string — treated as missing",
			row: backfillRow{
				ID:      6,
				Address: "1 Main St, Boston, MA, USA",
				Country: sql.NullString{String: "", Valid: true},
				City:    sql.NullString{String: "", Valid: true},
			},
			wantCountry:  sql.NullString{String: "US", Valid: true},
			wantCity:     sql.NullString{String: "Boston", Valid: true},
			wantWriteAny: true,
		},
		{
			name: "ID preserved",
			row: backfillRow{
				ID:      99999,
				Address: "Frankfurt, Germany",
			},
			wantCountry:  sql.NullString{String: "DE", Valid: true},
			wantCity:     sql.NullString{Valid: false},
			wantWriteAny: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeUpdate(tc.row)

			if got.ID != tc.row.ID {
				t.Errorf("ID = %d; want %d (must preserve)", got.ID, tc.row.ID)
			}
			if got.Country != tc.wantCountry {
				t.Errorf("Country = %+v; want %+v", got.Country, tc.wantCountry)
			}
			if got.City != tc.wantCity {
				t.Errorf("City = %+v; want %+v", got.City, tc.wantCity)
			}

			gotAnyWrite := got.Country.Valid || got.City.Valid
			if gotAnyWrite != tc.wantWriteAny {
				t.Errorf("any-write = %v; want %v", gotAnyWrite, tc.wantWriteAny)
			}
		})
	}
}
