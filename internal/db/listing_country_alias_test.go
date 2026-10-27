package db

import (
	"context"
	"testing"

	"github.com/sadewadee/serp-scraper/internal/testpg"
)

// The v2 alias backfill must resolve canonical names, aliases and bare ISO-2
// values, cover empty-string codes, leave junk NULL and never overwrite a code.
func TestBackfillListingCountryCodeAliases_V2(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_cc_"}).DB()
	defer d.Close()
	if err := Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	SeedCountries(ctx, d, []Country{{Code: "US", Name: "United States"}, {Code: "GB", Name: "United Kingdom"}})

	rows := []struct {
		domain, country string
		code            any // nil = NULL
		want            any
	}{
		{"bare-iso.example", "us", nil, "US"},
		{"canonical-empty.example", "United States", "", "US"},
		{"alias.example", "USA", nil, "US"},
		{"junk.example", "XX", nil, nil},
		{"keeps-code.example", "us", "GB", "GB"},
	}
	for _, r := range rows {
		if _, err := d.Exec(`INSERT INTO business_listings (domain, url, country, country_code) VALUES ($1, $2, $3, $4)`,
			r.domain, "https://"+r.domain, r.country, r.code); err != nil {
			t.Fatalf("seed %s: %v", r.domain, err)
		}
	}

	BackfillListingCountryCodeAliases(ctx, d, map[string]string{"usa": "US"})

	for _, r := range rows {
		var got *string
		if err := d.QueryRow(`SELECT country_code::text FROM business_listings WHERE domain = $1`, r.domain).Scan(&got); err != nil {
			t.Fatal(err)
		}
		switch w := r.want.(type) {
		case nil:
			if got != nil {
				t.Errorf("%s: country_code = %q, want NULL", r.domain, *got)
			}
		case string:
			if got == nil || *got != w {
				t.Errorf("%s: country_code = %v, want %q", r.domain, got, w)
			}
		}
	}
}
