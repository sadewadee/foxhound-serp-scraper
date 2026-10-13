package api

import (
	"context"
	"reflect"
	"testing"

	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/testpg"
)

func TestFetchListingMeta(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_apimeta_"}).DB()
	defer d.Close()
	if err := db.Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ids := make([]int64, 2)
	for i, dom := range []string{"meta-one.example", "meta-two.example"} {
		if err := d.QueryRow(`INSERT INTO business_listings (domain, url, business_name)
			VALUES ($1, $2, $3) RETURNING id`, dom, "https://"+dom, dom).Scan(&ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Inserted out of order: the helper returns values sorted per key.
	for _, m := range [][2]string{
		{"credential", "PCC"}, {"credential", "ACC"}, {"designations", "D2"}, {"designations", "D1"}, {"standard_rate", "100 USD"},
	} {
		if _, err := d.Exec(`INSERT INTO business_meta (business_id, meta_key, meta_value) VALUES ($1,$2,$3)`, ids[0], m[0], m[1]); err != nil {
			t.Fatal(err)
		}
	}

	got, err := fetchListingMeta(context.Background(), d, ids)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]map[string][]string{
		ids[0]: {"credential": {"ACC", "PCC"}, "designations": {"D1", "D2"}, "standard_rate": {"100 USD"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fetchListingMeta = %v, want %v", got, want)
	}
	if _, ok := got[ids[1]]; ok {
		t.Errorf("listing without meta must be absent from the map")
	}
}
