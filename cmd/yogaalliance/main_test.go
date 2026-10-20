package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/testpg"
)

const teacherJSON = `{"returnValue":{"Id":"003SYNTH0000000001","directoryName":"Jane Fixture",
"teacherEmail":"Jane.Fixture@example.com","isEmailPublished":true,"address":"1 Example St",
"mailingCity":"Exampleville","mailingCountry":"Atlantis","teacherInstagram":"@janefixture",
"teacherDesignation":"E-RYT 200, RYT 500, YACEP","languages":"ENGLISH, FRANÇAIS,",
"typesOfYogaTaught":"Flow Yoga, Hot/Heated Yoga","teachingHours":1500,"offerOnlineClasses":true,
"isYACEP":true,"originalMembershipBeginDate":"2015-01-26",
"teachingHistoryList":[{"locationName":"Fixture Studio","googleAddress":"2 Studio Rd","statusLabel":"Current"}]}}`

const schoolJSON = `{"returnValue":{"Id":"001SYNTH0000000001","directoryName":"Fixture Yoga School",
"schoolEmail":"hello@fixture-school.example","address":"3 School Ave, Exampleville","schoolWebsite":"fixture-school.example",
"schoolFacebook":"https://facebook.com/fixture","schoolInstagram":"https://instagram.com/fixture",
"schoolDesignation":"RYS® 200, RYS® 300","typesOfYogaTaught":"Flow Yoga, Gentle Yoga",
"trainingFormats":"Part-time (spread out over time), Weekends only",
"trainingFormatsList":["Part-time","(spread out over time)","Weekends only"],
"originalMembershipBeginDate":"2012-03-01"}}`

func TestParseTeacherMeta(t *testing.T) {
	e := parseTeacher([]byte(teacherJSON))
	if e == nil {
		t.Fatal("parseTeacher returned nil")
	}
	want := map[string][]string{
		"listing_type":     {"teacher"},
		"designation":      {"E-RYT 200", "RYT 500", "YACEP"},
		"yoga_types":       {"Flow Yoga", "Hot/Heated Yoga"},
		"languages":        {"ENGLISH", "FRANÇAIS"},
		"membership_begin": {"2015-01-26"},
		"school_name":      {"Fixture Studio"},
		"teaching_hours":   {"1500"},
		"offers_online":    {"true"},
		"yacep":            {"true"},
	}
	if got := e.meta(); !reflect.DeepEqual(got, want) {
		t.Errorf("meta = %v, want %v", got, want)
	}
	if e.domain() != "003synth0000000001.ryt.yogaalliance.org" {
		t.Errorf("domain = %q", e.domain())
	}
}

func TestParseSchoolMeta(t *testing.T) {
	e := parseSchool([]byte(schoolJSON))
	if e == nil {
		t.Fatal("parseSchool returned nil")
	}
	want := map[string][]string{
		"listing_type":     {"school"},
		"designation":      {"RYS® 200", "RYS® 300"},
		"yoga_types":       {"Flow Yoga", "Gentle Yoga"},
		"training_formats": {"Part-time (spread out over time)", "Weekends only"},
		"membership_begin": {"2012-03-01"},
	}
	if got := e.meta(); !reflect.DeepEqual(got, want) {
		t.Errorf("meta = %v, want %v", got, want)
	}
	if e.domain() != "001synth0000000001.rys.yogaalliance.org" || e.website != "fixture-school.example" {
		t.Errorf("domain/website = %q / %q", e.domain(), e.website)
	}
}

func TestParseRejectsEmpty(t *testing.T) {
	for _, body := range []string{`{"error":[{"message":"No valid School profile found"}]}`, `not json`, `{"returnValue":{}}`} {
		if parseTeacher([]byte(body)) != nil || parseSchool([]byte(body)) != nil {
			t.Errorf("expected nil for %q", body)
		}
	}
}

func TestSitemapRecordIDs(t *testing.T) {
	body := `<loc>https://app.yogaalliance.org/schoolpublicprofile/001AAAAAAAAAAAAAAA/household-smith</loc>
<loc>https://app.yogaalliance.org/schoolpublicprofile/001BBBBBBBBBBBBBBB/fixture-yoga-school</loc>
<loc>https://app.yogaalliance.org/teacherpublicprofile/003CCCCCCCCCCCCCCC/jane-fixture</loc>`
	if got, want := sitemapRecordIDs(body, kindSchool), []string{"001BBBBBBBBBBBBBBB", "003CCCCCCCCCCCCCCC"}; !reflect.DeepEqual(got, want) {
		t.Errorf("school ids = %v, want %v", got, want)
	}
	if got := sitemapRecordIDs(body, kindTeacher); len(got) != 3 {
		t.Errorf("teacher mode keeps every id, got %v", got)
	}
}

// TestUpsertMeta runs upsertEntry against a throwaway Postgres: meta is
// written, replaced per key on a re-run, and an empty value never blanks a row.
func TestUpsertMeta(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_yoga_"}).DB()
	defer d.Close()
	if err := db.Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	metaOf := func(domain string) map[string][]string {
		t.Helper()
		rows, err := d.Query(`SELECT m.meta_key, m.meta_value FROM business_meta m
			JOIN business_listings b ON b.id = m.business_id WHERE b.domain = $1`, domain)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string][]string{}
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			got[k] = append(got[k], v)
		}
		for _, vs := range got {
			sort.Strings(vs)
		}
		return got
	}

	teacher := parseTeacher([]byte(teacherJSON))
	school := parseSchool([]byte(schoolJSON))
	for _, e := range []*entry{teacher, school} {
		if err := upsertEntry(d, e); err != nil {
			t.Fatalf("upsert %s: %v", e.kind, err)
		}
	}
	if got := metaOf(teacher.domain())["designation"]; !reflect.DeepEqual(got, []string{"E-RYT 200", "RYT 500", "YACEP"}) {
		t.Errorf("teacher designation = %v", got)
	}
	if got := metaOf(school.domain())["listing_type"]; !reflect.DeepEqual(got, []string{"school"}) {
		t.Errorf("school listing_type = %v", got)
	}
	var website, category string
	if err := d.QueryRow(`SELECT website, category FROM business_listings WHERE domain = $1`, school.domain()).Scan(&website, &category); err != nil {
		t.Fatal(err)
	}
	if website != "fixture-school.example" || category != "yogaalliance" {
		t.Errorf("school website/category = %q / %q", website, category)
	}

	// Re-run with a changed designation, no languages and no yoga types:
	// designation is replaced, the keys the record no longer carries stay.
	teacher.designation = "RYT 200"
	teacher.languages, teacher.yogaTypes = "", ""
	if err := upsertEntry(d, teacher); err != nil {
		t.Fatal(err)
	}
	got := metaOf(teacher.domain())
	if !reflect.DeepEqual(got["designation"], []string{"RYT 200"}) {
		t.Errorf("designation after rerun = %v", got["designation"])
	}
	if !reflect.DeepEqual(got["languages"], []string{"ENGLISH", "FRANÇAIS"}) || len(got["yoga_types"]) != 2 {
		t.Errorf("empty values blanked meta: languages=%v yoga_types=%v", got["languages"], got["yoga_types"])
	}
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM business_listings WHERE category = 'yogaalliance'`).Scan(&n); err != nil || n != 2 {
		t.Errorf("listings = %d (err %v), want 2", n, err)
	}
}

func TestAddressCountry(t *testing.T) {
	for _, c := range []struct{ addr, want string }{
		{"3 School Ave, Exampleville, CA 90210, USA", "United States"},
		{"12 Rue Exemple, Paris, France", "France"},
		{"7 Example Rd, Ubud, Bali, Indonesia ", "Indonesia"},
		{"5 Example St, Springfield, Narnia", ""}, // unresolvable tail stays empty
		{"3 School Ave, Exampleville", ""},        // a trailing city is never a country
		{"Exampleville", ""},                      // lone part: no guess
		{"Indonesia", ""},                         // lone part, even if it is a country name
		{"", ""},
	} {
		if got := addressCountry(c.addr); got != c.want {
			t.Errorf("addressCountry(%q) = %q, want %q", c.addr, got, c.want)
		}
	}
}

func TestParseSchoolCountry(t *testing.T) {
	if e := parseSchool([]byte(schoolJSON)); e == nil || e.country != "" {
		t.Fatalf("school with city-only tail: want empty country, got %+v", e)
	}
	e := parseSchool([]byte(strings.Replace(schoolJSON, "3 School Ave, Exampleville", "3 School Ave, Exampleville, Indonesia", 1)))
	if e == nil || e.country != "Indonesia" {
		t.Fatalf("want Indonesia, got %+v", e)
	}
}
