package api

import (
	"net/url"
	"strings"
	"testing"
)

func TestBuildResultsFilter_NoParams(t *testing.T) {
	q := url.Values{}
	where, args, idx := buildResultsFilter(q)
	if where != "WHERE 1=1" {
		t.Fatalf("expected baseline WHERE 1=1, got %q", where)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %d", len(args))
	}
	if idx != 1 {
		t.Fatalf("expected argIdx=1, got %d", idx)
	}
}

func TestBuildResultsFilter_Country(t *testing.T) {
	q := url.Values{"country": {"id"}}
	where, args, _ := buildResultsFilter(q)
	if !strings.Contains(where, "UPPER(bl.country) = UPPER($1)") {
		t.Fatalf("country clause missing or wrong, got %q", where)
	}
	if len(args) != 1 || args[0] != "id" {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestBuildResultsFilter_City(t *testing.T) {
	q := url.Values{"city": {"Berlin"}}
	where, args, _ := buildResultsFilter(q)
	if !strings.Contains(where, "bl.city ILIKE $1") {
		t.Fatalf("city clause missing, got %q", where)
	}
	if args[0] != "Berlin%" {
		t.Fatalf("city must be prefix-wildcarded, got %v", args[0])
	}
}

func TestBuildResultsFilter_HasPhone(t *testing.T) {
	q := url.Values{"has_phone": {"true"}}
	where, args, _ := buildResultsFilter(q)
	if !strings.Contains(where, "bl.phone IS NOT NULL AND bl.phone <> ''") {
		t.Fatalf("has_phone clause missing, got %q", where)
	}
	if len(args) != 0 {
		t.Fatalf("has_phone should not bind args, got %d", len(args))
	}
}

func TestBuildResultsFilter_HasPhoneFalseIgnored(t *testing.T) {
	q := url.Values{"has_phone": {"false"}}
	where, _, _ := buildResultsFilter(q)
	if strings.Contains(where, "phone") {
		t.Fatalf("has_phone=false must not add a clause, got %q", where)
	}
}

func TestBuildResultsFilter_HasSocial(t *testing.T) {
	q := url.Values{"has_social": {"true"}}
	where, _, _ := buildResultsFilter(q)
	if !strings.Contains(where, "bl.social_links IS NOT NULL") {
		t.Fatalf("has_social clause missing, got %q", where)
	}
	if !strings.Contains(where, "NOT IN ('{}', '')") {
		t.Fatalf("has_social must exclude empty jsonb objects, got %q", where)
	}
}

func TestBuildResultsFilter_Combined(t *testing.T) {
	q := url.Values{
		"country":    {"DE"},
		"city":       {"Berlin"},
		"has_phone":  {"true"},
		"has_social": {"true"},
	}
	where, args, idx := buildResultsFilter(q)
	// 2 bound args (country + city); has_phone + has_social bind no args.
	if len(args) != 2 {
		t.Fatalf("expected 2 args (country + city), got %d: %v", len(args), args)
	}
	if idx != 3 {
		t.Fatalf("expected argIdx=3 after 2 binds, got %d", idx)
	}
	for _, frag := range []string{"UPPER(bl.country)", "bl.city ILIKE", "bl.phone IS NOT NULL", "bl.social_links"} {
		if !strings.Contains(where, frag) {
			t.Fatalf("combined clause missing %q in %q", frag, where)
		}
	}
}

func TestResultsOrderBy_Whitelist(t *testing.T) {
	cases := map[string]string{
		"":              "ORDER BY bl.id DESC",
		"id_desc":       "ORDER BY bl.id DESC",
		"id_asc":        "ORDER BY bl.id ASC",
		"updated_desc":  "ORDER BY bl.updated_at DESC, bl.id DESC",
		"updated_asc":   "ORDER BY bl.updated_at ASC, bl.id ASC",
		"created_desc":  "ORDER BY bl.created_at DESC, bl.id DESC",
		"created_asc":   "ORDER BY bl.created_at ASC, bl.id ASC",
		"DROP TABLE bl": "ORDER BY bl.id DESC", // unknown -> default
		"random()":      "ORDER BY bl.id DESC",
	}
	for sort, want := range cases {
		got := resultsOrderBy(sort)
		if got != want {
			t.Errorf("resultsOrderBy(%q) = %q, want %q", sort, got, want)
		}
	}
}
