// Command prodir crawls two curated professional directories — ACE Fitness
// "Find an ACE Pro" (~76k certified trainers) and the ICF coach finder (~36k
// coaches) — into append-only JSONL state files, exports them to CSV and
// loads them into the existing database.
//
// Why: both are curated, 100%-in-niche directories (fitness / coaching) with
// real names, locations and, for many records, a phone and an own website
// (ICF also publishes an email). Same idea as cmd/yogaalliance.
//
// API (no auth, cookies only):
//
//	ACE  GET  https://www.acefitness.org/api/FindACEProByName/?firstName=_&lastName=_&lowerLimit=N&upperLimit=N+24
//	     GET  https://www.acefitness.org<ProfileURL>                 (profile page, tokenized + parsed at export/load)
//	ICF  GET  https://apps.coachingfederation.org/eweb/OTI/Services/DataRepository.asmx/GetCountriesStatesRegions
//	     POST https://icf-ccf.azurewebsites.net/api/search           (take=1000 per page)
//	     GET  https://apps.coachingfederation.org/eweb/CCFDynamicPage.aspx?webcode=ccfcoachprofileview&coachcstkey=<key>&site=icfapp
//
// State dir (-state, default "data"; the file names and JSON shapes are those
// of the original Python scrapers, so a dir written by either is readable by
// the other):
//
//	ace_list.jsonl  ace_profiles.jsonl  ace_profiles_errors.jsonl
//	icf_list.jsonl  icf_profiles.jsonl  icf_countries.json
//
// DB mapping for `load` (EXISTING tables, plus business_meta which `load`
// creates if missing via db.EnsureBusinessMeta):
//   - business_listings ← one row per person; category='acefitness' |
//     'icfcoaching'; niche_category='fitness' (ACE; 'coaching' for pure Health
//     Coaches) | 'coaching' (ICF); off_niche=false; synthetic unique domain
//     "<id>.pro.acefitness.org" | "<key>.coach.coachingfederation.org";
//     country/country_code only from the curated country whitelist.
//   - emails            ← every email found: the ICF Email value, free text in
//     profiles / descriptions, an email typed into the ACE website field
//   - business_emails   ← link per email, source=category
//   - phones, social_links, website, meta other_websites / street_address ←
//     contact details found in the same text (internal/prodir/extract.go)
//   - business_meta     ← site-native labels as (business_id, meta_key, meta_value)
//     rows, one per value, rewritten per key on every load (a key the record
//     does not carry keeps its old rows). ACE keys: credential (split on ";"
//     and ","), years_ace_certified, years_in_industry, areas_of_focus,
//     programs_and_rates and every profile section column (split on ";").
//     ICF keys: credential, designations, standard_rate and every non-contact
//     profile label under its CSV column name. Returned as data[].meta by
//     GET /api/v2/results.
//
// Proxy: there is no proxy flag; HTTPS_PROXY / NO_PROXY are honoured.
//
// Exit codes: 0 ok, 1 some items failed, 2 circuit breaker tripped, 130 interrupted.
//
// Build & run:
//
//	go build -o prodir ./cmd/prodir
//	./prodir ace list     -state data -workers 4
//	./prodir ace profiles -state data -workers 6 -limit 500
//	./prodir ace export   -state data                       # → data/ace_pros.csv
//	./prodir icf load     -state data -dry-run              # mapping stats, no DB
//	./prodir icf load     -state data                       # → DB (POSTGRES_DSN)
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/prodir"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: prodir <ace|icf> <list|profiles|export|load> [flags]")
	os.Exit(prodir.ExitErrors)
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}
	source, action := os.Args[1], os.Args[2]
	if source != "ace" && source != "icf" {
		usage()
	}

	fs := flag.NewFlagSet(source+" "+action, flag.ExitOnError)
	state := fs.String("state", "data", "state directory")
	var workers, limit *int
	var out, dsn *string
	var dryRun *bool
	switch action {
	case "list":
		workers = fs.Int("workers", 4, "concurrent workers (ICF list pages sequentially and ignores it)")
		limit = fs.Int("limit", 0, "max rows (0 = all)")
	case "profiles":
		workers = fs.Int("workers", 6, "concurrent workers")
		limit = fs.Int("limit", 0, "max profiles to fetch (0 = all)")
	case "export":
		out = fs.String("out", "", "CSV path (default <state>/ace_pros.csv | <state>/icf_coaches.csv)")
	case "load":
		dsn = fs.String("dsn", os.Getenv("POSTGRES_DSN"), "Postgres DSN (default $POSTGRES_DSN)")
		limit = fs.Int("limit", 0, "max records (0 = all)")
		dryRun = fs.Bool("dry-run", false, "no DB: print mapping stats only")
	default:
		usage()
	}
	fs.Parse(os.Args[3:])

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	code, err := run(ctx, source, action, *state, workers, limit, out, dsn, dryRun)
	if err != nil {
		slog.Error("prodir failed", "source", source, "action", action, "error", err)
		if code == prodir.ExitOK {
			code = prodir.ExitErrors
		}
	}
	os.Exit(code)
}

func run(ctx context.Context, source, action, state string, workers, limit *int, out, dsn *string, dryRun *bool) (int, error) {
	switch action {
	case "list":
		if source == "ace" {
			return prodir.ACEList(ctx, state, *workers, *limit)
		}
		return prodir.ICFList(ctx, state)
	case "profiles":
		if source == "ace" {
			return prodir.ACEProfiles(ctx, state, *workers, *limit)
		}
		return prodir.ICFProfiles(ctx, state, *workers, *limit)
	case "export":
		if source == "ace" {
			return prodir.ExitOK, prodir.ACEExport(state, *out)
		}
		return prodir.ExitOK, prodir.ICFExport(state, *out)
	}

	// load
	if !*dryRun && *dsn == "" {
		return prodir.ExitErrors, fmt.Errorf("load requires -dsn or $POSTGRES_DSN (or -dry-run)")
	}
	ls, err := prodir.Listings(source, state)
	if err != nil {
		return prodir.ExitErrors, err
	}
	if *limit > 0 && len(ls) > *limit {
		ls = ls[:*limit]
	}
	if *dryRun {
		prodir.PrintStats(os.Stdout, source, ls)
		return prodir.ExitOK, nil
	}
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		return prodir.ExitErrors, err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return prodir.ExitErrors, fmt.Errorf("ping: %w", err)
	}
	slog.Info("prodir: loading", "source", source, "records", len(ls), "state", filepath.Clean(state))
	return prodir.Load(ctx, db, source, ls), nil
}
