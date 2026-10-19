// Command prodir crawls curated professional directories — ACE Fitness
// "Find an ACE Pro" (~76k certified trainers), the ICF coach finder (~36k
// coaches) and any number of generic sources (yoga / Pilates / fitness /
// CrossFit / swimming / sports coach finders) — into append-only JSONL state
// files, exports them to CSV and loads them into the existing database.
//
// Why: they are curated, 100%-in-niche directories with real names, locations
// and, for many records, a phone, an email and an own website. Same idea as
// cmd/yogaalliance.
//
// Generic sources: ace and icf are bespoke modules; every other directory is
// one small file in internal/prodir (src_<slug>.go) that registers a Source —
// a List func and an optional Detail func — and gets list / profiles / export /
// load, rate limiting, retries, resumable state and the circuit breaker for
// free. `prodir sources` prints what is registered (slug, category, niche,
// listing_type, host, name). `prodir <slug> <list|profiles|export|load>` takes
// the same flags as ace / icf. How to add one: internal/prodir/source.go.
//
// API (no auth, cookies only):
//
//	ACE  GET  https://www.acefitness.org/api/FindACEProByName/?firstName=_&lastName=_&lowerLimit=N&upperLimit=N+24
//	     GET  https://www.acefitness.org<ProfileURL>                 (profile page, tokenized + parsed at export/load)
//	ICF  GET  https://apps.coachingfederation.org/eweb/OTI/Services/DataRepository.asmx/GetCountriesStatesRegions
//	     POST https://icf-ccf.azurewebsites.net/api/search           (take=1000 per page)
//	     GET  https://apps.coachingfederation.org/eweb/CCFDynamicPage.aspx?webcode=ccfcoachprofileview&coachcstkey=<key>&site=icfapp
//
// State dir (-state, default "data"; the ace / icf file names and JSON shapes
// are those of the original Python scrapers, so a dir written by either is
// readable by the other):
//
//	ace_list.jsonl  ace_profiles.jsonl  ace_profiles_errors.jsonl
//	icf_list.jsonl  icf_profiles.jsonl  icf_countries.json
//
// and, per generic source, one Record (internal/prodir/source.go) per line:
//
//	<slug>_list.jsonl             from `list`; re-running may append duplicates,
//	                              readers dedupe by id, last wins
//	<slug>_profiles.jsonl         from `profiles` (list record merged with the
//	                              detail page); resumable, a 404 is stored as
//	                              missing=true and never refetched
//	<slug>_profiles_errors.jsonl  {"id","url","error"}
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
// Generic sources map the same way, with: category = the record's override or
// the source's Category; niche_category from the one category -> niche map
// below (an empty niche is stored as NULL); synthetic unique domain
// "<id>.<slug>.<host>" (an id that is not a DNS label is replaced by the first
// 16 hex chars of its sha1); website = the first structured own site, else the
// profile URL; emails / phones / social = structured values plus whatever
// extract.go finds in the bio; country only from the curated whitelist; meta =
// the record's labels plus source_directory, listing_type and business_name.
//
//	pilates_instructor                        pilates
//	yoga_teacher, hot_yoga_teacher            yoga
//	fitness_trainer, crossfit_trainer,
//	swimming_coach, sports_martial_arts       fitness
//	sports_tennis, sports_golf, sports_pickleball, sports_running,
//	sports_triathlon, sports_cycling,
//	sports_equestrian                         NULL
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
//	./prodir sources                                        # registered generic sources
//	./prodir iyengaruk list     -state data
//	./prodir iyengaruk profiles -state data -workers 2 -limit 50
//	./prodir iyengaruk load     -state data -dry-run
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
	fmt.Fprintln(os.Stderr, "usage: prodir <ace|icf|slug> <list|profiles|export|load> [flags]\n       prodir sources")
	os.Exit(prodir.ExitErrors)
}

// printSources lists the registered generic sources.
func printSources() {
	fmt.Printf("%-22s %-20s %-8s %-12s %-26s %s\n", "slug", "category", "niche", "listing_type", "host", "name")
	for _, s := range prodir.Sources() {
		niche := prodir.Niche(s.Category)
		if niche == "" {
			niche = "(null)"
		}
		fmt.Printf("%-22s %-20s %-8s %-12s %-26s %s\n", s.Slug, s.Category, niche, s.ListingType, s.Host, s.Name)
	}
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "sources" {
		printSources()
		return
	}
	if len(os.Args) < 3 {
		usage()
	}
	source, action := os.Args[1], os.Args[2]
	generic, isGeneric := prodir.LookupSource(source)
	if source != "ace" && source != "icf" && !isGeneric {
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
		out = fs.String("out", "", "CSV path (default <state>/ace_pros.csv | <state>/icf_coaches.csv | <state>/<slug>.csv)")
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

	code, err := run(ctx, source, generic, action, *state, workers, limit, out, dsn, dryRun)
	if err != nil {
		slog.Error("prodir failed", "source", source, "action", action, "error", err)
		if code == prodir.ExitOK {
			code = prodir.ExitErrors
		}
	}
	os.Exit(code)
}

// run executes one action; generic is the registered source of a generic slug
// (nil for ace / icf).
func run(ctx context.Context, source string, generic *prodir.Source, action, state string, workers, limit *int, out, dsn *string, dryRun *bool) (int, error) {
	switch action {
	case "list":
		switch {
		case generic != nil:
			return prodir.SourceList(ctx, state, generic, *limit)
		case source == "ace":
			return prodir.ACEList(ctx, state, *workers, *limit)
		}
		return prodir.ICFList(ctx, state)
	case "profiles":
		switch {
		case generic != nil:
			return prodir.SourceProfiles(ctx, state, generic, *workers, *limit)
		case source == "ace":
			return prodir.ACEProfiles(ctx, state, *workers, *limit)
		}
		return prodir.ICFProfiles(ctx, state, *workers, *limit)
	case "export":
		switch {
		case generic != nil:
			return prodir.ExitOK, prodir.SourceExport(state, generic, *out)
		case source == "ace":
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
