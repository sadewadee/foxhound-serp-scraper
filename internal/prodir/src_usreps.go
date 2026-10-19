package prodir

// USREPS, the US Registry of Exercise Professionals, https://usreps.org/registry/
// (ACE, ACSM, CSCCA, NCCPT, NCSF and NSCA credential holders; ~165-200k
// credential rows, individuals).
//
// The registry is an HTML form: POST /registry/ (urlencoded, no cookie or token
// needed; page=1 must always be sent) returns 12 rows per page and a
// "Showing a - b of N result(s)" line. Every query is capped at 500 results and
// the cap is reported as N=500, not as an error, so the list is partitioned
// adaptively: organizationName x stateProvince, and while a query still hits
// the cap its lastName (a prefix match) is deepened one character at a time.
// The first page of each query doubles as its count, so there are no extra
// probe requests. ~20k requests in a full crawl.
//
// ACSM's "ProFinder" has no tool of its own: it links to this registry
// filtered by organizationName=ACSM, so those rows are already in this source
// and there is no separate acsm source.
//
// There is no detail page, and the rows carry NO email, phone or website: only
// name, credential, city, state, country and postal code. Records are name +
// location seeds, to be enriched later by name. A person holding two
// credentials is two records (the ID is organisation-credential-number).
//
// Known gaps: rows without a state are not reachable by the state partition;
// last names whose prefix continues with a character outside a-z, ', - and
// space (at depth >= 1) inside a still-capped node are not reached.

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	usrepsURL = "https://usreps.org/registry/"
	// usrepsCap is the result count at which the registry truncates a query.
	usrepsCap = 500
	// usrepsPageSize is the fixed number of rows per page.
	usrepsPageSize = 12
)

var (
	usrepsOrgs     = []string{"ACE", "ACSM", "CSCCA", "NCCPT", "NCSF", "NSCA"}
	usrepsTotalRe  = regexp.MustCompile(`(\d+)\s+result\(s\)`)
	usrepsNonIDRe  = regexp.MustCompile(`[^a-z0-9]+`)
	usrepsChildren = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
	// usrepsDeepChildren also covers names like "O'Brien", "Smith-Jones", "De La Cruz".
	usrepsDeepChildren = append(append([]string{}, usrepsChildren...), "'", "-", " ")
)

func init() {
	register(&Source{
		Slug: "usreps", Name: "USREPS exercise professionals registry", Host: "usreps.org",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: usrepsList,
	})
}

func usrepsList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	states, err := usrepsStates(ctx, f)
	if err != nil {
		return err
	}
	seen := map[string]bool{} // partitions are disjoint; a repeated row is a registry quirk
	emitOnce := func(r Record) error {
		if seen[r.ID] {
			return nil
		}
		seen[r.ID] = true
		return emit(r)
	}
	for _, org := range usrepsOrgs {
		for _, st := range states {
			if err := usrepsWalk(ctx, f, org, st, "", emitOnce); err != nil {
				return err
			}
		}
	}
	return nil
}

// usrepsStates reads the stateProvince options of the search form.
func usrepsStates(ctx context.Context, f Fetcher) ([]string, error) {
	doc, _, err := getDoc(ctx, f, usrepsURL)
	if err != nil {
		return nil, err
	}
	var out []string
	doc.Find(`select[name="stateProvince"] option`).Each(func(_ int, o *goquery.Selection) {
		if v := strings.TrimSpace(o.AttrOr("value", "")); v != "" {
			out = append(out, v)
		}
	})
	if len(out) == 0 {
		return nil, fmt.Errorf("no state options on %s (layout changed?)", usrepsURL)
	}
	return out, nil
}

// usrepsWalk lists one (org, state, lastName prefix) partition: when its first
// page reports the cap it recurses into the next prefix level, otherwise it
// emits that page and the rest.
func usrepsWalk(ctx context.Context, f Fetcher, org, state, prefix string, emit func(Record) error) error {
	doc, err := usrepsQuery(ctx, f, org, state, prefix, 1)
	if err != nil {
		return err
	}
	total := usrepsTotal(doc)
	if total >= usrepsCap {
		kids := usrepsChildren
		if prefix != "" {
			kids = usrepsDeepChildren
		}
		for _, c := range kids {
			if err := usrepsWalk(ctx, f, org, state, prefix+c, emit); err != nil {
				return err
			}
		}
		return nil
	}
	pages := (total + usrepsPageSize - 1) / usrepsPageSize
	for page := 1; page <= pages; page++ {
		if page > 1 {
			if doc, err = usrepsQuery(ctx, f, org, state, prefix, page); err != nil {
				return err
			}
		}
		for _, r := range usrepsRows(doc) {
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func usrepsQuery(ctx context.Context, f Fetcher, org, state, prefix string, page int) (*goquery.Document, error) {
	form := url.Values{
		"organizationName": {org}, "stateProvince": {state}, "lastName": {prefix},
		"page": {strconv.Itoa(page)}, "theSubmitter": {"Search"},
	}
	p, err := f.Post(ctx, usrepsURL, map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
	if err != nil {
		return nil, err
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	return docFromBody(p.Body)
}

// usrepsTotal is N of the "Showing a - b of N result(s)" line (0 when absent,
// i.e. no results).
func usrepsTotal(doc *goquery.Document) int {
	m := usrepsTotalRe.FindStringSubmatch(doc.Find(".display-controller").First().Text())
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// usrepsRows parses the result rows of one page.
func usrepsRows(doc *goquery.Document) []Record {
	var out []Record
	doc.Find("div.table.cards.search-out > div.row").Each(func(_ int, row *goquery.Selection) {
		if row.HasClass("header") {
			return
		}
		cell := map[string]string{}
		row.Find("div.cell[data-title]").Each(func(_ int, c *goquery.Selection) {
			cell[c.AttrOr("data-title", "")] = clean(c.Text())
		})
		org, cred, num := cell["Organization"], cell["Credential"], cell["Credential Number"]
		name := usrepsName(row.Find("div.cell").First().Text())
		if org == "" || num == "" || name == "" { // header-like or empty row: not an entry
			return
		}
		r := Record{
			ID:   strings.Trim(usrepsNonIDRe.ReplaceAllString(strings.ToLower(org+"-"+cred+"-"+num), "-"), "-"),
			Name: name, City: cell["City"], Region: cell["State / Province"],
			PostalCode: cell["Postal Code"], Country: firstNonEmpty(cell["Country"], "United States"),
			Meta: map[string][]string{},
		}
		addMeta(r.Meta, "credential", strings.TrimSpace(org+" "+cred))
		addMeta(r.Meta, "credential_number", num)
		addMeta(r.Meta, "expires", cell["Credential Expiration Date"])
		addMeta(r.Meta, "portable", cell["Portable"])
		out = append(out, r)
	})
	return out
}

// usrepsName turns the registry's "Last, First" into "First Last".
func usrepsName(s string) string {
	s = clean(s)
	if last, first, ok := strings.Cut(s, ","); ok {
		return clean(first + " " + last)
	}
	return s
}
