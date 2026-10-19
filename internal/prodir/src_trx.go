package prodir

// TRX trainer directory, https://digital.trxtraining.com/directory (individuals,
// >10,000 trainers; the full size is unknown).
//
// The Next.js directory reads an unauthenticated JSON endpoint,
// /api/directory/trainer?type=trainers&size=N&page=P (page is 1-based), whose
// window is capped at 10,000 rows (page*size > 10000 is an HTTP 500, and
// totalElements saturates at 10000). List therefore walks the directory in
// partitions, deduping by id: (1) the unfiltered top 10,000 by rank, (2) one
// query per ISO-2 countryCode, (3) one query per two-digit postalCode prefix
// 00..99 across all countries (this splits the US, which alone exceeds the
// window, and also catches rows without a countryCode, ~45% of the data).
// Known gaps: a country other than the US above 10,000 rows, and rows with
// neither countryCode nor a numeric postal code that rank outside the top
// 10,000. About 320 requests (~6 minutes at 1s) plus one per extra 1,000 rows.
//
// The list items carry everything (there is no detail stage): website, postal
// code, a free-text address, the city names of the trainer's locations,
// services, languages, credentials and social handles. There is no email; a
// Whatsapp handle that is a number is taken as a phone. The facilities
// listing (type=facilities) is not crawled: it needs a per-record listing type.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	trxAPI    = "https://digital.trxtraining.com/api/directory/trainer"
	trxSize   = 1000
	trxWindow = 10000
)

func init() {
	register(&Source{
		Slug: "trx", Name: "TRX trainer directory", Host: "digital.trxtraining.com",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: trxList,
	})
}

type trxSocial struct {
	Type   string `json:"type"`
	Handle string `json:"handle"`
}

type trxTrainer struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Website     string      `json:"website"`
	PostalCode  string      `json:"postalCode"`
	CountryCode string      `json:"countryCode"`
	Address     string      `json:"address"`
	Locations   []string    `json:"locations"`
	Services    []string    `json:"services"`
	Languages   []string    `json:"languages"`
	Credentials []string    `json:"credentials"`
	Socials     []trxSocial `json:"socials"`
	Rank        string      `json:"rank"`
}

// trxCountries are the ISO-2 codes queried as countryCode partitions (the API
// ignores a code it does not know, so an unused one costs one request).
var trxCountries = strings.Fields(`AD AE AF AG AI AL AM AO AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BM BN BO BR BS BT BW BY BZ
CA CD CG CH CI CK CL CM CN CO CR CU CV CW CY CZ DE DJ DK DM DO DZ EC EE EG ER ES ET FI FJ FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GT GU GY
HK HN HR HT HU ID IE IL IM IN IQ IR IS IT JE JM JO JP KE KG KH KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MG MK ML MM MN MO MQ MR MT MU MV MW MX MY MZ
NA NC NE NG NI NL NO NP NZ OM PA PE PF PG PH PK PL PR PS PT PY QA RE RO RS RU RW SA SB SC SD SE SG SI SK SL SM SN SO SR SV SY SZ TC TG TH TJ TM TN TO TR TT TW TZ
UA UG US UY UZ VA VC VE VG VI VN VU WS XK YE YT ZA ZM ZW`)

// trxFilters lists the query-string filters of every partition, in order.
func trxFilters() []string {
	out := []string{""}
	for _, c := range trxCountries {
		out = append(out, "&countryCode="+c)
	}
	for i := 0; i < 100; i++ {
		out = append(out, fmt.Sprintf("&postalCode=%02d", i))
	}
	return out
}

func trxList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[string]bool{}
	for _, filter := range trxFilters() {
		for page := 1; page*trxSize <= trxWindow; page++ {
			p, err := f.Get(ctx, fmt.Sprintf("%s?type=trainers&size=%d&page=%d%s", trxAPI, trxSize, page, filter), nil)
			if err != nil {
				return err
			}
			if err := p.Err(); err != nil {
				return err
			}
			var resp struct {
				Items []trxTrainer `json:"items"`
			}
			if err := json.Unmarshal(p.Body, &resp); err != nil {
				return fmt.Errorf("page %d%s: %w", page, filter, err)
			}
			for _, t := range resp.Items {
				// rows without an id or a name are not real entries
				if t.ID == "" || clean(t.Name) == "" || seen[t.ID] {
					continue
				}
				seen[t.ID] = true
				if err := emit(t.record()); err != nil {
					return err
				}
			}
			if len(resp.Items) < trxSize {
				break
			}
		}
	}
	return nil
}

var trxHandleRe = regexp.MustCompile(`^[A-Za-z0-9._]{2,40}$`)

// trxSocialPrefix builds a profile URL from a bare handle for the platforms
// whose handle alone names a profile.
var trxSocialPrefix = map[string]string{
	"instagram": "https://www.instagram.com/", "tiktok": "https://www.tiktok.com/@",
	"x": "https://x.com/", "facebook": "https://www.facebook.com/",
}

func (t trxTrainer) record() Record {
	r := Record{
		ID: t.ID, Name: t.Name, Address: t.Address, PostalCode: t.PostalCode, Country: t.CountryCode,
		Bio: t.Description, Social: map[string]string{}, Meta: map[string][]string{},
	}
	if len(t.Locations) > 0 {
		r.City = t.Locations[0]
	}
	if w := strings.TrimSpace(t.Website); w != "" {
		r.Websites = []string{w}
	}
	for _, s := range t.Socials {
		h := strings.TrimSpace(s.Handle)
		typ := strings.ToLower(s.Type)
		switch low := strings.ToLower(h); {
		case typ == "whatsapp":
			if len(digitsOf(h)) >= 8 && strings.Trim(h, "+0123456789 ()-.") == "" {
				r.Phones = append(r.Phones, h)
			}
		case strings.HasPrefix(low, "http://") || strings.HasPrefix(low, "https://") || strings.HasPrefix(low, "www."):
			if p := socialPlatform(h); p != "" && r.Social[p] == "" {
				r.Social[p] = h
			}
		default:
			h = strings.TrimPrefix(h, "@")
			if pre := trxSocialPrefix[typ]; pre != "" && trxHandleRe.MatchString(h) {
				if p := socialPlatform(pre); r.Social[p] == "" {
					r.Social[p] = pre + h
				}
			}
		}
	}
	addMeta(r.Meta, "credential", t.Credentials...)
	addMeta(r.Meta, "services", t.Services...)
	addMeta(r.Meta, "languages", t.Languages...)
	addMeta(r.Meta, "locations", t.Locations...)
	addMeta(r.Meta, "rank", t.Rank)
	return r
}
