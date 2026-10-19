package prodir

// Body Control Pilates "Register of Teachers",
// https://www.bodycontrolpilates.com/find-your-local-teacher/the-register-of-body-control-pilates-teachers/
// (~2,100-2,300 teachers, individuals; robots.txt asks for a 10 s Crawl-delay,
// so MinInterval is 10 s).
//
// The finder plugin POSTs to wp-content/plugins/bcp-teacher-finder/ajax/
// bcp-tf-results.php. With `searchtype=name&name=<substring>` it answers with
// ONE HTML fragment (1.7-1.9 MB, no pagination) holding every teacher whose
// name contains the substring, so the list sweeps the letters a-z and dedupes
// on the teacher id (div.tf-single id="tf-<id>"): ~26 requests, no detail
// pages. A letter that matches nobody answers "Sorry, no results found". A
// teacher whose name has none of a-z is not reachable this way (none known).
//
// Per record (all from the fragment): name (first h3), address lines (p with
// <br/> lines: venue / street / area / city / postcode / country; the last
// line is the country as an ISO-3 code, or a name for a few), phones (the
// plain text after the address, ~90%), email (mailto link, ~99%), website
// (links with target="_bank" [sic], ~55%; the directory's own links are
// dropped, a social link becomes Social) and the qualification list
// (div.col-qual, one entry per <hr/>) as Meta credential. There is no
// business name, bio, language or social data.
//
// Country: the last address line; ISO-3 codes are mapped to ISO-2 here (the
// shared resolver knows ISO-2 and names only); an unknown code stays empty.
// City and postcode are the two lines before it when the second-last line
// looks like a postcode; otherwise every line before the country is Address.

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

const bodycontrolURL = "https://www.bodycontrolpilates.com/wp-content/plugins/bcp-teacher-finder/ajax/bcp-tf-results.php"

func init() {
	register(&Source{
		Slug: "bodycontrol", Name: "Body Control Pilates register of teachers", Host: "bodycontrolpilates.com",
		Category: "pilates_instructor", ListingType: "individual", MinInterval: 10 * time.Second,
		List: bodycontrolList,
	})
}

func bodycontrolList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
		"Accept":       "text/html, */*; q=0.01",
	}
	seen := map[string]bool{}
	for l := 'a'; l <= 'z'; l++ {
		body := "searchtype=name&name=" + url.QueryEscape(string(l))
		p, err := f.Post(ctx, bodycontrolURL, hdr, []byte(body))
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		doc, err := docFromBody(p.Body)
		if err != nil {
			return err
		}
		var recs []Record
		doc.Find("div.tf-single").Each(func(_ int, card *goquery.Selection) {
			if r, ok := bodycontrolRecord(card); ok && !seen[r.ID] {
				seen[r.ID] = true
				recs = append(recs, r)
			}
		})
		for _, r := range recs {
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	bodycontrolIDRe   = regexp.MustCompile(`^tf-([0-9]+)$`)
	bodycontrolZipRe  = regexp.MustCompile(`^[A-Za-z]{1,2}[0-9][A-Za-z0-9]?\s*[0-9][A-Za-z]{2}$`)
	bodycontrolCodeRe = regexp.MustCompile(`^[A-Z]{3}$`)
)

// bodycontrolRecord parses one div.tf-single card.
func bodycontrolRecord(card *goquery.Selection) (Record, bool) {
	m := bodycontrolIDRe.FindStringSubmatch(card.AttrOr("id", ""))
	col := card.Find("div.col").First()
	name := clean(col.Find("h3").First().Text())
	if m == nil || name == "" { // not a teacher card
		return Record{}, false
	}
	r := Record{ID: m[1], Name: name, Social: map[string]string{}, Meta: map[string][]string{}}

	// address lines: the <br/>-separated text of the first <p>
	var lines []string
	cur := ""
	flush := func() {
		if c := clean(cur); c != "" {
			lines = append(lines, c)
		}
		cur = ""
	}
	col.Find("p").First().Contents().Each(func(_ int, n *goquery.Selection) {
		if goquery.NodeName(n) == "br" {
			flush()
		} else {
			cur += " " + n.Text()
		}
	})
	flush()
	if k := len(lines); k > 0 {
		r.Country = bodycontrolCountry(lines[k-1])
		lines = lines[:k-1]
	}
	if k := len(lines); k >= 2 && bodycontrolPostcode(lines[k-1]) {
		r.PostalCode = lines[k-1]
		r.City = lines[k-2]
		lines = lines[:k-2]
	}
	r.Address = strings.Join(lines, ", ")

	// phones: the bare text nodes of the column (after the address, between the
	// links), one per <br/>-separated line
	col.Contents().Each(func(_ int, n *goquery.Selection) {
		if len(n.Nodes) == 1 && n.Nodes[0].Type == html.TextNode {
			if ph := clean(n.Text()); ph != "" {
				r.Phones = append(r.Phones, ph)
			}
		}
	})

	r.Emails = emailsIn(col)
	col.Find("a[href]").Each(func(_ int, a *goquery.Selection) {
		href := clean(a.AttrOr("href", ""))
		low := strings.ToLower(href)
		if !strings.HasPrefix(low, "http://") && !strings.HasPrefix(low, "https://") {
			return // mailto: and the like
		}
		switch h := urlHost(href); {
		case isOwnDomain(h):
			// the directory's own site / its London teacher-profiles page
		case socialPlatform(href) != "":
			if p := socialPlatform(href); r.Social[p] == "" {
				r.Social[p] = href
			}
		default:
			r.Websites = addString(r.Websites, href)
		}
	})

	// qualifications: one entry per <hr/> in both div.col-qual blocks
	card.Find("div.col-qual").Each(func(_ int, q *goquery.Selection) {
		q.Contents().Each(func(_ int, n *goquery.Selection) {
			if len(n.Nodes) == 1 && n.Nodes[0].Type == html.TextNode {
				addMeta(r.Meta, "credential", n.Text())
			}
		})
	})
	return r, true
}

// bodycontrolPostcode: a UK postcode, or a short postal code containing a digit
// (foreign addresses: "LT-01103", "75001").
func bodycontrolPostcode(s string) bool {
	if bodycontrolZipRe.MatchString(s) {
		return true
	}
	return len(s) <= 10 && strings.ContainsAny(s, "0123456789")
}

// bodycontrolCountry turns the register's country line into something the
// shared resolver understands: ISO-3 codes become ISO-2, names pass through.
func bodycontrolCountry(s string) string {
	s = clean(s)
	if bodycontrolCodeRe.MatchString(s) {
		return bodycontrolISO3[s] // "" for an unknown code: never guessed
	}
	return s
}

// bodycontrolISO3 maps the ISO-3 codes the register uses (and the usual
// neighbours) to ISO-2. "UAE" is the register's own spelling for the Emirates.
var bodycontrolISO3 = map[string]string{
	"GBR": "GB", "IRL": "IE", "FRA": "FR", "DEU": "DE", "ESP": "ES", "PRT": "PT", "ITA": "IT", "NLD": "NL",
	"BEL": "BE", "LUX": "LU", "CHE": "CH", "AUT": "AT", "DNK": "DK", "NOR": "NO", "SWE": "SE", "FIN": "FI",
	"ISL": "IS", "POL": "PL", "CZE": "CZ", "SVK": "SK", "HUN": "HU", "ROU": "RO", "BGR": "BG", "GRC": "GR",
	"CYP": "CY", "MLT": "MT", "EST": "EE", "LVA": "LV", "LTU": "LT", "HRV": "HR", "SVN": "SI", "SRB": "RS",
	"TUR": "TR", "UKR": "UA", "RUS": "RU", "USA": "US", "CAN": "CA", "MEX": "MX", "BRA": "BR", "ARG": "AR",
	"CHL": "CL", "COL": "CO", "PER": "PE", "BMU": "BM", "BRB": "BB", "BHS": "BS", "JAM": "JM", "CYM": "KY",
	"AUS": "AU", "NZL": "NZ", "JPN": "JP", "CHN": "CN", "HKG": "HK", "SGP": "SG", "MYS": "MY", "THA": "TH",
	"IDN": "ID", "PHL": "PH", "VNM": "VN", "KOR": "KR", "IND": "IN", "PAK": "PK", "LKA": "LK",
	"ARE": "AE", "UAE": "AE", "QAT": "QA", "SAU": "SA", "OMN": "OM", "KWT": "KW", "BHR": "BH", "ISR": "IL",
	"JOR": "JO", "LBN": "LB", "EGY": "EG", "MAR": "MA", "ZAF": "ZA", "KEN": "KE", "MUS": "MU", "NGA": "NG",
	"GHA": "GH", "TZA": "TZ", "UGA": "UG", "ZWE": "ZW", "BWA": "BW", "NAM": "NA", "SYC": "SC", "GIB": "GI",
	"JEY": "JE", "GGY": "GG", "IMN": "IM",
}
