package prodir

// NPCP, National Pilates Certification Program certified teachers,
// https://www.nationalpilatescertificationprogram.org/NPCP/Directory/CertifiedTeachersList.aspx
// (~4,200 teachers, individuals).
//
// The list is an iMIS / Telerik RadGrid driven by ASP.NET postbacks and needs
// the session cookie (the shared client keeps one jar). The walk: GET the page,
// POST the page size 50 (the grid command PageSize;50), then POST page 2, 3 ...
// with every hidden form field re-read from the PREVIOUS response (ViewState
// changes each time) and the pager link / "next" button of that response as the
// event target. ~85 pages of 50; the footer says "N items in M pages". Rows
// carry name, city, state, zip, country, certification type and ids; the ID is
// the iMIS contact id inside the name link (a JS popup URL).
//
// The detail page (Certified-Teachers.aspx?ID=N, a plain GET) adds what the
// teacher opted in to show: Email and Contact Number (plain text spans), Website,
// Company, bio, modalities. Many teachers show none of them. Region and postal
// code come from the list (no street address anywhere).

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	npcpHost    = "www.nationalpilatescertificationprogram.org"
	npcpListURL = "https://" + npcpHost + "/NPCP/Directory/CertifiedTeachersList.aspx"
	npcpDetail  = "https://" + npcpHost + "/NPCP/NPCP/Directory/Certified-Teachers.aspx?ID="
	npcpGrid    = "ctl01$TemplateBody$WebPartManager1$gwpciNewQueryMenuCommon$ciNewQueryMenuCommon$ResultsGrid$Grid1"
)

var (
	npcpIDRe     = regexp.MustCompile(`[?&]ID=(\d+)`)
	npcpPostback = regexp.MustCompile(`__doPostBack\('([^']+)'`)
)

func init() {
	register(&Source{
		Slug: "npcp", Name: "National Pilates Certification Program certified teachers", Host: npcpHost,
		Category: "pilates_instructor", ListingType: "individual", MinInterval: time.Second,
		List: npcpList, Detail: npcpDetail1,
	})
}

// npcpPost submits the form of doc to the list URL: all its hidden fields plus
// the event target / argument and optional extra fields.
func npcpPost(ctx context.Context, f Fetcher, doc *goquery.Document, target, arg string, extra map[string]string) (*goquery.Document, error) {
	v := url.Values{}
	doc.Find("input[type=hidden][name]").Each(func(_ int, in *goquery.Selection) {
		v.Set(in.AttrOr("name", ""), in.AttrOr("value", ""))
	})
	v.Set("__EVENTTARGET", target)
	v.Set("__EVENTARGUMENT", arg)
	for k, x := range extra {
		v.Set(k, x)
	}
	p, err := f.Post(ctx, npcpListURL, map[string]string{
		"Content-Type": "application/x-www-form-urlencoded", "Referer": npcpListURL,
	}, []byte(v.Encode()))
	if err != nil {
		return nil, err
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	return docFromBody(p.Body)
}

// npcpPager reads "N items in M pages" and the current page number.
func npcpPager(doc *goquery.Document) (pages, current int) {
	st := doc.Find(".rgInfoPart strong")
	if st.Length() >= 2 {
		pages, _ = strconv.Atoi(clean(st.Eq(1).Text()))
	}
	current, _ = strconv.Atoi(clean(doc.Find("a.rgCurrentPage span").First().Text()))
	return pages, current
}

func npcpList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, _, err := getDoc(ctx, f, npcpListURL)
	if err != nil {
		return err
	}
	if doc.Find("input[name=__VIEWSTATE]").Length() == 0 {
		return fmt.Errorf("no ViewState on %s (layout changed?)", npcpListURL)
	}
	if doc, err = npcpPost(ctx, f, doc, npcpGrid, "FireCommand:"+npcpGrid+"$ctl00;PageSize;50", nil); err != nil {
		return err
	}
	seen := map[string]bool{}
	for page := 1; ; page++ {
		pages, cur := npcpPager(doc)
		if cur != page {
			return fmt.Errorf("asked for page %d, the grid shows page %d", page, cur)
		}
		recs := npcpRows(doc)
		if len(recs) == 0 {
			return fmt.Errorf("page %d has no rows (layout changed?)", page)
		}
		for _, r := range recs {
			if seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			if err := emit(r); err != nil {
				return err
			}
		}
		if page >= pages {
			return nil
		}
		// The numbered pager only shows ~10 links: use the link to the next page
		// when it is there, else the "next page" button.
		var target string
		extra := map[string]string{}
		if a := doc.Find(fmt.Sprintf(`a[title="Go to Page %d"]`, page+1)).First(); a.Length() > 0 {
			if m := npcpPostback.FindStringSubmatch(a.AttrOr("href", "")); m != nil {
				target = m[1]
			}
		}
		if target == "" {
			btn := doc.Find("input.rgPageNext").First()
			if btn.Length() == 0 {
				return fmt.Errorf("no way to page %d on page %d", page+1, page)
			}
			extra[btn.AttrOr("name", "")] = btn.AttrOr("value", " ")
		}
		if doc, err = npcpPost(ctx, f, doc, target, "", extra); err != nil {
			return err
		}
	}
}

// npcpRows parses the grid rows: Name (link), City, State, Zip, Country,
// Cert Type, Teacher Certification ID, Certified Since, Certification Expiration.
func npcpRows(doc *goquery.Document) []Record {
	var out []Record
	doc.Find("tr.rgRow, tr.rgAltRow").Each(func(_ int, tr *goquery.Selection) {
		tds := tr.Find("td")
		if tds.Length() < 9 {
			return
		}
		cell := func(i int) string { return clean(tds.Eq(i).Text()) }
		m := npcpIDRe.FindStringSubmatch(tds.Eq(0).Find("a").AttrOr("href", ""))
		name := cell(0)
		if m == nil || name == "" { // a row without a teacher link is not an entry
			return
		}
		r := Record{
			ID: m[1], URL: npcpDetail + m[1], Name: name,
			City: cell(1), Region: cell(2), PostalCode: cell(3), Country: cell(4),
			Meta: map[string][]string{},
		}
		addMeta(r.Meta, "certification", cell(5))
		addMeta(r.Meta, "certification_id", cell(6))
		addMeta(r.Meta, "certified_since", cell(7))
		addMeta(r.Meta, "expires", cell(8))
		out = append(out, r)
	})
	return out
}

func npcpDetail1(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	if doc.Find("span[data-propName]").Length() == 0 {
		return Record{}, fmt.Errorf("no profile fields on %s (layout changed?)", base)
	}
	prop := func(name string) string {
		return clean(doc.Find(fmt.Sprintf(`span[data-propName="%s"]`, name)).First().Text())
	}
	d := Record{Meta: map[string][]string{}}
	d.Emails = structuredEmails(prop("Email"))
	if p := structuredPhone(prop("Contact Number")); p != "" {
		d.Phones = []string{p}
	}
	if w := prop("Website"); w != "" {
		d.Websites = []string{w}
	}
	d.Business = prop("Company")
	d.Bio = prop("Professional Bio")
	addMeta(d.Meta, "modalities_taught", prop("Modalities Taught"))
	addMeta(d.Meta, "formal_training", prop("Formal Training"))
	addMeta(d.Meta, "pmc_certification_id", prop("PMC Teacher Certification ID"))
	addMeta(d.Meta, "pmc_certified_since", prop("PMC Certified Since"))
	addMeta(d.Meta, "pmc_expires", prop("PMC Certification Expiration"))
	return d, nil
}
