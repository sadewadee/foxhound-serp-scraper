package prodir

// BCRPA Registry of Fitness Professionals, https://www.thefitnessregistry.com/fitness-leader-directory
// (~2,700 members: fitness leaders and fitness students, mostly British Columbia).
//
// bcrpa.bc.ca itself sits behind a Cloudflare challenge but only links to the
// registry, whose member directory is an unauthenticated JSON POST service
// (ServiceProxy.ashx, Method=WS_MemberProfile_Get_List_Paged). Pages are
// 0-based with limit 500 (6 requests); the last page is the one whose
// Pagination.NextPage is -1. Every list row is the full profile, so there is
// no detail stage: name, company, address, email (~99%), phone (~50%), website
// (~10%), sparse social links, specialties and a bio. Rows marked IsPrivate
// are skipped (the member opted out of the directory).
//
// The Country field is inconsistent ("CANADA", "Canada", "CA", empty); it is a
// provincial registry, so an empty one is Canada. Fitness students are kept,
// marked by Meta member_type, so they can be filtered downstream. The
// directory has no per-member page URL we could verify, so entries link to the
// directory itself.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const bcrpaURL = "https://www.thefitnessregistry.com/ServiceProxy.ashx?URLType=MemberProfileSvcURL&Method=WS_MemberProfile_Get_List_Paged"

func init() {
	register(&Source{
		Slug: "bcrpa", Name: "BCRPA Registry of Fitness Professionals", Host: "thefitnessregistry.com",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: bcrpaList,
	})
}

type bcrpaMember struct {
	ID          int64  `json:"MemberProfileID"`
	First       string `json:"FirstName"`
	Last        string `json:"LastName"`
	Designation string `json:"Desig_Display"`
	Email       string `json:"Email"`
	Phone       string `json:"Phone"`
	Address1    string `json:"Address1"`
	Address2    string `json:"Address2"`
	City        string `json:"City"`
	Province    string `json:"Province"`
	PostalCode  string `json:"PostalCode"`
	Country     string `json:"Country"`
	Title       string `json:"Title"`
	Company     string `json:"Company"`
	WWW         string `json:"WWW"`
	Description string `json:"MemberDescription"`
	Twitter     string `json:"TwitterURL"`
	Facebook    string `json:"FacebookURL"`
	LinkedIn    string `json:"LinkedInURL"`
	YouTube     string `json:"YouTubeURL"`
	IsPrivate   bool   `json:"IsPrivate"`
	MemberType  string `json:"MemberType"`
	Specialty   string `json:"Specialty"`
}

func bcrpaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"Content-Type": "application/json"}
	seen := map[int64]bool{}
	for page := 0; page < 100; page++ {
		body := fmt.Sprintf(`{"assoc":"TFRWeb","settings":{"OrderBy":"FirstName","IsCompany":false,"MemberTypes":[],"HideInheritedMemberBranches":false},"page":%d,"limit":500}`, page)
		p, err := f.Post(ctx, bcrpaURL, hdr, []byte(body))
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			D struct {
				List       []bcrpaMember `json:"List"`
				Pagination struct {
					NextPage int `json:"NextPage"`
				} `json:"Pagination"`
			} `json:"d"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		for _, m := range resp.D.List {
			// private members opted out; rows without an id or a name are not entries
			if m.IsPrivate || m.ID == 0 || seen[m.ID] || clean(m.First+" "+m.Last) == "" {
				continue
			}
			seen[m.ID] = true
			if err := emit(m.record()); err != nil {
				return err
			}
		}
		if len(resp.D.List) == 0 || resp.D.Pagination.NextPage < 0 {
			return nil
		}
	}
	return nil
}

var (
	bcrpaTagRe   = regexp.MustCompile(`<[^>]*>`)
	bcrpaSplitRe = regexp.MustCompile(`[|\n\r]+`)
)

// bcrpaLabels splits a "a|b\nc" (or HTML) label block into its labels.
func bcrpaLabels(s string) []string {
	var out []string
	for _, v := range bcrpaSplitRe.Split(bcrpaTagRe.ReplaceAllString(s, "|"), -1) {
		if v = clean(v); v != "" {
			out = addString(out, v)
		}
	}
	return out
}

func (m bcrpaMember) record() Record {
	r := Record{
		ID: fmt.Sprint(m.ID), Name: clean(m.First + " " + m.Last), Business: m.Company,
		Address:    clean(m.Address1 + " " + m.Address2),
		City:       m.City,
		PostalCode: m.PostalCode,
		Country:    firstNonEmpty(clean(m.Country), "Canada"),
		Bio:        m.Description,
		Social:     map[string]string{},
		Meta:       map[string][]string{},
	}
	// a province is a name or a 2-letter code; the data also holds stray punctuation
	if p := clean(m.Province); len(p) >= 2 {
		r.Region = p
	}
	if e := strings.TrimSpace(m.Email); e != "" {
		r.Emails = []string{e}
	}
	if p := strings.TrimSpace(m.Phone); p != "" {
		r.Phones = []string{p}
	}
	if w := strings.TrimSpace(m.WWW); w != "" {
		r.Websites = []string{w}
	}
	// the social fields often hold a bare handle or a name: only URLs are kept
	for _, u := range []string{m.Facebook, m.Twitter, m.LinkedIn, m.YouTube} {
		if u = strings.TrimSpace(u); u != "" {
			if p := socialPlatform(u); p != "" && socialProfile(u) && r.Social[p] == "" {
				r.Social[p] = u
			}
		}
	}
	addMeta(r.Meta, "member_type", m.MemberType)
	addMeta(r.Meta, "title", m.Title)
	addMeta(r.Meta, "credential", bcrpaLabels(m.Designation)...)
	addMeta(r.Meta, "specialties", bcrpaLabels(m.Specialty)...)
	return r
}
