package prodir

// U.S. Masters Swimming club finder, https://www.usms.org/clubs
// (~1,120 clubs, organisations).
//
// The React finder calls /apis/v2/clubs-algolia/ with a lat/long and a radius;
// a radius of 25000 from the middle of the US returns EVERY club as one JSON
// document (~1.6 MB, no paging). The row carries name, description, practice
// times, badges and one or more pool locations; the first location is the
// record's address. The id is the slug of /clubs/{slug}: the salesforceId is
// not unique (one value is shared by two clubs).
//
// The profile page (/clubs/{slug}) adds the contact: the club contact's name
// and email (plain text in .club-contact__content, no mailto), the club website
// and the social links. There is no phone. Some clubs have no contact block or
// no website. The US is the only country.

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	usmsBase     = "https://www.usms.org"
	usmsClubsURL = usmsBase + "/apis/v2/clubs-algolia/?q=&c=&pt=&l=&r=25000&placeText=&zipcode=&lat=39.75&long=-105"
)

func init() {
	register(&Source{
		Slug: "usms-clubs", Name: "U.S. Masters Swimming clubs", Host: "usms.org",
		Category: "swimming_coach", ListingType: "club", MinInterval: time.Second,
		List: usmsClubsList, Detail: usmsClubsDetail,
	})
}

type usmsClub struct {
	Title         string `json:"title"`
	URL           string `json:"url"`
	Description   string `json:"description"`
	PracticeTimes string `json:"practiceTimes"`
	Abbreviation  string `json:"abbreviation"`
	IsGold        bool   `json:"isGold"`
	TrialMember   bool   `json:"trialMembership"`
	TryMasters    bool   `json:"tryMasters"`
	FitnessSeries bool   `json:"fitnessSeries"`
	Badges        []struct {
		Label string `json:"label"`
	} `json:"badges"`
	Location []struct {
		Name     string `json:"name"`
		Address1 string `json:"address1"`
		Address2 string `json:"address2"`
		City     string `json:"city"`
		State    string `json:"state"`
		Zip      string `json:"zip"`
		PoolType string `json:"poolType"`
	} `json:"location"`
}

func usmsClubsList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, usmsClubsURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	var resp struct {
		Data struct {
			Clubs []usmsClub `json:"clubs"`
		} `json:"data"`
	}
	if err := json.Unmarshal(p.Body, &resp); err != nil {
		return err
	}
	if len(resp.Data.Clubs) == 0 {
		return fmt.Errorf("no clubs in the response (endpoint changed?)")
	}
	for _, c := range resp.Data.Clubs {
		id := strings.ToLower(path.Base(strings.TrimRight(c.URL, "/")))
		if id == "" || id == "." || id == "/" || clean(c.Title) == "" { // not a real entry
			continue
		}
		r := Record{
			ID: id, URL: absURL(usmsBase, c.URL), Name: clean(c.Title), Bio: clean(c.Description),
			Country: "United States", Meta: map[string][]string{},
		}
		if len(c.Location) > 0 {
			l := c.Location[0]
			r.Address = strings.Trim(clean(l.Address1)+", "+clean(l.Address2), ", ")
			r.City, r.Region, r.PostalCode = clean(l.City), clean(l.State), clean(l.Zip)
		}
		for _, l := range c.Location {
			addMeta(r.Meta, "pool", l.Name)
			addMeta(r.Meta, "pool_type", l.PoolType)
		}
		for _, b := range c.Badges {
			addMeta(r.Meta, "badge", b.Label)
		}
		addMeta(r.Meta, "club_code", c.Abbreviation)
		addMeta(r.Meta, "practice_times", c.PracticeTimes)
		for _, fl := range []struct {
			on    bool
			label string
		}{{c.IsGold, "gold club"}, {c.TrialMember, "trial membership"}, {c.TryMasters, "try masters"}, {c.FitnessSeries, "fitness series"}} {
			if fl.on {
				addMeta(r.Meta, "programs", fl.label)
			}
		}
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

func usmsClubsDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	if doc.Find("h1").Length() == 0 {
		return Record{}, fmt.Errorf("no heading on %s (layout changed?)", base)
	}
	d := Record{Social: map[string]string{}, Meta: map[string][]string{}}
	contact := doc.Find(".club-contact__content").First()
	contact.Find("p").Each(func(i int, p *goquery.Selection) {
		text := clean(p.Text())
		if emails := structuredEmails(text); len(emails) > 0 {
			for _, e := range emails {
				d.Emails = addString(d.Emails, e)
			}
		} else if i == 0 {
			addMeta(d.Meta, "contact_name", text)
		}
	})
	for _, e := range emailsIn(contact) {
		d.Emails = addString(d.Emails, e)
	}
	if href := clean(doc.Find(".club-website a[href]").First().AttrOr("href", "")); href != "" {
		d.Websites = []string{absURL(base, href)}
	}
	doc.Find(".club-social-links .social-share-icons__item a[href]").Each(func(_ int, a *goquery.Selection) {
		href := absURL(base, a.AttrOr("href", ""))
		if plat := socialPlatform(href); plat != "" && d.Social[plat] == "" {
			d.Social[plat] = href
		}
	})
	return d, nil
}
