package prodir

// Swim Smooth certified coaches, https://swimsmooth.com/coaches
// (~33 coaches, individuals).
//
// The marketing page only embeds a hard-coded array; the real data is the JSON
// endpoint behind the swimsmooth.guru app, one GET that returns every coach
// with their profile data (no paging, ~105 KB):
// https://europe-west1-swimsmooth.cloudfunctions.net/live-v4-2-coaches
// ({"success":true,"count":N,"payload":[{"id","data":{...}}]}). The profile
// page is a JS shell, but it is only the same data, so there is no Detail.
// The id is data.shortCode (lower-cased), the profile URL is
// https://www.swimsmooth.guru/coaches/{shortCode}.
//
// Contacts: email and phone (plain), mainWebsite and bookingWebsite, and the
// social links the coach filled in (a bare handle or a name is not a link and
// is dropped). Bio is the description, brandName the business, brandLocation
// "City, Region, Country": the last part is the country when it is one the
// country resolver knows ("USA", "UK"), otherwise the whole text is the city
// and the country stays unknown (never guessed).
//
// KNOWN FRAGILITY: the Cloud Function name carries the app version
// ("live-v4-2"); when it 404s, grep the swimsmooth.guru bundle
// (/assets/index-*.js) for "cloudfunctions.net/live-v4-2-coaches".

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	swimSmoothURL  = "https://europe-west1-swimsmooth.cloudfunctions.net/live-v4-2-coaches"
	swimSmoothGuru = "https://www.swimsmooth.guru/coaches/"
)

var swimSmoothCountries = sync.OnceValue(newCountryResolver)

func init() {
	register(&Source{
		Slug: "swimsmooth", Name: "Swim Smooth certified coaches", Host: "swimsmooth.com",
		Category: "swimming_coach", ListingType: "individual", MinInterval: time.Second,
		List: swimSmoothList,
	})
}

type swimSmoothLink struct {
	URL     string `json:"url"`
	Display string `json:"display"`
}

type swimSmoothCoach struct {
	ID   json.RawMessage `json:"id"`
	Data struct {
		ShortCode      string         `json:"shortCode"`
		DisplayName    string         `json:"displayName"`
		BrandName      string         `json:"brandName"`
		BrandLocation  string         `json:"brandLocation"`
		Email          string         `json:"email"`
		Phone          string         `json:"phone"`
		Description    string         `json:"description"`
		MainWebsite    swimSmoothLink `json:"mainWebsite"`
		BookingWebsite swimSmoothLink `json:"bookingWebsite"`
		Instagram      string         `json:"instagram"`
		Facebook       string         `json:"facebook"`
		LinkedIn       string         `json:"linkedin"`
		Twitter        string         `json:"twitter"`
		TikTok         string         `json:"tiktok"`
		Certified      bool           `json:"certified"`
		Tags           []string       `json:"tags"`
		Services       []string       `json:"services"`
	} `json:"data"`
}

func swimSmoothList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	// the app sends this Origin; the endpoint needs no other header
	p, err := f.Get(ctx, swimSmoothURL, map[string]string{"Origin": "https://www.swimsmooth.guru", "Accept": "application/json"})
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	var resp struct {
		Success bool              `json:"success"`
		Payload []swimSmoothCoach `json:"payload"`
	}
	if err := json.Unmarshal(p.Body, &resp); err != nil {
		return err
	}
	if len(resp.Payload) == 0 {
		return fmt.Errorf("no coaches in the response (success=%v, endpoint version changed?)", resp.Success)
	}
	seen := map[string]bool{}
	for _, c := range resp.Payload {
		d := c.Data
		code := clean(d.ShortCode)
		name := clean(d.DisplayName)
		id := strings.ToLower(firstNonEmpty(code, strings.Trim(string(c.ID), `"`)))
		if id == "" || name == "" || seen[id] { // not a real entry, or a repeat
			continue
		}
		seen[id] = true
		r := Record{
			ID: id, Name: name, Business: clean(d.BrandName), Bio: swimSmoothText(d.Description),
			Social: map[string]string{},
			Meta:   map[string][]string{},
		}
		if code != "" {
			r.URL = swimSmoothGuru + code
		}
		if e := clean(d.Email); e != "" {
			r.Emails = []string{e}
		}
		if ph := clean(d.Phone); ph != "" {
			r.Phones = []string{ph}
		}
		for _, l := range []swimSmoothLink{d.MainWebsite, d.BookingWebsite} {
			if w := clean(firstNonEmpty(l.URL, l.Display)); w != "" {
				r.Websites = append(r.Websites, w)
			}
		}
		for plat, v := range map[string]string{"instagram": d.Instagram, "facebook": d.Facebook, "linkedin": d.LinkedIn, "twitter": d.Twitter, "tiktok": d.TikTok} {
			if v = clean(v); socialPlatform(v) == plat {
				r.Social[plat] = v
			}
		}
		r.City, r.Region, r.Country = swimSmoothPlace(d.BrandLocation)
		if d.Certified {
			addMeta(r.Meta, "certification", "Swim Smooth Certified Coach")
		}
		addMeta(r.Meta, "tags", d.Tags...)
		addMeta(r.Meta, "services", d.Services...)
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// swimSmoothText is a description as plain text (a few carry HTML).
func swimSmoothText(s string) string {
	if !strings.Contains(s, "<") {
		return clean(s)
	}
	if doc, err := docFromBody([]byte(s)); err == nil {
		return clean(doc.Text())
	}
	return clean(s)
}

// swimSmoothPlace splits "City, Region, Country". The last part is the country
// only when the resolver knows it; otherwise the whole text is the city.
func swimSmoothPlace(loc string) (city, region, country string) {
	var parts []string
	for _, p := range strings.Split(loc, ",") {
		if p = clean(p); p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return
	}
	last := parts[len(parts)-1]
	if _, code := swimSmoothCountries().resolve(last); code != "" {
		parts = parts[:len(parts)-1]
		country = last
	}
	if len(parts) > 0 {
		city = parts[0]
		region = strings.Join(parts[1:], ", ")
	}
	if country == "" {
		city, region = strings.Join(parts, ", "), ""
	}
	return
}
