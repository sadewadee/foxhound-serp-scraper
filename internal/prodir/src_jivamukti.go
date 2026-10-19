package prodir

// Jivamukti Yoga teachers, https://jivamuktiyoga.com/all-jivamukti-yoga-teachers-3/
// (~989 teachers, individuals).
//
// One page embeds the whole directory as the ReactivePro dataset
// "var REACTIVE = {...}" (~720 KB): allPosts holds id, nicename, display name
// and email (plain, ~97% filled); metaToPost groups the ids by (meta_key,
// meta_value), which List inverts to get city, state, country and the
// certification. (The /teachers/ page shows only 28 static cards: a trap.)
// Detail fetches /teachers-global/{nicename}/ for the website, social links and
// bio; the page footer carries the studio's own contact details, so only the
// teacher's own modules are read. There is no phone or street address.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
)

const (
	jivamuktiBase    = "https://jivamuktiyoga.com"
	jivamuktiListURL = jivamuktiBase + "/all-jivamukti-yoga-teachers-3/"
)

func init() {
	register(&Source{
		Slug: "jivamukti", Name: "Jivamukti Yoga teacher directory", Host: "jivamuktiyoga.com",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: jivamuktiList, Detail: jivamuktiDetail,
	})
}

// jivamuktiData is the part of the REACTIVE dataset List reads.
type jivamuktiData struct {
	AllPosts []struct {
		ID           string `json:"ID"`
		UserNicename string `json:"user_nicename"`
		DisplayName  string `json:"display_name"`
		UserEmail    string `json:"user_email"`
	} `json:"allPosts"`
	MetaToPost []struct {
		AllPosts  string `json:"allPosts"` // comma-separated post ids
		MetaKey   string `json:"meta_key"`
		MetaValue string `json:"meta_value"`
	} `json:"metaToPost"`
}

// jivamuktiDataset decodes the JSON value that follows "REACTIVE = " (the JS
// assignment is not a JSON document on its own: the decoder stops after the
// first value).
func jivamuktiDataset(body []byte) (jivamuktiData, error) {
	var d jivamuktiData
	_, after, ok := bytes.Cut(body, []byte("REACTIVE = "))
	if !ok {
		return d, errors.New("no REACTIVE dataset in the page (layout changed?)")
	}
	err := json.NewDecoder(bytes.NewReader(after)).Decode(&d)
	return d, err
}

// hasLetter tells real place names from filler values ("—", ".").
func hasLetter(s string) bool { return strings.IndexFunc(s, unicode.IsLetter) >= 0 }

func jivamuktiList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, jivamuktiListURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	d, err := jivamuktiDataset(p.Body)
	if err != nil {
		return err
	}
	attr := map[string]map[string]string{} // post id -> meta key -> value
	for _, m := range d.MetaToPost {
		v := clean(html.UnescapeString(m.MetaValue))
		if v == "" || (m.MetaKey != "trccertification" && !hasLetter(v)) {
			continue
		}
		for _, id := range strings.Split(m.AllPosts, ",") {
			if id = strings.TrimSpace(id); id != "" {
				if attr[id] == nil {
					attr[id] = map[string]string{}
				}
				attr[id][m.MetaKey] = v
			}
		}
	}
	for _, u := range d.AllPosts {
		name := clean(html.UnescapeString(u.DisplayName))
		if u.ID == "" || name == "" { // no name: not an entry
			continue
		}
		a := attr[u.ID]
		r := Record{
			ID: u.ID, Name: name, City: a["trc_city"], Region: a["trc_state"], Country: a["trc_country"],
			Meta: map[string][]string{},
		}
		if n := strings.TrimSpace(u.UserNicename); n != "" {
			r.URL = fmt.Sprintf("%s/teachers-global/%s/", jivamuktiBase, n)
		}
		if e := strings.TrimSpace(u.UserEmail); e != "" {
			r.Emails = []string{e}
		}
		addMeta(r.Meta, "certification", a["trccertification"])
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

func jivamuktiDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	if r.URL == "" {
		return Record{}, nil
	}
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	if doc.Find(".et-l--body").Length() == 0 {
		return Record{}, fmt.Errorf("no body template on %s (layout changed?)", base)
	}
	d := Record{}
	doc.Find(".trc-teacher-social a").Each(func(_ int, a *goquery.Selection) {
		href := absURL(base, a.AttrOr("href", ""))
		if href == "" {
			return
		}
		if plat := socialPlatform(href); plat != "" {
			if d.Social == nil {
				d.Social = map[string]string{}
			}
			if d.Social[plat] == "" {
				d.Social[plat] = href
			}
		} else {
			d.Websites = addString(d.Websites, href)
		}
	})
	// the bio is the 7th text module of the profile template (the footer's
	// modules carry the studio's contact details and are not "_tb_body")
	d.Bio = clean(doc.Find(".et_pb_text_6_tb_body").First().Text())
	return d, nil
}
