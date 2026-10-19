package prodir

// REPs New Zealand exercise professional registry, https://reps.org.nz/registry/
// (~5,500 registrants, mostly individuals).
//
// Every registrant is a WordPress post, so the public REST API lists them:
// /wp-json/wp/v2/posts?per_page=100&page=N (55 pages; a page past the end is
// HTTP 400, a short page is the last). The tag and category ids are resolved
// through /wp-json/wp/v2/tags and /categories (fetched once). A record has only
// a name, level tags and a region: there is NO email, phone or website (the
// "Contact Trainer" link is a message form), so these are name/region seeds.
// The post content is not requested (it is a Gravity Forms render, mostly
// script).
//
// Mapping: Name = post title, Region = region category ("Uncategorised" = none),
// Country = New Zealand (single-country registry), Meta level = the level tags.
// Category: a registrant whose only person tags are Yoga Teacher / Pilates (one
// of the two) is a yoga_teacher / pilates_instructor, per the recon spec; anyone
// also holding a fitness tag stays fitness_trainer. "Exercise Facility" and
// "Education Provider" entries are businesses: they stay in the source's
// individual listing type (a record cannot override it) and carry
// registrant_type in Meta so they can be told apart.

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"
)

const repsNZAPI = "https://reps.org.nz/wp-json/wp/v2"

// repsNZPerPage is a var so the test can use a tiny page.
var repsNZPerPage = 100

func init() {
	register(&Source{
		Slug: "repsnz", Name: "REPs New Zealand exercise professional registry", Host: "reps.org.nz",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: repsNZList,
	})
}

// repsNZNames pages a taxonomy endpoint into an id -> name map.
func repsNZNames(ctx context.Context, f Fetcher, kind string) (map[string]string, error) {
	out := map[string]string{}
	for page := 1; page <= 20; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s/%s?per_page=100&page=%d&_fields=id,name", repsNZAPI, kind, page), nil)
		if err != nil {
			return nil, err
		}
		if p.Status == 400 && page > 1 {
			break
		}
		if err := p.Err(); err != nil {
			return nil, err
		}
		var items []struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(p.Body, &items); err != nil {
			return nil, fmt.Errorf("%s page %d: %w", kind, page, err)
		}
		for _, it := range items {
			out[strconv.Itoa(it.ID)] = html.UnescapeString(it.Name)
		}
		if len(items) < 100 {
			break
		}
	}
	return out, nil
}

func repsNZList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	tags, err := repsNZNames(ctx, f, "tags")
	if err != nil {
		return err
	}
	regions, err := repsNZNames(ctx, f, "categories")
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for page := 1; page <= 500; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s/posts?per_page=%d&page=%d&_fields=id,link,title,tags,categories", repsNZAPI, repsNZPerPage, page), nil)
		if err != nil {
			return err
		}
		if p.Status == 400 && page > 1 { // WP answers 400 past the last page
			return nil
		}
		if err := p.Err(); err != nil {
			return err
		}
		var posts []struct {
			ID    int    `json:"id"`
			Link  string `json:"link"`
			Title struct {
				Rendered string `json:"rendered"`
			} `json:"title"`
			Tags       []int `json:"tags"`
			Categories []int `json:"categories"`
		}
		if err := json.Unmarshal(p.Body, &posts); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		for _, post := range posts {
			r := Record{
				ID: strconv.Itoa(post.ID), URL: post.Link, Name: clean(html.UnescapeString(post.Title.Rendered)),
				Country: "New Zealand", Meta: map[string][]string{},
			}
			if r.Name == "" || seen[r.ID] { // no name: not a real entry
				continue
			}
			seen[r.ID] = true
			var yoga, pilates, business, other bool
			for _, id := range post.Tags {
				name := tags[strconv.Itoa(id)]
				low := strings.ToLower(name)
				if low == "exercise facility" || low == "education provider" {
					business = true
					addMeta(r.Meta, "registrant_type", name)
					continue
				}
				switch {
				case name == "":
					continue
				case strings.Contains(low, "yoga"):
					yoga = true
				case strings.Contains(low, "pilates"):
					pilates = true
				default:
					other = true
				}
				addMeta(r.Meta, "level", name)
			}
			if !other && !business {
				switch {
				case yoga && !pilates:
					r.Category = "yoga_teacher"
				case pilates && !yoga:
					r.Category = "pilates_instructor"
				}
			}
			for _, id := range post.Categories {
				if reg := regions[strconv.Itoa(id)]; reg != "" && !strings.EqualFold(reg, "Uncategorised") {
					r.Region = reg
					break
				}
			}
			if err := emit(r); err != nil {
				return err
			}
		}
		if len(posts) < repsNZPerPage {
			return nil
		}
	}
	return nil
}
