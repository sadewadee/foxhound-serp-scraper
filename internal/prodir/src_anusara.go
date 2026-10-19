package prodir

// Anusara Yoga teacher finder, https://www.anusarayoga.com/find-a-teacher
// (~650-700 teachers, individuals).
//
// A Wix Velo site. The finder calls the web method getTeachers.ajax, which
// answers with the teachers' contacts to an anonymous instance token:
//  1. GET /find-a-teacher (sets the session cookies);
//  2. GET /_api/v1/access-tokens and take the apps entry whose instance starts
//     with "wixcode-pub" (any other entry is a 401);
//  3. POST the web method with body [null,null,null,PAGE], that instance in the
//     Authorization and x-wix-app-instance headers: {result:{data, pagination}}.
//
// A page holds 16-24 teachers (the filter runs after the 50-row page), so the
// page count is not fixed: List runs until hasNext is false. Rows give name,
// email(s), phone(s), country (custom.pais) and the role / certification
// (parsedRoles, e.g. L2-IAYT). There is no website, city or address. Wix may
// rotate the web method path; re-derive it from the finder page's network
// calls when it starts answering 404.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	anusaraBase   = "https://www.anusarayoga.com"
	anusaraMethod = anusaraBase + "/_api/wix-code-public-dispatcher-ng/siteview/_webMethods/backend/getTeachers.web.js/getTeachers.ajax?gridAppId=3a08acd3-90cd-4d42-ab0e-295037b36bfb&viewMode=site"
)

func init() {
	register(&Source{
		Slug: "anusara", Name: "Anusara Yoga teacher finder", Host: "anusarayoga.com",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: anusaraList,
	})
}

type anusaraTeacher struct {
	ID   string `json:"_id"`
	Info struct {
		Name struct {
			First string `json:"first"`
			Last  string `json:"last"`
		} `json:"name"`
		Emails []struct {
			Email   string `json:"email"`
			Primary bool   `json:"primary"`
		} `json:"emails"`
		Phones []struct {
			Phone          string `json:"phone"`
			FormattedPhone string `json:"formattedPhone"`
			Primary        bool   `json:"primary"`
		} `json:"phones"`
		ExtendedFields map[string]any `json:"extendedFields"`
	} `json:"info"`
	PrimaryInfo struct {
		Email string `json:"email"`
		Phone string `json:"phone"`
	} `json:"primaryInfo"`
	ParsedRoles []string `json:"parsedRoles"`
}

// anusaraInstance is the wixcode-pub instance token of the site.
func anusaraInstance(ctx context.Context, f Fetcher) (string, error) {
	page, err := f.Get(ctx, anusaraBase+"/find-a-teacher", nil)
	if err != nil {
		return "", err
	}
	if err := page.Err(); err != nil {
		return "", err
	}
	p, err := f.Get(ctx, anusaraBase+"/_api/v1/access-tokens", nil)
	if err != nil {
		return "", err
	}
	if err := p.Err(); err != nil {
		return "", err
	}
	var tok struct {
		Apps map[string]struct {
			Instance string `json:"instance"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(p.Body, &tok); err != nil {
		return "", fmt.Errorf("access tokens: %w", err)
	}
	for _, a := range tok.Apps {
		if strings.HasPrefix(a.Instance, "wixcode-pub") {
			return a.Instance, nil
		}
	}
	return "", errors.New("no wixcode-pub instance in the access tokens (Wix changed?)")
}

func anusaraList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	inst, err := anusaraInstance(ctx, f)
	if err != nil {
		return err
	}
	hdr := map[string]string{"Content-Type": "application/json", "Authorization": inst, "x-wix-app-instance": inst}
	seen := map[string]bool{}
	for page := 1; page <= 500; page++ {
		p, err := f.Post(ctx, anusaraMethod, hdr, []byte(fmt.Sprintf("[null,null,null,%d]", page)))
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Result struct {
				Data       []anusaraTeacher `json:"data"`
				Pagination struct {
					HasNext bool `json:"hasNext"`
				} `json:"pagination"`
			} `json:"result"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		for _, t := range resp.Result.Data {
			r, ok := t.record()
			if !ok || seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			if err := emit(r); err != nil {
				return err
			}
		}
		if !resp.Result.Pagination.HasNext || len(resp.Result.Data) == 0 {
			return nil
		}
	}
	return nil
}

func (t anusaraTeacher) record() (Record, bool) {
	name := clean(t.Info.Name.First + " " + t.Info.Name.Last)
	if t.ID == "" || name == "" { // no id or name: not an entry
		return Record{}, false
	}
	field := func(k string) string { s, _ := t.Info.ExtendedFields[k].(string); return clean(s) }
	r := Record{ID: t.ID, Name: name, Country: field("custom.pais"), Meta: map[string][]string{}}
	// the primary address / number first, then the rest
	add := func(list []string, v string) []string {
		if v = strings.TrimSpace(v); v != "" {
			return addString(list, v)
		}
		return list
	}
	r.Emails = add(r.Emails, t.PrimaryInfo.Email)
	for _, e := range t.Info.Emails {
		r.Emails = add(r.Emails, e.Email)
	}
	r.Phones = add(r.Phones, t.PrimaryInfo.Phone)
	for _, p := range t.Info.Phones {
		r.Phones = add(r.Phones, firstNonEmpty(p.FormattedPhone, p.Phone))
	}
	addMeta(r.Meta, "credential", t.ParsedRoles...)
	if len(t.ParsedRoles) == 0 {
		addMeta(r.Meta, "credential", field("custom.role"))
	}
	return r, true
}
