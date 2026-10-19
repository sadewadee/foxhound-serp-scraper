package prodir

// Shared reader for Sport80 public widgets, the "club / coach finder" engine
// behind usacycling, usajudo, usatf and usataekwondo (src_*.go). Every tenant
// serves the same JSON API on its own subdomain, no auth and no cookies:
//
//	POST https://<tenant>.sport80.com/api/public/widget/data/new/<widget>?p=<page0>&i=100&s=&l=&d=0&f=
//
// with any multipart/form-data body. The page size is capped at 100 and an
// empty query returns the whole widget. A row carries id, name, address,
// telephone, email and a list of titled info values; a value is either a plain
// string or an object whose "text" is the label. A source supplies conv, which
// maps a row to a Record (false = skip the row).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"mime/multipart"
	"regexp"
	"strconv"
	"strings"
)

const s80PageSize = 100

// s80Val is a JSON value read as text: a string as is, an object as its
// "text", a number or bool as its literal, null as "".
type s80Val string

func (v *s80Val) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		*v = s80Val(s)
		return nil
	}
	var o struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(b, &o) == nil {
		*v = s80Val(o.Text)
		return nil
	}
	*v = s80Val(strings.Trim(string(b), `"`))
	return nil
}

type s80Row struct {
	ID        s80Val `json:"id"`
	Name      s80Val `json:"name"`
	Address   s80Val `json:"address"`
	Telephone s80Val `json:"telephone"`
	Email     s80Val `json:"email"`
	Info      []struct {
		Title string `json:"title"`
		Value s80Val `json:"value"`
	} `json:"info"`
}

var s80TagRe = regexp.MustCompile(`<[^>]*>`)

// s80Text is a widget value as plain text: tags removed (State values come as
// "<b>California</b>"), entities decoded, whitespace collapsed.
func s80Text(v s80Val) string {
	return clean(html.UnescapeString(s80TagRe.ReplaceAllString(string(v), " ")))
}

// info is the text of the first info value titled title ("" when absent).
func (r s80Row) info(title string) string {
	for _, i := range r.Info {
		if i.Title == title {
			return s80Text(i.Value)
		}
	}
	return ""
}

// splitList splits a "a,  b,  c" value into its non-empty trimmed parts.
func splitList(v string) []string {
	var out []string
	for _, p := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ';' || r == '\n' }) {
		if p = clean(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// sport80List pages through widget on host (the tenant's own subdomain) and
// emits conv(row) for every new row. It ends on a short or empty page, or when
// the reported total is reached.
func sport80List(ctx context.Context, f Fetcher, host string, widget int, conv func(s80Row) (Record, bool), emit func(Record) error) error {
	seen := map[string]bool{}
	for page := 0; page < 1000; page++ {
		var body bytes.Buffer
		w := multipart.NewWriter(&body)
		if err := w.WriteField("x", "1"); err != nil { // the API wants a form body, any field will do
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		u := fmt.Sprintf("https://%s/api/public/widget/data/new/%d?p=%d&i=%d&s=&l=&d=0&f=", host, widget, page, s80PageSize)
		p, err := f.Post(ctx, u, map[string]string{"Accept": "application/json", "Content-Type": w.FormDataContentType()}, body.Bytes())
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Total   s80Val   `json:"total"`
			PerPage int      `json:"items_per_page"`
			Data    []s80Row `json:"data"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("widget %d page %d: %w", widget, page, err)
		}
		for _, row := range resp.Data {
			id := clean(string(row.ID))
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			if rec, ok := conv(row); ok {
				if err := emit(rec); err != nil {
					return err
				}
			}
		}
		per := resp.PerPage
		if per <= 0 {
			per = s80PageSize
		}
		total, _ := strconv.Atoi(string(resp.Total))
		if len(resp.Data) < per || (total > 0 && (page+1)*per >= total) {
			return nil
		}
	}
	return nil
}
