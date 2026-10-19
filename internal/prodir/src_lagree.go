package prodir

// Lagree Fitness studio locator, https://www.lagreefitness.com/locator
// (~830 Megaformer studios, listing_type studio).
//
// The locator page inlines the whole studio list as a JavaScript array,
// `const stores = [ { id: 1396, name: '...', phone: '...', website: '...',
// address: `...` }, ... ];`, so one plain GET returns everything and there is
// no detail page. The array is hand-maintained: strings are single-quoted or
// backtick-quoted with backslash escapes, so lagreeStores is a small literal
// scanner rather than a JSON conversion.
//
// Fields: name, phone (free text, ~70%), website (~90%; a social page typed
// into the field becomes a social link, a scheme-less host gets http://),
// address (one free-text string) and info (free text, Bio). There is no email;
// the load-time extractor finds one in Bio when info carries it, otherwise
// the enrich stage must visit the website.
//
// Country: the `country` field of the array is empty or a US state code, never
// a country, so it is not used as one. The country is the address's last part
// when that names a country ("..., France", "..., USA"), or, for a US address
// ending "ST 12345", the United States (the ZIP pattern is unambiguous) with
// the state code as Region. Otherwise it stays empty: never guessed from a
// city. Known gaps: no email; ~1/3 of the addresses carry no country text.
// Rows named "... Upload Test" (a vendor test entry) are skipped.

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

const lagreeURL = "https://www.lagreefitness.com/locator"

func init() {
	register(&Source{
		Slug: "lagree", Name: "Lagree Fitness studio locator", Host: "lagreefitness.com",
		Category: "pilates_instructor", ListingType: "studio", MinInterval: time.Second,
		List: lagreeList,
	})
}

func lagreeList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, lagreeURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	stores, err := lagreeStores(string(p.Body))
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, s := range stores {
		r, ok := lagreeRecord(s)
		if !ok || seen[r.ID] {
			continue
		}
		seen[r.ID] = true
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

var (
	lagreeTestRe = regexp.MustCompile(`(?i)\bupload test\b|^test\b`)
	lagreeZipRe  = regexp.MustCompile(`(?:^|[\s,])([A-Z]{2})\s+\d{5}(?:-\d{4})?\s*$`)
	lagreeIDRe   = regexp.MustCompile(`^[0-9]+$`)
	lagreeRes    = sync.OnceValue(newCountryResolver)
)

func lagreeRecord(s map[string]string) (Record, bool) {
	id, name := clean(s["id"]), clean(s["name"])
	// the id must be numeric: it is the stable key and a DNS label of the
	// synthetic domain; a test row or a row without a name is not a studio
	if !lagreeIDRe.MatchString(id) || name == "" || lagreeTestRe.MatchString(name) {
		return Record{}, false
	}
	r := Record{ID: id, Name: name, Address: clean(s["address"]), Bio: clean(s["info"]), Social: map[string]string{}}
	if ph := clean(s["phone"]); ph != "" {
		r.Phones = []string{ph}
	}
	if w := clean(s["website"]); w != "" {
		switch {
		case socialPlatform(withScheme(w)) != "":
			r.Social[socialPlatform(withScheme(w))] = withScheme(w)
		default:
			r.Websites = []string{withScheme(w)}
		}
	}
	r.Country, r.Region = lagreeCountry(r.Address)
	return r, true
}

// lagreeCountry reads the country (and, for US ZIP endings, the state) from a
// free-text address.
func lagreeCountry(addr string) (country, region string) {
	flat := clean(strings.NewReplacer("\n", ",", "\r", ",").Replace(addr))
	if m := lagreeZipRe.FindStringSubmatch(flat); m != nil {
		return "United States", m[1]
	}
	var parts []string
	for _, p := range strings.Split(flat, ",") {
		if p = clean(p); p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return "", ""
	}
	last := parts[len(parts)-1]
	// "Jeddah Saudi Arabia": also try the trailing 1-3 words. A bare two-letter
	// token is a state or province code, not a country, except "UK".
	words := strings.Fields(last)
	for n := min(3, len(words)); n >= 1; n-- {
		cand := strings.Join(words[len(words)-n:], " ")
		if len(cand) == 2 && !strings.EqualFold(cand, "uk") {
			continue
		}
		if name, _ := lagreeRes().resolve(cand); name != "" {
			return name, ""
		}
	}
	return "", ""
}

// lagreeStores parses the `const stores = [ {...}, ... ];` literal into one
// map of raw string values per object (numbers stay their literal text).
func lagreeStores(page string) ([]map[string]string, error) {
	const marker = "const stores = ["
	i := strings.Index(page, marker)
	if i < 0 {
		return nil, fmt.Errorf("no %q in the locator page (layout changed?)", marker)
	}
	sc := &lagreeScan{s: page, i: i + len(marker)}
	var out []map[string]string
	for {
		sc.skip(" \t\r\n,")
		switch sc.peek() {
		case ']':
			return out, nil
		case '{':
			sc.i++
			obj, err := sc.object()
			if err != nil {
				return nil, err
			}
			out = append(out, obj)
		default:
			return nil, fmt.Errorf("unexpected %q at offset %d of the stores array", sc.peek(), sc.i)
		}
	}
}

// lagreeScan walks a JavaScript object-literal array.
type lagreeScan struct {
	s string
	i int
}

func (sc *lagreeScan) peek() byte {
	if sc.i >= len(sc.s) {
		return 0
	}
	return sc.s[sc.i]
}

func (sc *lagreeScan) skip(set string) {
	for sc.i < len(sc.s) && strings.IndexByte(set, sc.s[sc.i]) >= 0 {
		sc.i++
	}
}

// object reads `key: value, ...}` after the opening brace.
func (sc *lagreeScan) object() (map[string]string, error) {
	obj := map[string]string{}
	for {
		sc.skip(" \t\r\n,")
		switch c := sc.peek(); {
		case c == 0:
			return nil, fmt.Errorf("unterminated object in the stores array")
		case c == '}':
			sc.i++
			return obj, nil
		}
		key, err := sc.token()
		if err != nil {
			return nil, err
		}
		sc.skip(" \t\r\n")
		if sc.peek() != ':' {
			return nil, fmt.Errorf("expected ':' after key %q at offset %d", key, sc.i)
		}
		sc.i++
		sc.skip(" \t\r\n")
		val, err := sc.token()
		if err != nil {
			return nil, err
		}
		obj[key] = val
	}
}

// token reads a quoted string (', " or `, backslash escapes) or a bare word
// (identifier or number) up to a delimiter.
func (sc *lagreeScan) token() (string, error) {
	switch q := sc.peek(); q {
	case '\'', '"', '`':
		sc.i++
		var b strings.Builder
		for sc.i < len(sc.s) {
			c := sc.s[sc.i]
			switch {
			case c == '\\' && sc.i+1 < len(sc.s):
				sc.i++
				switch e := sc.s[sc.i]; e {
				case 'n':
					b.WriteByte('\n')
				case 't':
					b.WriteByte(' ')
				default:
					b.WriteByte(e)
				}
			case c == q:
				sc.i++
				return b.String(), nil
			default:
				b.WriteByte(c)
			}
			sc.i++
		}
		return "", fmt.Errorf("unterminated string in the stores array")
	case 0:
		return "", fmt.Errorf("unexpected end of the stores array")
	}
	start := sc.i
	for sc.i < len(sc.s) && strings.IndexByte(" \t\r\n,:}]", sc.s[sc.i]) < 0 {
		sc.i++
	}
	if sc.i == start {
		return "", fmt.Errorf("empty token at offset %d of the stores array", start)
	}
	return sc.s[start:sc.i], nil
}
