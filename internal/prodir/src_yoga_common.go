package prodir

// Helpers shared by the yoga-teacher sources (yogapros, jivamukti, iynaus, yaa,
// kri, anusara, yogaaustralia, iyengar-au, iyengar-nz, iyengar-be).

import (
	"regexp"
	"strings"
	"sync"
)

var yogaNonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// yogaSlugID lower-cases s into a DNS-safe [a-z0-9-] record id ("" when no
// letter or digit is left).
func yogaSlugID(s string) string {
	return strings.Trim(yogaNonAlnum.ReplaceAllString(strings.ToLower(s), "-"), "-")
}

var yogaCountries = sync.OnceValue(newCountryResolver)

// yogaSplitLocation splits "City, Region, Country" into the head and the
// country: the last comma-separated part is the country only when the
// country resolver recognises it, otherwise the whole text is the head.
func yogaSplitLocation(loc string) (head, country string) {
	loc = clean(loc)
	parts := strings.Split(loc, ",")
	last := clean(parts[len(parts)-1])
	if last != "" {
		if _, code := yogaCountries().resolve(last); code != "" {
			return clean(strings.Join(parts[:len(parts)-1], ",")), last
		}
	}
	return loc, ""
}

// yogaHTMLText is the text of an HTML fragment (a JSON "rendered" field).
func yogaHTMLText(h string) string {
	doc, err := docFromBody([]byte(h))
	if err != nil {
		return clean(h)
	}
	return clean(doc.Find("body").Text())
}

// yogaSplitList splits a delimited label list ("Hatha Yoga,Yin Yoga") on the
// separators and drops the empties (addMeta cleans and dedupes the rest).
func yogaSplitList(s string, seps ...string) []string {
	var out []string
	for _, p := range splitMeta(s, seps...) {
		if p = clean(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
