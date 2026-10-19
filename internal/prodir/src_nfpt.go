package prodir

// NFPT certified trainers (National Federation of Professional Trainers),
// https://nfpt.com/trainer-search/ (individuals, US / Canada / UK / Australia).
//
// The trainer search is a pair of plain JSON helpers. GetTrainers.php takes a
// latitude / longitude (form-encoded POST) and returns every trainer within 25
// miles: name, nfptid, city, province, country. There is no query without a
// position, so List walks a bounded grid with ~35 mile spacing (a square grid
// of spacing s covers every point once s <= 25*sqrt(2) ~ 35.4 miles): the US
// lower 48, southern Canada, the Maritimes, Great Britain and south-east
// Australia as boxes, plus a few single points (Alaska, Hawaii, other
// Australian cities). A short list of metro points is walked FIRST so that a
// limited run (smoke test) gets dense output; the grid repeats them, ids are
// deduped. The full grid is ~5,900 requests (~1.7 hours at 1s); trainers
// outside these regions are not reached.
//
// GetProfile.php (POST profile_address=<nfptid>) adds phone, website, bio, the
// years certified and the specialist credentials. There is no email, street
// address or social link. A profile with success=false (no longer certified)
// is stored as Missing.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	nfptHelpers    = "https://nfpt.com/wp-content/themes/hello-elementor-child/trainer/helpers/"
	nfptGridMiles  = 35.0
	nfptMilesPerLt = 69.0 // miles per degree of latitude
)

func init() {
	register(&Source{
		Slug: "nfpt", Name: "NFPT certified trainers", Host: "nfpt.com",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: nfptList, Detail: nfptDetail,
	})
}

type nfptPoint struct{ lat, lng float64 }

// nfptBox is a lat/lng rectangle walked as a grid.
type nfptBox struct{ south, north, west, east float64 }

// nfptMetros are walked before the grid (dense output first).
var nfptMetros = []nfptPoint{
	{40.71, -74.01}, {34.05, -118.24}, {41.88, -87.63}, {29.76, -95.37}, {33.45, -112.07},
	{43.65, -79.38}, {49.28, -123.12}, {51.51, -0.13}, {53.48, -2.24}, {-33.87, 151.21}, {-37.81, 144.96},
}

// nfptPoints are single points outside the boxes (Alaska, Hawaii, Australian cities).
var nfptPoints = []nfptPoint{
	{61.22, -149.90}, {64.84, -147.72}, {58.30, -134.42}, {21.31, -157.86}, {19.71, -155.09},
	{-31.95, 115.86}, {-34.93, 138.60}, {-12.46, 130.84}, {-42.88, 147.33}, {-16.92, 145.77}, {-27.47, 153.03},
}

var nfptBoxes = []nfptBox{
	{24.5, 49.4, -125.0, -66.9},  // US lower 48
	{49.4, 54.0, -130.0, -95.0},  // BC interior, Prairies (south of 49.4 the US box covers Ontario and Quebec)
	{43.0, 49.4, -66.9, -52.0},   // the Maritimes and Newfoundland
	{50.0, 58.7, -8.2, 1.8},      // Great Britain (and Northern Ireland)
	{-39.0, -27.0, 144.0, 154.0}, // south-east Australia
}

// nfptGrid returns the search points: metros, the box grids, the single points.
func nfptGrid() []nfptPoint {
	pts := append([]nfptPoint{}, nfptMetros...)
	for _, b := range nfptBoxes {
		for lat := b.south; lat <= b.north; lat += nfptGridMiles / nfptMilesPerLt {
			// longitude degrees shrink with latitude: keep the spacing in miles
			step := nfptGridMiles / (nfptMilesPerLt * math.Cos(lat*math.Pi/180))
			for lng := b.west; lng <= b.east; lng += step {
				pts = append(pts, nfptPoint{math.Round(lat*100) / 100, math.Round(lng*100) / 100})
			}
		}
	}
	return append(pts, nfptPoints...)
}

type nfptTrainer struct {
	First    string `json:"name_first"`
	Last     string `json:"name_last"`
	ID       int64  `json:"nfptid"`
	City     string `json:"city"`
	Province string `json:"province"`
	Country  string `json:"country"`
}

func nfptList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	seen := map[int64]bool{}
	for _, pt := range nfptGrid() {
		form := url.Values{"latitude": {fmt.Sprint(pt.lat)}, "longitude": {fmt.Sprint(pt.lng)}}
		p, err := f.Post(ctx, nfptHelpers+"GetTrainers.php", hdr, []byte(form.Encode()))
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Success bool          `json:"success"`
			Data    []nfptTrainer `json:"data"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("point %v,%v: %w", pt.lat, pt.lng, err)
		}
		for _, t := range resp.Data {
			if t.ID == 0 || seen[t.ID] {
				continue
			}
			seen[t.ID] = true
			id := strconv.FormatInt(t.ID, 10)
			r := Record{
				ID: id, URL: "https://nfpt.com/trainer-profile/?id=" + id, Name: clean(t.First + " " + t.Last),
				City: t.City, Region: t.Province, Country: t.Country,
			}
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// nfptNANP are the country spellings of the +1 numbering plan seen in the data.
var nfptNANP = map[string]bool{"usa": true, "us": true, "united states": true, "canada": true, "ca": true}

func nfptDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	hdr := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	form := url.Values{"profile_address": {r.ID}}
	p, err := f.Post(ctx, nfptHelpers+"GetProfile.php", hdr, []byte(form.Encode()))
	if err != nil {
		return Record{}, err
	}
	if err := p.Err(); err != nil {
		return Record{}, err
	}
	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Info struct {
				About     string      `json:"about"`
				URL       string      `json:"url"`
				Telephone json.Number `json:"telephone_number"`
				Display   int         `json:"display_profile"`
				Years     json.Number `json:"years_certified"`
			} `json:"info"`
			Education []string `json:"education"`
		} `json:"data"`
	}
	if err := json.Unmarshal(p.Body, &resp); err != nil {
		return Record{}, fmt.Errorf("profile %s: %w", r.ID, err)
	}
	if !resp.Success {
		// "The user is not currently certified by NFPT": the profile is gone.
		return Record{}, fmt.Errorf("%w: profile %s not available", ErrNotFound, r.ID)
	}
	info := resp.Data.Info
	out := Record{Bio: info.About, Meta: map[string][]string{}}
	if w := strings.TrimSpace(info.URL); w != "" {
		out.Websites = []string{w}
	}
	// A 10-digit number of a US/Canada trainer is a +1 number; anything
	// else is kept as written.
	if d := digitsOf(info.Telephone.String()); d != "" {
		if len(d) == 10 && nfptNANP[strings.ToLower(clean(r.Country))] {
			d = "+1" + d
		}
		out.Phones = []string{d}
	}
	addMeta(out.Meta, "years_certified", info.Years.String())
	addMeta(out.Meta, "specialist_credentials", resp.Data.Education...)
	return out, nil
}
