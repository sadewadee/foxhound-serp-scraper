package scraper

import (
	"testing"
)

func TestQueryTokens(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		wantContains  []string
		wantExcludes  []string
		exactExpected []string
	}{
		{
			name:          "day spa honolulu drops day",
			query:         "day spa honolulu",
			wantContains:  []string{"spa", "honolulu"},
			wantExcludes:  []string{"day"},
			exactExpected: []string{"spa", "honolulu"},
		},
		{
			name:          "yogafx aix-en-provence @yahoo.com excludes yahoo and com",
			query:         `yogafx aix-en-provence "@yahoo.com"`,
			wantContains:  []string{"yogafx", "aix", "provence"},
			wantExcludes:  []string{"yahoo", "com", "en"},
			exactExpected: []string{"yogafx", "aix", "provence"},
		},
		{
			name:          "chiropractor denver contact drops contact",
			query:         "chiropractor denver contact",
			wantContains:  []string{"chiropractor", "denver"},
			wantExcludes:  []string{"contact"},
			exactExpected: []string{"chiropractor", "denver"},
		},
		{
			name:          "operators and stopwords dropped",
			query:         "site:yelp.com best yoga studio in austin inurl:contact",
			wantContains:  []string{"yoga", "studio", "austin"},
			wantExcludes:  []string{"best", "in", "site", "yelp", "inurl", "contact"},
			exactExpected: []string{"yoga", "studio", "austin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QueryTokens(tt.query)
			if tt.exactExpected != nil {
				if len(got) != len(tt.exactExpected) {
					t.Fatalf("QueryTokens(%q) = %v; want %v", tt.query, got, tt.exactExpected)
				}
				for i := range got {
					if got[i] != tt.exactExpected[i] {
						t.Errorf("QueryTokens(%q)[%d] = %q; want %q", tt.query, i, got[i], tt.exactExpected[i])
					}
				}
			}
			for _, w := range tt.wantContains {
				found := false
				for _, g := range got {
					if g == w {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("QueryTokens(%q) missing expected token %q, got: %v", tt.query, w, got)
				}
			}
			for _, w := range tt.wantExcludes {
				for _, g := range got {
					if g == w {
						t.Errorf("QueryTokens(%q) contains excluded token %q", tt.query, w)
					}
				}
			}
		})
	}
}

func TestIsRelevant_Stems(t *testing.T) {
	tests := []struct {
		name   string
		r      SERPResult
		tokens []string
		want   bool
	}{
		{
			name: "chiropractor query matches chiropractic title",
			r: SERPResult{
				URL:     "https://example.com/clinic",
				Title:   "Denver Chiropractic Clinic",
				Snippet: "Expert spine and joint care.",
			},
			tokens: []string{"chiropractor", "denver"},
			want:   true,
		},
		{
			name: "chiropractic query matches chiropractor in snippet",
			r: SERPResult{
				URL:     "https://example.com/team",
				Title:   "Meet Our Specialists",
				Snippet: "Dr. Smith is a licensed chiropractor in Denver.",
			},
			tokens: []string{"chiropractic", "denver"},
			want:   true,
		},
		{
			name: "physiotherapy query matches physio in path",
			r: SERPResult{
				URL:     "https://dublinhealth.ie/physio-services",
				Title:   "Dublin Health Centre",
				Snippet: "Physical rehabilitation and wellness.",
			},
			tokens: []string{"physiotherapy", "dublin"},
			want:   true,
		},
		{
			name: "massage query matches massages and massag stem",
			r: SERPResult{
				URL:     "https://example.co.uk/services",
				Title:   "Brighton Massages & Holistic Care",
				Snippet: "Deep tissue and relaxation treatments.",
			},
			tokens: []string{"massage", "brighton"},
			want:   true,
		},
		{
			name: "unrelated page does not match stems",
			r: SERPResult{
				URL:     "https://www.merriam-webster.com/dictionary/day",
				Title:   "DAY Definition & Meaning - Merriam-Webster",
				Snippet: "The meaning of DAY is the time of light between one night and the next.",
			},
			tokens: []string{"spa", "honolulu"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRelevant(tt.r, tt.tokens)
			if got != tt.want {
				t.Errorf("IsRelevant() = %v; want %v", got, tt.want)
			}
		})
	}
}

func TestFilterRelevant_PoisonedCase(t *testing.T) {
	// The real production bug case for query "day spa honolulu":
	// Bing returned calendar and dictionary sites where title echoed the query.
	poisoned := []SERPResult{
		{
			URL:     "https://www.calendardate.com/todays.htm",
			Title:   "Today's Date - CalendarDate.com",
			Snippet: "Details about today's date with holidays and calendar info.",
		},
		{
			URL:     "https://todaydateandtime.com/",
			Title:   "Today's Date and Time - Current Date, Time & More",
			Snippet: "Find out today's date, current time, and day of the week.",
		},
		{
			URL:     "https://www.merriam-webster.com/dictionary/day",
			Title:   "DAY Definition & Meaning - Merriam-Webster",
			Snippet: "The meaning of DAY is the time of light between one night and the next.",
		},
	}

	kept, ratio := FilterRelevant(poisoned, "day spa honolulu")
	if len(kept) != 0 {
		t.Errorf("FilterRelevant(poisoned) kept %d results; want 0", len(kept))
	}
	if ratio != 0.0 {
		t.Errorf("FilterRelevant(poisoned) ratio = %f; want 0.0", ratio)
	}
}

func TestFilterRelevant_GoodCase(t *testing.T) {
	good := []SERPResult{
		{
			URL:     "https://www.spaluanahonolulu.com/",
			Title:   "Spa Luana Honolulu",
			Snippet: "Premier luxury day spa in Honolulu offering facials and massage.",
		},
		{
			URL:     "https://www.yelp.com/search?cflt=spas&find_loc=Honolulu%2C+HI",
			Title:   "Best Day Spas in Honolulu - Yelp",
			Snippet: "Top 10 Best Day Spas in Honolulu, HI. Reviews and recommendations.",
		},
		{
			URL:     "https://honolulumedspa.com/",
			Title:   "Medical Spa Treatments | Honolulu",
			Snippet: "Advanced aesthetic treatments and relaxing body care in Hawaii.",
		},
	}

	kept, ratio := FilterRelevant(good, "day spa honolulu")
	if len(kept) != len(good) {
		t.Errorf("FilterRelevant(good) kept %d; want %d", len(kept), len(good))
	}
	if ratio < 0.6 {
		t.Errorf("FilterRelevant(good) ratio = %f; want >= 0.6", ratio)
	}
}
