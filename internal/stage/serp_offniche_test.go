//go:build playwright

package stage

import "testing"

// TestIsOffTargetQuery validates the beauty/grooming off-target guard used in
// expandCompletedQueries to prevent legacy queries from being re-expanded.
//
// This is a regression guard — the substring list must ONLY include
// beauty/grooming. Wellness/fitness/yoga/pilates/spa must NOT appear (those are
// on-target categories for this pipeline).
func TestIsOffTargetQuery(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		want    bool
		comment string
	}{
		// ---- MUST be blocked (beauty/grooming) ----
		{
			name:    "hair stylist exact",
			text:    "hair stylist london",
			want:    true,
			comment: "classic off-target beauty query",
		},
		{
			name:    "hair salon mixed case",
			text:    "Hair Salon New York",
			want:    true,
			comment: "case-insensitive match",
		},
		{
			name:    "barbershop single word",
			text:    "barbershop near me",
			want:    true,
			comment: "grooming substring",
		},
		{
			name:    "barber substring match",
			text:    "best barber in Berlin",
			want:    true,
			comment: "barber as standalone word",
		},
		{
			name:    "nail salon",
			text:    "nail salon reviews",
			want:    true,
			comment: "off-target nail category",
		},
		{
			name:    "nail technician",
			text:    "nail technician contact email",
			want:    true,
			comment: "off-target nail technician",
		},
		{
			name:    "beauty salon",
			text:    "beauty salon @gmail.com",
			want:    true,
			comment: "beauty salon category",
		},
		{
			name:    "beauty therapist",
			text:    "beauty therapist classes",
			want:    true,
			comment: "beauty therapist category",
		},
		{
			name:    "lash technician",
			text:    "lash technician best rated",
			want:    true,
			comment: "lash category",
		},
		{
			name:    "makeup artist",
			text:    "makeup artist instagram",
			want:    true,
			comment: "makeup artist category",
		},
		{
			name:    "esthetician",
			text:    "esthetician near me",
			want:    true,
			comment: "esthetician category",
		},
		{
			name:    "skin therapist",
			text:    "skin therapist contact",
			want:    true,
			comment: "skin therapist category",
		},
		{
			name:    "uppercased full string",
			text:    "HAIR STYLIST BERLIN",
			want:    true,
			comment: "all-caps must still match",
		},
		{
			name:    "mid-sentence match",
			text:    "looking for a hair salon downtown",
			want:    true,
			comment: "substring anywhere in text",
		},

		// ---- MUST NOT be blocked (on-target wellness/fitness) ----
		{
			name:    "yoga studio",
			text:    "yoga studio near me",
			want:    false,
			comment: "yoga is on-target",
		},
		{
			name:    "pilates",
			text:    "pilates classes @gmail.com",
			want:    false,
			comment: "pilates is on-target",
		},
		{
			name:    "massage therapist",
			text:    "massage therapist contact",
			want:    false,
			comment: "massage is on-target (not skin therapist)",
		},
		{
			name:    "spa",
			text:    "spa wellness center",
			want:    false,
			comment: "spa is on-target",
		},
		{
			name:    "wellness",
			text:    "wellness center email",
			want:    false,
			comment: "wellness is on-target",
		},
		{
			name:    "barre studio",
			text:    "barre studio near me",
			want:    false,
			comment: "barre is on-target",
		},
		{
			name:    "acupuncture",
			text:    "acupuncture clinic @yahoo.com",
			want:    false,
			comment: "acupuncture is on-target",
		},
		{
			name:    "detox",
			text:    "detox retreat contact",
			want:    false,
			comment: "detox is on-target",
		},
		{
			name:    "cryotherapy",
			text:    "cryotherapy studio reviews",
			want:    false,
			comment: "cryo is on-target",
		},
		{
			name:    "fitness gym",
			text:    "fitness gym email",
			want:    false,
			comment: "fitness is on-target",
		},
		{
			name:    "meditation",
			text:    "meditation center near me",
			want:    false,
			comment: "meditation is on-target",
		},
		{
			name:    "personal trainer",
			text:    "personal trainer instagram",
			want:    false,
			comment: "personal trainer is on-target",
		},
		{
			name:    "empty string",
			text:    "",
			want:    false,
			comment: "empty query must not match",
		},
		// Edge: 'barber' as part of a longer word must still match (substring)
		{
			name:    "barbershop in longer phrase",
			text:    "tony's barbershop and lounge",
			want:    true,
			comment: "barbershop substring inside phrase",
		},
		// Edge: 'nail' ambiguity — 'nail salon' blocks, but 'nail' alone would not
		// (we use the full substring, not just 'nail')
		{
			name:    "nail alone does not block",
			text:    "nail care products wholesale",
			want:    false,
			comment: "bare 'nail' is not in the substring list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isOffTargetQuery(tt.text)
			if got != tt.want {
				t.Errorf("isOffTargetQuery(%q) = %v, want %v [%s]", tt.text, got, tt.want, tt.comment)
			}
		})
	}
}

// TestOffTargetBeautySubstrings_NoWellnessLeak verifies that none of the
// on-target wellness substrings appear in the offTargetBeautySubstrings list.
// If a wellness term accidentally ends up in the list, on-target queries would
// be silently suppressed from re-expansion.
func TestOffTargetBeautySubstrings_NoWellnessLeak(t *testing.T) {
	// These are on-target and must never appear in the block-list.
	onTarget := []string{
		"yoga", "pilates", "massage", "wellness", "spa", "barre", "cryo",
		"detox", "acupuncture", "meditation", "fitness", "gym", "personal trainer",
		"holistic", "ayurveda", "reiki", "chiropractic",
	}

	for _, sub := range offTargetBeautySubstrings {
		for _, on := range onTarget {
			if sub == on {
				t.Errorf("on-target keyword %q found in offTargetBeautySubstrings — this would suppress on-target query expansion", sub)
			}
		}
	}
}
