package health

import (
	"testing"
)

func TestParseCamoufoxVersionOutput(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		want    string
		wantErr bool
	}{
		{
			name:   "standard output",
			output: "Camoufox 0.4.11 (Firefox 135.0.1)",
			want:   "135.0.1",
		},
		{
			name:   "beta suffix",
			output: "Camoufox 0.4.9 (Firefox 135.0.1-beta.24)",
			want:   "135.0.1-beta.24",
		},
		{
			name:   "multiline output",
			output: "fetching browser...\nCamoufox 0.4.11 (Firefox 135.0.1)\ndone",
			want:   "135.0.1",
		},
		{
			name:   "uppercase Firefox",
			output: "FIREFOX 142.0",
			want:   "142.0",
		},
		{
			name:    "no version present",
			output:  "something went wrong",
			wantErr: true,
		},
		{
			name:    "empty output",
			output:  "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCamoufoxVersionOutput(tc.output)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (result=%q)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseMajor(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"135.0.1", 135},
		{"135.0.1-beta.24", 135},
		{"150.0.1", 150},
		{"0.4.11", 0},
		{"", 0},
		{"abc", 0},
		{"135", 135},
	}

	for _, tc := range tests {
		got := parseMajor(tc.input)
		if got != tc.want {
			t.Errorf("parseMajor(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}
