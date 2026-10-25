package config

import "testing"

// ENRICH_TIMEOUT_MS must reach cfg.Enrich.TimeoutMs through the env-only path
// (production has no config.yaml); unset keeps the 15000 ms default.
func TestEnrichTimeoutFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want int
	}{
		{"unset", "", 15000},
		{"override", "60000", 60000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ENRICH_TIMEOUT_MS", tc.env)
			cfg, err := LoadFromEnv()
			if err != nil {
				t.Fatalf("LoadFromEnv: %v", err)
			}
			if cfg.Enrich.TimeoutMs != tc.want {
				t.Errorf("Enrich.TimeoutMs = %d, want %d", cfg.Enrich.TimeoutMs, tc.want)
			}
		})
	}
}

// The YAML path must honour the same variable via ${VAR:-default} expansion.
func TestEnrichTimeoutYAMLExpansion(t *testing.T) {
	const y = "enrich:\n  timeout_ms: ${ENRICH_TIMEOUT_MS:-15000}\n"
	t.Setenv("ENRICH_TIMEOUT_MS", "")
	cfg, err := LoadFromString(y)
	if err != nil {
		t.Fatalf("LoadFromString: %v", err)
	}
	if cfg.Enrich.TimeoutMs != 15000 {
		t.Errorf("default = %d, want 15000", cfg.Enrich.TimeoutMs)
	}
	t.Setenv("ENRICH_TIMEOUT_MS", "60000")
	cfg, err = LoadFromString(y)
	if err != nil {
		t.Fatalf("LoadFromString: %v", err)
	}
	if cfg.Enrich.TimeoutMs != 60000 {
		t.Errorf("override = %d, want 60000", cfg.Enrich.TimeoutMs)
	}
}

// ENRICH_DISCOVER_LINKS ships off and is enabled only by "1", on both the
// env-only and the YAML paths.
func TestEnrichDiscoverLinks(t *testing.T) {
	const y = "enrich:\n  discover_links: false\n"
	for _, tc := range []struct {
		env  string
		want bool
	}{{"", false}, {"0", false}, {"1", true}} {
		t.Run("env="+tc.env, func(t *testing.T) {
			t.Setenv("ENRICH_DISCOVER_LINKS", tc.env)
			cfg, err := LoadFromEnv()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Enrich.DiscoverLinks != tc.want {
				t.Errorf("LoadFromEnv = %v, want %v", cfg.Enrich.DiscoverLinks, tc.want)
			}
			cfg, err = LoadFromString(y)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Enrich.DiscoverLinks != tc.want {
				t.Errorf("LoadFromString = %v, want %v", cfg.Enrich.DiscoverLinks, tc.want)
			}
		})
	}
}
