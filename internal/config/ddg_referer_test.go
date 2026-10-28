package config

import "testing"

// SERP_DDG_NO_REFERER is off by default and enabled only by "1", on both the
// env-only and the YAML load paths.
func TestDDGNoRefererFlag(t *testing.T) {
	for _, tc := range []struct {
		env  string
		want bool
	}{{"", false}, {"0", false}, {"1", true}} {
		t.Setenv("SERP_DDG_NO_REFERER", tc.env)
		cfg, err := LoadFromEnv()
		if err != nil {
			t.Fatalf("LoadFromEnv: %v", err)
		}
		if cfg.SERP.DDGNoReferer != tc.want {
			t.Errorf("env %q: LoadFromEnv = %v, want %v", tc.env, cfg.SERP.DDGNoReferer, tc.want)
		}
		cfg, err = LoadFromString("serp:\n  pages_per_query: 1\n")
		if err != nil {
			t.Fatalf("LoadFromString: %v", err)
		}
		if cfg.SERP.DDGNoReferer != tc.want {
			t.Errorf("env %q: LoadFromString = %v, want %v", tc.env, cfg.SERP.DDGNoReferer, tc.want)
		}
	}
}
