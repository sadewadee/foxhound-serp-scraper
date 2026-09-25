package config

import (
	"os"
	"testing"
)

// SearXNG config must behave identically through the YAML path and the env
// path (CLAUDE.md convention): field names, defaults, and values in sync.
func TestSearXNGConfigDefaults(t *testing.T) {
	yml := `
serp:
  engines: searxng,duckduckgo
`
	// YAML without searxng keys: defaults apply.
	cfg, err := LoadFromString(yml)
	if err != nil {
		t.Fatalf("LoadFromString: %v", err)
	}
	if cfg.SERP.SearXNGMaxPages != 2 {
		t.Errorf("SearXNGMaxPages default = %d, want 2", cfg.SERP.SearXNGMaxPages)
	}
	if cfg.SERP.SearXNGTimeoutMs != 15000 {
		t.Errorf("SearXNGTimeoutMs default = %d, want 15000", cfg.SERP.SearXNGTimeoutMs)
	}
	if cfg.SERP.SearXNGDelayMs != 1000 {
		t.Errorf("SearXNGDelayMs default = %d, want 1000", cfg.SERP.SearXNGDelayMs)
	}
	if cfg.SERP.SearXNGURL != "" {
		t.Errorf("SearXNGURL default = %q, want empty", cfg.SERP.SearXNGURL)
	}

	yml2 := `
serp:
  engines: searxng
  searxng_url: http://searxng:8080
  searxng_max_pages: 3
  searxng_timeout_ms: 5000
  searxng_engines: google,brave
  searxng_delay_ms: 2000
`
	cfg2, err := LoadFromString(yml2)
	if err != nil {
		t.Fatalf("LoadFromString: %v", err)
	}
	if cfg2.SERP.SearXNGURL != "http://searxng:8080" {
		t.Errorf("SearXNGURL = %q", cfg2.SERP.SearXNGURL)
	}
	if cfg2.SERP.SearXNGMaxPages != 3 {
		t.Errorf("SearXNGMaxPages = %d", cfg2.SERP.SearXNGMaxPages)
	}
	if cfg2.SERP.SearXNGTimeoutMs != 5000 {
		t.Errorf("SearXNGTimeoutMs = %d", cfg2.SERP.SearXNGTimeoutMs)
	}
	if cfg2.SERP.SearXNGEngines != "google,brave" {
		t.Errorf("SearXNGEngines = %q", cfg2.SERP.SearXNGEngines)
	}
	if cfg2.SERP.SearXNGDelayMs != 2000 {
		t.Errorf("SearXNGDelayMs = %d", cfg2.SERP.SearXNGDelayMs)
	}

	t.Run("env", func(t *testing.T) {
		for _, kv := range [][2]string{
			{"SEARXNG_URL", "http://searxng:8080"},
			{"SEARXNG_MAX_PAGES", "4"},
			{"SEARXNG_TIMEOUT_MS", "7000"},
			{"SEARXNG_ENGINES", "brave"},
			{"SEARXNG_DELAY_MS", "500"},
		} {
			old, had := os.LookupEnv(kv[0])
			os.Setenv(kv[0], kv[1])
			defer func(k, v string, h bool) {
				if h {
					os.Setenv(k, v)
				} else {
					os.Unsetenv(k)
				}
			}(kv[0], old, had)
		}
		cfg3, err := LoadFromEnv()
		if err != nil {
			t.Fatalf("LoadFromEnv: %v", err)
		}
		if cfg3.SERP.SearXNGURL != "http://searxng:8080" {
			t.Errorf("env SearXNGURL = %q", cfg3.SERP.SearXNGURL)
		}
		if cfg3.SERP.SearXNGMaxPages != 4 {
			t.Errorf("env SearXNGMaxPages = %d", cfg3.SERP.SearXNGMaxPages)
		}
		if cfg3.SERP.SearXNGTimeoutMs != 7000 {
			t.Errorf("env SearXNGTimeoutMs = %d", cfg3.SERP.SearXNGTimeoutMs)
		}
		if cfg3.SERP.SearXNGEngines != "brave" {
			t.Errorf("env SearXNGEngines = %q", cfg3.SERP.SearXNGEngines)
		}
		if cfg3.SERP.SearXNGDelayMs != 500 {
			t.Errorf("env SearXNGDelayMs = %d", cfg3.SERP.SearXNGDelayMs)
		}
	})
}
