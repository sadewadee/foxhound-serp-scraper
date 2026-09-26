package config

import (
	"os"
	"testing"
)

// TestDirectoryConfig_Defaults pins the shipped state: the DataDome module is
// OFF and unconfigured, which is what every compose file sets. If this test
// ever fails on DataDomeEnabled being true by default, the module would go
// live without a residential proxy.
func TestDirectoryConfig_Defaults(t *testing.T) {
	for _, key := range []string{
		"DIRECTORY_DATADOME_ENABLED", "DIRECTORY_PROXY_URL", "DIRECTORY_PROXY_STICKY",
		"DATADOME_SOLVER", "CAPSOLVER_API_KEY",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.Directory.DataDomeEnabled {
		t.Error("DataDomeEnabled defaults to true — the module must ship OFF")
	}
	if cfg.Directory.ProxyURL != "" {
		t.Error("ProxyURL defaults to non-empty")
	}
	if cfg.Directory.Solver != "none" {
		t.Errorf("Solver defaults to %q, want none", cfg.Directory.Solver)
	}
}

func TestDirectoryConfig_FromEnv(t *testing.T) {
	t.Setenv("DIRECTORY_DATADOME_ENABLED", "1")
	t.Setenv("DIRECTORY_PROXY_URL", "http://residential.example:8080")
	t.Setenv("DIRECTORY_PROXY_STICKY", "1")
	t.Setenv("DATADOME_SOLVER", "capsolver")
	t.Setenv("CAPSOLVER_API_KEY", "secret-key-value")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if !cfg.Directory.DataDomeEnabled {
		t.Error("DataDomeEnabled not read from env")
	}
	if cfg.Directory.ProxyURL != "http://residential.example:8080" {
		t.Errorf("ProxyURL = %q", cfg.Directory.ProxyURL)
	}
	if !cfg.Directory.ProxySticky {
		t.Error("ProxySticky not read from env")
	}
	if cfg.Directory.Solver != "capsolver" {
		t.Errorf("Solver = %q", cfg.Directory.Solver)
	}
	if cfg.Directory.CapsolverAPIKey != "secret-key-value" {
		t.Errorf("CapsolverAPIKey = %q", cfg.Directory.CapsolverAPIKey)
	}
}

// TestDirectoryConfig_SolverFallsBackToNone keeps an unset or blank solver
// from reaching the DataDome path as an empty string, which DecideSolve
// treats as "no solver" but which is worth normalizing in config.
func TestDirectoryConfig_SolverFallsBackToNone(t *testing.T) {
	t.Setenv("DATADOME_SOLVER", "")
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.Directory.Solver != "none" {
		t.Errorf("Solver = %q, want none", cfg.Directory.Solver)
	}
}
