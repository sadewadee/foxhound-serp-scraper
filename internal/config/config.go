package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres     PostgresConfig     `yaml:"postgres"`
	Redis        RedisConfig        `yaml:"redis"`
	SERP         SERPConfig         `yaml:"serp"`
	Website      WebsiteConfig      `yaml:"website"`
	Contact      ContactConfig      `yaml:"contact"`
	Proxy        ProxyConfig        `yaml:"proxy"`
	Fetch        FetchConfig        `yaml:"fetch"`
	Monitor      MonitorConfig      `yaml:"monitor"`
	Mordibouncer MordibouncerConfig `yaml:"mordibouncer"`
	API          APIConfig          `yaml:"api"`
	Dokploy      DokployConfig      `yaml:"dokploy"`
}

type APIConfig struct {
	Addr   string     `yaml:"addr"`
	Secret string     `yaml:"secret"`
	Users  []APIUser  `yaml:"users"`
}

type DokployConfig struct {
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

type APIUser struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	APIKey   string `yaml:"api_key"`
	Role     string `yaml:"role"`
}

type PostgresConfig struct {
	DSN          string `yaml:"dsn"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type SERPConfig struct {
	PagesPerQuery        int `yaml:"pages_per_query"`
	ResultsPerPage       int `yaml:"results_per_page"`
	DelayBetweenPagesMs  int `yaml:"delay_between_pages_ms"`
	DelayBetweenQueriesMs int `yaml:"delay_between_queries_ms"`
	Workers              int `yaml:"workers"`
}

type WebsiteConfig struct {
	Workers      int  `yaml:"workers"`
	ContactPages bool `yaml:"contact_pages"`
	TimeoutMs    int  `yaml:"timeout_ms"`
}

type ContactConfig struct {
	Workers          int  `yaml:"workers"`
	TimeoutMs        int  `yaml:"timeout_ms"`
	ValidateMX       bool `yaml:"validate_mx"`
	SocialExtraction bool `yaml:"social_extraction"`
	ValidateEmail    bool `yaml:"validate_email"`
}

type MordibouncerConfig struct {
	APIURL string `yaml:"api_url"`
	Secret string `yaml:"secret"`
}

type ProxyConfig struct {
	URL string `yaml:"url"`
}

type FetchConfig struct {
	Headless    bool `yaml:"headless"`
	BlockImages bool `yaml:"block_images"`
}

type MonitorConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

// envVarRe matches ${VAR} or ${VAR:-default} patterns.
var envVarRe = regexp.MustCompile(`\$\{([a-zA-Z_][a-zA-Z0-9_]*)(?::-([^}]*))?\}`)

// expandEnv replaces ${VAR} and ${VAR:-default} in s with env values.
func expandEnv(s string) string {
	return envVarRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := envVarRe.FindStringSubmatch(match)
		if parts == nil {
			return match
		}
		name := parts[1]
		defaultVal := parts[2]
		if val := os.Getenv(name); val != "" {
			return val
		}
		return defaultVal
	})
}

// Load reads a YAML config file and expands environment variables.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}

	// Expand env vars before parsing YAML.
	expanded := expandEnv(string(data))

	cfg := &Config{}
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("config: parsing %s: %w", path, err)
	}

	setDefaults(cfg)
	return cfg, nil
}

// LoadFromString parses config from a YAML string (for testing).
func LoadFromString(s string) (*Config, error) {
	expanded := expandEnv(s)
	cfg := &Config{}
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("config: parsing: %w", err)
	}
	setDefaults(cfg)
	return cfg, nil
}

func setDefaults(cfg *Config) {
	if cfg.Redis.Addr == "" {
		cfg.Redis.Addr = "localhost:6379"
	}
	if cfg.Postgres.MaxOpenConns == 0 {
		cfg.Postgres.MaxOpenConns = 50
	}
	if cfg.Postgres.MaxIdleConns == 0 {
		cfg.Postgres.MaxIdleConns = 10
	}
	if cfg.SERP.PagesPerQuery == 0 {
		cfg.SERP.PagesPerQuery = 10
	}
	if cfg.SERP.ResultsPerPage == 0 {
		cfg.SERP.ResultsPerPage = 10
	}
	if cfg.SERP.DelayBetweenPagesMs == 0 {
		cfg.SERP.DelayBetweenPagesMs = 5000
	}
	if cfg.SERP.DelayBetweenQueriesMs == 0 {
		cfg.SERP.DelayBetweenQueriesMs = 30000
	}
	if cfg.SERP.Workers == 0 {
		cfg.SERP.Workers = 2
	}
	if cfg.Website.Workers == 0 {
		cfg.Website.Workers = 5
	}
	if cfg.Website.TimeoutMs == 0 {
		cfg.Website.TimeoutMs = 20000
	}
	if cfg.Contact.Workers == 0 {
		cfg.Contact.Workers = 10
	}
	if cfg.Contact.TimeoutMs == 0 {
		cfg.Contact.TimeoutMs = 15000
	}
	if cfg.Monitor.Port == 0 {
		cfg.Monitor.Port = 9090
	}
	// Default contact_pages and social_extraction to true.
	// YAML unmarshals missing bools as false, so we check string presence.
	// For simplicity, just default these in the config example.
}

// DSN returns the Postgres DSN, trying env var POSTGRES_DSN if config is empty.
func (c *Config) DSN() string {
	dsn := c.Postgres.DSN
	if dsn == "" {
		dsn = os.Getenv("POSTGRES_DSN")
	}
	// Strip quotes if present.
	dsn = strings.Trim(dsn, "\"'")
	return dsn
}
