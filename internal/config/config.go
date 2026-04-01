package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres     PostgresConfig     `yaml:"postgres"`
	Redis        RedisConfig        `yaml:"redis"`
	SERP         SERPConfig         `yaml:"serp"`
	Website      WebsiteConfig      `yaml:"website"`
	Enrich       EnrichConfig       `yaml:"enrich"`
	Proxy        ProxyConfig        `yaml:"proxy"`
	Fetch        FetchConfig        `yaml:"fetch"`
	Monitor      MonitorConfig      `yaml:"monitor"`
	Mordibouncer MordibouncerConfig `yaml:"mordibouncer"`
	API          APIConfig          `yaml:"api"`
	Telegram     TelegramConfig     `yaml:"telegram"`
}

type APIConfig struct {
	Addr   string    `yaml:"addr"`
	Secret string    `yaml:"secret"`
	Users  []APIUser `yaml:"users"`
}

type TelegramConfig struct {
	BotToken       string  `yaml:"bot_token"`
	AllowedChatIDs []int64 `yaml:"allowed_chat_ids"`
}

type APIUser struct {
	Username string `yaml:"username"`
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
	PagesPerQuery         int    `yaml:"pages_per_query"`
	ResultsPerPage        int    `yaml:"results_per_page"`
	DelayBetweenPagesMs   int    `yaml:"delay_between_pages_ms"`
	DelayBetweenQueriesMs int    `yaml:"delay_between_queries_ms"`
	Concurrency           int    `yaml:"concurrency"`       // number of tabs (goroutines)
	SERPDelayMs           int    `yaml:"serp_delay_ms"`     // inter-page delay override (0 = use timing profile)
	Engines               string `yaml:"engines"`           // "all", "google", "bing", "duckduckgo" (default: "all")
	GoogleMaxPages        int    `yaml:"google_max_pages"`  // default 3
	BingMaxPages          int    `yaml:"bing_max_pages"`    // default 5
	DDGMaxPages           int    `yaml:"ddg_max_pages"`     // default 1
}

type WebsiteConfig struct {
	Concurrency  int  `yaml:"concurrency"`
	ContactPages bool `yaml:"contact_pages"`
	TimeoutMs    int  `yaml:"timeout_ms"`
}

type EnrichConfig struct {
	Concurrency      int  `yaml:"concurrency"`
	TimeoutMs        int  `yaml:"timeout_ms"`
	ValidateMX       bool `yaml:"validate_mx"`
	SocialExtraction bool `yaml:"social_extraction"`
	ValidateEmail    bool `yaml:"validate_email"`
	ContactPages     bool `yaml:"contact_pages"`
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

	// Browser lifecycle tuning — controls memory usage vs restart overhead.
	PageReuseLimit       int `yaml:"page_reuse_limit"`       // requests before browser page recycle (default 200)
	BrowserReuseLimit    int `yaml:"browser_reuse_limit"`    // page recycles before full browser restart (default 2)
	StealthRecycleAfter  int `yaml:"stealth_recycle_after"`  // requests before stealth fetcher recycle (default 500)
	SERPMaxRequests      int `yaml:"serp_max_requests"`      // MaxBrowserRequests for SERP browser (default 200)
	EnrichMaxRequests    int `yaml:"enrich_max_requests"`    // MaxBrowserRequests for enrich browser (default 300)
	BrowserTimeoutSec    int `yaml:"browser_timeout_sec"`    // browser-level timeout in seconds (default 60)
	ReconcilerIntervalMs int `yaml:"reconciler_interval_ms"` // reconciler tick interval in ms (default 60000)

	// Persister — batch flush from Redis result queues to DB.
	PersistIntervalMs int `yaml:"persist_interval_ms"` // flush interval in ms (default 5000)
	PersistBatchSize  int `yaml:"persist_batch_size"`  // max items per flush (default 500)

	// Beta features (foxhound v0.0.7) — set BETA_FEATURES=1 to enable all.
	BetaFeatures     bool `yaml:"beta_features"`      // master switch for all beta features
	CircuitBreaker   bool `yaml:"circuit_breaker"`     // per-domain circuit breaker on SERP fetches
	DomainScorer     bool `yaml:"domain_scorer"`       // Bayesian domain risk scoring on enrich (stealth→browser auto-escalation)
	SessionFatigue   bool `yaml:"session_fatigue"`     // human-like warmup/fatigue timing on browser
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

// LoadFromEnv creates config purely from environment variables with defaults.
// Used when no config.yaml file exists (e.g. Docker containers).
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		Postgres: PostgresConfig{
			DSN:          os.Getenv("POSTGRES_DSN"),
			MaxOpenConns: parseEnvInt("PG_MAX_OPEN_CONNS", 0),
			MaxIdleConns: parseEnvInt("PG_MAX_IDLE_CONNS", 0),
		},
		Redis: RedisConfig{
			Addr:     os.Getenv("REDIS_ADDR"),
			Password: os.Getenv("REDIS_PASSWORD"),
		},
		Proxy: ProxyConfig{
			URL: os.Getenv("PROXY_URL"),
		},
		SERP: SERPConfig{
			SERPDelayMs:    parseEnvInt("SERP_DELAY_MS", 0),
			Engines:        os.Getenv("SERP_ENGINES"),
			GoogleMaxPages: parseEnvInt("GOOGLE_MAX_PAGES", 0),
			BingMaxPages:   parseEnvInt("BING_MAX_PAGES", 0),
			DDGMaxPages:    parseEnvInt("DDG_MAX_PAGES", 0),
		},
		Fetch: FetchConfig{
			Headless:             true,
			BlockImages:          true,
			PageReuseLimit:       parseEnvInt("PAGE_REUSE_LIMIT", 0),
			BrowserReuseLimit:    parseEnvInt("BROWSER_REUSE_LIMIT", 0),
			StealthRecycleAfter:  parseEnvInt("STEALTH_RECYCLE_AFTER", 0),
			SERPMaxRequests:      parseEnvInt("SERP_MAX_REQUESTS", 0),
			EnrichMaxRequests:    parseEnvInt("ENRICH_MAX_REQUESTS", 0),
			BrowserTimeoutSec:    parseEnvInt("BROWSER_TIMEOUT_SEC", 0),
			ReconcilerIntervalMs: parseEnvInt("RECONCILER_INTERVAL_MS", 0),
			PersistIntervalMs:    parseEnvInt("PERSIST_INTERVAL_MS", 0),
			PersistBatchSize:     parseEnvInt("PERSIST_BATCH_SIZE", 0),
			BetaFeatures:         os.Getenv("BETA_FEATURES") == "1",
			CircuitBreaker:       os.Getenv("BETA_CIRCUIT_BREAKER") == "1",
			DomainScorer:         os.Getenv("BETA_DOMAIN_SCORER") == "1",
			SessionFatigue:       os.Getenv("BETA_SESSION_FATIGUE") == "1",
		},
		Mordibouncer: MordibouncerConfig{
			APIURL: os.Getenv("MORDIBOUNCER_API_URL"),
			Secret: os.Getenv("MORDIBOUNCER_SECRET"),
		},
		API: APIConfig{
			Addr:   ":8080",
			Secret: os.Getenv("API_SECRET"),
			Users: []APIUser{{
				Username: "admin",
				APIKey:   os.Getenv("API_KEY"),
				Role:     "admin",
			}},
		},
		Telegram: TelegramConfig{
			BotToken:       os.Getenv("TELEGRAM_BOT_TOKEN"),
			AllowedChatIDs: parseIntList(os.Getenv("TELEGRAM_ALLOWED_USERS")),
		},
		Monitor: MonitorConfig{
			Enabled: true,
			Port:    9090,
		},
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
		cfg.Postgres.MaxOpenConns = 5
	}
	if cfg.Postgres.MaxIdleConns == 0 {
		cfg.Postgres.MaxIdleConns = 2
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
	if cfg.SERP.Concurrency == 0 {
		cfg.SERP.Concurrency = 4
	}
	if cfg.SERP.Engines == "" {
		cfg.SERP.Engines = "all"
	}
	if cfg.SERP.GoogleMaxPages == 0 {
		cfg.SERP.GoogleMaxPages = 3
	}
	if cfg.SERP.BingMaxPages == 0 {
		cfg.SERP.BingMaxPages = 5
	}
	if cfg.SERP.DDGMaxPages == 0 {
		cfg.SERP.DDGMaxPages = 1
	}
	if cfg.Website.Concurrency == 0 {
		cfg.Website.Concurrency = 5
	}
	if cfg.Website.TimeoutMs == 0 {
		cfg.Website.TimeoutMs = 20000
	}
	if cfg.Enrich.Concurrency == 0 {
		cfg.Enrich.Concurrency = 10
	}
	if cfg.Enrich.TimeoutMs == 0 {
		cfg.Enrich.TimeoutMs = 15000
	}
	if cfg.Monitor.Port == 0 {
		cfg.Monitor.Port = 9090
	}
	if cfg.Fetch.PageReuseLimit == 0 {
		cfg.Fetch.PageReuseLimit = 200
	}
	if cfg.Fetch.BrowserReuseLimit == 0 {
		cfg.Fetch.BrowserReuseLimit = 2
	}
	if cfg.Fetch.StealthRecycleAfter == 0 {
		cfg.Fetch.StealthRecycleAfter = 500
	}
	if cfg.Fetch.SERPMaxRequests == 0 {
		cfg.Fetch.SERPMaxRequests = 200
	}
	if cfg.Fetch.EnrichMaxRequests == 0 {
		cfg.Fetch.EnrichMaxRequests = 300
	}
	if cfg.Fetch.BrowserTimeoutSec == 0 {
		cfg.Fetch.BrowserTimeoutSec = 60
	}
	if cfg.Fetch.ReconcilerIntervalMs == 0 {
		cfg.Fetch.ReconcilerIntervalMs = 60000
	}
	if cfg.Fetch.PersistIntervalMs == 0 {
		cfg.Fetch.PersistIntervalMs = 5000
	}
	if cfg.Fetch.PersistBatchSize == 0 {
		cfg.Fetch.PersistBatchSize = 500
	}
	// Default bools that YAML unmarshals as false when missing.
	// We use a separate "defaults applied" check via Concurrency > 0 above.
	if cfg.Enrich.Concurrency > 0 && !cfg.Enrich.ContactPages {
		cfg.Enrich.ContactPages = true
	}
	if cfg.Enrich.Concurrency > 0 && !cfg.Enrich.SocialExtraction {
		cfg.Enrich.SocialExtraction = true
	}
}

// parseEnvInt reads an integer from an environment variable, returning defaultVal if unset or invalid.
func parseEnvInt(key string, defaultVal int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return v
}

// parseIntList parses a comma-separated string of int64 values.
func parseIntList(s string) []int64 {
	if s == "" {
		return nil
	}
	var result []int64
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err == nil {
			result = append(result, n)
		}
	}
	return result
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
