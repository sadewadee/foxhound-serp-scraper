package dedup

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sadewadee/serp-scraper/internal/config"
)

// Redis key constants for dedup sets.
const (
	KeyURLs    = "serp:dedup:urls"
	KeyDomains = "serp:dedup:domains"
)

// Store wraps a Redis client for dedup operations using SETs.
type Store struct {
	client *redis.Client
}

// New creates a dedup Store connected to Redis.
func New(cfg *config.RedisConfig) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("dedup: redis ping: %w", err)
	}
	return &Store{client: client}, nil
}

// NewFromClient creates a dedup Store from an existing redis.Client.
func NewFromClient(client *redis.Client) *Store {
	return &Store{client: client}
}

// Add tries to add a value to the dedup set. Returns true if the value was new
// (SADD returned 1), false if it already existed.
func (s *Store) Add(ctx context.Context, key, value string) (bool, error) {
	n, err := s.client.SAdd(ctx, key, value).Result()
	if err != nil {
		return false, fmt.Errorf("dedup: sadd %s: %w", key, err)
	}
	return n > 0, nil
}

// Exists checks whether a value exists in the dedup set.
func (s *Store) Exists(ctx context.Context, key, value string) (bool, error) {
	ok, err := s.client.SIsMember(ctx, key, value).Result()
	if err != nil {
		return false, fmt.Errorf("dedup: sismember %s: %w", key, err)
	}
	return ok, nil
}

// Count returns the number of elements in a dedup set.
func (s *Store) Count(ctx context.Context, key string) (int64, error) {
	n, err := s.client.SCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("dedup: scard %s: %w", key, err)
	}
	return n, nil
}

// Client returns the underlying redis.Client for shared use.
func (s *Store) Client() *redis.Client {
	return s.client
}

// Close releases the Redis client.
func (s *Store) Close() error {
	return s.client.Close()
}

// --- Helper functions ---

// SHA256 computes hex-encoded SHA256 of s.
func SHA256(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}

// HashQuery returns SHA256(lower(trim(query))).
func HashQuery(query string) string {
	return SHA256(strings.ToLower(strings.TrimSpace(query)))
}

// HashURL returns SHA256 of the canonical URL.
func HashURL(rawURL string) string {
	return SHA256(CanonicalURL(rawURL))
}

// HashEmail returns SHA256(lower(email)).
func HashEmail(email string) string {
	return SHA256(strings.ToLower(strings.TrimSpace(email)))
}

// CanonicalURL normalizes a URL for consistent dedup:
// lowercase scheme+host, strip trailing slash, strip fragment, sort query params.
func CanonicalURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return strings.ToLower(rawURL)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	u.Path = strings.TrimRight(u.Path, "/")
	if u.Path == "" {
		u.Path = "/"
	}
	// Sort query params for consistent hashing.
	q := u.Query()
	u.RawQuery = q.Encode()
	return u.String()
}

// ExtractDomain extracts the domain (host without port) from a URL.
func ExtractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	return strings.ToLower(host)
}
