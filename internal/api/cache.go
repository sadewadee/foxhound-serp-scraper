package api

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// buildCountCacheKey hashes WHERE + args into a stable Redis key.
// %q separates adjacent string args so ["a","bc"] and ["ab","c"] don't collide.
func buildCountCacheKey(where string, args []any) string {
	h := sha1.New()
	h.Write([]byte(where))
	for _, a := range args {
		fmt.Fprintf(h, "|%q|", a)
	}
	return "v2:count:" + hex.EncodeToString(h.Sum(nil))
}

// cachedCount returns COUNT(*) for `SELECT COUNT(*) FROM <fromClause> <where>`.
// Caches result in Redis with TTL. On query timeout or DB error returns (-1, err)
// so callers can serve `count_known=false` instead of HTTP 500.
//
// fromClause includes any join alias, e.g. "business_listings bl".
// where includes the leading "WHERE", e.g. "WHERE bl.domain = $1" or "WHERE 1=1".
//
// SECURITY: fromClause and where are interpolated directly into the SQL string.
// Both MUST be compile-time constants. NEVER pass user input as either; pass
// user values through args and reference them via $N placeholders in where.
func (s *Server) cachedCount(ctx context.Context, fromClause, where string, args []any, ttl, timeout time.Duration) (int, error) {
	// %q quotes both parts so concatenation is unambiguous: an unquoted "|"
	// inside either string cannot collide with the separator.
	key := buildCountCacheKey(fmt.Sprintf("%q|%q", fromClause, where), args)

	if s.redis != nil {
		if v, err := s.redis.Get(ctx, key).Result(); err == nil {
			if n, perr := strconv.Atoi(v); perr == nil {
				return n, nil
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return -1, err
	}
	defer tx.Rollback()

	timeoutMs := int(timeout.Milliseconds())
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%d'", timeoutMs)); err != nil {
		return -1, err
	}

	var total int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", fromClause, where)
	if err := tx.QueryRowContext(ctx, q, args...).Scan(&total); err != nil {
		return -1, err
	}
	if err := tx.Commit(); err != nil {
		return -1, err
	}

	if s.redis != nil {
		_ = s.redis.Set(ctx, key, strconv.Itoa(total), ttl).Err()
	}
	return total, nil
}

// trySingleFlight acquires a Redis NX EX lock for the given key.
// Returns true if caller should compute the value, false if another caller
// is already computing. lockTTL bounds the maximum work duration.
func (s *Server) trySingleFlight(ctx context.Context, key string, lockTTL time.Duration) bool {
	if s.redis == nil {
		return true // no Redis = no coordination = every caller computes
	}
	ok, err := s.redis.SetNX(ctx, key+":lock", "1", lockTTL).Result()
	if err != nil {
		slog.Debug("single-flight lock error", "key", key, "error", err)
		return true // Redis errored — degrade to compute-anyway
	}
	return ok
}

// releaseSingleFlight removes the lock. Best-effort; lock TTL is the safety net.
func (s *Server) releaseSingleFlight(ctx context.Context, key string) {
	if s.redis == nil {
		return
	}
	_ = s.redis.Del(ctx, key+":lock").Err()
}

// waitForCache polls a cache key up to maxWait. Returns value+true if populated,
// "" + false on timeout.
func (s *Server) waitForCache(ctx context.Context, key string, maxWait time.Duration) (string, bool) {
	if s.redis == nil {
		return "", false
	}
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if v, err := s.redis.Get(ctx, key).Result(); err == nil {
			return v, true
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-time.After(50 * time.Millisecond):
		}
	}
	return "", false
}

// cachedJSON serves a JSON response from Redis cache or computes and caches it.
// Adds X-Cache: HIT|WAITED|MISS|BYPASS header for observability.
//
// On cache miss the caller acquires a single-flight lock. Other callers wait
// up to waitOnMiss for the cache to populate. If they time out, they compute
// anyway (degraded mode — better than failing).
func (s *Server) cachedJSON(
	ctx context.Context,
	w http.ResponseWriter,
	key string,
	ttl time.Duration,
	waitOnMiss time.Duration,
	compute func() (any, error),
) {
	// Try cache.
	if s.redis != nil {
		if cached, err := s.redis.Get(ctx, key).Result(); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			io.WriteString(w, cached)
			return
		}
	}

	// Single-flight.
	gotLock := s.trySingleFlight(ctx, key, 20*time.Second)
	if !gotLock {
		if cached, ok := s.waitForCache(ctx, key, waitOnMiss); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "WAITED")
			io.WriteString(w, cached)
			return
		}
		// Timed out waiting — fall through to compute (degraded).
	}
	// Only release if WE acquired the lock. BYPASS callers (timed out waiting)
	// must NOT delete a key they don't own — that would let other concurrent
	// callers re-acquire and partially defeat thundering-herd protection.
	// context.Background() (not ctx) so release fires even if request ctx is
	// already cancelled by the time compute() returns.
	if gotLock {
		defer s.releaseSingleFlight(context.Background(), key)
	}

	value, err := compute()
	if err != nil {
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to compute response")
		return
	}

	body, err := json.Marshal(value)
	if err != nil {
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to marshal response")
		return
	}

	if s.redis != nil {
		_ = s.redis.Set(ctx, key, body, ttl).Err()
	}
	w.Header().Set("Content-Type", "application/json")
	if gotLock {
		w.Header().Set("X-Cache", "MISS")
	} else {
		w.Header().Set("X-Cache", "BYPASS")
	}
	w.Write(body)
}
