package api

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestBuildCountCacheKey_StableAcrossCalls(t *testing.T) {
	where := "WHERE bl.domain = $1"
	args := []any{"example.com"}
	a := buildCountCacheKey(where, args)
	b := buildCountCacheKey(where, args)
	if a != b {
		t.Fatalf("expected stable key, got %q vs %q", a, b)
	}
	if a == "" {
		t.Fatal("expected non-empty key")
	}
}

func TestBuildCountCacheKey_DifferentArgs(t *testing.T) {
	where := "WHERE bl.domain = $1"
	a := buildCountCacheKey(where, []any{"a.com"})
	b := buildCountCacheKey(where, []any{"b.com"})
	if a == b {
		t.Fatalf("expected different keys for different args")
	}
}

// Regression: %v formatting would collide ["a","bc"] with ["ab","c"].
func TestBuildCountCacheKey_NoTransposeCollision(t *testing.T) {
	a := buildCountCacheKey("WHERE x = $1 AND y = $2", []any{"a", "bc"})
	b := buildCountCacheKey("WHERE x = $1 AND y = $2", []any{"ab", "c"})
	if a == b {
		t.Fatal("transpose collision: adjacent string args produced the same key")
	}
}

func newTestServer(t *testing.T) (*Server, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := &Server{redis: rdb}
	return s, mr
}

func TestTrySingleFlight_FirstCallerWins(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()

	if !s.trySingleFlight(ctx, "test:key", 5*time.Second) {
		t.Fatal("first caller must win the lock")
	}
	if s.trySingleFlight(ctx, "test:key", 5*time.Second) {
		t.Fatal("second caller must NOT get the lock while held")
	}
}

func TestTrySingleFlight_AfterRelease(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()

	s.trySingleFlight(ctx, "test:key", 5*time.Second)
	s.releaseSingleFlight(ctx, "test:key")
	if !s.trySingleFlight(ctx, "test:key", 5*time.Second) {
		t.Fatal("lock must be acquirable after release")
	}
}

func TestTrySingleFlight_NoRedis(t *testing.T) {
	s := &Server{redis: nil}
	if !s.trySingleFlight(context.Background(), "test:key", 5*time.Second) {
		t.Fatal("nil redis must degrade to compute-anyway (true)")
	}
}

func TestWaitForCache_Populated(t *testing.T) {
	s, mr := newTestServer(t)
	ctx := context.Background()

	mr.Set("test:key", `{"hello":"world"}`)
	v, ok := s.waitForCache(ctx, "test:key", 1*time.Second)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if v != `{"hello":"world"}` {
		t.Fatalf("unexpected value: %q", v)
	}
}

func TestWaitForCache_TimesOut(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()

	start := time.Now()
	_, ok := s.waitForCache(ctx, "test:nonexistent", 200*time.Millisecond)
	elapsed := time.Since(start)
	if ok {
		t.Fatal("expected timeout")
	}
	if elapsed < 200*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Fatalf("waitForCache elapsed %v outside expected 200-400ms window", elapsed)
	}
}
