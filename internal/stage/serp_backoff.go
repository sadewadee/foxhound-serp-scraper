package stage

import (
	"sync"
	"time"
)

// Engine-level suspension backoff.
//
// A suspended SearXNG upstream is a property of the ENGINE, not of the worker
// that happened to hit it: every tab on the host would learn the same thing on
// its next request. Sleeping the whole tab (the pre-#64 behaviour) therefore
// bought nothing and starved the host's other engines — prod showed 0
// duckduckgo pages in 12 minutes because both tabs were parked behind a 4
// minute SearXNG backoff.
//
// Instead the deadline is shared per stage. While an engine is inside its
// backoff window its buffer list is simply not read, so tabs keep draining the
// other engines; a tab only waits when EVERY engine it serves is backed off.
//
// This file carries no build tag: the state and the key-selection rules are
// plain stdlib so both build variants can test them.

// MaxAllEnginesWait caps how long a worker sleeps when every engine it serves
// is inside a backoff window. Short enough to re-check often, long enough not
// to spin.
const MaxAllEnginesWait = 30 * time.Second

// EngineBackoff is the shared suspension state for one engine across every tab
// worker in a stage. The zero value is ready to use and means "not backing off".
type EngineBackoff struct {
	mu     sync.Mutex
	streak int
	until  time.Time
}

// NewEngineBackoff returns an engine backoff that is not currently in a window.
func NewEngineBackoff() *EngineBackoff { return &EngineBackoff{} }

// RecordSuspension escalates the streak and returns the new deadline. The
// escalation is the same 30s → 10min curve the tab-local streak used, so a
// sustained outage still backs off to the cap instead of hammering SearXNG.
func (b *EngineBackoff) RecordSuspension(now time.Time) (deadline time.Time, streak int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streak++
	delay := SuspensionBackoff(b.streak)
	b.until = now.Add(delay)
	return b.until, b.streak
}

// RecordSuccess clears the window: the first usable page proves the upstream
// recovered, so the next suspension starts from the base delay again.
func (b *EngineBackoff) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streak = 0
	b.until = time.Time{}
}

// State returns the current deadline and streak, for logging.
func (b *EngineBackoff) State() (deadline time.Time, streak int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.until, b.streak
}

// Remaining reports how much of the backoff window is left. Zero once the
// deadline has passed, or when the engine was never backed off.
func (b *EngineBackoff) Remaining(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.until.IsZero() {
		return 0
	}
	if d := b.until.Sub(now); d > 0 {
		return d
	}
	return 0
}

// BackedOff reports whether the engine is still inside its window. A worker
// uses it to skip this engine's buffer list entirely.
func (b *EngineBackoff) BackedOff(now time.Time) bool {
	return b.Remaining(now) > 0
}

// SelectBufferKeys returns the Redis LISTs a worker should read: every engine
// key that is not currently backed off, plus legacyKey appended last so a
// deploy landing on top of a non-empty shared buffer still drains it.
//
// BLPOP pops from the first non-empty key, so key order is a strict priority.
// The readable engine keys are therefore rotated by turn (a per-tab counter):
// each engine is first in turn, and BLPOP still falls through to the next key
// when a list is empty. Without it an engine whose buffer is always non-empty
// starved every engine after it (prod: 0 duckduckgo pops in 30 min).
//
// backedOff reports whether a given key is parked right now. allBackedOff is
// true when every engine key was filtered out, which is the only case where a
// worker has nothing to read and should wait.
func SelectBufferKeys(engineKeys []string, legacyKey string, turn int, backedOff func(string) bool) (keys []string, allBackedOff bool) {
	keys = make([]string, 0, len(engineKeys)+1)
	allBackedOff = len(engineKeys) > 0
	for _, k := range engineKeys {
		if backedOff != nil && backedOff(k) {
			continue
		}
		keys = append(keys, k)
		allBackedOff = false
	}
	if n := len(keys); n > 1 {
		r := turn % n
		if r < 0 {
			r += n
		}
		keys = append(keys[r:], keys[:r]...)
	}
	// The legacy list is always readable: items for a parked engine are
	// released on arrival rather than fetched, so draining it is always safe.
	if legacyKey != "" {
		keys = append(keys, legacyKey)
	}
	return keys, allBackedOff
}

// WaitForAnyBackoff returns how long to sleep when every engine is parked: the
// shortest remaining window, capped at maxWait. Zero when nothing is backed
// off, so callers can treat a non-positive result as "proceed".
func WaitForAnyBackoff(remaining []time.Duration, maxWait time.Duration) time.Duration {
	var soonest time.Duration
	for _, d := range remaining {
		if d <= 0 {
			// This engine is not parked, so the caller has work to do.
			return 0
		}
		if soonest == 0 || d < soonest {
			soonest = d
		}
	}
	if soonest <= 0 {
		return 0
	}
	if maxWait > 0 && soonest > maxWait {
		return maxWait
	}
	return soonest
}
