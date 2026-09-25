package stage

import (
	"testing"
	"time"
)

func TestEngineBackoff_EscalatesAndResets(t *testing.T) {
	b := NewEngineBackoff()
	now := time.Now()

	if b.BackedOff(now) {
		t.Fatal("fresh backoff must not be parked")
	}
	if b.Remaining(now) != 0 {
		t.Fatalf("fresh backoff remaining = %v, want 0", b.Remaining(now))
	}

	// First suspension: 30s window.
	dl, streak := b.RecordSuspension(now)
	if streak != 1 {
		t.Fatalf("streak = %d, want 1", streak)
	}
	if dl.Sub(now) != 30*time.Second {
		t.Errorf("first deadline = %v after now, want 30s", dl.Sub(now))
	}
	if !b.BackedOff(now) {
		t.Error("engine must be parked right after a suspension")
	}

	// Expiry: the window lapses without touching the streak.
	if b.BackedOff(dl.Add(time.Second)) {
		t.Error("engine must stop being parked once the deadline passes")
	}

	// Second suspension escalates to 60s even though the first window lapsed.
	dl2, streak2 := b.RecordSuspension(dl.Add(time.Second))
	if streak2 != 2 {
		t.Fatalf("streak = %d, want 2", streak2)
	}
	if dl2.Sub(dl.Add(time.Second)) != 60*time.Second {
		t.Errorf("second deadline = %v, want 60s", dl2.Sub(now))
	}

	// The first usable page clears everything.
	b.RecordSuccess()
	if b.BackedOff(now) {
		t.Error("success must clear the window even retroactively")
	}
	if _, s := b.State(); s != 0 {
		t.Errorf("streak after success = %d, want 0", s)
	}
}

func TestEngineBackoff_CapsAtTenMinutes(t *testing.T) {
	b := NewEngineBackoff()
	now := time.Now()
	var streak int
	for i := 0; i < 20; i++ {
		_, streak = b.RecordSuspension(now)
	}
	if streak != 20 {
		t.Fatalf("streak = %d, want 20", streak)
	}
	dl, _ := b.State()
	if dl.Sub(now) > SuspensionBackoffCap+time.Second {
		t.Errorf("deadline %v exceeds the 10-minute cap", dl.Sub(now))
	}
}

func TestSelectBufferKeys_ExcludesParkedEngines(t *testing.T) {
	parked := map[string]bool{"serp:buffer:searxng": true}
	keys, allParked := SelectBufferKeys(
		[]string{"serp:buffer:searxng", "serp:buffer:duckduckgo"},
		"serp:buffer",
		func(k string) bool { return parked[k] },
	)
	if allParked {
		t.Fatal("duckduckgo is readable — allParked must be false")
	}
	if len(keys) != 2 || keys[0] != "serp:buffer:duckduckgo" {
		t.Fatalf("keys = %v, want duckduckgo first", keys)
	}
	if keys[1] != "serp:buffer" {
		t.Errorf("legacy key = %q, want it appended last so it still drains", keys[1])
	}
}

func TestSelectBufferKeys_AllParked(t *testing.T) {
	keys, allParked := SelectBufferKeys(
		[]string{"serp:buffer:searxng", "serp:buffer:duckduckgo"},
		"serp:buffer",
		func(string) bool { return true },
	)
	if !allParked {
		t.Fatal("every engine parked — allParked must be true")
	}
	// The worker only waits; it may still drain the legacy list afterwards.
	for _, k := range keys {
		if k == "serp:buffer:searxng" || k == "serp:buffer:duckduckgo" {
			t.Errorf("parked engine key %q must not be returned", k)
		}
	}
}

func TestSelectBufferKeys_NoneParked(t *testing.T) {
	keys, allParked := SelectBufferKeys(
		[]string{"serp:buffer:searxng", "serp:buffer:duckduckgo"},
		"serp:buffer",
		func(string) bool { return false },
	)
	if allParked {
		t.Fatal("nothing parked — allParked must be false")
	}
	if len(keys) != 3 || keys[2] != "serp:buffer" {
		t.Fatalf("keys = %v, want both engines plus the legacy key last", keys)
	}
}

func TestWaitForAnyBackoff(t *testing.T) {
	// Shortest remaining window wins, capped.
	if got := WaitForAnyBackoff([]time.Duration{4 * time.Minute, 90 * time.Second}, MaxAllEnginesWait); got != MaxAllEnginesWait {
		t.Errorf("got %v, want the 30s cap", got)
	}
	if got := WaitForAnyBackoff([]time.Duration{20 * time.Second, 90 * time.Second}, MaxAllEnginesWait); got != 20*time.Second {
		t.Errorf("got %v, want 20s", got)
	}
	// Any unparked engine means proceed now.
	if got := WaitForAnyBackoff([]time.Duration{0, 5 * time.Minute}, MaxAllEnginesWait); got != 0 {
		t.Errorf("got %v, want 0 (work to do)", got)
	}
	if got := WaitForAnyBackoff(nil, MaxAllEnginesWait); got != 0 {
		t.Errorf("got %v, want 0 (nothing parked)", got)
	}
}
