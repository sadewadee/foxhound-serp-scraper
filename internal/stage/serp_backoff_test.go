package stage

import (
	"strings"
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
		"serp:buffer", 0,
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
		"serp:buffer", 0,
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
		"serp:buffer", 0,
		func(string) bool { return false },
	)
	if allParked {
		t.Fatal("nothing parked — allParked must be false")
	}
	if len(keys) != 3 || keys[2] != "serp:buffer" {
		t.Fatalf("keys = %v, want both engines plus the legacy key last", keys)
	}
}

func TestSelectBufferKeys_RoundRobin(t *testing.T) {
	none := func(string) bool { return false }
	join := func(k []string) string { return strings.Join(k, " ") }

	for turn, want := range map[int]string{0: "a b L", 1: "b a L", 2: "a b L"} {
		keys, _ := SelectBufferKeys([]string{"a", "b"}, "L", turn, none)
		if got := join(keys); got != want {
			t.Errorf("2 engines turn %d = %q, want %q", turn, got, want)
		}
	}

	first := map[string]int{}
	for turn := 0; turn < 3; turn++ {
		keys, _ := SelectBufferKeys([]string{"a", "b", "c"}, "L", turn, none)
		first[keys[0]]++
		if keys[len(keys)-1] != "L" {
			t.Errorf("turn %d keys = %v, legacy key must stay last", turn, keys)
		}
	}
	if first["a"] != 1 || first["b"] != 1 || first["c"] != 1 {
		t.Errorf("first-position counts = %v, want each engine first once", first)
	}

	// Turn beyond the key count wraps: 7 % 2 = 1.
	keys, _ := SelectBufferKeys([]string{"a", "b"}, "L", 7, none)
	if got := join(keys); got != "b a L" {
		t.Errorf("turn 7 = %q, want %q", got, "b a L")
	}
}

func TestSelectBufferKeys_RoundRobinParkedAndEmpty(t *testing.T) {
	for turn := 0; turn < 6; turn++ {
		keys, all := SelectBufferKeys([]string{"a", "b", "c"}, "L", turn, func(k string) bool { return k == "b" })
		if all {
			t.Errorf("turn %d: allBackedOff must be false", turn)
		}
		for _, k := range keys {
			if k == "b" {
				t.Errorf("turn %d: parked engine returned in %v", turn, keys)
			}
		}
		if len(keys) != 3 || keys[2] != "L" {
			t.Errorf("turn %d keys = %v, want two engines + L last", turn, keys)
		}
		keys, all = SelectBufferKeys([]string{"a", "b"}, "L", turn, func(string) bool { return true })
		if !all || len(keys) != 1 || keys[0] != "L" {
			t.Errorf("turn %d all parked = %v %v, want [L] and allBackedOff", turn, keys, all)
		}
	}

	keys, all := SelectBufferKeys(nil, "L", 5, nil)
	if all || len(keys) != 1 || keys[0] != "L" {
		t.Errorf("no engines = %v %v, want [L] and allBackedOff=false", keys, all)
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
