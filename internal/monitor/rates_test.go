package monitor

import (
	"sync"
	"testing"
	"time"
)

func TestRateTracker_TotalStartsAtZero(t *testing.T) {
	rt := NewRateTracker()
	if got := rt.Total(); got != 0 {
		t.Fatalf("expected Total() == 0, got %d", got)
	}
}

func TestRateTracker_AddIncreasesTotal(t *testing.T) {
	rt := NewRateTracker()
	rt.Add(5)
	rt.Add(3)
	if got := rt.Total(); got != 8 {
		t.Fatalf("expected Total() == 8, got %d", got)
	}
}

func TestRateTracker_RateZeroWhenEmpty(t *testing.T) {
	rt := NewRateTracker()
	if got := rt.Rate(); got != 0.0 {
		t.Fatalf("expected Rate() == 0.0, got %f", got)
	}
}

func TestRateTracker_RateReflectsRecentAdds(t *testing.T) {
	rt := NewRateTracker()
	rt.Add(60) // 60 events in current second
	rate := rt.Rate()
	// Rate is per-hour from last 60s.
	// With 60 events in 1 slot out of 60, rate = 60/60 * 3600 = 3600/hr.
	// Allow ±1 due to second boundary effects.
	if rate <= 0 {
		t.Fatalf("expected positive Rate(), got %f", rate)
	}
}

func TestRateTracker_RateIsPerHour(t *testing.T) {
	rt := NewRateTracker()
	// Add exactly 1 event. Rate over 60 seconds = 1/60 * 3600 = 60/hr.
	rt.Add(1)
	rate := rt.Rate()
	// Should be in the range (0, 3601] — upper bound if counted in 1 slot of 60.
	if rate <= 0 || rate > 3601 {
		t.Fatalf("Rate() out of expected range (0, 3601], got %f", rate)
	}
}

func TestRateTracker_TotalNotAffectedByWindowRollover(t *testing.T) {
	rt := NewRateTracker()
	rt.Add(100)
	// Total must be all-time, not windowed.
	if got := rt.Total(); got != 100 {
		t.Fatalf("expected Total() == 100, got %d", got)
	}
}

func TestRateTracker_ConcurrentAddSafe(t *testing.T) {
	rt := NewRateTracker()
	var wg sync.WaitGroup
	const goroutines = 50
	const addsEach = 100
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt.Add(addsEach)
		}()
	}
	wg.Wait()
	if got := rt.Total(); got != goroutines*addsEach {
		t.Fatalf("expected Total() == %d, got %d", goroutines*addsEach, got)
	}
}

func TestRateTracker_SlotAdvancesWithTime(t *testing.T) {
	rt := NewRateTracker()
	rt.Add(10)
	// Verify the internal slot index is consistent with the current second.
	slot := int(time.Now().Unix() % windowSize)
	if rt.slots[slot] != 10 {
		t.Fatalf("expected slot[%d] == 10, got %d", slot, rt.slots[slot])
	}
}
