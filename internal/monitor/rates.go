package monitor

import (
	"sync"
	"sync/atomic"
	"time"
)

// windowSize is the number of one-second slots in the circular buffer (1 hour).
const windowSize = 3600

// rateWindowSeconds is the lookback window used when computing the current rate.
const rateWindowSeconds = 60

// RateTracker tracks counts over a sliding time window for rate calculation.
// Thread-safe via a mutex-protected circular buffer for rate data and an
// atomic counter for the all-time total.
type RateTracker struct {
	mu    sync.Mutex
	slots [windowSize]int64 // circular buffer: one slot per second
	total atomic.Int64      // all-time total, never reset
}

// NewRateTracker creates a ready-to-use RateTracker.
func NewRateTracker() *RateTracker {
	return &RateTracker{}
}

// Add increments the current second's slot by n and the all-time total by n.
func (r *RateTracker) Add(n int64) {
	r.total.Add(n)

	slot := int(time.Now().Unix() % windowSize)
	r.mu.Lock()
	r.slots[slot] += n
	r.mu.Unlock()
}

// Rate returns the per-hour rate computed from the last rateWindowSeconds of data.
func (r *RateTracker) Rate() float64 {
	now := time.Now().Unix()

	r.mu.Lock()
	var sum int64
	for delta := int64(0); delta < rateWindowSeconds; delta++ {
		slot := int((now - delta) % windowSize)
		sum += r.slots[slot]
	}
	r.mu.Unlock()

	// sum events over rateWindowSeconds → scale to per hour.
	return float64(sum) / float64(rateWindowSeconds) * 3600
}

// Total returns the all-time total count of events added.
func (r *RateTracker) Total() int64 {
	return r.total.Load()
}
