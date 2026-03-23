package monitor

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// Ticker prints pipeline stats to stderr at regular intervals.
// Uses RateTracker for rate calculations — zero DB queries.
type Ticker struct {
	// Per-stage rate trackers.
	SERPQueries *RateTracker
	SERPURLs    *RateTracker
	Domains     *RateTracker
	Pages       *RateTracker
	Emails      *RateTracker
	Phones      *RateTracker

	// Queue depths set externally via Store().
	WebsiteQueueDepth atomic.Int64
	ContactQueueDepth atomic.Int64

	startTime time.Time
}

// NewTicker creates a Ticker with all trackers initialised.
func NewTicker() *Ticker {
	return &Ticker{
		SERPQueries: NewRateTracker(),
		SERPURLs:    NewRateTracker(),
		Domains:     NewRateTracker(),
		Pages:       NewRateTracker(),
		Emails:      NewRateTracker(),
		Phones:      NewRateTracker(),
		startTime:   time.Now(),
	}
}

// Start blocks until ctx is cancelled, printing stats to stderr at each interval.
func (t *Ticker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.Print()
		}
	}
}

// Print writes a single stats line to stderr using a carriage return so it
// updates in place on a terminal.
func (t *Ticker) Print() {
	t.print(os.Stderr)
}

// print writes the stats line to the given writer.
// Extracted so tests can capture output without touching stderr.
func (t *Ticker) print(w io.Writer) {
	elapsed := time.Since(t.startTime).Round(time.Second)

	serpQryRate := t.SERPQueries.Rate()
	emailRate := t.Emails.Rate()

	fmt.Fprintf(w,
		"\r[%s] serp: %d qry (%.0f/hr) %d urls | disc: %d dom | enrich: %d pg, %d email (%.0f/hr), %d phone | queue: web=%d contact=%d",
		elapsed,
		t.SERPQueries.Total(), serpQryRate,
		t.SERPURLs.Total(),
		t.Domains.Total(),
		t.Pages.Total(),
		t.Emails.Total(), emailRate,
		t.Phones.Total(),
		t.WebsiteQueueDepth.Load(),
		t.ContactQueueDepth.Load(),
	)
}
