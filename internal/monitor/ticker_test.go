package monitor

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewTicker_InitializesTrackers(t *testing.T) {
	tk := NewTicker()
	if tk.SERPQueries == nil {
		t.Fatal("SERPQueries tracker is nil")
	}
	if tk.SERPURLs == nil {
		t.Fatal("SERPURLs tracker is nil")
	}
	if tk.Domains == nil {
		t.Fatal("Domains tracker is nil")
	}
	if tk.Pages == nil {
		t.Fatal("Pages tracker is nil")
	}
	if tk.Emails == nil {
		t.Fatal("Emails tracker is nil")
	}
	if tk.Phones == nil {
		t.Fatal("Phones tracker is nil")
	}
}

func TestTicker_PrintContainsExpectedSegments(t *testing.T) {
	tk := NewTicker()
	tk.SERPQueries.Add(45)
	tk.SERPURLs.Add(450)
	tk.Domains.Add(204)
	tk.Pages.Add(891)
	tk.Emails.Add(342)
	tk.Phones.Add(89)
	tk.WebsiteQueueDepth.Store(200)
	tk.ContactQueueDepth.Store(1300)

	var buf bytes.Buffer
	tk.print(&buf)
	line := buf.String()

	checks := []struct {
		desc    string
		want    string
	}{
		{"elapsed marker", "["},
		{"serp section", "serp:"},
		{"query count", "45 qry"},
		{"url count", "450 urls"},
		{"domain section", "disc:"},
		{"domain count", "204 dom"},
		{"enrich section", "enrich:"},
		{"page count", "891 pg"},
		{"email count", "342 email"},
		{"phone count", "89 phone"},
		{"queue section", "queue:"},
		{"website queue", "web=200"},
		{"contact queue", "contact=1300"},
		{"carriage return", "\r"},
	}

	for _, c := range checks {
		if !strings.Contains(line, c.want) {
			t.Errorf("Print() output missing %s: want %q in %q", c.desc, c.want, line)
		}
	}
}

func TestTicker_PrintIncludesRateForEmails(t *testing.T) {
	tk := NewTicker()
	tk.Emails.Add(148)

	var buf bytes.Buffer
	tk.print(&buf)
	line := buf.String()

	// Should include a rate annotation like "(N/hr)" after email count.
	if !strings.Contains(line, "/hr)") {
		t.Errorf("Print() output missing rate annotation '/hr)': got %q", line)
	}
}

func TestTicker_PrintIncludesRateForSERPQueries(t *testing.T) {
	tk := NewTicker()
	tk.SERPQueries.Add(8)

	var buf bytes.Buffer
	tk.print(&buf)
	line := buf.String()

	// Should include a rate annotation after query count.
	if !strings.Contains(line, "/hr)") {
		t.Errorf("Print() output missing rate annotation for SERP queries: got %q", line)
	}
}

func TestTicker_StartStopsOnContextCancel(t *testing.T) {
	tk := NewTicker()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		tk.Start(ctx, 50*time.Millisecond)
		close(done)
	}()

	// Let it tick at least once.
	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Good — goroutine exited after cancel.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() did not return after context cancellation")
	}
}

func TestTicker_ElapsedTimeIsNonNegative(t *testing.T) {
	tk := NewTicker()
	var buf bytes.Buffer
	// Sleep a tiny bit so elapsed > 0.
	time.Sleep(2 * time.Millisecond)
	tk.print(&buf)
	line := buf.String()

	// Line must start with "\r[" and contain something like "0s]" or "2ms]".
	if !strings.HasPrefix(line, "\r[") {
		t.Errorf("Print() line does not start with '\\r[': got %q", line)
	}
}
