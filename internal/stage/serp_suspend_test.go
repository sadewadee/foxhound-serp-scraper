package stage

import (
	"errors"
	"testing"
	"time"
)

func TestClassifySearXNGOutcome_Suspended(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		fetchErr   error
	}{
		{"http 429", 429, "", nil},
		{"http 503", 503, "", nil},
		{"json with unresponsive engines", 200,
			`{"results":[],"unresponsive_engines":[["google cse","Suspended: too many requests"]]}`, nil},
		{"single-element unresponsive entry", 200,
			`{"results":[],"unresponsive_engines":[["duckduckgo"]]}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := ClassifySearXNGOutcome(tt.statusCode, []byte(tt.body), tt.fetchErr)
			if d.Outcome != OutcomeSuspended {
				t.Errorf("Outcome = %v, want OutcomeSuspended", d.Outcome)
			}
			if d.IncrementAttempt {
				t.Error("suspension must NOT increment attempt_count — it is SearXNG capacity, not a job fault")
			}
			if d.ErrMsg == "" {
				t.Error("ErrMsg must be set so the job records why it was requeued")
			}
		})
	}
}

func TestClassifySearXNGOutcome_ReportsEngineNames(t *testing.T) {
	body := `{"results":[],"unresponsive_engines":[["google cse","Suspended: too many requests"],["brave","CAPTCHA"]]}`
	d := ClassifySearXNGOutcome(200, []byte(body), nil)
	if len(d.Engines) != 2 {
		t.Fatalf("Engines = %v, want 2 entries", d.Engines)
	}
	if d.Engines[0] != "google cse (Suspended: too many requests)" {
		t.Errorf("Engines[0] = %q", d.Engines[0])
	}
}

func TestClassifySearXNGOutcome_GenuineFailure(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		fetchErr   error
	}{
		{"non-JSON body", 200, "<html>gateway error</html>", nil},
		{"connection refused", 0, "", errors.New("dial tcp 10.44.0.2:8080: connect: connection refused")},
		{"dns error", 0, "", errors.New("dial tcp: lookup searxng: no such host")},
		{"timeout", 0, "", errors.New("context deadline exceeded")},
		{"transport error with partial body", 200, "<html>truncated", errors.New("unexpected EOF")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := ClassifySearXNGOutcome(tt.statusCode, []byte(tt.body), tt.fetchErr)
			if d.Outcome != OutcomeGenuineFailure {
				t.Errorf("Outcome = %v, want OutcomeGenuineFailure", d.Outcome)
			}
			if !d.IncrementAttempt {
				t.Error("genuine failure MUST increment attempt_count (normal retry path)")
			}
			if d.ErrMsg == "" {
				t.Error("ErrMsg must be set")
			}
		})
	}
}

func TestClassifySearXNGOutcome_Success(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"results present", `{"results":[{"url":"https://spa.example.com"}],"unresponsive_engines":[]}`},
		{"empty page, no unresponsive engines", `{"results":[],"unresponsive_engines":[]}`},
		{"unresponsive list absent", `{"results":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := ClassifySearXNGOutcome(200, []byte(tt.body), nil)
			if d.Outcome != OutcomeSuccess {
				t.Errorf("Outcome = %v, want OutcomeSuccess", d.Outcome)
			}
			if d.IncrementAttempt {
				t.Error("success must not increment attempt_count")
			}
		})
	}
}

func TestSuspensionBackoff_EscalatesAndCaps(t *testing.T) {
	if got := SuspensionBackoff(0); got != 0 {
		t.Errorf("SuspensionBackoff(0) = %v, want 0 (no backoff owed)", got)
	}
	if got := SuspensionBackoff(-1); got != 0 {
		t.Errorf("SuspensionBackoff(-1) = %v, want 0", got)
	}
	want := []time.Duration{
		30 * time.Second,
		60 * time.Second,
		120 * time.Second,
		240 * time.Second,
		480 * time.Second,
		10 * time.Minute, // capped from 16m
		10 * time.Minute, // stays capped
	}
	for i, w := range want {
		streak := i + 1
		if got := SuspensionBackoff(streak); got != w {
			t.Errorf("SuspensionBackoff(%d) = %v, want %v", streak, got, w)
		}
	}
}
