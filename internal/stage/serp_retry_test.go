package stage

import (
	"testing"
)

func TestEvaluateRetryDecision_FetchFailure(t *testing.T) {
	maxAttempts := 5

	// Attempt 0 -> 1: new, 30s backoff
	d1 := EvaluateRetryDecision(0, maxAttempts, 0)
	if d1.NewAttempt != 1 {
		t.Errorf("d1.NewAttempt = %d, want 1", d1.NewAttempt)
	}
	if d1.Status != "new" {
		t.Errorf("d1.Status = %q, want 'new'", d1.Status)
	}
	if d1.BackoffSec != 30 {
		t.Errorf("d1.BackoffSec = %d, want 30", d1.BackoffSec)
	}

	// Attempt 1 -> 2: new, 60s backoff
	d2 := EvaluateRetryDecision(1, maxAttempts, 0)
	if d2.NewAttempt != 2 {
		t.Errorf("d2.NewAttempt = %d, want 2", d2.NewAttempt)
	}
	if d2.Status != "new" {
		t.Errorf("d2.Status = %q, want 'new'", d2.Status)
	}
	if d2.BackoffSec != 60 {
		t.Errorf("d2.BackoffSec = %d, want 60", d2.BackoffSec)
	}

	// Attempt 2 -> 3: new, 120s backoff
	d3 := EvaluateRetryDecision(2, maxAttempts, 0)
	if d3.NewAttempt != 3 {
		t.Errorf("d3.NewAttempt = %d, want 3", d3.NewAttempt)
	}
	if d3.Status != "new" {
		t.Errorf("d3.Status = %q, want 'new'", d3.Status)
	}
	if d3.BackoffSec != 120 {
		t.Errorf("d3.BackoffSec = %d, want 120", d3.BackoffSec)
	}

	// Attempt 3 -> 4: new, 240s backoff
	d4 := EvaluateRetryDecision(3, maxAttempts, 0)
	if d4.NewAttempt != 4 {
		t.Errorf("d4.NewAttempt = %d, want 4", d4.NewAttempt)
	}
	if d4.Status != "new" {
		t.Errorf("d4.Status = %q, want 'new'", d4.Status)
	}
	if d4.BackoffSec != 240 {
		t.Errorf("d4.BackoffSec = %d, want 240", d4.BackoffSec)
	}

	// Attempt 4 -> 5: reaches maxAttempts -> failed, 0 backoff
	d5 := EvaluateRetryDecision(4, maxAttempts, 0)
	if d5.NewAttempt != 5 {
		t.Errorf("d5.NewAttempt = %d, want 5", d5.NewAttempt)
	}
	if d5.Status != "failed" {
		t.Errorf("d5.Status = %q, want 'failed'", d5.Status)
	}
	if d5.BackoffSec != 0 {
		t.Errorf("d5.BackoffSec = %d, want 0", d5.BackoffSec)
	}

	// Attempt 5 -> 6: already exceeded -> failed
	d6 := EvaluateRetryDecision(5, maxAttempts, 0)
	if d6.NewAttempt != 6 {
		t.Errorf("d6.NewAttempt = %d, want 6", d6.NewAttempt)
	}
	if d6.Status != "failed" {
		t.Errorf("d6.Status = %q, want 'failed'", d6.Status)
	}
}

func TestEvaluateRetryDecision_IrrelevantCap(t *testing.T) {
	// Standard max_attempts is 10, but irrelevant pages should cap at MaxIrrelevantAttempts (3)
	maxAttempts := 10

	// 1st irrelevant attempt: attempt 0 -> 1, status new, 30s
	d1 := EvaluateRetryDecision(0, maxAttempts, MaxIrrelevantAttempts)
	if d1.NewAttempt != 1 || d1.Status != "new" || d1.BackoffSec != 30 {
		t.Errorf("d1 = %+v; want attempt=1, status=new, backoff=30", d1)
	}

	// 2nd irrelevant attempt: attempt 1 -> 2, status new, 60s
	d2 := EvaluateRetryDecision(1, maxAttempts, MaxIrrelevantAttempts)
	if d2.NewAttempt != 2 || d2.Status != "new" || d2.BackoffSec != 60 {
		t.Errorf("d2 = %+v; want attempt=2, status=new, backoff=60", d2)
	}

	// 3rd irrelevant attempt: attempt 2 -> 3, reaches cap -> failed!
	d3 := EvaluateRetryDecision(2, maxAttempts, MaxIrrelevantAttempts)
	if d3.NewAttempt != 3 || d3.Status != "failed" || d3.BackoffSec != 0 {
		t.Errorf("d3 = %+v; want attempt=3, status=failed, backoff=0", d3)
	}

	// Invariant #1: attempt count monotonically increases, never resets or decrements
	attempts := []int{0, d1.NewAttempt, d2.NewAttempt, d3.NewAttempt}
	for i := 1; i < len(attempts); i++ {
		if attempts[i] <= attempts[i-1] {
			t.Errorf("attempt non-increasing: %d <= %d", attempts[i], attempts[i-1])
		}
	}
}

func TestEvaluateRetryDecision_CapEdgeCases(t *testing.T) {
	// If maxAttempts is 0 or negative, defaults safely to 3
	d0 := EvaluateRetryDecision(0, 0, 0)
	if d0.NewAttempt != 1 || d0.Status != "new" {
		t.Errorf("d0 = %+v", d0)
	}
	d2 := EvaluateRetryDecision(2, 0, 0)
	if d2.NewAttempt != 3 || d2.Status != "failed" {
		t.Errorf("d2 = %+v; want failed at attempt 3", d2)
	}
}
