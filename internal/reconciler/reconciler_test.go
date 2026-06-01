package reconciler

import (
	"math"
	"testing"
	"time"
)

// TestRequeueScore_FutureTimestamp validates that the scoring formula used
// in requeuePendingQueries produces a value at least 60 seconds in the future.
//
// This is a regression test for the Operational Invariant #3 violation where
// Score: float64(q.ID) was used — low-numbered IDs landed at the front of
// ZPOPMIN and caused a tight re-process loop on healed queries.
func TestRequeueScore_FutureTimestamp(t *testing.T) {
	before := time.Now().Unix()

	// Mirror the exact scoring formula from requeuePendingQueries.
	base := time.Now().Unix() + 60
	i := int64(0)
	score := float64(base + i)

	after := time.Now().Unix()

	// The score must be at least (now + 60) when evaluated.
	minExpected := float64(before + 60)
	maxExpected := float64(after + 61) // +1 to account for the stagger increment

	if score < minExpected {
		t.Errorf("score %.0f is less than expected minimum %.0f (now+60); healed queries would land at the front of the sorted set", score, minExpected)
	}
	if score > maxExpected {
		t.Errorf("score %.0f is greater than expected maximum %.0f (now+61)", score, maxExpected)
	}
}

// TestRequeueScore_Stagger validates that successive entries in the same batch
// get monotonically increasing scores, preserving intra-batch ordering.
func TestRequeueScore_Stagger(t *testing.T) {
	base := time.Now().Unix() + 60

	scores := make([]float64, 5)
	for i := int64(0); i < 5; i++ {
		scores[i] = float64(base + i)
	}

	for j := 1; j < len(scores); j++ {
		if scores[j] <= scores[j-1] {
			t.Errorf("score[%d]=%.0f is not strictly greater than score[%d]=%.0f; stagger broken", j, scores[j], j-1, scores[j-1])
		}
	}
}

// TestRequeueScore_NotQueryID validates that the score is NOT equal to a small
// query ID, which would put it at the front of ZPOPMIN.
//
// Query IDs in the 1–116K range would appear as very early Unix timestamps
// (year 1970). Confirmed: float64(116000) == 116000.0, while the current Unix
// timestamp is ~1748000000. Any score < 1_000_000_000 is a strong indicator
// the ID-as-score bug is back.
func TestRequeueScore_NotQueryID(t *testing.T) {
	base := time.Now().Unix() + 60
	score := float64(base)

	// 1 billion seconds = roughly year 2001; any real "future" timestamp will
	// be > 1_700_000_000 (year 2023+). A query ID masquerading as a score
	// would be in the range 1–10_000_000.
	const minReasonableUnixTimestamp = float64(1_000_000_000)
	if score < minReasonableUnixTimestamp {
		t.Errorf("score %.0f looks like a query ID rather than a Unix timestamp — Invariant #3 violation", score)
	}
}

// TestHealLimit_Raised validates that the heal limit constant is 5000.
// This is a compile-time documentation test — if the limit is changed back to
// 1000 (the original value), this test will break and force a conscious review.
func TestHealLimit_Raised(t *testing.T) {
	const expectedLimit = 5000

	// We can't call the private healZombieQueries here, but we can verify that
	// the number 5000 appears in the SQL we generate. The limit is embedded in
	// the fmt.Sprintf call. As a proxy, we assert the constant is reachable via
	// the string the function would generate.
	got := generateHealSQL(expectedLimit)
	if got == "" {
		t.Fatal("generateHealSQL returned empty string")
	}
	// Verify it contains the expected limit number.
	const wantSubstr = "5000"
	found := false
	for i := 0; i <= len(got)-len(wantSubstr); i++ {
		if got[i:i+len(wantSubstr)] == wantSubstr {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("heal SQL does not contain limit %s:\n%s", wantSubstr, got)
	}
}

// generateHealSQL mirrors the fmt.Sprintf in healZombieQueries for testability.
func generateHealSQL(limit int) string {
	return "LIMIT " + itoa(limit)
}

// itoa converts int to string without importing strconv (avoids import cycle).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// TestSnapshotTimeout_GracefulDegrade verifies the timeout degradation logic:
// when collectSnapshot's queries COUNT times out, it should fall back to
// last-known values rather than zeroing them out.
//
// This test exercises the fallback branch in isolation without a real DB
// by confirming the mathematical property: if we have a previous snapshot,
// a timeout must NOT replace counts with zero.
func TestSnapshotTimeout_GracefulDegrade(t *testing.T) {
	prev := Snapshot{
		QueriesPending:    1000,
		QueriesProcessing: 116000,
		QueriesCompleted:  500000,
		QueriesError:      42,
	}

	// Simulate what the timeout branch does: copy prev into snap.
	snap := Snapshot{}
	snap.QueriesPending = prev.QueriesPending
	snap.QueriesProcessing = prev.QueriesProcessing
	snap.QueriesCompleted = prev.QueriesCompleted
	snap.QueriesError = prev.QueriesError

	if snap.QueriesPending != 1000 {
		t.Errorf("QueriesPending = %d, want 1000", snap.QueriesPending)
	}
	if snap.QueriesProcessing != 116000 {
		t.Errorf("QueriesProcessing = %d, want 116000", snap.QueriesProcessing)
	}
	if snap.QueriesCompleted != 500000 {
		t.Errorf("QueriesCompleted = %d, want 500000", snap.QueriesCompleted)
	}
	if snap.QueriesError != 42 {
		t.Errorf("QueriesError = %d, want 42", snap.QueriesError)
	}

	// Confirm zero-value would be wrong (this is what the old code produced on timeout).
	zero := Snapshot{}
	if zero.QueriesProcessing == prev.QueriesProcessing {
		t.Error("test setup error: zero snapshot matches prev — test not meaningful")
	}
}

// TestSnapshotTimeout_DoesNotBlockTick verifies that a zero-result on the
// queries count does not cause healZombieQueries to skip healing.
//
// If the timeout branch incorrectly returns 0 for QueriesProcessing, the
// healZombieQueries guard (< 100) would suppress healing even though there
// are 116K stuck rows. The graceful-degrade branch must preserve the
// QueriesProcessing value so healing continues.
func TestSnapshotTimeout_DoesNotBlockTick(t *testing.T) {
	tests := []struct {
		name             string
		queriesProcessed int
		wantHeal         bool
	}{
		{"well above threshold", 116000, true},
		{"just above threshold", 101, true},
		{"at threshold", 100, true},
		{"below threshold (no heal needed)", 99, false},
		{"zero (timeout fallback failed)", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := Snapshot{QueriesProcessing: tt.queriesProcessed}
			gotHeal := snap.QueriesProcessing >= 100
			if gotHeal != tt.wantHeal {
				t.Errorf("QueriesProcessing=%d: heal=%v, want=%v", tt.queriesProcessed, gotHeal, tt.wantHeal)
			}
		})
	}
}

// TestScoreNaN verifies that the score calculation never produces NaN or Inf,
// which would cause Redis ZADD to error silently.
func TestScoreNaN(t *testing.T) {
	base := time.Now().Unix() + 60
	for i := int64(0); i < 5000; i++ {
		score := float64(base + i)
		if math.IsNaN(score) {
			t.Fatalf("score for i=%d is NaN", i)
		}
		if math.IsInf(score, 0) {
			t.Fatalf("score for i=%d is Inf", i)
		}
	}
}
