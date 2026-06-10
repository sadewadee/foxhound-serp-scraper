package api

import (
	"errors"
	"testing"

	pq "github.com/lib/pq"
)

// category_stats is created WITH NO DATA, so reads hit SQLSTATE 55000
// (object_not_in_prerequisite_state) until the manager's first REFRESH seeds it.
// handleV2Categories must classify that as "warming up" and return an empty 200,
// not a 500.
func TestIsMatviewNotPopulated(t *testing.T) {
	selectErr := &pq.Error{Code: "55000", Message: `materialized view "category_stats" has not been populated`}
	if !isMatviewNotPopulated(selectErr) {
		t.Error("pq 55000 'has not been populated' (SELECT) should classify as not-populated")
	}
	// REFRESH ... CONCURRENTLY on an unpopulated matview raises 0A000
	// (feature_not_supported), NOT 55000 — the regression that left category_stats
	// empty on the v0.9.2 deploy because the plain-REFRESH seed fallback was skipped.
	concurrentErr := &pq.Error{Code: "0A000", Message: "CONCURRENTLY cannot be used when the materialized view is not populated"}
	if !isMatviewNotPopulated(concurrentErr) {
		t.Error("pq 0A000 CONCURRENTLY-not-populated (REFRESH) should classify as not-populated")
	}
	if isMatviewNotPopulated(&pq.Error{Code: "0A000", Message: "some other unsupported feature"}) {
		t.Error("an unrelated 0A000 without 'populated' must NOT classify as not-populated")
	}
	if isMatviewNotPopulated(&pq.Error{Code: "57014"}) {
		t.Error("statement timeout (57014) must NOT classify as not-populated")
	}
	if isMatviewNotPopulated(&pq.Error{Code: "55000", Message: "lock not available"}) {
		t.Error("an unrelated 55000 without 'populated' must NOT classify as not-populated")
	}
	if isMatviewNotPopulated(nil) {
		t.Error("nil must not classify as not-populated")
	}
	if isMatviewNotPopulated(errors.New("some unrelated error")) {
		t.Error("unrelated error must not classify as not-populated")
	}
}
