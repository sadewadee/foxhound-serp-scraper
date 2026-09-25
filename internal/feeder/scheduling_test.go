package feeder

import "testing"

// TestNextPassDry locks the scheduling rule that fixed feeder starvation with
// per-engine buffers: a saturated engine is skipped and the round-robin
// advances, so it cannot stall the engines behind it. Only when a whole pass
// produced nothing does the caller sleep — a sleep on the first full engine
// would have starved duckduckgo behind hachibi's slow-consuming searxng list
// (full by design: 3s delay per request).
func TestNextPassDry(t *testing.T) {
	engines := []string{"searxng", "duckduckgo"}

	// Mid-pass: not a wrap, no sleep — the loop must move to the next engine.
	if nextPassDry(1, engines) {
		t.Error("sleep after the first of two engines — a full searxng would stall duckduckgo")
	}
	// Wrap: the pass is complete, so sleeping is correct.
	if !nextPassDry(2, engines) {
		t.Error("no sleep after a full dry pass — the feeder would spin")
	}
	// Rotation keeps wrapping correctly across passes.
	if !nextPassDry(4, engines) {
		t.Error("second full pass must also report dry")
	}

	// A single-engine host: every skip is a full pass.
	if !nextPassDry(1, []string{"searxng"}) {
		t.Error("single-engine host must sleep once per pass")
	}
	// No engines configured: never report a completed pass.
	if nextPassDry(0, nil) {
		t.Error("empty engine set must not report a dry pass")
	}
}
