package db

// Tests for the trigger-function downgrade guard (2026-09-25 incident): a
// stale remote-worker binary (v0.8.3-niche) called db.Migrate() on every
// boot and its unconditional CREATE OR REPLACE FUNCTION silently downgraded
// the live trg_normalize_enrichment to its old definition, so the v0.9.0
// v0.9.8 classifier fixes never took effect in prod. These tests validate,
// WITHOUT a live DB, the three pieces that make a stale binary's replace a
// no-op instead of a downgrade:
//
//  1. shouldReplaceTriggerFn — the pure live-vs-ours comparison.
//  2. parseTriggerVersionMarker — the regexp used to read back the version
//     embedded in a live function's pg_proc.prosrc.
//  3. The rendered trgEnqueueEnrichmentFnBody / trgNormalizeEnrichmentFnBody
//     SQL actually contains the marker at the const's own version, so the
//     two things (what gets written, what gets read back) can never drift.

import (
	"fmt"
	"strings"
	"testing"
)

func TestShouldReplaceTriggerFn(t *testing.T) {
	tests := []struct {
		name string
		live int
		ours int
		want bool
	}{
		{"missing function (live=0) always replaceable", 0, 20260925, true},
		{"pre-guard version (no marker, live=0) always replaceable", 0, 1, true},
		{"equal versions replace (idempotent same-version boot)", 20260925, 20260925, true},
		{"live older than ours replaces (normal upgrade)", 20260101, 20260925, true},
		{"live newer than ours does NOT replace (the downgrade guard)", 20991231, 20260925, false},
		{"live one day newer does NOT replace", 20260926, 20260925, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldReplaceTriggerFn(tt.live, tt.ours); got != tt.want {
				t.Errorf("shouldReplaceTriggerFn(live=%d, ours=%d) = %v, want %v", tt.live, tt.ours, got, tt.want)
			}
		})
	}
}

func TestParseTriggerVersionMarker(t *testing.T) {
	tests := []struct {
		name   string
		prosrc string
		want   int
	}{
		{
			name:   "marker present",
			prosrc: "BEGIN\n  -- trg_normalize_enrichment version: 20260925\n  IF NEW.status = 'completed' THEN\n",
			want:   20260925,
		},
		{
			name:   "marker present with different function name and surrounding text",
			prosrc: "DECLARE\n  x INT;\nBEGIN\n  -- trg_enqueue_enrichment version: 1\n  INSERT INTO foo VALUES (1);\nEND;",
			want:   1,
		},
		{
			name:   "marker absent — pre-guard function body",
			prosrc: "BEGIN\n  INSERT INTO enrichment_jobs (url) VALUES (NEW.url);\n  RETURN NEW;\nEND;",
			want:   0,
		},
		{
			name:   "empty body",
			prosrc: "",
			want:   0,
		},
		{
			name:   "garbage marker — non-numeric version",
			prosrc: "-- trg_normalize_enrichment version: banana\nBEGIN\n",
			want:   0,
		},
		{
			name:   "garbage marker — version keyword with no colon",
			prosrc: "-- trg_normalize_enrichment version 20260925 (missing colon)\nBEGIN\n",
			want:   0,
		},
		{
			name:   "garbage marker — empty value after colon",
			prosrc: "-- trg_normalize_enrichment version:\nBEGIN\n",
			want:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseTriggerVersionMarker(tt.prosrc); got != tt.want {
				t.Errorf("parseTriggerVersionMarker(%q) = %d, want %d", tt.prosrc, got, tt.want)
			}
		})
	}
}

// TestTriggerFnBodiesEmbedVersionMarker renders both trigger function
// bodies exactly as runMigrations does (fmt.Sprintf with the version const)
// and asserts the rendered SQL contains the matching
// "-- <fn> version: NNNNNNNN" marker. This is the single-source-of-truth
// guarantee: the SQL executed at boot and the marker liveTriggerFnVersion
// reads back afterwards can never drift apart, because both come from the
// same fmt.Sprintf call over the same const.
func TestTriggerFnBodiesEmbedVersionMarker(t *testing.T) {
	t.Run("trg_enqueue_enrichment", func(t *testing.T) {
		rendered := fmt.Sprintf(trgEnqueueEnrichmentFnBody, enqueueTriggerVersion)
		wantMarker := fmt.Sprintf("-- trg_enqueue_enrichment version: %d", enqueueTriggerVersion)
		if !strings.Contains(rendered, wantMarker) {
			t.Errorf("rendered trg_enqueue_enrichment body missing marker %q\nbody:\n%s", wantMarker, rendered)
		}
		if !strings.Contains(rendered, "CREATE OR REPLACE FUNCTION trg_enqueue_enrichment()") {
			t.Error("rendered trg_enqueue_enrichment body missing the CREATE OR REPLACE FUNCTION statement")
		}
		// The marker must parse back to the exact same version fed in.
		if got := parseTriggerVersionMarker(rendered); got != enqueueTriggerVersion {
			t.Errorf("parseTriggerVersionMarker(rendered trg_enqueue_enrichment) = %d, want %d", got, enqueueTriggerVersion)
		}
	})

	t.Run("trg_normalize_enrichment", func(t *testing.T) {
		rendered := fmt.Sprintf(trgNormalizeEnrichmentFnBody, normalizeTriggerVersion)
		wantMarker := fmt.Sprintf("-- trg_normalize_enrichment version: %d", normalizeTriggerVersion)
		if !strings.Contains(rendered, wantMarker) {
			t.Errorf("rendered trg_normalize_enrichment body missing marker %q\nbody:\n%s", wantMarker, rendered)
		}
		if !strings.Contains(rendered, "CREATE OR REPLACE FUNCTION trg_normalize_enrichment()") {
			t.Error("rendered trg_normalize_enrichment body missing the CREATE OR REPLACE FUNCTION statement")
		}
		if got := parseTriggerVersionMarker(rendered); got != normalizeTriggerVersion {
			t.Errorf("parseTriggerVersionMarker(rendered trg_normalize_enrichment) = %d, want %d", got, normalizeTriggerVersion)
		}
	})
}
