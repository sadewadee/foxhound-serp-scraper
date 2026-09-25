//go:build playwright

package stage

import "testing"

func TestSERPStage_PagesIrrelevantMetric(t *testing.T) {
	var s SERPStage
	if s.PagesIrrelevant() != 0 {
		t.Errorf("initial PagesIrrelevant = %d, want 0", s.PagesIrrelevant())
	}
	s.pagesIrrelevant.Add(1)
	if s.PagesIrrelevant() != 1 {
		t.Errorf("after Add(1) PagesIrrelevant = %d, want 1", s.PagesIrrelevant())
	}
}
