package api

import (
	"context"
	"errors"
	"testing"

	pq "github.com/lib/pq"
)

// Issue #32 Bug 3: OFFSET responses never emitted a next_cursor, so a client
// could not bootstrap into keyset mode. offsetNextCursor is the helper that
// decides whether (and what) cursor to hand back from an OFFSET response.
func TestOffsetNextCursor(t *testing.T) {
	tests := []struct {
		name     string
		sort     string
		page     int
		perPage  int
		total    int
		returned int
		lastID   int64
		wantSet  bool
	}{
		{
			name: "id desc default, more rows remain -> cursor offered",
			sort: "", page: 1, perPage: 3, total: 100, returned: 3, lastID: 4280383,
			wantSet: true,
		},
		{
			name: "explicit id_desc, more rows remain -> cursor offered",
			sort: "id_desc", page: 2, perPage: 50, total: 1000, returned: 50, lastID: 777,
			wantSet: true,
		},
		{
			name: "last page (no more rows) -> no cursor",
			sort: "", page: 10, perPage: 10, total: 100, returned: 10, lastID: 5,
			wantSet: false,
		},
		{
			name: "empty page -> no cursor",
			sort: "", page: 50, perPage: 10, total: 100, returned: 0, lastID: 0,
			wantSet: false,
		},
		{
			name: "non-id sort -> no cursor (keyset is id-keyed only)",
			sort: "updated_desc", page: 1, perPage: 3, total: 100, returned: 3, lastID: 9,
			wantSet: false,
		},
		{
			name: "id_asc -> no cursor (cursor walks id DESC)",
			sort: "id_asc", page: 1, perPage: 3, total: 100, returned: 3, lastID: 9,
			wantSet: false,
		},
		{
			name: "count unknown but full page -> cursor offered (Bug 2 forward path)",
			sort: "", page: 1, perPage: 3, total: -1, returned: 3, lastID: 4280383,
			wantSet: true,
		},
		{
			name: "count unknown and partial page -> no cursor",
			sort: "", page: 1, perPage: 50, total: -1, returned: 12, lastID: 4280383,
			wantSet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := offsetNextCursor(tt.sort, tt.page, tt.perPage, tt.total, tt.returned, tt.lastID)
			if tt.wantSet {
				if got == "" {
					t.Fatalf("expected a cursor, got empty")
				}
				id, err := decodeCursor(got)
				if err != nil {
					t.Fatalf("offered cursor is not decodable: %v", err)
				}
				if id != tt.lastID {
					t.Fatalf("cursor id = %d, want %d", id, tt.lastID)
				}
			} else if got != "" {
				t.Fatalf("expected no cursor, got %q", got)
			}
		})
	}
}

func TestIsIDDescOrder(t *testing.T) {
	for _, s := range []string{"", "id_desc"} {
		if !isIDDescOrder(s) {
			t.Errorf("isIDDescOrder(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"id_asc", "updated_desc", "created_asc", "garbage"} {
		if isIDDescOrder(s) {
			t.Errorf("isIDDescOrder(%q) = true, want false", s)
		}
	}
}

// Issue #32 bonus: sort=id_desc must not 500 — it must resolve to a valid,
// keyset-consistent ORDER BY bl.id DESC.
func TestResultsOrderBy_IDDescExplicit(t *testing.T) {
	if got := resultsOrderBy("id_desc"); got != "ORDER BY bl.id DESC" {
		t.Fatalf("resultsOrderBy(id_desc) = %q, want ORDER BY bl.id DESC", got)
	}
}

// Issue #32 fix #3: a deep OFFSET that blows the statement timeout must be
// classified so the handler can return a clean 4xx instead of a generic 500.
func TestIsStatementTimeout(t *testing.T) {
	if !isStatementTimeout(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded should classify as timeout")
	}
	if !isStatementTimeout(&pq.Error{Code: "57014"}) {
		t.Error("pq 57014 (query_canceled) should classify as timeout")
	}
	if !isStatementTimeout(errors.New("pq: canceling statement due to statement timeout")) {
		t.Error("statement timeout error string should classify as timeout")
	}
	if isStatementTimeout(nil) {
		t.Error("nil must not classify as timeout")
	}
	if isStatementTimeout(errors.New("syntax error at or near")) {
		t.Error("unrelated error must not classify as timeout")
	}
}
