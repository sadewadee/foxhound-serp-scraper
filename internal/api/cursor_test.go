package api

import (
	"testing"
)

func TestCursor_RoundTrip(t *testing.T) {
	encoded := encodeCursor(12345)
	if encoded == "" {
		t.Fatal("expected non-empty cursor")
	}
	id, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if id != 12345 {
		t.Fatalf("expected id=12345, got %d", id)
	}
}

func TestCursor_EmptyDecode(t *testing.T) {
	_, err := decodeCursor("")
	if err == nil {
		t.Fatal("empty cursor must error")
	}
}

func TestCursor_MalformedDecode(t *testing.T) {
	_, err := decodeCursor("not-base64!!!")
	if err == nil {
		t.Fatal("malformed cursor must error")
	}
}

func TestCursor_MalformedJSON(t *testing.T) {
	// Valid base64 ("hello") but not JSON.
	_, err := decodeCursor("aGVsbG8")
	if err == nil {
		t.Fatal("non-JSON cursor body must error")
	}
}

func TestCursor_NegativeID(t *testing.T) {
	encoded := encodeCursor(-1)
	id, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if id != -1 {
		t.Fatalf("expected -1, got %d", id)
	}
}

func TestCursor_LargeID(t *testing.T) {
	const big int64 = 9_223_372_036_854_775_000 // near max int64
	encoded := encodeCursor(big)
	id, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if id != big {
		t.Fatalf("expected %d, got %d", big, id)
	}
}
