package stage

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDecodeBodyUTF8_EmptyBody(t *testing.T) {
	if got := decodeBodyUTF8(nil, ""); got != "" {
		t.Fatalf("nil body: want empty, got %q", got)
	}
	if got := decodeBodyUTF8([]byte{}, "text/html"); got != "" {
		t.Fatalf("zero-length body: want empty, got %q", got)
	}
}

func TestDecodeBodyUTF8_PassThroughUTF8(t *testing.T) {
	body := []byte("<html><body>Hello — café — 日本語</body></html>")
	got := decodeBodyUTF8(body, "text/html; charset=utf-8")
	if !utf8.ValidString(got) {
		t.Fatalf("output not valid UTF-8: %q", got)
	}
	if !strings.Contains(got, "café") || !strings.Contains(got, "日本語") {
		t.Fatalf("UTF-8 round-trip lost characters: %q", got)
	}
}

func TestDecodeBodyUTF8_Windows1255Hebrew(t *testing.T) {
	// "שלום" (shalom) in windows-1255: 0xF9 0xEC 0xE5 0xED. These bytes are
	// invalid UTF-8 — the bug causing 'pq: invalid byte sequence for encoding
	// "UTF8"' on yo-yoo.co.il was exactly this shape.
	hebrew := []byte{0xF9, 0xEC, 0xE5, 0xED}
	body := append([]byte("<html><body>"), hebrew...)
	body = append(body, []byte("</body></html>")...)

	got := decodeBodyUTF8(body, "text/html; charset=windows-1255")
	if !utf8.ValidString(got) {
		t.Fatalf("output not valid UTF-8: %q", got)
	}
	if !strings.Contains(got, "שלום") { // שלום
		t.Fatalf("Hebrew not decoded: got %q", got)
	}
}

func TestDecodeBodyUTF8_Latin1German(t *testing.T) {
	// "§ 2 Geschäft" — the §, ä, é bytes (0xA7, 0xE4, 0xE9) are invalid UTF-8
	// alone. Matches the dejure.org failure.
	body := []byte{
		'<', 'p', '>',
		0xA7, ' ', '2', ' ', 'G', 'e', 's', 'c', 'h', 0xE4, 'f', 't',
		'<', '/', 'p', '>',
	}
	got := decodeBodyUTF8(body, "text/html; charset=iso-8859-1")
	if !utf8.ValidString(got) {
		t.Fatalf("output not valid UTF-8: %q", got)
	}
	if !strings.Contains(got, "§") || !strings.Contains(got, "Geschäft") {
		t.Fatalf("Latin-1 not decoded: got %q", got)
	}
}

func TestDecodeBodyUTF8_MetaCharsetFallback(t *testing.T) {
	// No Content-Type header — charset must be detected from the meta tag.
	body := []byte(
		"<html><head><meta charset=\"windows-1255\"></head><body>" +
			string([]byte{0xF9, 0xEC, 0xE5, 0xED}) + "</body></html>",
	)
	got := decodeBodyUTF8(body, "")
	if !utf8.ValidString(got) {
		t.Fatalf("output not valid UTF-8: %q", got)
	}
	if !strings.Contains(got, "שלום") {
		t.Fatalf("meta-charset detection failed: got %q", got)
	}
}

func TestDecodeBodyUTF8_UnknownChasetSanitizes(t *testing.T) {
	// Garbage bytes with a bogus Content-Type label. Decoder should fall back
	// and sanitize so the output is at least valid UTF-8 (won't crash pq).
	// Note: avoids leading BOM (0xFE/0xFF/0xEF 0xBB 0xBF) since those would
	// override the bogus label and invoke a real decoder.
	body := []byte{'a', 'b', 'c', 0x80, 0xC0, 'd'}
	got := decodeBodyUTF8(body, "text/html; charset=fake-9000")
	if !utf8.ValidString(got) {
		t.Fatalf("output not valid UTF-8 after fallback: %q", got)
	}
	if !strings.Contains(got, "abc") || !strings.Contains(got, "d") {
		t.Fatalf("ASCII chars dropped: got %q", got)
	}
}

func TestDecodeBodyUTF8_TruncatedMultibyteSanitized(t *testing.T) {
	// UTF-8 declaration but truncated multi-byte rune at end (real-world
	// when stealth fetch hits a partial body). Output must still be valid
	// UTF-8 — invalid trailing bytes stripped.
	body := append([]byte("hello "), 0xE6, 0x97) // truncated 3-byte rune
	got := decodeBodyUTF8(body, "text/html; charset=utf-8")
	if !utf8.ValidString(got) {
		t.Fatalf("truncated UTF-8 not sanitized: % x", []byte(got))
	}
	if !strings.HasPrefix(got, "hello ") {
		t.Fatalf("prefix lost: %q", got)
	}
}
