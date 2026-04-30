package stage

import (
	"bytes"
	"io"
	"strings"

	"golang.org/x/net/html/charset"
)

// decodeBodyUTF8 converts a raw HTTP response body to a valid UTF-8 string.
// Charset is detected from Content-Type, BOM, and HTML <meta charset> in that
// order, with HTML5 fallback. Required because some pages (Hebrew
// windows-1255, Latin-1 German/French) ship non-UTF-8 bytes that PostgreSQL
// rejects when written into TEXT columns. Final ToValidUTF8 strips any
// leftover invalid sequences from truncated multi-byte runes or unknown
// labels.
func decodeBodyUTF8(body []byte, contentType string) string {
	if len(body) == 0 {
		return ""
	}
	reader, err := charset.NewReader(bytes.NewReader(body), contentType)
	if err != nil {
		return strings.ToValidUTF8(string(body), "")
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return strings.ToValidUTF8(string(body), "")
	}
	return strings.ToValidUTF8(string(decoded), "")
}
