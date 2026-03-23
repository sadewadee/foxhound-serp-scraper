package query

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

// ImportFile reads queries from a CSV or plain text file.
// CSV: auto-detects column with header "keyword", "query", or "search" (case-insensitive).
// Plain text: one query per line.
func ImportFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("import: opening %s: %w", path, err)
	}
	defer f.Close()

	// Try CSV first.
	if strings.HasSuffix(strings.ToLower(path), ".csv") {
		return readCSV(f)
	}
	return readPlainText(f)
}

func readCSV(r io.Reader) ([]string, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // Allow variable fields.
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("import: reading CSV: %w", err)
	}
	if len(records) == 0 {
		return nil, nil
	}

	// Find the query column index from the header.
	header := records[0]
	keywordIdx := -1
	validHeaders := []string{"keyword", "query", "search", "q"}
	for i, col := range header {
		col = strings.TrimSpace(strings.ToLower(col))
		for _, h := range validHeaders {
			if col == h {
				keywordIdx = i
				break
			}
		}
		if keywordIdx >= 0 {
			break
		}
	}

	startRow := 0
	if keywordIdx >= 0 {
		startRow = 1 // Skip header row.
	} else {
		// No known header found — check if first row looks like a header.
		if isHeaderLike(header[0]) {
			keywordIdx = 0
			startRow = 1
		} else {
			keywordIdx = 0
		}
	}

	var queries []string
	seen := make(map[string]bool)
	for _, row := range records[startRow:] {
		if keywordIdx >= len(row) {
			continue
		}
		q := strings.TrimSpace(row[keywordIdx])
		if q == "" {
			continue
		}
		key := strings.ToLower(q)
		if seen[key] {
			continue
		}
		seen[key] = true
		queries = append(queries, q)
	}
	return queries, nil
}

func readPlainText(r io.Reader) ([]string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("import: reading file: %w", err)
	}

	var queries []string
	seen := make(map[string]bool)
	for _, line := range strings.Split(string(data), "\n") {
		q := strings.TrimSpace(line)
		if q == "" || strings.HasPrefix(q, "#") {
			continue
		}
		key := strings.ToLower(q)
		if seen[key] {
			continue
		}
		seen[key] = true
		queries = append(queries, q)
	}
	return queries, nil
}

// isHeaderLike returns true if the string looks like a CSV column header.
func isHeaderLike(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	headers := []string{
		"keyword", "query", "search", "q", "term", "text",
		"name", "id", "no", "number", "index",
	}
	for _, h := range headers {
		if lower == h {
			return true
		}
	}
	return false
}
