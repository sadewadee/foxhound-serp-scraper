package export

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	pq "github.com/lib/pq"
)

// CSVOptions configures the CSV export.
type CSVOptions struct {
	Output    string
	EmailOnly bool
}

// ExportCSV exports enriched contacts from PostgreSQL to a CSV file.
func ExportCSV(db *sql.DB, opts CSVOptions) (int, error) {
	query := `
		SELECT emails, phones, domain, url, social_links, address
		FROM enrich_jobs
		WHERE status = 'completed' AND array_length(emails, 1) > 0
		ORDER BY id ASC
	`

	rows, err := db.Query(query)
	if err != nil {
		return 0, fmt.Errorf("export: query enrich_jobs: %w", err)
	}
	defer rows.Close()

	f, err := os.Create(opts.Output)
	if err != nil {
		return 0, fmt.Errorf("export: creating file %s: %w", opts.Output, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Write header.
	header := []string{"emails", "phones", "domain", "url", "social_links", "address"}
	if err := w.Write(header); err != nil {
		return 0, fmt.Errorf("export: writing header: %w", err)
	}

	count := 0
	for rows.Next() {
		var emails, phones []string
		var domain, url, address string
		var socialLinksJSON []byte

		if err := rows.Scan(
			pq.Array(&emails),
			pq.Array(&phones),
			&domain,
			&url,
			&socialLinksJSON,
			&address,
		); err != nil {
			return count, fmt.Errorf("export: scanning row: %w", err)
		}

		record := []string{
			strings.Join(emails, ";"),
			strings.Join(phones, ";"),
			domain,
			url,
			string(socialLinksJSON),
			address,
		}
		if err := w.Write(record); err != nil {
			return count, fmt.Errorf("export: writing row: %w", err)
		}
		count++
	}

	return count, rows.Err()
}
