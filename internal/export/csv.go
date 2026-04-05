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
		SELECT
			COALESCE(
				(SELECT array_agg(e.email) FROM business_emails be JOIN emails e ON e.id = be.email_id
				 WHERE be.business_id = bl.id AND e.validation_status IN ('valid', 'pending', 'unknown')),
				'{}'
			) AS emails,
			COALESCE(bl.phone, '') AS phone,
			bl.domain, bl.url, bl.social_links, COALESCE(bl.address, '')
		FROM business_listings bl
		WHERE EXISTS (
			SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id
			WHERE be.business_id = bl.id AND e.validation_status IN ('valid', 'pending', 'unknown')
		)
		ORDER BY bl.id ASC
	`

	rows, err := db.Query(query)
	if err != nil {
		return 0, fmt.Errorf("export: query business_listings: %w", err)
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
	header := []string{"emails", "phone", "domain", "url", "social_links", "address"}
	if err := w.Write(header); err != nil {
		return 0, fmt.Errorf("export: writing header: %w", err)
	}

	count := 0
	for rows.Next() {
		var emails []string
		var phone, domain, url, address string
		var socialLinksJSON []byte

		if err := rows.Scan(
			pq.Array(&emails),
			&phone,
			&domain,
			&url,
			&socialLinksJSON,
			&address,
		); err != nil {
			return count, fmt.Errorf("export: scanning row: %w", err)
		}

		record := []string{
			strings.Join(emails, ";"),
			phone,
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
