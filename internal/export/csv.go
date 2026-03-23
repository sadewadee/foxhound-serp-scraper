package export

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
)

// CSVOptions configures the CSV export.
type CSVOptions struct {
	Output    string
	EmailOnly bool
}

// ExportCSV exports contacts from PostgreSQL to a CSV file.
func ExportCSV(db *sql.DB, opts CSVOptions) (int, error) {
	whereClause := ""
	if opts.EmailOnly {
		whereClause = "WHERE email IS NOT NULL AND email != ''"
	}

	query := fmt.Sprintf(`
		SELECT COALESCE(email,''), COALESCE(phone,''), domain, source_url,
		       COALESCE(instagram,''), COALESCE(facebook,''), COALESCE(twitter,''),
		       COALESCE(linkedin,''), COALESCE(whatsapp,''), COALESCE(address,'')
		FROM contacts
		%s
		ORDER BY id ASC
	`, whereClause)

	rows, err := db.Query(query)
	if err != nil {
		return 0, fmt.Errorf("export: query contacts: %w", err)
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
	header := []string{
		"email", "phone", "domain", "source_url",
		"instagram", "facebook", "twitter", "linkedin", "whatsapp", "address",
	}
	if err := w.Write(header); err != nil {
		return 0, fmt.Errorf("export: writing header: %w", err)
	}

	count := 0
	for rows.Next() {
		var email, phone, domain, sourceURL string
		var instagram, facebook, twitter, linkedin, whatsapp, address string

		if err := rows.Scan(&email, &phone, &domain, &sourceURL,
			&instagram, &facebook, &twitter, &linkedin, &whatsapp, &address); err != nil {
			return count, fmt.Errorf("export: scanning row: %w", err)
		}

		record := []string{
			email, phone, domain, sourceURL,
			instagram, facebook, twitter, linkedin, whatsapp, address,
		}
		if err := w.Write(record); err != nil {
			return count, fmt.Errorf("export: writing row: %w", err)
		}
		count++
	}

	return count, rows.Err()
}
