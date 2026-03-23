package cmd

import (
	"fmt"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/export"
)

// RunExport exports contacts to a file.
func RunExport(cfg *config.Config, format, output string, emailOnly bool) error {
	// Connect to database.
	database, err := db.Open(cfg)
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}
	defer database.Close()

	switch format {
	case "csv", "":
		opts := export.CSVOptions{
			Output:    output,
			EmailOnly: emailOnly,
		}
		count, err := export.ExportCSV(database, opts)
		if err != nil {
			return fmt.Errorf("export: %w", err)
		}
		fmt.Printf("Exported %d contacts to %s\n", count, output)
	default:
		return fmt.Errorf("export: unsupported format %q (supported: csv)", format)
	}

	return nil
}
