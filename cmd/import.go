package cmd

import (
	"fmt"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/query"
)

// RunImport imports queries from a file into the database.
func RunImport(cfg *config.Config, filePath string) error {
	// Read queries from file.
	queries, err := query.ImportFile(filePath)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	if len(queries) == 0 {
		fmt.Println("No queries found in file")
		return nil
	}
	fmt.Printf("Read %d queries from %s\n", len(queries), filePath)

	// Connect to database.
	database, err := db.Open(cfg)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	defer database.Close()

	if err := db.Migrate(database); err != nil {
		return fmt.Errorf("import: %w", err)
	}

	// Insert queries.
	repo := query.NewRepository(database)
	inserted, err := repo.InsertBatch(queries)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	fmt.Printf("Inserted %d new queries (%d skipped as duplicates)\n", inserted, len(queries)-inserted)

	return nil
}
