package cmd

import (
	"fmt"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/query"
)

// RunImport imports queries from a file into the database and pushes to Redis queue.
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

	// Connect to Redis.
	dd, err := dedup.New(&cfg.Redis)
	if err != nil {
		return fmt.Errorf("import: redis: %w", err)
	}
	defer dd.Close()

	// Insert queries (auto-pushed to Redis queue).
	repo := query.NewRepositoryWithRedis(database, dd.Client())
	inserted, err := repo.InsertBatch(queries)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	fmt.Printf("Inserted %d new queries (%d skipped as duplicates)\n", inserted, len(queries)-inserted)

	return nil
}
