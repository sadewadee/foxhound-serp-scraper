package cmd

import (
	"fmt"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/query"
)

// RunGenerate generates queries from templates and inserts into the database.
func RunGenerate(cfg *config.Config, templatePath string) error {
	// Load templates.
	tmplCfg, err := query.LoadTemplates(templatePath)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	if len(tmplCfg.Templates) == 0 {
		fmt.Println("No templates found in config")
		return nil
	}

	// Connect to database.
	database, err := db.Open(cfg)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	defer database.Close()

	if err := db.Migrate(database); err != nil {
		return fmt.Errorf("generate: %w", err)
	}

	repo := query.NewRepository(database)

	// Generate and insert queries per template.
	totalGenerated := 0
	totalInserted := 0
	results := query.Generate(tmplCfg.Templates)

	for name, queries := range results {
		inserted, err := repo.InsertBatch(queries, name)
		if err != nil {
			return fmt.Errorf("generate: template %s: %w", name, err)
		}
		fmt.Printf("Template %q: generated %d, inserted %d new\n", name, len(queries), inserted)
		totalGenerated += len(queries)
		totalInserted += inserted
	}

	fmt.Printf("Total: generated %d queries, inserted %d new\n", totalGenerated, totalInserted)
	return nil
}
