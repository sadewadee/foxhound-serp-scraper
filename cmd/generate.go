//go:build playwright

package cmd

import (
	"fmt"
	"strings"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/query"
)

// RunGenerate generates queries and inserts them into the database.
//
// When templatePath is non-empty it uses the YAML template system.
// When country is non-empty it uses the wellness/fitness keyword generator.
func RunGenerate(cfg *config.Config, templatePath string) error {
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

	// Load templates from YAML file.
	tmplCfg, err := query.LoadTemplates(templatePath)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	if len(tmplCfg.Templates) == 0 {
		fmt.Println("No templates found in config")
		return nil
	}

	// Generate and insert queries per template.
	totalGenerated := 0
	totalInserted := 0
	results := query.Generate(tmplCfg.Templates)

	for name, queries := range results {
		inserted, err := repo.InsertBatch(queries)
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

// RunGenerateWellness generates wellness/fitness keywords for the given country
// (or all countries when country == "all") and inserts them via the repository.
// An optional niche filter narrows generation to a single niche; pass "" for all niches.
func RunGenerateWellness(cfg *config.Config, country, niche string) error {
	database, err := db.Open(cfg)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	defer database.Close()

	if err := db.Migrate(database); err != nil {
		return fmt.Errorf("generate: %w", err)
	}

	repo := query.NewRepository(database)

	niches := query.Niches
	if niche != "" && strings.ToLower(niche) != "all" {
		niches = []string{niche}
	}

	var keywords []string
	if strings.ToLower(country) == "all" {
		var allCities []string
		for _, cities := range query.Cities {
			allCities = append(allCities, cities...)
		}
		keywords = query.GenerateKeywords(niches, allCities, query.WellnessTemplates)
	} else {
		cities, ok := query.Cities[country]
		if !ok {
			return fmt.Errorf("generate: country %q not found; run with --country all to see available countries", country)
		}
		keywords = query.GenerateKeywords(niches, cities, query.WellnessTemplates)
	}

	fmt.Printf("Generated %d keywords for country=%q niche=%q\n", len(keywords), country, niche)

	const batchSize = 500
	totalInserted := 0
	for i := 0; i < len(keywords); i += batchSize {
		end := i + batchSize
		if end > len(keywords) {
			end = len(keywords)
		}
		inserted, err := repo.InsertBatch(keywords[i:end])
		if err != nil {
			return fmt.Errorf("generate: batch %d: %w", i/batchSize, err)
		}
		totalInserted += inserted
	}

	dupes := len(keywords) - totalInserted
	fmt.Printf("Inserted: %d  Duplicates skipped: %d\n", totalInserted, dupes)
	return nil
}
