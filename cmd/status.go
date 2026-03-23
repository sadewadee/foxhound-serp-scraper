package cmd

import (
	"fmt"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/monitor"
)

// RunStatus prints current pipeline status.
func RunStatus(cfg *config.Config) error {
	// Connect to database.
	database, err := db.Open(cfg)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	defer database.Close()

	// Connect to Redis.
	dd, err := dedup.New(&cfg.Redis)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	defer dd.Close()

	status, err := monitor.GetStatus(database, dd.Client())
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}

	// Print status.
	fmt.Println("=== SERP Scraper Status ===")
	fmt.Println()

	printTable := func(name string, ts monitor.TableStatus) {
		fmt.Printf("%-12s total: %d\n", name+":", ts.Total)
		for status, count := range ts.ByStatus {
			fmt.Printf("  %-12s %d\n", status+":", count)
		}
	}

	printTable("Queries", status.Queries)
	printTable("SERP Seeds", status.Seeds)
	printTable("Websites", status.Websites)
	printTable("Contacts", status.Contacts)

	fmt.Println()
	fmt.Println("--- Redis Queues ---")
	for _, q := range status.Queues {
		fmt.Printf("  %-30s depth: %d\n", q.Name, q.Depth)
	}

	fmt.Println()
	fmt.Println("--- Dedup Sets ---")
	for _, d := range status.Dedup {
		fmt.Printf("  %-30s size: %d\n", d.Name, d.Size)
	}

	return nil
}
