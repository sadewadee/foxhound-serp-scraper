//go:build playwright

package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/sadewadee/serp-scraper/cmd"
	"github.com/sadewadee/serp-scraper/internal/config"
)

var version = "dev"

func main() {
	// Global flags.
	configPath := flag.String("config", "config.yaml", "Path to config file")
	verbose := flag.Bool("v", false, "Verbose logging")

	// Subcommand detection.
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "serp-scraper %s — Google SERP email scraping pipeline\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper [flags] <command> [command-flags]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  scrape    Standalone scrape — no PG/Redis needed, results to CSV\n")
		fmt.Fprintf(os.Stderr, "  import    Import queries from CSV/text file (pipeline mode)\n")
		fmt.Fprintf(os.Stderr, "  generate  Generate queries from templates (pipeline mode)\n")
		fmt.Fprintf(os.Stderr, "  run       Run the pipeline — requires PG + Redis\n")
		fmt.Fprintf(os.Stderr, "  status    Show pipeline status\n")
		fmt.Fprintf(os.Stderr, "  export    Export contacts from PG to CSV\n\n")
		fmt.Fprintf(os.Stderr, "Global flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper scrape -q \"gym in bali\" -o results.csv\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper scrape -q \"dentist jakarta\" -pages 5 -workers 20\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper import -file queries.csv\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper run\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper run -stage contact -workers 20\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper status\n")
		fmt.Fprintf(os.Stderr, "  serp-scraper export -format csv -output contacts.csv -email-only\n")
	}

	// Parse global flags, stop at first non-flag argument.
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Set up logging.
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	// Load config.
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Route to subcommand.
	command := args[0]
	subArgs := args[1:]

	switch command {
	case "scrape":
		err = runScrape(cfg, subArgs)
	case "import":
		err = runImport(cfg, subArgs)
	case "generate":
		err = runGenerate(cfg, subArgs)
	case "run":
		err = runPipeline(cfg, subArgs)
	case "status":
		err = cmd.RunStatus(cfg)
	case "export":
		err = runExport(cfg, subArgs)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runImport(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	file := fs.String("file", "", "Path to query file (CSV or text)")
	fs.Parse(args)

	if *file == "" {
		return fmt.Errorf("import: -file is required")
	}
	return cmd.RunImport(cfg, *file)
}

func runGenerate(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	templates := fs.String("templates", "", "Path to templates YAML file")
	configFile := fs.String("config", "", "Alias for -templates")
	fs.Parse(args)

	path := *templates
	if path == "" {
		path = *configFile
	}
	if path == "" {
		return fmt.Errorf("generate: -templates is required")
	}
	return cmd.RunGenerate(cfg, path)
}

func runPipeline(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	stage := fs.String("stage", "all", "Stage to run: all|serp|website|contact")
	workers := fs.Int("workers", 0, "Override worker count for the stage")
	fs.Parse(args)

	return cmd.RunPipeline(cfg, *stage, *workers)
}

func runScrape(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("scrape", flag.ExitOnError)
	query := fs.String("q", "", "Search query (e.g. \"gym in bali\")")
	output := fs.String("o", "results.csv", "Output CSV file")
	pages := fs.Int("pages", 0, "Override pages per query")
	workers := fs.Int("workers", 0, "Override contact extraction workers")
	contactPages := fs.Bool("contact-pages", true, "Also visit /contact, /about pages")
	fs.Parse(args)

	if *query == "" {
		return fmt.Errorf("scrape: -q is required")
	}
	if *pages > 0 {
		cfg.SERP.PagesPerQuery = *pages
	}
	cfg.Website.ContactPages = *contactPages
	return cmd.RunStandalone(cfg, *query, *output, *workers)
}

func runExport(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	format := fs.String("format", "csv", "Export format: csv")
	output := fs.String("output", "contacts.csv", "Output file path")
	emailOnly := fs.Bool("email-only", false, "Export only contacts with email")
	fs.Parse(args)

	return cmd.RunExport(cfg, *format, *output, *emailOnly)
}
