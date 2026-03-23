# serp-scraper

Multi-stage pipeline to collect emails from Google SERP results. Uses [Foxhound](https://github.com/sadewadee/foxhound) as the scraping engine with Camoufox for Google SERP and SmartFetch (stealth HTTP + browser fallback) for target websites.

**Target math**: 5M emails / 23% yield = ~21.7M SERP results needed → ~21.7K queries × 10 pages × 100 results/query.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│  Stage 1: Query │     │  Stage 2: SERP   │     │  Stage 3: Site  │     │  Stage 4: Contact│
│  Management     │────>│  Discovery       │────>│  Discovery      │────>│  Enrichment      │
│                 │     │                  │     │                 │     │                  │
│ import CSV/txt  │     │ Google Camoufox  │     │ SmartFetch      │     │ Multi-worker     │
│ template gen    │     │ 2 workers        │     │ 5 workers       │     │ 10+ workers      │
│ dedup + store   │     │ proxy rotation   │     │ contact pages   │     │ email/phone/soc  │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └──────────────────┘
        │                       │                        │                        │
        v                       v                        v                        v
   ┌─────────┐           ┌───────────┐            ┌───────────┐           ┌───────────┐
   │ PG:     │           │ Redis:    │            │ Redis:    │           │ PG:       │
   │ queries │           │ queue:    │            │ queue:    │           │ contacts  │
   │         │           │ websites  │            │ contacts  │           │           │
   └─────────┘           └───────────┘            └───────────┘           └───────────┘
```

Each stage is independently scalable. Redis sorted sets (ZPOPMIN) for inter-stage queues. PostgreSQL for persistent state. Workers on different servers can share the same Redis + PG.

## Requirements

- Go 1.25+
- PostgreSQL 15+
- Redis 7+
- Playwright browsers (installed via `npx playwright install`)

## Build

```bash
go build -tags "playwright" -o serp-scraper .
```

## Quick Start

```bash
# 1. Set up database
export POSTGRES_DSN="postgres://user:pass@localhost:5432/serp?sslmode=disable"
export REDIS_ADDR="localhost:6379"

# 2. Import queries
./serp-scraper import -file queries.csv

# 3. Run pipeline
./serp-scraper run

# 4. Check progress
./serp-scraper status

# 5. Export results
./serp-scraper export -output contacts.csv
```

## Commands

### `import` — Import queries from file

```bash
./serp-scraper import -file <path>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-file` | *required* | Path to query file. CSV (auto-detects `keyword`, `query`, `search`, `q` column) or plain text (one query per line). Lines starting with `#` are skipped. |

### `generate` — Generate queries from templates

```bash
./serp-scraper generate -templates <path>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-templates` | *required* | Path to templates YAML file |
| `-config` | | Alias for `-templates` |

Template file format:

```yaml
templates:
  - name: "industry_city"
    pattern: "{industry} {city} email contact"
    variables:
      industry: ["plumber", "electrician", "dentist", "lawyer"]
      city: ["jakarta", "surabaya", "bandung", "medan"]
```

Produces cartesian product: `plumber jakarta email contact`, `plumber surabaya email contact`, etc.

### `run` — Run the scraping pipeline

```bash
./serp-scraper run [-stage <stage>] [-workers <n>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-stage` | `all` | Stage to run: `all`, `serp`, `website`, `contact` |
| `-workers` | `0` (use config) | Override worker count for the specified stage |

Stages:

| Stage | What it does | Default workers |
|-------|-------------|-----------------|
| `serp` | Scrape Google SERP via Camoufox, extract result URLs | 2 |
| `website` | Visit URLs, discover contact pages (/contact, /about, etc.) | 5 |
| `contact` | Extract emails, phones, social media from pages | 10 |

### `status` — Show pipeline status

```bash
./serp-scraper status
```

Displays table counts by status, Redis queue depths, and dedup set sizes. No flags.

### `export` — Export contacts to file

```bash
./serp-scraper export [-format <fmt>] [-output <path>] [-email-only]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-format` | `csv` | Export format (currently only `csv`) |
| `-output` | `contacts.csv` | Output file path |
| `-email-only` | `false` | Only export contacts that have an email |

CSV columns: `email, phone, domain, source_url, instagram, facebook, twitter, linkedin, whatsapp, address`

## Global Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-config` | `config.yaml` | Path to config file |
| `-v` | `false` | Verbose logging (debug level) |

## Configuration

All config supports environment variable expansion with `${VAR}` and `${VAR:-default}` syntax.

```yaml
postgres:
  dsn: "${POSTGRES_DSN}"             # PostgreSQL connection string
  max_open_conns: 50                  # Max open connections
  max_idle_conns: 10                  # Max idle connections

redis:
  addr: "${REDIS_ADDR:-localhost:6379}"
  password: "${REDIS_PASSWORD:-}"
  db: 0

serp:
  pages_per_query: 10                 # Google SERP pages to scrape per query
  results_per_page: 10                # Results per SERP page (Google's num param)
  delay_between_pages_ms: 5000        # Mean delay between pages (log-normal)
  delay_between_queries_ms: 30000     # Mean delay between queries
  workers: 2                          # SERP discovery workers (keep low)

website:
  workers: 5                          # Website discovery workers
  contact_pages: true                 # Also visit /contact, /about, etc.
  timeout_ms: 20000                   # Per-page timeout

contact:
  workers: 10                         # Contact extraction workers
  timeout_ms: 15000                   # Per-page timeout
  validate_mx: false                  # Check MX records for emails
  social_extraction: true             # Extract Instagram, Facebook, etc.

proxy:
  url: "${PROXY_URL}"                 # SOCKS5 or HTTP proxy URL

fetch:
  headless: true                      # Run browser in headless mode
  block_images: true                  # Block images/media for faster loading

monitor:
  enabled: true                       # Enable Prometheus metrics endpoint
  port: 9090                          # Prometheus metrics port
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `POSTGRES_DSN` | PostgreSQL connection string |
| `REDIS_ADDR` | Redis address (default: `localhost:6379`) |
| `REDIS_PASSWORD` | Redis password |
| `PROXY_URL` | Proxy URL for all fetchers |

## Distributed Workers

Since Redis + PG are the shared state, workers on any server can connect to the same queues:

```
[Server A - Central]                  [Server B - Worker]
  PostgreSQL :5432                      serp-scraper run -stage contact -workers 20
  Redis :6379                           → connects to Server A's Redis + PG
  serp-scraper run -stage serp

[Server C - Worker]
  serp-scraper run -stage website -workers 10
  → connects to Server A's Redis + PG
```

ZPOPMIN is atomic — multiple workers across servers pop from the same queue without duplicate work.

### Docker

```bash
# Build
docker build -t serp-scraper .

# Run worker connecting to remote PG + Redis
docker run -e POSTGRES_DSN="postgres://user:pass@100.x.x.1:5432/serp?sslmode=disable" \
           -e REDIS_ADDR="100.x.x.1:6379" \
           serp-scraper run -stage contact -workers 20
```

## Dedup Strategy

3-layer dedup prevents duplicate work at every stage:

| Layer | What | Method |
|-------|------|--------|
| Redis SET | URL, domain, email (hot path) | `SADD` returns 0 = skip |
| PostgreSQL UNIQUE | query hash, URL hash, email+domain | `ON CONFLICT DO NOTHING` |
| In-memory map | Per-worker batch dedup | `map[string]bool` |

| Stage | Dedup key | Redis key |
|-------|-----------|-----------|
| Query import | `SHA256(lower(trim(query)))` | — (PG only) |
| SERP extract | `SHA256(canonical(url))` | `serp:dedup:urls` |
| Site discovery | domain string | `serp:dedup:domains` |
| Contact page | `SHA256(canonical(url))` | `serp:dedup:urls` (shared) |
| Email store | `SHA256(lower(email))` | `serp:dedup:emails` |

## Resume

On restart, queries and websites stuck in `processing` status are re-queued to `pending`. No duplicate work, no lost progress.

## Foxhound Integration

| What | Foxhound Package | Usage |
|------|-----------------|-------|
| Browser scraping | `fetch.NewCamoufox()` | Google SERP (Camoufox required) |
| HTTP scraping | `fetch.NewStealth()` | Target websites (10x faster) |
| Identity profiles | `identity.Generate()` | Per-worker fingerprints |
| Email extraction | `parse.ExtractEmails()` | CloudFlare cfemail + mailto + regex |
| Phone extraction | `parse.ExtractPhones()` | International phone patterns |
| Human-like delays | `behavior.NewTiming()` | Anti-detection timing |

## Database Schema

### queries
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| text | TEXT | Query string |
| text_hash | TEXT | SHA256(lower(trim(text))), UNIQUE |
| template_id | TEXT | Template name (NULL if imported) |
| status | TEXT | pending / processing / completed / failed |
| result_count | INTEGER | URLs found from this query |
| error_msg | TEXT | Error message if failed |

### serp_seeds
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| query_id | BIGINT | FK to queries |
| google_url | TEXT | Google SERP URL |
| page_num | INTEGER | Page number (0-indexed) |
| status | TEXT | pending / processing / completed / failed |
| result_count | INTEGER | Results found on this page |

### websites
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| domain | TEXT | Extracted domain |
| url | TEXT | Full URL |
| url_hash | TEXT | SHA256(canonical(url)), UNIQUE |
| source_query_id | BIGINT | FK to queries |
| source_serp_id | BIGINT | FK to serp_seeds |
| page_type | TEXT | serp_result / contact / about |
| status | TEXT | pending / processing / completed / failed |

### contacts
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key |
| email | TEXT | Email address |
| email_hash | TEXT | SHA256(lower(email)), UNIQUE with domain |
| phone | TEXT | Phone number |
| domain | TEXT | Source domain |
| website_id | BIGINT | FK to websites |
| source_url | TEXT | Page URL where found |
| source_query_id | BIGINT | FK to queries |
| instagram | TEXT | Instagram profile URL |
| facebook | TEXT | Facebook profile URL |
| twitter | TEXT | Twitter/X profile URL |
| linkedin | TEXT | LinkedIn profile URL |
| whatsapp | TEXT | WhatsApp number |
| address | TEXT | Physical address |
| mx_valid | BOOLEAN | MX record validation result |

## Project Structure

```
serp-scraper/
├── main.go                          # CLI entry point
├── config.yaml                      # Default config
├── go.mod
│
├── cmd/
│   ├── import.go                    # serp-scraper import
│   ├── generate.go                  # serp-scraper generate
│   ├── run.go                       # serp-scraper run
│   ├── status.go                    # serp-scraper status
│   └── export.go                    # serp-scraper export
│
└── internal/
    ├── config/config.go             # YAML config + env var expansion
    ├── db/
    │   ├── postgres.go              # Connection pool
    │   ├── migrate.go               # Auto CREATE TABLE IF NOT EXISTS
    │   └── models.go                # Go structs
    ├── dedup/dedup.go               # Redis SET wrapper + hash helpers
    ├── query/
    │   ├── importer.go              # CSV/text file reader
    │   ├── generator.go             # Template expansion
    │   └── repository.go            # PostgreSQL CRUD for queries
    ├── scraper/
    │   ├── fetcher.go               # Camoufox + Stealth init
    │   ├── google.go                # SERP URL builder + parser
    │   └── contact.go               # Email/phone/social extraction
    ├── stage/
    │   ├── serp.go                  # Stage 2: SERP discovery workers
    │   ├── website.go               # Stage 3: Website discovery workers
    │   └── contact.go               # Stage 4: Contact enrichment workers
    ├── pipeline/orchestrator.go     # Start/stop all stages
    ├── export/csv.go                # CSV export
    └── monitor/
        ├── metrics.go               # Prometheus counters
        └── status.go                # CLI status display
```
