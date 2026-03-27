# Telegram Bot for SERP Scraper

## Problem
The serp-scraper has no UI. Managing scraping jobs requires curl commands or SSH. A Telegram bot provides a simple chat-based interface for submitting keywords, checking status, and exporting results.

## Design

### Architecture
The bot runs as a goroutine inside the serp-scraper process — same binary, same container. It uses Telegram Bot API long-polling (getUpdates) with no external dependencies. The bot calls internal Go functions directly (query.Repository, db, redis) — no HTTP round-trip to the API server.

If `TELEGRAM_BOT_TOKEN` env var is empty, the bot does not start. No error, no log noise.

### New Files
- `internal/telegram/bot.go` — Bot struct, polling loop, command router
- `internal/telegram/handlers.go` — One handler function per command

### Modified Files
- `internal/config/config.go` — Add `Telegram.BotToken` field, read from `TELEGRAM_BOT_TOKEN` env
- `cmd/run.go` — Start bot goroutine if token is set

### Env Vars
- `TELEGRAM_BOT_TOKEN` — Bot token from @BotFather. Optional — bot disabled when empty.

### Commands

| Command | Handler | Internal Call | Response |
|---------|---------|--------------|----------|
| `/start` | handleStart | — | Welcome text + command list |
| `/scrape <keywords>` | handleScrape | `queryRepo.InsertBatch(keywords, "")` | "Inserted X keywords (Y duplicates)" |
| `/status` | handleStatus | DB table counts + Redis queue/dedup sizes | Formatted stats message |
| `/queries` | handleQueries | `queryRepo.CountByStatus()` | "Pending: X, Processing: Y, Done: Z, Error: W" |
| `/contacts` | handleContacts | DB aggregate queries on contacts table | "Total: X, With email: Y, Last 24h: Z" |
| `/export` | handleExport | DB query → CSV in memory → sendDocument | CSV file attachment |
| `/retry` | handleRetry | `queryRepo.RetryErrors()` | "Retried X queries" |
| `/reset` | handleReset | Redis DEL on dedup keys | "Cleared: urls=X, domains=Y, emails=Z" |

### Telegram API — No External Library
Use `net/http` directly against `https://api.telegram.org/bot<token>/`:
- `getUpdates` — long poll with `timeout=30`, track `offset`
- `sendMessage` — text replies with Markdown parse mode
- `sendDocument` — for CSV export (multipart/form-data)

### Bot Struct
```go
type Bot struct {
    token     string
    apiURL    string
    db        *sql.DB
    redis     *redis.Client
    queryRepo *query.Repository
    client    *http.Client
    offset    int64
}
```

### Lifecycle
1. `cmd/run.go` checks `cfg.Telegram.BotToken`
2. If set, creates `telegram.Bot` with shared `db`, `redis`, `queryRepo`
3. Starts `bot.Run(ctx)` in a goroutine
4. Bot polls `getUpdates` in a loop until context is cancelled
5. On shutdown signal, context cancels, bot stops polling

### Keyword Input Format for /scrape
Users send keywords one per line after `/scrape`:
```
/scrape
jasa web design jakarta
marketing agency bali
SEO company surabaya
```

Or inline: `/scrape jasa web design jakarta` (single keyword).

### Error Handling
- Telegram API errors: log and continue polling
- DB/Redis errors: reply to user with error message, continue
- Network timeout on getUpdates: retry with backoff (1s, 2s, 4s, max 30s)

### Docker
No changes needed. Bot runs inside the existing container. Only add `TELEGRAM_BOT_TOKEN` to docker-compose environment for the manager service.
