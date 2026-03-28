//go:build playwright

package telegram

import (
	"context"
	"fmt"
	"strings"
	"time"

	pq "github.com/lib/pq"
	"github.com/sadewadee/serp-scraper/internal/query"
)

func (b *Bot) handleMessage(ctx context.Context, msg *Message) {
	// Access control: if allowedChatIDs is configured, reject unauthorized users.
	if len(b.allowedChatIDs) > 0 && !b.allowedChatIDs[msg.Chat.ID] {
		b.sendMessage(msg.Chat.ID, "Unauthorized. Your chat ID: "+fmt.Sprintf("%d", msg.Chat.ID))
		return
	}

	text := strings.TrimSpace(msg.Text)

	// Extract command and args.
	cmd := text
	args := ""
	if i := strings.Index(text, " "); i > 0 {
		cmd = text[:i]
		args = strings.TrimSpace(text[i+1:])
	}
	// Strip @botname suffix from command.
	if i := strings.Index(cmd, "@"); i > 0 {
		cmd = cmd[:i]
	}

	switch cmd {
	case "/start", "/help":
		b.handleStart(msg)
	case "/scrape":
		b.handleScrape(msg, args)
	case "/status":
		b.handleStatus(ctx, msg)
	case "/queries":
		b.handleQueries(msg)
	case "/contacts":
		b.handleContacts(msg)
	case "/export":
		b.handleExport(msg)
	case "/retry":
		b.handleRetry(msg)
	case "/reset":
		b.handleReset(ctx, msg)
	case "/generate":
		b.handleGenerate(msg, args)
	}
}

func (b *Bot) handleStart(msg *Message) {
	text := `*SERP Scraper Bot*

Available commands:

/scrape <keywords> — Submit keywords (one per line)
/generate <country> — Generate wellness keywords ("all" for global)
/status — Full dashboard (queries, SERP, enrich, emails, providers)
/queries — Query status breakdown
/contacts — Contact stats + top email providers
/export — Export contacts as CSV file
/retry — Retry failed queries
/reset — Reset dedup sets`

	b.sendMessage(msg.Chat.ID, text)
}

func (b *Bot) handleScrape(msg *Message, args string) {
	if args == "" {
		b.sendMessage(msg.Chat.ID, "Usage: /scrape keyword1\nkeyword2\nkeyword3")
		return
	}

	var keywords []string
	for _, line := range strings.Split(args, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			keywords = append(keywords, line)
		}
	}
	if len(keywords) == 0 {
		b.sendMessage(msg.Chat.ID, "No keywords provided.")
		return
	}
	if len(keywords) > 500 {
		b.sendMessage(msg.Chat.ID, "Max 500 keywords per request.")
		return
	}

	inserted, err := b.queryRepo.InsertBatch(keywords)
	if err != nil {
		b.sendMessage(msg.Chat.ID, "Error: "+err.Error())
		return
	}

	dupes := len(keywords) - inserted
	text := fmt.Sprintf("Inserted *%d* keywords", inserted)
	if dupes > 0 {
		text += fmt.Sprintf(" (%d duplicates skipped)", dupes)
	}
	b.sendMessage(msg.Chat.ID, text)
}

func (b *Bot) handleGenerate(msg *Message, args string) {
	country := strings.TrimSpace(args)
	if country == "" {
		// List available countries.
		countries := query.CountryList()
		text := "*Available countries:*\n\n"
		for _, c := range countries {
			cities := query.Cities[c]
			text += fmt.Sprintf("• %s (%d cities)\n", c, len(cities))
		}
		text += "\nUsage: /generate <country> or /generate all"
		b.sendMessage(msg.Chat.ID, text)
		return
	}

	var keywords []string
	if strings.ToLower(country) == "all" {
		keywords = query.GenerateAllKeywords()
	} else {
		keywords = query.GenerateKeywordsForCountry(country)
		if keywords == nil {
			b.sendMessage(msg.Chat.ID, fmt.Sprintf("Country '%s' not found. Send /generate to see available countries.", country))
			return
		}
	}

	b.sendMessage(msg.Chat.ID, fmt.Sprintf("Generating *%d* keywords for %s...", len(keywords), country))

	const batchSize = 500
	totalInserted := 0
	for i := 0; i < len(keywords); i += batchSize {
		end := i + batchSize
		if end > len(keywords) {
			end = len(keywords)
		}
		inserted, err := b.queryRepo.InsertBatch(keywords[i:end])
		if err != nil {
			b.sendMessage(msg.Chat.ID, fmt.Sprintf("Error at batch %d: %s", i/batchSize, err.Error()))
			return
		}
		totalInserted += inserted
	}

	dupes := len(keywords) - totalInserted
	text := fmt.Sprintf("*Done!*\nGenerated: %d\nInserted: %d\nDuplicates: %d", len(keywords), totalInserted, dupes)
	b.sendMessage(msg.Chat.ID, text)
}

func (b *Bot) handleStatus(ctx context.Context, msg *Message) {
	lines := []string{"*Dashboard*"}

	// ── Queries ── (single query)
	var qTotal, qPending, qProcessing, qCompleted, qError int
	b.db.QueryRow(`
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'error')
		FROM queries
	`).Scan(&qTotal, &qPending, &qProcessing, &qCompleted, &qError)
	lines = append(lines, "",
		"_Queries:_",
		fmt.Sprintf("  Total: *%d*  Pending: %d", qTotal, qPending),
		fmt.Sprintf("  Processing: %d  Done: %d  Error: %d", qProcessing, qCompleted, qError))

	// ── SERP ── (single query)
	var serpTotal, serpPending, serpCompleted, serpFailed, serpURLs int
	var serpHour, serpToday int
	b.db.QueryRow(`
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'new'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COALESCE(SUM(result_count) FILTER (WHERE status = 'completed'), 0),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '24 hours')
		FROM serp_jobs
	`).Scan(&serpTotal, &serpPending, &serpCompleted, &serpFailed, &serpURLs, &serpHour, &serpToday)
	lines = append(lines, "",
		"_SERP:_",
		fmt.Sprintf("  Pages: *%d*  Pending: %d  Done: %d  Failed: %d", serpTotal, serpPending, serpCompleted, serpFailed),
		fmt.Sprintf("  URLs found: *%d*", serpURLs),
		fmt.Sprintf("  Rate: %d/hour  Today: %d", serpHour, serpToday))

	// ── Enrich ── (single query)
	var enTotal, enPending, enCompleted, enFailed, enDead int
	var enHour, enToday int
	b.db.QueryRow(`
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'dead'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours')
		FROM enrich_jobs
	`).Scan(&enTotal, &enPending, &enCompleted, &enFailed, &enDead, &enHour, &enToday)
	lines = append(lines, "",
		"_Enrich:_",
		fmt.Sprintf("  Total: *%d*  Pending: %d  Done: %d", enTotal, enPending, enCompleted),
		fmt.Sprintf("  Failed: %d  Dead: %d", enFailed, enDead),
		fmt.Sprintf("  Rate: %d/hour  Today: %d", enHour, enToday))

	// ── Contacts ── (single query)
	var uniqueEmails, emailsToday, emailsHour, uniqueDomains int
	b.db.QueryRow(`
		SELECT
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours'),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'),
			COUNT(DISTINCT domain)
		FROM enrich_jobs
		WHERE status = 'completed'
	`).Scan(&uniqueEmails, &emailsToday, &emailsHour, &uniqueDomains)
	lines = append(lines, "",
		"_Contacts:_",
		fmt.Sprintf("  Unique emails: *%d*", uniqueEmails),
		fmt.Sprintf("  Emails: %d/hour  Today: %d", emailsHour, emailsToday),
		fmt.Sprintf("  Domains: %d", uniqueDomains))

	// ── Top providers ──
	providerRows, _ := b.db.Query(`
		SELECT split_part(e, '@', 2), COUNT(*)
		FROM enrich_jobs, unnest(emails) AS e
		WHERE status = 'completed'
		GROUP BY 1 ORDER BY 2 DESC LIMIT 5
	`)
	if providerRows != nil {
		lines = append(lines, "", "_Top providers:_")
		for providerRows.Next() {
			var p string
			var c int
			providerRows.Scan(&p, &c)
			lines = append(lines, fmt.Sprintf("  %s: %d", p, c))
		}
		providerRows.Close()
	}

	// ── Queues ──
	var qWeb, qEnrich int64
	qWeb, _ = b.redis.ZCard(ctx, "serp:queue:websites").Result()
	qEnrich, _ = b.redis.ZCard(ctx, "serp:queue:enrich").Result()
	if qWeb > 0 || qEnrich > 0 {
		lines = append(lines, "",
			"_Queues:_",
			fmt.Sprintf("  websites: %d  enrich: %d", qWeb, qEnrich))
	}

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

func (b *Bot) handleQueries(msg *Message) {
	counts, err := b.queryRepo.CountByStatus()
	if err != nil {
		b.sendMessage(msg.Chat.ID, "Error: "+err.Error())
		return
	}

	total := 0
	for _, c := range counts {
		total += c
	}

	text := fmt.Sprintf("*Query Stats*\n\nTotal: *%d*\nPending: %d\nProcessing: %d\nDone: %d\nError: %d",
		total,
		counts["pending"],
		counts["processing"],
		counts["completed"],
		counts["error"],
	)
	b.sendMessage(msg.Chat.ID, text)
}

func (b *Bot) handleContacts(msg *Message) {
	var total, withEmail, uniqueDomains, lastHour, last24h int

	b.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed'").Scan(&total)
	b.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND array_length(emails, 1) > 0").Scan(&withEmail)
	b.db.QueryRow("SELECT COUNT(DISTINCT domain) FROM enrich_jobs WHERE status = 'completed'").Scan(&uniqueDomains)
	b.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'").Scan(&lastHour)
	b.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours'").Scan(&last24h)

	lines := []string{
		"*Contact Stats*",
		"",
		fmt.Sprintf("Total: *%d*", total),
		fmt.Sprintf("With email: %d", withEmail),
		fmt.Sprintf("Domains: %d", uniqueDomains),
		fmt.Sprintf("Last hour: %d", lastHour),
		fmt.Sprintf("Last 24h: %d", last24h),
	}

	// Top email providers.
	providerRows, _ := b.db.Query(`
		SELECT split_part(e, '@', 2) AS provider, COUNT(*) AS cnt
		FROM enrich_jobs, unnest(emails) AS e
		WHERE status = 'completed'
		GROUP BY provider ORDER BY cnt DESC LIMIT 5
	`)
	if providerRows != nil {
		defer providerRows.Close()
		lines = append(lines, "", "_Top providers:_")
		for providerRows.Next() {
			var provider string
			var cnt int
			providerRows.Scan(&provider, &cnt)
			lines = append(lines, fmt.Sprintf("  %s: %d", provider, cnt))
		}
	}

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

func (b *Bot) handleExport(msg *Message) {
	b.sendMessage(msg.Chat.ID, "Generating CSV export...")

	rows, err := b.db.Query(`
		SELECT emails, phones, domain, url, social_links, address
		FROM enrich_jobs
		WHERE status = 'completed' AND array_length(emails, 1) > 0
		ORDER BY id ASC LIMIT 50000
	`)
	if err != nil {
		b.sendMessage(msg.Chat.ID, "Error: "+err.Error())
		return
	}
	defer rows.Close()

	var buf strings.Builder
	buf.WriteString("emails,phones,domain,url,social_links,address\n")

	count := 0
	for rows.Next() {
		var emails, phones []string
		var domain, url, address string
		var socialLinksJSON []byte
		rows.Scan(pq.Array(&emails), pq.Array(&phones), &domain, &url, &socialLinksJSON, &address)
		emailsStr := strings.Join(emails, ";")
		phonesStr := strings.Join(phones, ";")
		fmt.Fprintf(&buf, "%s,%s,%s,%s,%s,%s\n",
			csvEscape(emailsStr), csvEscape(phonesStr), csvEscape(domain), csvEscape(url),
			csvEscape(string(socialLinksJSON)), csvEscape(address))
		count++
	}

	if count == 0 {
		b.sendMessage(msg.Chat.ID, "No contacts with email found.")
		return
	}

	filename := fmt.Sprintf("contacts_%s.csv", time.Now().Format("2006-01-02"))
	b.sendDocument(msg.Chat.ID, filename, []byte(buf.String()))
	if count >= 50000 {
		b.sendMessage(msg.Chat.ID, fmt.Sprintf("Exported *%d* contacts (truncated at 50K). Use API for full export.", count))
	} else {
		b.sendMessage(msg.Chat.ID, fmt.Sprintf("Exported *%d* contacts.", count))
	}
}

func (b *Bot) handleRetry(msg *Message) {
	retried, err := b.queryRepo.RetryErrors()
	if err != nil {
		b.sendMessage(msg.Chat.ID, "Error: "+err.Error())
		return
	}
	if retried == 0 {
		b.sendMessage(msg.Chat.ID, "No failed queries to retry.")
		return
	}
	b.sendMessage(msg.Chat.ID, fmt.Sprintf("Retried *%d* queries.", retried))
}

func (b *Bot) handleReset(ctx context.Context, msg *Message) {
	lines := []string{"*Dedup Reset*", ""}

	for _, key := range []string{"serp:dedup:urls", "serp:dedup:domains"} {
		n, _ := b.redis.SCard(ctx, key).Result()
		b.redis.Del(ctx, key)
		short := key[len("serp:dedup:"):]
		lines = append(lines, fmt.Sprintf("%s: %d cleared", short, n))
	}

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

func csvEscape(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
	}
	return s
}
