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
	case "/analytics":
		b.handleAnalytics(ctx, msg)
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

/status — Dashboard with rates, totals, ETA
/analytics — Historical: 1hr vs prev hr, today vs yesterday, 7d trends
/scrape <keywords> — Submit keywords (one per line)
/generate <country> — Generate wellness keywords ("all" for global)
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

// ────────────────────────────────────────────────────────────────────
// /status — Live dashboard with rates, totals, ETA
// ────────────────────────────────────────────────────────────────────

func (b *Bot) handleStatus(ctx context.Context, msg *Message) {
	// ── Active workers (Redis lock keys) ──
	serpLocks, _ := b.redis.Keys(ctx, "serp:lock:*").Result()
	enrichLocks, _ := b.redis.Keys(ctx, "enrich:lock:*").Result()

	// ── Queries ──
	var qPending, qProcessing, qCompleted int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed')
		FROM queries
	`).Scan(&qPending, &qProcessing, &qCompleted)

	// ── SERP (current hour + prev hour for comparison) ──
	var serpCompleted, serpFailed, serpHour, serpPrevHour int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour')
		FROM serp_jobs
	`).Scan(&serpCompleted, &serpFailed, &serpHour, &serpPrevHour)

	// ── Enrich (current hour + prev hour) ──
	var enCompleted, enFailed, enDead, enHour, enPrevHour int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'dead'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour')
		FROM enrich_jobs
	`).Scan(&enCompleted, &enFailed, &enDead, &enHour, &enPrevHour)

	// ── Emails (current hour + prev hour + today + yesterday + total) ──
	var emailsHour, emailsPrevHour, emailsToday, emailsYesterday, emailsTotal int
	b.db.QueryRow(`
		SELECT
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed' AND completed_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour'),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed' AND completed_at > date_trunc('day', NOW())),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed' AND completed_at BETWEEN date_trunc('day', NOW()) - INTERVAL '1 day' AND date_trunc('day', NOW())),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed')
	`).Scan(&emailsHour, &emailsPrevHour, &emailsToday, &emailsYesterday, &emailsTotal)

	// ── Queues ──
	qSerp, _ := b.redis.ZCard(ctx, "serp:queue:serp").Result()
	qEnrich, _ := b.redis.ZCard(ctx, "serp:queue:enrich").Result()

	// ── Top email providers ──
	type provider struct {
		name  string
		count int
	}
	var providers []provider
	providerRows, _ := b.db.Query(`
		SELECT split_part(e, '@', 2), COUNT(*)
		FROM enrich_jobs, unnest(emails) AS e
		WHERE status = 'completed'
		GROUP BY 1 ORDER BY 2 DESC LIMIT 5
	`)
	if providerRows != nil {
		for providerRows.Next() {
			var p provider
			providerRows.Scan(&p.name, &p.count)
			providers = append(providers, p)
		}
		providerRows.Close()
	}

	// ── Email yield rate ──
	var enrichWithEmail int
	b.db.QueryRow(`SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND array_length(emails, 1) > 0`).Scan(&enrichWithEmail)
	yieldPct := 0.0
	if enCompleted > 0 {
		yieldPct = float64(enrichWithEmail) / float64(enCompleted) * 100
	}

	// ── ETA ──
	var etaStr string
	if emailsHour > 0 {
		remaining := 10_000_000 - emailsTotal
		if remaining > 0 {
			daysLeft := remaining / (emailsHour * 24)
			etaStr = fmt.Sprintf("~%d days", daysLeft)
		} else {
			etaStr = "REACHED!"
		}
	} else {
		etaStr = "--"
	}

	// ── Build message ──
	lines := []string{
		fmt.Sprintf("*Dashboard* (%s)", time.Now().Format("15:04 MST")),
		"",
		fmt.Sprintf("_Active:_ SERP: *%d* locks  Enrich: *%d* locks", len(serpLocks), len(enrichLocks)),
		"",
		"_Rates (this hr vs prev hr):_",
		fmt.Sprintf("  SERP: *%d*/hr %s", serpHour, delta(serpHour, serpPrevHour)),
		fmt.Sprintf("  Enrich: *%d*/hr %s", enHour, delta(enHour, enPrevHour)),
		fmt.Sprintf("  Emails: *%d*/hr %s", emailsHour, delta(emailsHour, emailsPrevHour)),
		fmt.Sprintf("  Yield: %.1f%% pages have email", yieldPct),
		fmt.Sprintf("  ETA 10M: *%s*", etaStr),
		"",
		"_Today vs Yesterday:_",
		fmt.Sprintf("  Emails today: *%d*  yesterday: %d %s", emailsToday, emailsYesterday, delta(emailsToday, emailsYesterday)),
		fmt.Sprintf("  Total: *%s*", fmtK(emailsTotal)),
		"",
		"_Totals:_",
		fmt.Sprintf("  SERP: %s done, %d failed", fmtK(serpCompleted), serpFailed),
		fmt.Sprintf("  Enrich: %s done, %d failed, %s dead", fmtK(enCompleted), enFailed, fmtK(enDead)),
	}
	if len(providers) > 0 {
		lines = append(lines, "", "_Top providers:_")
		for _, p := range providers {
			lines = append(lines, fmt.Sprintf("  %s: %s", p.name, fmtK(p.count)))
		}
	}
	lines = append(lines, "",
		"_Pipeline:_",
		fmt.Sprintf("  Queries: %s pending, %s processing, %d done", fmtK(qPending), fmtK(qProcessing), qCompleted),
		fmt.Sprintf("  Queues: serp=%s  enrich=%s", fmtK(int(qSerp)), fmtK(int(qEnrich))),
	)

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

// ────────────────────────────────────────────────────────────────────
// /analytics — Historical analytics: 1hr, 24hr, 7d with comparisons
// ────────────────────────────────────────────────────────────────────

func (b *Bot) handleAnalytics(ctx context.Context, msg *Message) {
	// ── Hourly breakdown (last 6 hours) ──
	type hourBucket struct {
		hour    string
		enrich  int
		emails  int
		serp    int
	}
	var hours []hourBucket
	hourRows, _ := b.db.Query(`
		SELECT
			to_char(h, 'HH24:MI') as hr,
			(SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed'
				AND completed_at >= h AND completed_at < h + INTERVAL '1 hour'),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e
				WHERE status = 'completed' AND completed_at >= h AND completed_at < h + INTERVAL '1 hour'),
			(SELECT COUNT(*) FROM serp_jobs WHERE status = 'completed'
				AND updated_at >= h AND updated_at < h + INTERVAL '1 hour')
		FROM generate_series(
			date_trunc('hour', NOW()) - INTERVAL '5 hours',
			date_trunc('hour', NOW()),
			INTERVAL '1 hour'
		) AS h
		ORDER BY h
	`)
	if hourRows != nil {
		for hourRows.Next() {
			var hb hourBucket
			hourRows.Scan(&hb.hour, &hb.enrich, &hb.emails, &hb.serp)
			hours = append(hours, hb)
		}
		hourRows.Close()
	}

	// ── Daily breakdown (last 7 days) ──
	type dayBucket struct {
		day    string
		enrich int
		emails int
		serp   int
	}
	var days []dayBucket
	dayRows, _ := b.db.Query(`
		SELECT
			to_char(d, 'Mon DD') as dy,
			(SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed'
				AND completed_at >= d AND completed_at < d + INTERVAL '1 day'),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e
				WHERE status = 'completed' AND completed_at >= d AND completed_at < d + INTERVAL '1 day'),
			(SELECT COUNT(*) FROM serp_jobs WHERE status = 'completed'
				AND updated_at >= d AND updated_at < d + INTERVAL '1 day')
		FROM generate_series(
			date_trunc('day', NOW()) - INTERVAL '6 days',
			date_trunc('day', NOW()),
			INTERVAL '1 day'
		) AS d
		ORDER BY d
	`)
	if dayRows != nil {
		for dayRows.Next() {
			var db dayBucket
			dayRows.Scan(&db.day, &db.enrich, &db.emails, &db.serp)
			days = append(days, db)
		}
		dayRows.Close()
	}

	// ── Summary comparisons ──
	var emailsToday, emailsYesterday, emails7d, emailsPrev7d int
	b.db.QueryRow(`
		SELECT
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'
				AND completed_at > date_trunc('day', NOW())),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'
				AND completed_at BETWEEN date_trunc('day', NOW()) - INTERVAL '1 day' AND date_trunc('day', NOW())),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'
				AND completed_at > NOW() - INTERVAL '7 days'),
			(SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'
				AND completed_at BETWEEN NOW() - INTERVAL '14 days' AND NOW() - INTERVAL '7 days')
	`).Scan(&emailsToday, &emailsYesterday, &emails7d, &emailsPrev7d)

	var enrichToday, enrichYesterday int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE completed_at > date_trunc('day', NOW())),
			COUNT(*) FILTER (WHERE completed_at BETWEEN date_trunc('day', NOW()) - INTERVAL '1 day' AND date_trunc('day', NOW()))
		FROM enrich_jobs WHERE status = 'completed'
	`).Scan(&enrichToday, &enrichYesterday)

	var emailsTotal int
	b.db.QueryRow(`SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'`).Scan(&emailsTotal)
	pctOf10M := float64(emailsTotal) / 10_000_000 * 100

	// ── Dead job breakdown ──
	var dead404, dead403, deadOther int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE error_msg LIKE 'HTTP 404%'),
			COUNT(*) FILTER (WHERE error_msg LIKE 'HTTP 403%'),
			COUNT(*) FILTER (WHERE error_msg NOT LIKE 'HTTP 404%' AND error_msg NOT LIKE 'HTTP 403%')
		FROM enrich_jobs WHERE status = 'dead'
	`).Scan(&dead404, &dead403, &deadOther)

	// ── Build message ──
	lines := []string{
		fmt.Sprintf("*Analytics* (%s)", time.Now().Format("15:04 MST")),
	}

	// Hourly chart
	lines = append(lines, "", "_Last 6 hours (emails/hr):_")
	hourMax := 1
	for _, h := range hours {
		if h.emails > hourMax {
			hourMax = h.emails
		}
	}
	for _, h := range hours {
		bar := chartBar(h.emails, hourMax)
		lines = append(lines, fmt.Sprintf("  `%s` %s *%d*", h.hour, bar, h.emails))
	}

	// Daily chart
	lines = append(lines, "", "_Last 7 days (emails/day):_")
	dayMax := 1
	for _, d := range days {
		if d.emails > dayMax {
			dayMax = d.emails
		}
	}
	for _, d := range days {
		bar := chartBar(d.emails, dayMax)
		lines = append(lines, fmt.Sprintf("  `%s` %s *%s*", d.day, bar, fmtK(d.emails)))
	}

	// Comparisons
	lines = append(lines, "",
		"_Comparisons:_",
		fmt.Sprintf("  Today vs yesterday:"),
		fmt.Sprintf("    Emails: *%s* vs %s %s", fmtK(emailsToday), fmtK(emailsYesterday), delta(emailsToday, emailsYesterday)),
		fmt.Sprintf("    Enrich: *%s* vs %s %s", fmtK(enrichToday), fmtK(enrichYesterday), delta(enrichToday, enrichYesterday)),
		fmt.Sprintf("  This week vs last week:"),
		fmt.Sprintf("    Emails: *%s* vs %s %s", fmtK(emails7d), fmtK(emailsPrev7d), delta(emails7d, emailsPrev7d)),
	)

	// Progress
	lines = append(lines, "",
		"_Progress to 10M:_",
		fmt.Sprintf("  Total: *%s* (%.2f%%)", fmtK(emailsTotal), pctOf10M),
		progressBar(emailsTotal, 10_000_000),
	)

	// Dead breakdown
	lines = append(lines, "",
		"_Dead jobs breakdown:_",
		fmt.Sprintf("  404 Not Found: %s (contact pages don't exist)", fmtK(dead404)),
		fmt.Sprintf("  403 Forbidden: %s (site blocks scraper)", fmtK(dead403)),
		fmt.Sprintf("  Other: %s", fmtK(deadOther)),
	)

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

// ────────────────────────────────────────────────────────────────────
// Existing handlers (unchanged)
// ────────────────────────────────────────────────────────────────────

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
		SELECT
			emails, phones, domain, url,
			COALESCE(contact_name, ''),
			COALESCE(business_name, ''),
			COALESCE(business_category, ''),
			COALESCE(page_title, ''),
			COALESCE(website, ''),
			COALESCE(address, ''),
			COALESCE(location, ''),
			COALESCE(opening_hours, ''),
			COALESCE(rating, ''),
			social_links
		FROM enrich_jobs
		WHERE status = 'completed' AND array_length(emails, 1) > 0
		ORDER BY completed_at DESC LIMIT 50000
	`)
	if err != nil {
		b.sendMessage(msg.Chat.ID, "Error: "+err.Error())
		return
	}
	defer rows.Close()

	var buf strings.Builder
	buf.WriteString("emails,phones,domain,url,contact_name,business_name,category,page_title,website,address,location,opening_hours,rating,social_links\n")

	count := 0
	for rows.Next() {
		var emails, phones []string
		var domain, url, contactName, bizName, category, pageTitle, website, address, location, hours, rating string
		var socialLinksJSON []byte
		rows.Scan(
			pq.Array(&emails), pq.Array(&phones), &domain, &url,
			&contactName, &bizName, &category, &pageTitle, &website,
			&address, &location, &hours, &rating, &socialLinksJSON,
		)
		fmt.Fprintf(&buf, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			csvEscape(strings.Join(emails, ";")),
			csvEscape(strings.Join(phones, ";")),
			csvEscape(domain), csvEscape(url),
			csvEscape(contactName), csvEscape(bizName),
			csvEscape(category), csvEscape(pageTitle),
			csvEscape(website), csvEscape(address),
			csvEscape(location), csvEscape(hours),
			csvEscape(rating), csvEscape(string(socialLinksJSON)),
		)
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

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

func csvEscape(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
	}
	return s
}

// delta returns a formatted change indicator like "+15%" or "-8%".
func delta(current, previous int) string {
	if previous == 0 {
		if current > 0 {
			return "(new)"
		}
		return ""
	}
	pct := float64(current-previous) / float64(previous) * 100
	if pct > 0 {
		return fmt.Sprintf("(+%.0f%%)", pct)
	} else if pct < 0 {
		return fmt.Sprintf("(%.0f%%)", pct)
	}
	return "(=)"
}

// fmtK formats large numbers as "123K" or "1.2M" for readability.
func fmtK(n int) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 10_000 {
		return fmt.Sprintf("%dK", n/1000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}

// chartBar returns a simple text bar for Telegram monospace display.
func chartBar(value, maxValue int) string {
	if maxValue == 0 {
		return ""
	}
	barLen := value * 8 / maxValue
	if barLen < 0 {
		barLen = 0
	}
	if barLen > 8 {
		barLen = 8
	}
	return strings.Repeat("█", barLen) + strings.Repeat("░", 8-barLen)
}

// progressBar returns a 10M progress bar.
func progressBar(current, target int) string {
	pct := current * 20 / target
	if pct > 20 {
		pct = 20
	}
	return "  `[" + strings.Repeat("█", pct) + strings.Repeat("░", 20-pct) + "]`"
}

