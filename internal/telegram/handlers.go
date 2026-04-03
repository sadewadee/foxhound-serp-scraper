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
	if len(b.allowedChatIDs) > 0 && !b.allowedChatIDs[msg.Chat.ID] {
		b.sendMessage(msg.Chat.ID, "Unauthorized. Your chat ID: "+fmt.Sprintf("%d", msg.Chat.ID))
		return
	}

	text := strings.TrimSpace(msg.Text)

	cmd := text
	args := ""
	if i := strings.Index(text, " "); i > 0 {
		cmd = text[:i]
		args = strings.TrimSpace(text[i+1:])
	}
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
		b.handleExport(msg, args)
	case "/categories":
		b.handleCategories(msg)
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
/export — Export contacts as CSV (filter: gmail, bali, yoga…)
/categories — Top 20 business categories
/retry — Retry failed queries
/reset — Reset buffers`

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

// /status
func (b *Bot) handleStatus(ctx context.Context, msg *Message) {
	serpLocks, _ := b.redis.Keys(ctx, "serp:lock:*").Result()
	enrichLocks, _ := b.redis.Keys(ctx, "enrich:lock:*").Result()

	var qPending, qProcessing, qCompleted int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed')
		FROM queries
	`).Scan(&qPending, &qProcessing, &qCompleted)

	var serpCompleted, serpFailed, serpHour, serpPrevHour int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour')
		FROM serp_jobs
	`).Scan(&serpCompleted, &serpFailed, &serpHour, &serpPrevHour)

	var enCompleted, enFailed, enDead, enHour, enPrevHour int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'dead'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour')
		FROM enrichment_jobs
	`).Scan(&enCompleted, &enFailed, &enDead, &enHour, &enPrevHour)

	// Emails from normalized table.
	var emailsHour, emailsPrevHour, emailsToday, emailsYesterday, emailsTotal int
	b.db.QueryRow(`
		SELECT
			(SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '1 hour'),
			(SELECT COUNT(*) FROM emails WHERE created_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour'),
			(SELECT COUNT(*) FROM emails WHERE created_at > date_trunc('day', NOW())),
			(SELECT COUNT(*) FROM emails WHERE created_at BETWEEN date_trunc('day', NOW()) - INTERVAL '1 day' AND date_trunc('day', NOW())),
			(SELECT COUNT(*) FROM emails)
	`).Scan(&emailsHour, &emailsPrevHour, &emailsToday, &emailsYesterday, &emailsTotal)

	// Worker count from DB.
	var serpWorkers, enrichWorkers int
	b.db.QueryRow(`SELECT
		COUNT(*) FILTER (WHERE worker_type='serp' AND status='working'),
		COUNT(*) FILTER (WHERE worker_type='enrich' AND status='working')
	FROM workers`).Scan(&serpWorkers, &enrichWorkers)

	// Current rate from DB timestamps (5-min window * 12 = hourly projection).
	var emailRate5m, serpRate5m, urlRate5m, enrichRate5m int
	b.db.QueryRow(`SELECT
		(SELECT COUNT(*) * 12 FROM emails WHERE created_at > NOW() - INTERVAL '5 minutes'),
		(SELECT COUNT(*) * 12 FROM serp_jobs WHERE status='completed' AND updated_at > NOW() - INTERVAL '5 minutes'),
		(SELECT COUNT(*) * 12 FROM serp_results WHERE created_at > NOW() - INTERVAL '5 minutes'),
		(SELECT COUNT(*) * 12 FROM enrichment_jobs WHERE status='completed' AND completed_at > NOW() - INTERVAL '5 minutes')
	`).Scan(&emailRate5m, &serpRate5m, &urlRate5m, &enrichRate5m)

	// Queues.
	serpBuf, _ := b.redis.LLen(ctx, "serp:buffer").Result()
	enrichBuf, _ := b.redis.LLen(ctx, "enrich:buffer").Result()

	// Top email providers.
	type provider struct {
		name  string
		count int
	}
	var providers []provider
	providerRows, _ := b.db.Query(`
		SELECT domain, COUNT(*)
		FROM emails
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

	// Email yield rate + validation stats.
	var enrichWithEmail, validatedCount int
	b.db.QueryRow(`SELECT COUNT(DISTINCT be.business_id) FROM business_emails be`).Scan(&enrichWithEmail)
	b.db.QueryRow(`SELECT COUNT(*) FROM emails WHERE validation_status = 'valid'`).Scan(&validatedCount)
	yieldPct := 0.0
	if enCompleted > 0 {
		yieldPct = float64(enrichWithEmail) / float64(enCompleted) * 100
	}
	validPct := 0.0
	if emailsTotal > 0 {
		validPct = float64(validatedCount) / float64(emailsTotal) * 100
	}

	// ETA.
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

	// Build message.
	lines := []string{
		fmt.Sprintf("*Dashboard* (%s)", time.Now().Format("15:04 MST")),
		"",
		fmt.Sprintf("_Active:_ SERP: *%d* locks  Enrich: *%d* locks", len(serpLocks), len(enrichLocks)),
		fmt.Sprintf("_Workers:_ SERP *%d* online  Enrich *%d* online", serpWorkers, enrichWorkers),
		"",
		"_Current Rate (5min window):_",
		fmt.Sprintf("  SERP: *%s*/h  URLs: *%s*/h", fmtK(serpRate5m), fmtK(urlRate5m)),
		fmt.Sprintf("  Enrich: *%s*/h  Emails: *%s*/h", fmtK(enrichRate5m), fmtK(emailRate5m)),
		"",
		"_Rates (this hr vs prev hr):_",
		fmt.Sprintf("  SERP: *%d*/hr %s", serpHour, delta(serpHour, serpPrevHour)),
		fmt.Sprintf("  Enrich: *%d*/hr %s", enHour, delta(enHour, enPrevHour)),
		fmt.Sprintf("  Emails: *%d*/hr %s", emailsHour, delta(emailsHour, emailsPrevHour)),
		fmt.Sprintf("  Yield: %.1f%% pages have email", yieldPct),
		fmt.Sprintf("  Validated: *%s* (%.1f%% of emails)", fmtK(validatedCount), validPct),
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

	// Target providers.
	type targetProv struct {
		domain string
		count  int
	}
	var targets []targetProv
	targetRows, tErr := b.db.Query(`
		SELECT domain, COUNT(*)
		FROM emails
		WHERE domain IN ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com')
		GROUP BY 1
	`)
	if tErr == nil && targetRows != nil {
		for targetRows.Next() {
			var tp targetProv
			targetRows.Scan(&tp.domain, &tp.count)
			targets = append(targets, tp)
		}
		targetRows.Close()
	}
	if len(targets) > 0 {
		parts := make([]string, 0, len(targets))
		for _, tp := range targets {
			short := strings.TrimSuffix(tp.domain, ".com")
			short = strings.ToUpper(short[:1]) + short[1:]
			parts = append(parts, fmt.Sprintf("%s: %s", short, fmtK(tp.count)))
		}
		lines = append(lines, "", "_Target providers:_")
		lines = append(lines, "  "+strings.Join(parts, "  "))
	}

	// Top 3 categories.
	type catCount struct {
		name  string
		count int
	}
	var topCats []catCount
	catRows, cErr := b.db.Query(`
		SELECT category, COUNT(*)
		FROM business_listings
		WHERE category IS NOT NULL AND category != ''
		GROUP BY 1 ORDER BY 2 DESC LIMIT 3
	`)
	if cErr == nil && catRows != nil {
		for catRows.Next() {
			var cc catCount
			catRows.Scan(&cc.name, &cc.count)
			topCats = append(topCats, cc)
		}
		catRows.Close()
	}
	if len(topCats) > 0 {
		lines = append(lines, "", "_Top categories:_")
		for _, cc := range topCats {
			lines = append(lines, fmt.Sprintf("  %s: %s", cc.name, fmtK(cc.count)))
		}
	}

	// Success rate.
	successRate := 0.0
	if enCompleted+enDead > 0 {
		successRate = float64(enCompleted) / float64(enCompleted+enDead) * 100
	}
	lines = append(lines, "", fmt.Sprintf("_Success rate:_ %.1f%% (completed / completed+dead)", successRate))

	// Per-engine SERP rates.
	type engineRate struct {
		engine string
		count  int
	}
	var engines []engineRate
	engineRows, eErr := b.db.Query(`
		SELECT engine, COUNT(*)
		FROM serp_jobs
		WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '1 hour'
		GROUP BY engine
	`)
	if eErr == nil && engineRows != nil {
		for engineRows.Next() {
			var er engineRate
			engineRows.Scan(&er.engine, &er.count)
			engines = append(engines, er)
		}
		engineRows.Close()
	}
	if len(engines) > 0 {
		lines = append(lines, "", "_SERP engines (last hr):_")
		for _, er := range engines {
			lines = append(lines, fmt.Sprintf("  %s: %d/hr", er.engine, er.count))
		}
	}

	lines = append(lines, "",
		"_Pipeline:_",
		fmt.Sprintf("  Queries: %s pending, %s processing, %d done", fmtK(qPending), fmtK(qProcessing), qCompleted),
		fmt.Sprintf("  Buffers: serp=%s  enrich=%s", fmtK(int(serpBuf)), fmtK(int(enrichBuf))),
	)

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

// /analytics
func (b *Bot) handleAnalytics(ctx context.Context, msg *Message) {
	type hourBucket struct {
		hour   string
		enrich int
		emails int
		serp   int
	}
	var hours []hourBucket
	hourRows, _ := b.db.Query(`
		SELECT
			to_char(h, 'HH24:MI') as hr,
			(SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'completed'
				AND completed_at >= h AND completed_at < h + INTERVAL '1 hour'),
			(SELECT COUNT(*) FROM emails
				WHERE created_at >= h AND created_at < h + INTERVAL '1 hour'),
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
			(SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'completed'
				AND completed_at >= d AND completed_at < d + INTERVAL '1 day'),
			(SELECT COUNT(*) FROM emails
				WHERE created_at >= d AND created_at < d + INTERVAL '1 day'),
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

	var emailsToday, emailsYesterday, emails7d, emailsPrev7d int
	b.db.QueryRow(`
		SELECT
			(SELECT COUNT(*) FROM emails WHERE created_at > date_trunc('day', NOW())),
			(SELECT COUNT(*) FROM emails WHERE created_at BETWEEN date_trunc('day', NOW()) - INTERVAL '1 day' AND date_trunc('day', NOW())),
			(SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '7 days'),
			(SELECT COUNT(*) FROM emails WHERE created_at BETWEEN NOW() - INTERVAL '14 days' AND NOW() - INTERVAL '7 days')
	`).Scan(&emailsToday, &emailsYesterday, &emails7d, &emailsPrev7d)

	var enrichToday, enrichYesterday int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE completed_at > date_trunc('day', NOW())),
			COUNT(*) FILTER (WHERE completed_at BETWEEN date_trunc('day', NOW()) - INTERVAL '1 day' AND date_trunc('day', NOW()))
		FROM enrichment_jobs WHERE status = 'completed'
	`).Scan(&enrichToday, &enrichYesterday)

	var emailsTotal, validatedTotal int
	b.db.QueryRow(`SELECT COUNT(*) FROM emails`).Scan(&emailsTotal)
	b.db.QueryRow(`SELECT COUNT(*) FROM emails WHERE validation_status = 'valid'`).Scan(&validatedTotal)
	pctOf10M := float64(emailsTotal) / 10_000_000 * 100

	// Dead job breakdown.
	var dead404, dead403, deadOther int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE error_msg LIKE 'HTTP 404%'),
			COUNT(*) FILTER (WHERE error_msg LIKE 'HTTP 403%'),
			COUNT(*) FILTER (WHERE error_msg NOT LIKE 'HTTP 404%' AND error_msg NOT LIKE 'HTTP 403%')
		FROM enrichment_jobs WHERE status = 'dead'
	`).Scan(&dead404, &dead403, &deadOther)

	// Build message.
	lines := []string{
		fmt.Sprintf("*Analytics* (%s)", time.Now().Format("15:04 MST")),
	}

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

	lines = append(lines, "",
		"_Comparisons:_",
		"  Today vs yesterday:",
		fmt.Sprintf("    Emails: *%s* vs %s %s", fmtK(emailsToday), fmtK(emailsYesterday), delta(emailsToday, emailsYesterday)),
		fmt.Sprintf("    Enrich: *%s* vs %s %s", fmtK(enrichToday), fmtK(enrichYesterday), delta(enrichToday, enrichYesterday)),
		"  This week vs last week:",
		fmt.Sprintf("    Emails: *%s* vs %s %s", fmtK(emails7d), fmtK(emailsPrev7d), delta(emails7d, emailsPrev7d)),
	)

	validPctA := 0.0
	if emailsTotal > 0 {
		validPctA = float64(validatedTotal) / float64(emailsTotal) * 100
	}
	lines = append(lines, "",
		"_Progress to 10M:_",
		fmt.Sprintf("  Total: *%s* (%.2f%%)", fmtK(emailsTotal), pctOf10M),
		fmt.Sprintf("  Validated: *%s* (%.1f%%)", fmtK(validatedTotal), validPctA),
		progressBar(emailsTotal, 10_000_000),
	)

	lines = append(lines, "",
		"_Dead jobs breakdown:_",
		fmt.Sprintf("  404 Not Found: %s", fmtK(dead404)),
		fmt.Sprintf("  403 Forbidden: %s", fmtK(dead403)),
		fmt.Sprintf("  Other: %s", fmtK(deadOther)),
	)

	// Gmail trend.
	type gmailDay struct {
		day   string
		count int
	}
	var gmailDays []gmailDay
	gmailRows, gmErr := b.db.Query(`
		SELECT
			to_char(d, 'Mon DD') AS dy,
			(SELECT COUNT(*) FROM emails
				WHERE domain = 'gmail.com'
				AND created_at >= d AND created_at < d + INTERVAL '1 day')
		FROM generate_series(
			date_trunc('day', NOW()) - INTERVAL '6 days',
			date_trunc('day', NOW()),
			INTERVAL '1 day'
		) AS d
		ORDER BY d
	`)
	if gmErr == nil && gmailRows != nil {
		for gmailRows.Next() {
			var gd gmailDay
			gmailRows.Scan(&gd.day, &gd.count)
			gmailDays = append(gmailDays, gd)
		}
		gmailRows.Close()
	}
	if len(gmailDays) > 0 {
		lines = append(lines, "", "_Gmail trend (7 days):_")
		gmailMax := 1
		for _, gd := range gmailDays {
			if gd.count > gmailMax {
				gmailMax = gd.count
			}
		}
		for _, gd := range gmailDays {
			bar := chartBar(gd.count, gmailMax)
			lines = append(lines, fmt.Sprintf("  `%s` %s *%s*", gd.day, bar, fmtK(gd.count)))
		}
	}

	// Category top 5.
	type catStat struct {
		name  string
		count int
	}
	var catStats []catStat
	catStatRows, csErr := b.db.Query(`
		SELECT category, COUNT(*)
		FROM business_listings
		WHERE category IS NOT NULL AND category != ''
		GROUP BY 1 ORDER BY 2 DESC LIMIT 5
	`)
	if csErr == nil && catStatRows != nil {
		for catStatRows.Next() {
			var cs catStat
			catStatRows.Scan(&cs.name, &cs.count)
			catStats = append(catStats, cs)
		}
		catStatRows.Close()
	}
	if len(catStats) > 0 {
		lines = append(lines, "", "_Top 5 categories:_")
		catMax := 1
		if catStats[0].count > catMax {
			catMax = catStats[0].count
		}
		for _, cs := range catStats {
			bar := chartBar(cs.count, catMax)
			lines = append(lines, fmt.Sprintf("  %s *%s* %s", bar, fmtK(cs.count), cs.name))
		}
	}

	// Per-worker throughput from delta.
	type workerRate struct {
		id      string
		wtype   string
		pagesH  int
		emailsH int
	}
	var wRates []workerRate
	wRows, wErr := b.db.Query(`
		SELECT worker_id, worker_type,
			pages_delta * 120, emails_delta * 120
		FROM workers WHERE status='working'
		ORDER BY worker_type, emails_delta DESC
	`)
	if wErr == nil && wRows != nil {
		for wRows.Next() {
			var wr workerRate
			wRows.Scan(&wr.id, &wr.wtype, &wr.pagesH, &wr.emailsH)
			wRates = append(wRates, wr)
		}
		wRows.Close()
	}
	if len(wRates) > 0 {
		lines = append(lines, "", "_Worker Throughput (30s delta):_")
		for _, wr := range wRates {
			lines = append(lines, fmt.Sprintf("  %s  %d pg/h  %d em/h", wr.id, wr.pagesH, wr.emailsH))
		}
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
		total, counts["pending"], counts["processing"], counts["completed"], counts["error"])
	b.sendMessage(msg.Chat.ID, text)
}

func (b *Bot) handleContacts(msg *Message) {
	var totalBiz, withEmail, uniqueDomains, lastHour, last24h int

	b.db.QueryRow("SELECT COUNT(*) FROM business_listings").Scan(&totalBiz)
	b.db.QueryRow("SELECT COUNT(DISTINCT be.business_id) FROM business_emails be").Scan(&withEmail)
	b.db.QueryRow("SELECT COUNT(DISTINCT domain) FROM business_listings").Scan(&uniqueDomains)
	b.db.QueryRow("SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '1 hour'").Scan(&lastHour)
	b.db.QueryRow("SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '24 hours'").Scan(&last24h)

	lines := []string{
		"*Contact Stats*",
		"",
		fmt.Sprintf("Total businesses: *%d*", totalBiz),
		fmt.Sprintf("With email: %d", withEmail),
		fmt.Sprintf("Domains: %d", uniqueDomains),
		fmt.Sprintf("Emails last hour: %d", lastHour),
		fmt.Sprintf("Emails last 24h: %d", last24h),
	}

	providerRows, _ := b.db.Query(`
		SELECT domain, COUNT(*) AS cnt
		FROM emails
		GROUP BY domain ORDER BY cnt DESC LIMIT 5
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

func (b *Bot) handleExport(msg *Message, args string) {
	filter := strings.TrimSpace(strings.ToLower(args))

	whereClause := "EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id)"
	filterLabel := "all"

	switch {
	case filter == "gmail":
		whereClause += " AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.domain = 'gmail.com')"
		filterLabel = "gmail"
	case filter == "yahoo":
		whereClause += " AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.domain = 'yahoo.com')"
		filterLabel = "yahoo"
	case filter == "hotmail":
		whereClause += " AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.domain = 'hotmail.com')"
		filterLabel = "hotmail"
	case filter == "outlook":
		whereClause += " AND EXISTS (SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id AND e.domain = 'outlook.com')"
		filterLabel = "outlook"
	case filter != "":
		safe := strings.ReplaceAll(filter, "'", "''")
		whereClause += fmt.Sprintf(
			" AND (bl.location ILIKE '%%%s%%' OR bl.address ILIKE '%%%s%%' OR bl.category ILIKE '%%%s%%')",
			safe, safe, safe,
		)
		filterLabel = filter
	}

	b.sendMessage(msg.Chat.ID, fmt.Sprintf("Generating CSV export (filter: %s)...", filterLabel))

	rows, err := b.db.Query(fmt.Sprintf(`
		SELECT
			COALESCE(
				(SELECT array_agg(e.email) FROM business_emails be JOIN emails e ON e.id = be.email_id WHERE be.business_id = bl.id),
				'{}'
			) AS emails,
			COALESCE(bl.phone, ''), bl.domain, bl.url,
			COALESCE(bl.business_name, ''),
			COALESCE(bl.category, ''),
			COALESCE(bl.page_title, ''),
			COALESCE(bl.website, ''),
			COALESCE(bl.address, ''),
			COALESCE(bl.location, ''),
			COALESCE(bl.opening_hours, ''),
			COALESCE(bl.rating, ''),
			bl.social_links
		FROM business_listings bl
		WHERE %s
		ORDER BY bl.created_at DESC LIMIT 50000
	`, whereClause))
	if err != nil {
		b.sendMessage(msg.Chat.ID, "Error: "+err.Error())
		return
	}
	defer rows.Close()

	var buf strings.Builder
	buf.WriteString("emails,phone,domain,url,business_name,category,page_title,website,address,location,opening_hours,rating,social_links\n")

	count := 0
	for rows.Next() {
		var emails []string
		var phone, domain, url, bizName, category, pageTitle, website, address, location, hours, rating string
		var socialLinksJSON []byte
		rows.Scan(
			pq.Array(&emails), &phone, &domain, &url,
			&bizName, &category, &pageTitle, &website,
			&address, &location, &hours, &rating, &socialLinksJSON,
		)
		fmt.Fprintf(&buf, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			csvEscape(strings.Join(emails, ";")),
			csvEscape(phone),
			csvEscape(domain), csvEscape(url),
			csvEscape(bizName), csvEscape(category),
			csvEscape(pageTitle), csvEscape(website),
			csvEscape(address), csvEscape(location),
			csvEscape(hours), csvEscape(rating),
			csvEscape(string(socialLinksJSON)),
		)
		count++
	}

	if count == 0 {
		b.sendMessage(msg.Chat.ID, "No contacts with email found.")
		return
	}

	fileSuffix := ""
	if filterLabel != "all" {
		fileSuffix = "_" + filterLabel
	}
	filename := fmt.Sprintf("contacts%s_%s.csv", fileSuffix, time.Now().Format("2006-01-02"))
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
	lines := []string{"*Buffer Reset*", ""}

	for _, key := range []string{"serp:buffer", "enrich:buffer"} {
		n, _ := b.redis.LLen(ctx, key).Result()
		b.redis.Del(ctx, key)
		lines = append(lines, fmt.Sprintf("%s: %d cleared", key, n))
	}

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

// /categories
func (b *Bot) handleCategories(msg *Message) {
	type catRow struct {
		name  string
		count int
	}
	var cats []catRow
	rows, err := b.db.Query(`
		SELECT category, COUNT(*)
		FROM business_listings
		WHERE category IS NOT NULL AND category != ''
		GROUP BY 1 ORDER BY 2 DESC LIMIT 20
	`)
	if err != nil {
		b.sendMessage(msg.Chat.ID, "Error fetching categories: "+err.Error())
		return
	}
	defer rows.Close()

	for rows.Next() {
		var c catRow
		rows.Scan(&c.name, &c.count)
		cats = append(cats, c)
	}

	if len(cats) == 0 {
		b.sendMessage(msg.Chat.ID, "No categories found yet.")
		return
	}

	lines := []string{"*Top 20 Categories*", ""}
	for i, c := range cats {
		lines = append(lines, fmt.Sprintf("%2d. %s — %s", i+1, c.name, fmtK(c.count)))
	}

	b.sendMessage(msg.Chat.ID, strings.Join(lines, "\n"))
}

// Helpers

func csvEscape(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
	}
	return s
}

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
	return strings.Repeat("\xe2\x96\x88", barLen) + strings.Repeat("\xe2\x96\x91", 8-barLen)
}

func progressBar(current, target int) string {
	pct := current * 20 / target
	if pct > 20 {
		pct = 20
	}
	return "  `[" + strings.Repeat("\xe2\x96\x88", pct) + strings.Repeat("\xe2\x96\x91", 20-pct) + "]`"
}
