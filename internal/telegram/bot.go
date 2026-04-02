//go:build playwright

package telegram

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sadewadee/serp-scraper/internal/query"
)

type Bot struct {
	token          string
	apiURL         string
	db             *sql.DB
	redis          *redis.Client
	queryRepo      *query.Repository
	client         *http.Client
	offset         int64
	allowedChatIDs map[int64]bool
}

type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

type Message struct {
	MessageID int64  `json:"message_id"`
	Chat      Chat   `json:"chat"`
	Text      string `json:"text"`
}

type Chat struct {
	ID int64 `json:"id"`
}

func New(token string, db *sql.DB, redisClient *redis.Client, allowedChatIDs []int64) *Bot {
	allowed := make(map[int64]bool)
	for _, id := range allowedChatIDs {
		allowed[id] = true
	}
	return &Bot{
		token:          token,
		apiURL:         "https://api.telegram.org/bot" + token,
		db:             db,
		redis:          redisClient,
		queryRepo:      query.NewRepositoryWithRedis(db, redisClient),
		client:         &http.Client{Timeout: 60 * time.Second},
		allowedChatIDs: allowed,
	}
}

func (b *Bot) Run(ctx context.Context) {
	slog.Info("telegram: bot starting")

	// Start hourly auto-report in background.
	go b.hourlyReport(ctx)

	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			slog.Info("telegram: bot stopped")
			return
		default:
		}

		updates, err := b.getUpdates(ctx)
		if err != nil {
			slog.Warn("telegram: getUpdates failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		for _, u := range updates {
			if u.Message != nil && u.Message.Text != "" {
				b.handleMessage(ctx, u.Message)
			}
			b.offset = u.UpdateID + 1
		}
	}
}

// hourlyReport sends a pipeline rate summary to all allowed chat IDs every hour.
func (b *Bot) hourlyReport(ctx context.Context) {
	// Wait until the next full hour to align reports.
	now := time.Now()
	nextHour := now.Truncate(time.Hour).Add(time.Hour)
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Until(nextHour)):
	}

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		b.broadcastReport(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// broadcastReport builds the rate report and sends to all allowed chats.
func (b *Bot) broadcastReport(ctx context.Context) {
	report := b.buildRateReport(ctx)
	if report == "" {
		return
	}
	for chatID := range b.allowedChatIDs {
		b.sendMessage(chatID, report)
	}
}

// buildRateReport generates the auto hourly report (broadcast to all chats).
func (b *Bot) buildRateReport(ctx context.Context) string {
	serpLocks, _ := b.redis.Keys(ctx, "serp:lock:*").Result()
	enrichLocks, _ := b.redis.Keys(ctx, "enrich:lock:*").Result()

	var serpHour, serpPrevHour int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour')
		FROM serp_jobs
	`).Scan(&serpHour, &serpPrevHour)

	var enrichHour, enrichPrevHour int
	b.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour')
		FROM enrichment_jobs
	`).Scan(&enrichHour, &enrichPrevHour)

	var emailsHour, emailsPrevHour, emailsToday, emailsTotal int
	b.db.QueryRow(`
		SELECT
			(SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '1 hour'),
			(SELECT COUNT(*) FROM emails WHERE created_at BETWEEN NOW() - INTERVAL '2 hours' AND NOW() - INTERVAL '1 hour'),
			(SELECT COUNT(*) FROM emails WHERE created_at > date_trunc('day', NOW())),
			(SELECT COUNT(*) FROM emails)
	`).Scan(&emailsHour, &emailsPrevHour, &emailsToday, &emailsTotal)

	var etaStr string
	if emailsHour > 0 {
		remaining := 10_000_000 - emailsTotal
		if remaining > 0 {
			etaStr = fmt.Sprintf("~%d days", remaining/(emailsHour*24))
		} else {
			etaStr = "REACHED!"
		}
	} else {
		etaStr = "--"
	}

	pctOf10M := float64(emailsTotal) / 10_000_000 * 100

	lines := []string{
		fmt.Sprintf("*Hourly Report* (%s)", time.Now().Format("15:04 MST")),
		"",
		fmt.Sprintf("_Active:_ SERP: *%d*  Enrich: *%d*", len(serpLocks), len(enrichLocks)),
		"",
		fmt.Sprintf("  SERP: *%d*/hr %s", serpHour, delta(serpHour, serpPrevHour)),
		fmt.Sprintf("  Enrich: *%d*/hr %s", enrichHour, delta(enrichHour, enrichPrevHour)),
		fmt.Sprintf("  Emails: *%d*/hr %s", emailsHour, delta(emailsHour, emailsPrevHour)),
		"",
		fmt.Sprintf("  Today: *%s*  Total: *%s* (%.1f%%)", fmtK(emailsToday), fmtK(emailsTotal), pctOf10M),
		fmt.Sprintf("  ETA 10M: *%s*", etaStr),
	}

	return strings.Join(lines, "\n")
}

func (b *Bot) getUpdates(ctx context.Context) ([]Update, error) {
	url := fmt.Sprintf("%s/getUpdates?offset=%d&timeout=30", b.apiURL, b.offset)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		OK     bool     `json:"ok"`
		Result []Update `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if !result.OK {
		return nil, fmt.Errorf("telegram API returned ok=false")
	}
	return result.Result, nil
}

func (b *Bot) sendMessage(chatID int64, text string) {
	payload := map[string]any{
		"chat_id":    chatID,
		"text":       text,
		"parse_mode": "Markdown",
	}
	data, _ := json.Marshal(payload)

	resp, err := b.client.Post(b.apiURL+"/sendMessage", "application/json", bytes.NewReader(data))
	if err != nil {
		slog.Warn("telegram: sendMessage failed", "error", err)
		return
	}
	resp.Body.Close()
}

func (b *Bot) sendDocument(chatID int64, filename string, content []byte) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	_ = w.WriteField("chat_id", fmt.Sprintf("%d", chatID))

	part, err := w.CreateFormFile("document", filename)
	if err != nil {
		slog.Warn("telegram: create form file failed", "error", err)
		return
	}
	part.Write(content)
	w.Close()

	resp, err := b.client.Post(b.apiURL+"/sendDocument", w.FormDataContentType(), &buf)
	if err != nil {
		slog.Warn("telegram: sendDocument failed", "error", err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
