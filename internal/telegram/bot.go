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
		queryRepo:      query.NewRepository(db),
		client:         &http.Client{Timeout: 60 * time.Second},
		allowedChatIDs: allowed,
	}
}

func (b *Bot) Run(ctx context.Context) {
	slog.Info("telegram: bot starting")
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
