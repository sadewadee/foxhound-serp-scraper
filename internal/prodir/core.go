// Package prodir crawls two curated professional directories (ACE "Find an ACE
// Pro" and the ICF coach finder) into append-only JSONL state files, exports
// them to CSV and loads them into the shared business_listings tables. It is a
// port of two reviewed Python scrapers; the state-file names and JSON shapes
// are identical, so a state dir written by either implementation is readable
// by the other.
//
// This file holds the source-independent core: HTTP with the shared retry
// policy, the worker pool with its item-level circuit breaker, JSONL state
// helpers and the CSV writer.
//
// Proxy: there is deliberately no proxy flag. The HTTP transport keeps
// http.ProxyFromEnvironment, so HTTPS_PROXY / NO_PROXY work as usual.
package prodir

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Process exit codes shared by every subcommand.
const (
	ExitOK          = 0
	ExitErrors      = 1   // some items failed
	ExitBreaker     = 2   // circuit breaker tripped
	ExitInterrupted = 130 // SIGINT / SIGTERM
)

const (
	userAgent    = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
	attempts     = 5  // per request
	breakerLimit = 20 // consecutive failed items before the run stops
)

// Endpoint bases and retry timing. Package-level so tests can point them at an
// httptest.Server and shrink the backoff.
var (
	ACEBase      = "https://www.acefitness.org"
	ICFBase      = "https://apps.coachingfederation.org/eweb/"
	ICFSearchURL = "https://icf-ccf.azurewebsites.net/api/search"

	// RetryBase * 2^n (+ up to RetryJitter) is the wait before retry n (n=1..4):
	// 2s, 4s, 8s, 16s with the defaults.
	RetryBase   = time.Second
	RetryJitter = 1500 * time.Millisecond
)

// errStopped marks an item abandoned because the run is stopping; it is not a
// failure and is never recorded.
var errStopped = errors.New("stopped")

// response is a fully-read HTTP response. url is the final URL after redirects.
type response struct {
	status int
	url    string
	body   []byte
}

// client is the single HTTP client of a run. One cookie jar is shared by all
// workers (the Python scripts used one jar per thread; neither site needs
// per-worker session state).
type client struct{ hc *http.Client }

func newClient() *client {
	// Clone keeps Proxy: http.ProxyFromEnvironment.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	tr.ResponseHeaderTimeout = 45 * time.Second
	tr.MaxIdleConnsPerHost = 16
	jar, _ := cookiejar.New(nil)
	return &client{hc: &http.Client{Transport: tr, Jar: jar, Timeout: 2 * time.Minute}}
}

// do sends the request with the retry policy: up to `attempts` tries with
// exponential backoff and jitter on 403 / 429 / 5xx and connection errors;
// any other status is returned to the caller as-is (so other 4xx fail fast
// without retrying). ctx only gates starting attempts and the backoff waits —
// an in-flight request is allowed to finish, like the Python scripts.
func (c *client) do(ctx context.Context, method, url string, hdr map[string]string, body []byte) (*response, error) {
	last := "unknown error"
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := RetryBase<<attempt + time.Duration(rand.Float64()*float64(RetryJitter))
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, errStopped
			case <-t.C:
			}
		}
		if ctx.Err() != nil {
			return nil, errStopped
		}
		resp, err := c.once(method, url, hdr, body)
		if err != nil {
			last = err.Error()
			continue
		}
		if resp.status == 403 || resp.status == 429 || resp.status >= 500 {
			last = fmt.Sprintf("HTTP %d", resp.status)
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("%s for %s (gave up after %d attempts)", last, url, attempts)
}

func (c *client) once(method, url string, hdr map[string]string, body []byte) (*response, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	return &response{status: resp.StatusCode, url: resp.Request.URL.String(), body: b}, nil
}

// poolResult is what runPool reports back.
type poolResult struct {
	done, errs int
	tripped    bool
}

// runPool runs fn over items in `workers` goroutines. A run stops starting new
// items when ctx is cancelled or when breakerLimit items in a row have failed
// (the source or database is failing); in-flight items finish. fn returning errStopped is
// neither success nor failure.
func runPool[T any](parent context.Context, items []T, workers int, label string, every int,
	fn func(context.Context, T) error) poolResult {
	if workers < 1 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var (
		mu     sync.Mutex
		next   int
		streak int
		res    poolResult
	)
	t0 := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if ctx.Err() != nil || next >= len(items) {
					mu.Unlock()
					return
				}
				item := items[next]
				next++
				mu.Unlock()

				err := fn(ctx, item)
				if errors.Is(err, errStopped) {
					return
				}

				mu.Lock()
				if err == nil {
					res.done++
					streak = 0
				} else {
					res.errs++
					streak++
					slog.Warn("prodir: item failed", "stage", label, "error", err)
					if streak >= breakerLimit && !res.tripped {
						res.tripped = true
						slog.Error("prodir: circuit breaker — consecutive items failed; stopping (work already written is intact)",
							"stage", label, "limit", breakerLimit, "last_error", err)
						cancel()
					}
				}
				if n := res.done + res.errs; n%every == 0 {
					slog.Info("prodir: progress", "stage", label, "n", n, "total", len(items),
						"per_sec", float64(n)/max(time.Since(t0).Seconds(), 1e-9), "errors", res.errs)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return res
}

// exitCode applies the shared exit policy: breaker > failed items > interrupt.
func exitCode(r poolResult, parent context.Context) int {
	switch {
	case r.tripped:
		return ExitBreaker
	case r.errs > 0:
		return ExitErrors
	case parent.Err() != nil:
		return ExitInterrupted
	}
	return ExitOK
}

// ---------- JSONL state ----------

// readJSONL calls fn with every line of path, skipping blank lines. A missing
// file is an empty file. Corrupt lines (a crash can leave a partial last line)
// are the caller's to skip: fn simply fails to unmarshal them.
func readJSONL(path string, fn func(line []byte)) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if line = bytes.TrimSpace(line); len(line) > 0 {
			fn(line)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// appender appends whole JSON lines to a state file; safe for concurrent use.
type appender struct {
	mu sync.Mutex
	f  *os.File
}

// openAppend opens path for appending, first terminating a truncated last line
// left by a hard kill.
func openAppend(path string) (*appender, error) {
	needNL := false
	if b, err := os.ReadFile(path); err == nil && len(b) > 0 && b[len(b)-1] != '\n' {
		needNL = true
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if needNL {
		if _, err := f.WriteString("\n"); err != nil {
			f.Close()
			return nil, err
		}
	}
	return &appender{f: f}, nil
}

// write appends each value as one JSON line, in a single write call.
func (a *appender) write(vals ...any) error {
	var buf bytes.Buffer
	for _, v := range vals {
		b, err := marshalLine(v)
		if err != nil {
			return err
		}
		buf.Write(b)
	}
	return a.writeBytes(buf.Bytes())
}

func (a *appender) writeBytes(b []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.f.Write(b)
	return err
}

func (a *appender) Close() error { return a.f.Close() }

// marshalLine is json.Marshal without HTML escaping (the state files keep
// "&", "<", ">" literal like the Python writers) plus a trailing newline.
func marshalLine(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// marshalNoEscape is marshalLine without the trailing newline, for custom
// MarshalJSON methods (encoding/json would otherwise HTML-escape their output).
func marshalNoEscape(v any) ([]byte, error) {
	b, err := marshalLine(v)
	return bytes.TrimSuffix(b, []byte("\n")), err
}

// ---------- text + CSV helpers ----------

// isPySpace is Python's str.isspace / regex \s: unicode.IsSpace plus the
// C0 separators U+001C..U+001F.
func isPySpace(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) }

// clean collapses every whitespace run to one space and trims, like the
// Python re.sub(r"\s+", " ", s).strip().
func clean(s string) string { return strings.Join(strings.FieldsFunc(s, isPySpace), " ") }

// csvWriter writes UTF-8-with-BOM CSV with CRLF line ends and minimal quoting,
// matching Python's csv.DictWriter(encoding="utf-8-sig"). encoding/csv is not
// used because it also quotes fields with a leading space.
type csvWriter struct {
	w *bufio.Writer
	f *os.File
}

func newCSVWriter(path string) (*csvWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	w.WriteString("\xef\xbb\xbf")
	return &csvWriter{w: w, f: f}, nil
}

func (c *csvWriter) row(fields []string) {
	for i, s := range fields {
		if i > 0 {
			c.w.WriteByte(',')
		}
		if strings.ContainsAny(s, ",\"\r\n") {
			c.w.WriteByte('"')
			c.w.WriteString(strings.ReplaceAll(s, `"`, `""`))
			c.w.WriteByte('"')
		} else {
			c.w.WriteString(s)
		}
	}
	c.w.WriteString("\r\n")
}

func (c *csvWriter) close() error {
	err := c.w.Flush()
	if cerr := c.f.Close(); err == nil {
		err = cerr
	}
	return err
}
