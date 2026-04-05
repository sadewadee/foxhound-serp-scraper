// proxyforward is a local HTTP CONNECT proxy that rotates through
// SOCKS5 proxies fetched from 1024proxy (or compatible) API.
//
// Usage:
//
//	PROXY_API_URL="https://apisocks.1024proxy.com/api/getIpInfo?key=KEY&port=443&num=20&country=US&type=2" \
//	  go run ./cmd/proxyforward
//
// Then point scraper at: PROXY_URL=http://localhost:8888
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

type socks5Proxy struct {
	Host     string
	Port     string
	Username string
	Password string
}

func (p socks5Proxy) Addr() string { return net.JoinHostPort(p.Host, p.Port) }

// pool holds a rotating list of SOCKS5 proxies fetched from the API.
type pool struct {
	mu       sync.Mutex
	proxies  []socks5Proxy
	idx      int
	apiURL   string
	poolSize int
	minPool  int
	client   *http.Client
}

func newPool(apiURL string, poolSize, minPool int) *pool {
	return &pool{
		apiURL:   apiURL,
		poolSize: poolSize,
		minPool:  minPool,
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

// fetch calls the API and parses the response lines: IP:PORT:USER:PASS
func (p *pool) fetch() ([]socks5Proxy, error) {
	url := p.apiURL
	if !strings.Contains(url, "num=") {
		url += "&num=" + strconv.Itoa(p.poolSize)
	}
	resp, err := p.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("proxy api: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("proxy api HTTP %d: %s", resp.StatusCode, string(body))
	}

	var proxies []socks5Proxy
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 4)
		if len(parts) < 4 {
			slog.Warn("proxyforward: skipping malformed line", "line", line)
			continue
		}
		proxies = append(proxies, socks5Proxy{
			Host:     parts[0],
			Port:     parts[1],
			Username: parts[2],
			Password: parts[3],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("proxy api read: %w", err)
	}
	return proxies, nil
}

// refresh fetches new proxies and appends to the pool.
func (p *pool) refresh() {
	fresh, err := p.fetch()
	if err != nil {
		slog.Error("proxyforward: refresh failed", "error", err)
		return
	}
	if len(fresh) == 0 {
		slog.Warn("proxyforward: API returned 0 proxies")
		return
	}
	p.mu.Lock()
	p.proxies = append(p.proxies, fresh...)
	slog.Info("proxyforward: pool refreshed", "added", len(fresh), "total", len(p.proxies))
	p.mu.Unlock()
}

// next returns the next proxy in round-robin order, refreshing if pool is low.
func (p *pool) next() (socks5Proxy, error) {
	p.mu.Lock()
	if len(p.proxies) <= p.minPool {
		p.mu.Unlock()
		p.refresh()
		p.mu.Lock()
	}
	if len(p.proxies) == 0 {
		p.mu.Unlock()
		return socks5Proxy{}, fmt.Errorf("no proxies available")
	}
	px := p.proxies[p.idx%len(p.proxies)]
	p.idx++
	// Remove used proxy so each request gets a fresh IP.
	p.proxies = append(p.proxies[:p.idx-1], p.proxies[p.idx:]...)
	p.idx = 0
	p.mu.Unlock()
	return px, nil
}

func (p *pool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.proxies)
}

// backgroundRefresh periodically tops up the pool.
func (p *pool) backgroundRefresh(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.size() < p.poolSize {
				p.refresh()
			}
		}
	}
}

func dialSOCKS5(px socks5Proxy, target string) (net.Conn, error) {
	auth := &proxy.Auth{User: px.Username, Password: px.Password}
	dialer, err := proxy.SOCKS5("tcp", px.Addr(), auth, &net.Dialer{Timeout: 15 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("socks5 dialer: %w", err)
	}
	conn, err := dialer.Dial("tcp", target)
	if err != nil {
		return nil, fmt.Errorf("socks5 dial %s via %s: %w", target, px.Addr(), err)
	}
	return conn, nil
}

func handleConnect(w http.ResponseWriter, r *http.Request, p *pool) {
	px, err := p.next()
	if err != nil {
		http.Error(w, "no proxy available", http.StatusServiceUnavailable)
		slog.Error("proxyforward: no proxy", "error", err)
		return
	}

	targetConn, err := dialSOCKS5(px, r.Host)
	if err != nil {
		http.Error(w, "proxy dial failed", http.StatusBadGateway)
		slog.Warn("proxyforward: dial failed", "target", r.Host, "proxy", px.Addr(), "error", err)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		targetConn.Close()
		return
	}

	w.WriteHeader(http.StatusOK)
	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		targetConn.Close()
		return
	}

	// Flush any buffered data from the 200 response.
	clientBuf.Flush()

	go transfer(targetConn, clientConn)
	go transfer(clientConn, targetConn)
}

func handleHTTP(w http.ResponseWriter, r *http.Request, p *pool) {
	px, err := p.next()
	if err != nil {
		http.Error(w, "no proxy available", http.StatusServiceUnavailable)
		return
	}

	auth := &proxy.Auth{User: px.Username, Password: px.Password}
	dialer, err := proxy.SOCKS5("tcp", px.Addr(), auth, &net.Dialer{Timeout: 15 * time.Second})
	if err != nil {
		http.Error(w, "proxy setup failed", http.StatusBadGateway)
		return
	}

	transport := &http.Transport{
		Dial: dialer.Dial,
	}

	r.RequestURI = ""
	resp, err := transport.RoundTrip(r)
	if err != nil {
		http.Error(w, "upstream failed", http.StatusBadGateway)
		slog.Warn("proxyforward: upstream failed", "url", r.URL, "proxy", px.Addr(), "error", err)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func transfer(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	io.Copy(dst, src)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	apiURL := os.Getenv("PROXY_API_URL")
	if apiURL == "" {
		fmt.Fprintln(os.Stderr, "PROXY_API_URL is required")
		os.Exit(1)
	}

	listenAddr := envOrDefault("PROXY_LISTEN", ":8888")
	poolSize := envIntOrDefault("PROXY_POOL_SIZE", 20)
	refreshSec := envIntOrDefault("PROXY_REFRESH_SEC", 30)
	minPool := envIntOrDefault("PROXY_MIN_POOL", 5)

	slog.Info("proxyforward: starting",
		"listen", listenAddr,
		"pool_size", poolSize,
		"refresh_sec", refreshSec,
		"min_pool", minPool,
	)

	p := newPool(apiURL, poolSize, minPool)
	p.refresh() // initial fill

	ctx := context.Background()
	go p.backgroundRefresh(ctx, time.Duration(refreshSec)*time.Second)

	server := &http.Server{
		Addr: listenAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodConnect {
				handleConnect(w, r, p)
			} else {
				handleHTTP(w, r, p)
			}
		}),
	}

	slog.Info("proxyforward: listening", "addr", listenAddr, "pool", p.size())
	if err := server.ListenAndServe(); err != nil {
		slog.Error("proxyforward: server error", "error", err)
		os.Exit(1)
	}
}
