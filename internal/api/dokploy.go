package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// DokployConfig holds Dokploy API connection settings.
type DokployConfig struct {
	URL    string `yaml:"url"`    // e.g. https://kurama.mordenhost.com
	APIKey string `yaml:"api_key"`
}

// DokployClient interacts with the Dokploy API to manage worker deployments.
type DokployClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewDokployClient creates a Dokploy API client.
// Returns nil if not configured.
func NewDokployClient(cfg DokployConfig) *DokployClient {
	if cfg.URL == "" || cfg.APIKey == "" {
		return nil
	}
	return &DokployClient{
		baseURL: strings.TrimRight(cfg.URL, "/"),
		apiKey:  cfg.APIKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ── Dokploy API types ──

// ComposeCreateRequest creates a new Compose app in Dokploy.
type ComposeCreateRequest struct {
	Name          string `json:"name"`
	EnvironmentID string `json:"environmentId"`
	ComposeFile   string `json:"composeFile,omitempty"`
	ComposeType   string `json:"composeType,omitempty"` // "docker-compose"
	Description   string `json:"description,omitempty"`
	ServerID      string `json:"serverId,omitempty"` // target remote server
}

// ComposeCreateResponse from Dokploy.
type ComposeCreateResponse struct {
	ComposeID string `json:"composeId"`
	AppName   string `json:"appName"`
}

// ComposeUpdateRequest updates a Compose app.
type ComposeUpdateRequest struct {
	ComposeID   string `json:"composeId"`
	Env         string `json:"env,omitempty"`
	ComposeFile string `json:"composeFile,omitempty"`
}

// ComposeDeployRequest triggers deployment.
type ComposeDeployRequest struct {
	ComposeID   string `json:"composeId"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// WorkerDeployRequest is the high-level request from our API.
type WorkerDeployRequest struct {
	Name        string `json:"name"`                  // worker name (e.g. "worker-sg-1")
	ServerID    string `json:"server_id"`             // Dokploy server ID
	ProjectID   string `json:"project_id,omitempty"`  // Dokploy project ID (for environment lookup)
	EnvID       string `json:"environment_id"`        // Dokploy environment ID
	Stage       string `json:"stage"`                 // contact | website | serp
	Workers     int    `json:"workers"`               // concurrent workers
	PostgresDSN string `json:"postgres_dsn"`          // remote PG DSN
	RedisAddr   string `json:"redis_addr"`            // remote Redis address
	RedisPW     string `json:"redis_password,omitempty"`
	ProxyURL    string `json:"proxy_url,omitempty"`
	Memory      string `json:"memory,omitempty"`      // e.g. "1g"
	ImageTag    string `json:"image_tag,omitempty"`   // default: "main"
}

// WorkerInfo holds info about a deployed worker.
type WorkerInfo struct {
	ComposeID string `json:"compose_id"`
	Name      string `json:"name"`
	Stage     string `json:"stage"`
	Workers   int    `json:"workers"`
	ServerID  string `json:"server_id"`
	Status    string `json:"status"` // idle, running, done, error
}

// DeployWorker creates a Compose app in Dokploy and deploys it.
func (d *DokployClient) DeployWorker(req WorkerDeployRequest) (*WorkerInfo, error) {
	if req.Workers <= 0 {
		req.Workers = 20
	}
	if req.Stage == "" {
		req.Stage = "contact"
	}
	if req.Memory == "" {
		req.Memory = "1g"
	}
	imageTag := req.ImageTag
	if imageTag == "" {
		imageTag = "main"
	}

	// Build docker-compose content for the worker.
	composeFile := buildWorkerCompose(imageTag, req.Stage, req.Workers, req.Memory)

	// Build env string.
	env := buildWorkerEnv(req)

	// Step 1: Create Compose app in Dokploy.
	createReq := ComposeCreateRequest{
		Name:          req.Name,
		EnvironmentID: req.EnvID,
		ComposeFile:   composeFile,
		ComposeType:   "docker-compose",
		Description:   fmt.Sprintf("serp-scraper worker: %s stage, %d workers", req.Stage, req.Workers),
		ServerID:      req.ServerID,
	}

	var createResp ComposeCreateResponse
	if err := d.call("POST", "/api/compose.create", createReq, &createResp); err != nil {
		return nil, fmt.Errorf("dokploy: create compose: %w", err)
	}

	slog.Info("dokploy: compose created",
		"compose_id", createResp.ComposeID,
		"name", req.Name)

	// Step 2: Update env vars.
	updateReq := ComposeUpdateRequest{
		ComposeID:   createResp.ComposeID,
		Env:         env,
		ComposeFile: composeFile,
	}
	if err := d.call("POST", "/api/compose.update", updateReq, nil); err != nil {
		return nil, fmt.Errorf("dokploy: update env: %w", err)
	}

	// Step 3: Deploy.
	deployReq := ComposeDeployRequest{
		ComposeID:   createResp.ComposeID,
		Title:       fmt.Sprintf("Deploy %s", req.Name),
		Description: fmt.Sprintf("stage=%s workers=%d", req.Stage, req.Workers),
	}
	if err := d.call("POST", "/api/compose.deploy", deployReq, nil); err != nil {
		return nil, fmt.Errorf("dokploy: deploy: %w", err)
	}

	slog.Info("dokploy: worker deployed",
		"compose_id", createResp.ComposeID,
		"name", req.Name,
		"stage", req.Stage,
		"workers", req.Workers)

	return &WorkerInfo{
		ComposeID: createResp.ComposeID,
		Name:      req.Name,
		Stage:     req.Stage,
		Workers:   req.Workers,
		ServerID:  req.ServerID,
		Status:    "deploying",
	}, nil
}

// StopWorker stops a deployed worker.
func (d *DokployClient) StopWorker(composeID string) error {
	req := map[string]string{"composeId": composeID}
	return d.call("POST", "/api/compose.stop", req, nil)
}

// StartWorker starts a stopped worker.
func (d *DokployClient) StartWorker(composeID string) error {
	req := map[string]string{"composeId": composeID}
	return d.call("POST", "/api/compose.start", req, nil)
}

// DeleteWorker deletes a worker Compose app.
func (d *DokployClient) DeleteWorker(composeID string) error {
	req := map[string]string{"composeId": composeID}
	return d.call("POST", "/api/compose.delete", req, nil)
}

// ── Internal helpers ──

func (d *DokployClient) call(method, path string, body any, result any) error {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, d.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", d.apiKey)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func buildWorkerCompose(imageTag, stage string, workers int, memory string) string {
	return fmt.Sprintf(`services:
  worker:
    image: ghcr.io/sadewadee/foxhound-serp-scraper:%s
    restart: unless-stopped
    environment:
      POSTGRES_DSN: "${POSTGRES_DSN}"
      REDIS_ADDR: "${REDIS_ADDR}"
      REDIS_PASSWORD: "${REDIS_PASSWORD:-}"
      PROXY_URL: "${PROXY_URL:-}"
      MORDIBOUNCER_API_URL: "${MORDIBOUNCER_API_URL:-https://mailexchange.kremlit.dev}"
      MORDIBOUNCER_SECRET: "${MORDIBOUNCER_SECRET:-}"
      TZ: "${TZ:-Asia/Jakarta}"
    shm_size: "512mb"
    command: ["run", "-stage", "%s", "-workers", "%d"]
    deploy:
      resources:
        limits:
          memory: %s
`, imageTag, stage, workers, memory)
}

func buildWorkerEnv(req WorkerDeployRequest) string {
	lines := []string{
		fmt.Sprintf("POSTGRES_DSN=%s", req.PostgresDSN),
		fmt.Sprintf("REDIS_ADDR=%s", req.RedisAddr),
	}
	if req.RedisPW != "" {
		lines = append(lines, fmt.Sprintf("REDIS_PASSWORD=%s", req.RedisPW))
	}
	if req.ProxyURL != "" {
		lines = append(lines, fmt.Sprintf("PROXY_URL=%s", req.ProxyURL))
	}
	lines = append(lines, "TZ=Asia/Jakarta")
	return strings.Join(lines, "\n")
}
