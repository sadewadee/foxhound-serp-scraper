package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// handleV2Login authenticates via API key and returns a token.
func (s *Server) handleV2Login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		APIKey string `json:"api_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeV2Error(w, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}

	user, err := s.auth.AuthenticateByKey(req.APIKey)
	if err != nil {
		writeV2Error(w, http.StatusUnauthorized, "invalid_key", "invalid API key")
		return
	}

	token, err := s.auth.GenerateToken(user.Username, user.Role, 24*time.Hour)
	if err != nil {
		writeV2Error(w, http.StatusInternalServerError, "token_error", "token generation failed")
		return
	}

	writeV2Single(w, map[string]any{
		"token":      token,
		"expires_in": 86400,
		"user": map[string]string{
			"username": user.Username,
			"role":     string(user.Role),
		},
	})
}

// handleV2Refresh generates a new token for the authenticated user.
func (s *Server) handleV2Refresh(w http.ResponseWriter, r *http.Request) {
	user := getUserFromContext(r.Context())
	if user == nil {
		writeV2Error(w, http.StatusUnauthorized, "not_authenticated", "not authenticated")
		return
	}

	token, err := s.auth.GenerateToken(user.Username, user.Role, 24*time.Hour)
	if err != nil {
		writeV2Error(w, http.StatusInternalServerError, "token_error", "token generation failed")
		return
	}

	writeV2Single(w, map[string]any{
		"token":      token,
		"expires_in": 86400,
	})
}

// handleV2Health returns service health status.
func (s *Server) handleV2Health(w http.ResponseWriter, r *http.Request) {
	health := map[string]string{"status": "ok"}

	if err := s.db.Ping(); err != nil {
		slog.Warn("v2: health: postgres error", "error", err)
		health["postgres"] = "error"
		health["status"] = "degraded"
	} else {
		health["postgres"] = "ok"
	}

	if err := s.redis.Ping(context.Background()).Err(); err != nil {
		slog.Warn("v2: health: redis error", "error", err)
		health["redis"] = "error"
		health["status"] = "degraded"
	} else {
		health["redis"] = "ok"
	}

	httpStatus := http.StatusOK
	if health["status"] != "ok" {
		httpStatus = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(SingleResponse{Data: health})
}
