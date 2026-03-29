package api

import (
	"encoding/json"
	"net/http"
	"time"
)

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		APIKey string `json:"api_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	user, err := s.auth.AuthenticateByKey(req.APIKey)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid API key"})
		return
	}

	token, err := s.auth.GenerateToken(user.Username, user.Role, 24*time.Hour)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "token generation failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"expires_in": 86400,
		"user":       map[string]string{"username": user.Username, "role": string(user.Role)},
	})
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	user := getUserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "not authenticated"})
		return
	}

	token, err := s.auth.GenerateToken(user.Username, user.Role, 24*time.Hour)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "token generation failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"expires_in": 86400,
	})
}
