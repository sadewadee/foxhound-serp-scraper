package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Role defines user access levels.
type Role string

const (
	RoleAdmin  Role = "admin"
	RoleViewer Role = "viewer"
)

// User represents an API user.
type User struct {
	Username string `json:"username" yaml:"username"`
	APIKey   string `json:"api_key" yaml:"api_key"` // static API key for auth
	Role     Role   `json:"role" yaml:"role"`
}

// TokenClaims holds JWT-like token data (simple HMAC-based, no external deps).
type TokenClaims struct {
	Username  string `json:"username"`
	Role      Role   `json:"role"`
	ExpiresAt int64  `json:"exp"`
	IssuedAt  int64  `json:"iat"`
}

// Auth handles authentication and authorization.
type Auth struct {
	users     map[string]*User // username → user
	apiKeys   map[string]*User // api_key → user
	secretKey []byte
}

// AuthConfig holds auth configuration.
type AuthConfig struct {
	Secret string `yaml:"secret"` // HMAC secret for tokens
	Users  []User `yaml:"users"`
}

// NewAuth creates an auth handler.
func NewAuth(cfg AuthConfig) *Auth {
	secret := cfg.Secret
	if secret == "" {
		slog.Error("auth: API_SECRET is not set — tokens will not be secure")
		secret = fmt.Sprintf("insecure-%d", time.Now().UnixNano())
	}

	a := &Auth{
		users:     make(map[string]*User),
		apiKeys:   make(map[string]*User),
		secretKey: []byte(secret),
	}

	for i := range cfg.Users {
		u := &cfg.Users[i]
		a.users[u.Username] = u
		if u.APIKey != "" {
			a.apiKeys[u.APIKey] = u
		}
	}

	if len(a.users) == 0 {
		slog.Warn("auth: NO USERS CONFIGURED — API will reject all requests until users are added to config")
	}

	return a
}

// GenerateToken creates a signed token for a user.
func (a *Auth) GenerateToken(username string, role Role, duration time.Duration) (string, error) {
	claims := TokenClaims{
		Username:  username,
		Role:      role,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(duration).Unix(),
	}

	data, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	// HMAC-SHA256 signature.
	mac := hmac.New(sha256.New, a.secretKey)
	mac.Write(data)
	sig := hex.EncodeToString(mac.Sum(nil))

	// Token = base64(claims).signature
	payload := hex.EncodeToString(data)
	return payload + "." + sig, nil
}

// ValidateToken verifies and parses a token.
func (a *Auth) ValidateToken(token string) (*TokenClaims, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid token format")
	}

	payload, err := hex.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid token payload")
	}

	// Verify signature.
	mac := hmac.New(sha256.New, a.secretKey)
	mac.Write(payload)
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(parts[1]), []byte(expectedSig)) {
		return nil, fmt.Errorf("invalid token signature")
	}

	var claims TokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("invalid token claims")
	}

	if time.Now().Unix() > claims.ExpiresAt {
		return nil, fmt.Errorf("token expired")
	}

	return &claims, nil
}

// AuthenticateByKey checks an API key and returns the user.
func (a *Auth) AuthenticateByKey(apiKey string) (*User, error) {
	user, ok := a.apiKeys[apiKey]
	if !ok {
		return nil, fmt.Errorf("invalid API key")
	}
	return user, nil
}

// AuthMiddleware extracts and validates auth from request.
// Supports: Bearer token, x-api-key header.
func (a *Auth) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for health check and login.
		if r.URL.Path == "/api/health" || r.URL.Path == "/api/v2/health" ||
			r.URL.Path == "/api/auth/login" || r.URL.Path == "/api/v2/auth/login" {
			next.ServeHTTP(w, r)
			return
		}

		// Try API key first.
		if apiKey := r.Header.Get("x-api-key"); apiKey != "" {
			user, ok := a.apiKeys[apiKey]
			if !ok {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid API key"})
				return
			}
			r = r.WithContext(withUser(r.Context(), user))
			next.ServeHTTP(w, r)
			return
		}

		// Try Bearer token.
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing authorization"})
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == authHeader {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid authorization format"})
			return
		}

		claims, err := a.ValidateToken(token)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}

		user, exists := a.users[claims.Username]
		if !exists {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "user no longer exists"})
			return
		}
		r = r.WithContext(withUser(r.Context(), user))
		next.ServeHTTP(w, r)
	})
}

// RequireRole middleware checks if user has the required role.
func RequireRole(role Role, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := getUserFromContext(r.Context())
		if user == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "not authenticated"})
			return
		}
		// Admin can do everything.
		if user.Role != RoleAdmin && user.Role != role {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient permissions"})
			return
		}
		next(w, r)
	}
}
