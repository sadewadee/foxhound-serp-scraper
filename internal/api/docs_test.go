package api

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// The OpenAPI spec is embedded at build time — these guards catch an empty or
// truncated embed and route/spec drift for the endpoints most likely to change.
func TestOpenAPISpecServed(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleOpenAPISpec(rec, httptest.NewRequest("GET", "/api/openapi.yaml", nil))

	if rec.Code != 200 {
		t.Fatalf("spec status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "openapi: 3.0") {
		t.Errorf("spec must start with an openapi 3.0 version marker, got %q", body[:min(40, len(body))])
	}
	for _, p := range []string{
		"/api/v2/results:", "/api/v2/results/categories:", "/api/health:",
		"country_code", "geo_source", "x-api-key",
	} {
		if !strings.Contains(body, p) {
			t.Errorf("spec missing %q", p)
		}
	}
}

func TestSwaggerUIServed(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleSwaggerUI(rec, httptest.NewRequest("GET", "/api/docs", nil))

	if rec.Code != 200 {
		t.Fatalf("docs status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "swagger-ui") || !strings.Contains(body, "/api/openapi.yaml") {
		t.Error("docs page must embed Swagger UI pointed at /api/openapi.yaml")
	}
}
