package geo

import (
	"context"
	"testing"
)

// TestResolveCountryFromProxy_EmptyURL covers the early-return guard.
// External HTTP behaviour (success, timeout, malformed body) is exercised
// in production and difficult to mock cleanly because the resolver hardcodes
// ipinfo.io. The function is small and stateless; the meaningful failure
// modes are observed via integration logging at startup.
func TestResolveCountryFromProxy_EmptyURL(t *testing.T) {
	_, err := ResolveCountryFromProxy(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty proxy URL, got nil")
	}
}

func TestResolveCountryFromProxy_InvalidURL(t *testing.T) {
	_, err := ResolveCountryFromProxy(context.Background(), "://not a url")
	if err == nil {
		t.Fatal("expected error for malformed proxy URL, got nil")
	}
}
