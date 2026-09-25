package testpg

import (
	"testing"
	"time"
)

func TestReadyTimeout(t *testing.T) {
	tests := []struct {
		name string
		env  string
		set  bool
		want time.Duration
	}{
		{"unset falls back to default", "", false, DefaultReadyTimeout},
		{"empty falls back to default", "", true, DefaultReadyTimeout},
		{"whitespace-only falls back to default", "   ", true, DefaultReadyTimeout},
		{"valid override", "90s", true, 90 * time.Second},
		{"valid minutes override", "5m", true, 5 * time.Minute},
		{"malformed falls back to default", "ninety", true, DefaultReadyTimeout},
		{"negative falls back to default", "-30s", true, DefaultReadyTimeout},
		{"zero falls back to default", "0s", true, DefaultReadyTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv("TEST_PG_READY_TIMEOUT", tt.env)
			}
			if got := ReadyTimeout(); got != tt.want {
				t.Errorf("ReadyTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}
