package directory

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDetectChallenge(t *testing.T) {
	tests := []struct {
		name   string
		status int
		header map[string]string
		body   string
		want   bool
	}{
		{"interstitial host", 200, nil, `<script src="https://ct.captcha-delivery.com/c.js">`, true},
		{"geo captcha host", 403, nil, `https://geo.captcha-delivery.com/captcha/?t=abc`, true},
		{"403 with datadome marker", 403, nil, `{"dd":{"cid":"1"}}`, true},
		{"datadome cookie pointing at the challenge", 200, map[string]string{"Set-Cookie": "datadome=tok; domain=captcha-delivery.com"}, "", true},
		{"solved datadome cookie is not a challenge", 200, map[string]string{"Set-Cookie": "datadome=longsolvedvalue"}, "<html>welcome", false},
		{"normal page", 200, nil, "<html><p>day spa honolulu</p>", false},
		{"403 without datadome", 403, nil, "forbidden", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := DetectChallenge(tt.status, tt.header, []byte(tt.body))
			if got != tt.want {
				t.Errorf("DetectChallenge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDetectChallenge_ExtractsURL(t *testing.T) {
	body := `var u = "https://geo.captcha-delivery.com/captcha/?initialCid=1";`
	ch, ok := DetectChallenge(403, nil, []byte(body))
	if !ok {
		t.Fatal("expected a challenge")
	}
	if !strings.HasPrefix(ch.URL, "https://geo.captcha-delivery.com/") {
		t.Errorf("challenge URL = %q", ch.URL)
	}
}

// fakeSolver records what it was called with and returns a scripted result.
type fakeSolver struct {
	cookie                 string
	err                    error
	calls                  int
	lastProxy, lastSession string
}

func (f *fakeSolver) Solve(_, proxyURL, sessionID string) (string, error) {
	f.calls++
	f.lastProxy = proxyURL
	f.lastSession = sessionID
	return f.cookie, f.err
}

func TestDecideSolve(t *testing.T) {
	t.Run("no solver gives up without retrying", func(t *testing.T) {
		d := DecideSolve("none", nil, "https://c", "http://proxy", "s")
		if !d.GiveUp || d.Solved {
			t.Fatalf("decision = %+v, want give up", d)
		}
	})

	t.Run("solver cookie is returned and the proxy is forwarded", func(t *testing.T) {
		f := &fakeSolver{cookie: "solved-cookie"}
		d := DecideSolve("capsolver", f, "https://c", "http://proxy", "sess-1")
		if !d.Solved || d.Cookie != "solved-cookie" {
			t.Fatalf("decision = %+v", d)
		}
		if f.lastProxy != "http://proxy" || f.lastSession != "sess-1" {
			t.Errorf("solver called with proxy %q session %q", f.lastProxy, f.lastSession)
		}
	})

	t.Run("solver failure retries then gives up", func(t *testing.T) {
		f := &fakeSolver{err: errors.New("solver down")}
		d := DecideSolve("capsolver", f, "https://c", "http://proxy", "s")
		if !d.GiveUp || d.Solved {
			t.Fatalf("decision = %+v, want give up", d)
		}
		if f.calls != DataDomeMaxSolveAttempts {
			t.Errorf("solver calls = %d, want %d", f.calls, DataDomeMaxSolveAttempts)
		}
		if strings.Contains(d.Reason, "http://proxy") {
			t.Errorf("reason leaks the proxy URL: %q", d.Reason)
		}
	})
}

func TestDataDomeBackoff_AttemptBudget(t *testing.T) {
	var b DataDomeBackoff
	now := time.Now()

	_, first := b.RecordBlock(now)
	if !first {
		t.Fatal("first block in a window must be allowed to cost one attempt")
	}
	if _, first = b.RecordBlock(now.Add(time.Second)); first {
		t.Fatal("second block inside the window must not cost another attempt")
	}

	// The window escalates: 30s then 60s.
	if got := datadomeBackoffDelay(1); got != 30*time.Second {
		t.Errorf("delay(1) = %s, want 30s", got)
	}
	if got := datadomeBackoffDelay(2); got != 60*time.Second {
		t.Errorf("delay(2) = %s, want 60s", got)
	}
	if got := datadomeBackoffDelay(20); got != DataDomeBackoffCap {
		t.Errorf("delay(20) = %s, want the cap", got)
	}

	b.RecordSolved()
	if _, first = b.RecordBlock(now.Add(time.Hour)); !first {
		t.Fatal("after a solved challenge the next block starts a new window")
	}
}

func TestModuleActive(t *testing.T) {
	if ModuleActive(false, "http://proxy") {
		t.Error("flag off must stay off even with a proxy")
	}
	if ModuleActive(true, "") || ModuleActive(true, "   ") {
		t.Error("enabled without a proxy must fall open to off")
	}
	if !ModuleActive(true, "http://proxy") {
		t.Error("enabled with a proxy must be active")
	}
}

func TestUnwrapRedirectURL(t *testing.T) {
	got := UnwrapRedirectURL("https://www.yelp.com/biz_redir?url=https%3A%2F%2Fspa.example.com%2F", "url")
	if got != "https://spa.example.com/" {
		t.Errorf("unwrap = %q", got)
	}
	if got := UnwrapRedirectURL("https://spa.example.com/", "url"); got != "https://spa.example.com/" {
		t.Errorf("non-redirect changed: %q", got)
	}
	if got := UnwrapRedirectURL("https://www.yelp.com/biz_redir?url=javascript:alert(1)", "url"); got != "https://www.yelp.com/biz_redir?url=javascript:alert(1)" {
		t.Errorf("non-http target accepted: %q", got)
	}
}

// TestShouldSkipDomain locks the one shared skip decision: a blocklisted
// DataDome directory site is admitted only while the module is active.
// Both the SERP pre-INSERT filter and the enrich early skip call this, so the
// two sites can never disagree about a domain.
func TestShouldSkipDomain(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		block    bool
		active   bool
		wantSkip bool
	}{
		{"yelp blocked, module off", "www.yelp.com", true, false, true},
		{"yelp blocked, module on", "www.yelp.com", true, true, false},
		{"tripadvisor blocked, module off", "www.tripadvisor.com", true, false, true},
		{"tripadvisor blocked, module on", "www.tripadvisor.com", true, true, false},
		{"not blocklisted, module on", "www.smallbiz.com", false, true, false},
		{"not blocklisted, module off", "www.smallbiz.com", false, false, false},
		{"blocklisted non-directory, module on", "www.linkedin.com", true, true, true},
		{"subdomain of a module site", "fr.yelp.com", true, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldSkipDomain(tt.block, tt.active, tt.domain); got != tt.wantSkip {
				t.Errorf("ShouldSkipDomain(block=%v, active=%v, %q) = %v, want %v",
					tt.block, tt.active, tt.domain, got, tt.wantSkip)
			}
		})
	}
}
