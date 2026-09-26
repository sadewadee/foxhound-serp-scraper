package directory

import (
	"net/url"
	"strings"
	"time"
)

// DataDome challenge handling for the Yelp and TripAdvisor directory module.
//
// This file is pure decision logic and carries no build tag on purpose: it
// must be unit-testable without the playwright tag, and it must never import
// foxhound. Nothing here performs a network call. The HTTP solver and the
// Camoufox fetch live behind the Solver interface and are wired in the enrich
// stage; tests drive them with a fake.
//
// Nothing in this file has been verified against a live DataDome challenge.
// Yelp and TripAdvisor return a DataDome 403 on every method available today
// (plain HTTP, stealth HTTP through a datacenter proxy, and a browser with a
// captcha extension), so there is no real challenge page to test against. The
// markers below come from DataDome's documented integration, and the
// activation checklist in docs/directory-datadome.md lists exactly what has to
// be confirmed against a live page before the module is switched on.

const (
	// datadomeMarkerInterstitial is the host DataDome serves its challenge from.
	datadomeMarkerInterstitial = "captcha-delivery.com"
	// datadomeMarkerGeo is the geo-captcha variant of the same challenge.
	datadomeMarkerGeo = "geo.captcha-delivery.com"

	// DataDomeBackoffBase is the site-level backoff after the first challenge
	// that could not be solved.
	DataDomeBackoffBase = 30 * time.Second
	// DataDomeBackoffCap stops the escalation. A DataDome block is the site
	// refusing the exit IP; hammering it only deepens the block.
	DataDomeBackoffCap = 10 * time.Minute
	// DataDomeMaxSolveAttempts bounds how many times one fetch may ask the
	// solver before giving up and backing the site off.
	DataDomeMaxSolveAttempts = 2
)

// Solver obtains a datadome cookie for a challenge. Implementations must use
// the same proxy and session the page was fetched with — a cookie solved
// against a different exit IP is rejected. Solve must never log its
// arguments: they can carry the solver API key.
type Solver interface {
	Solve(challengeURL, proxyURL, sessionID string) (datadomeCookie string, err error)
}

// Challenge is a detected DataDome block.
type Challenge struct {
	// URL is the challenge page URL when one could be extracted, else "".
	URL string
}

// DetectChallenge reports whether a response is a DataDome block.
//
// It matches three documented signals: the challenge is served from
// captcha-delivery.com or geo.captcha-delivery.com, the response sets a
// datadome cookie that is itself a challenge token, or the body is a DataDome
// 403 (the "dd" marker together with a 403 status). A normal page that merely
// mentions DataDome in passing is not a challenge.
func DetectChallenge(statusCode int, headers map[string]string, body []byte) (Challenge, bool) {
	lowerBody := strings.ToLower(string(body))

	if strings.Contains(lowerBody, datadomeMarkerGeo) || strings.Contains(lowerBody, datadomeMarkerInterstitial) {
		return Challenge{URL: extractChallengeURL(lowerBody)}, true
	}

	for name, value := range headers {
		if strings.EqualFold(name, "set-cookie") && isDatadomeChallengeCookie(value) {
			return Challenge{}, true
		}
	}

	if statusCode == 403 && (strings.Contains(lowerBody, "datadome") || strings.Contains(lowerBody, `"dd"`)) {
		return Challenge{}, true
	}
	return Challenge{}, false
}

// isDatadomeChallengeCookie reports whether a Set-Cookie value is DataDome's
// challenge cookie rather than a solved one. DataDome sets "datadome=" on
// every response; the challenge form carries a short token and is paired with
// the challenge host, while a solved cookie is a long opaque value.
func isDatadomeChallengeCookie(value string) bool {
	lower := strings.ToLower(value)
	if !strings.Contains(lower, "datadome=") {
		return false
	}
	return strings.Contains(lower, datadomeMarkerInterstitial)
}

// extractChallengeURL pulls the first captcha-delivery.com URL out of a body.
func extractChallengeURL(lowerBody string) string {
	for _, host := range []string{datadomeMarkerGeo, datadomeMarkerInterstitial} {
		idx := strings.Index(lowerBody, host)
		if idx < 0 {
			continue
		}
		start := strings.LastIndex(lowerBody[:idx], "http")
		if start < 0 {
			continue
		}
		end := strings.IndexAny(lowerBody[start:], "\"' <")
		if end < 0 {
			return lowerBody[start:]
		}
		return lowerBody[start : start+end]
	}
	return ""
}

// SolveDecision is what to do with one detected challenge.
type SolveDecision struct {
	// Solved is true when the solver returned a cookie.
	Solved bool
	// Cookie is the datadome cookie to reuse for the sticky session.
	Cookie string
	// GiveUp is true when solving was attempted and failed, or when no solver
	// is configured: the caller must back the site off instead of retrying.
	GiveUp bool
	// Reason describes the outcome for logs. It never contains secrets.
	Reason string
}

// DecideSolve chooses how to handle a challenge.
//
// A nil solver or solver "none" gives up immediately — there is nothing to
// retry. Otherwise the solver is asked up to DataDomeMaxSolveAttempts times
// with the same proxy and session, and the first cookie it returns wins.
func DecideSolve(solverName string, solver Solver, challengeURL, proxyURL, sessionID string) SolveDecision {
	if solver == nil || strings.EqualFold(strings.TrimSpace(solverName), "none") || strings.TrimSpace(solverName) == "" {
		return SolveDecision{GiveUp: true, Reason: "no datadome solver configured"}
	}
	var lastErr string
	for attempt := 1; attempt <= DataDomeMaxSolveAttempts; attempt++ {
		cookie, err := solver.Solve(challengeURL, proxyURL, sessionID)
		if err == nil && strings.TrimSpace(cookie) != "" {
			return SolveDecision{Solved: true, Cookie: strings.TrimSpace(cookie), Reason: "solved"}
		}
		if err != nil {
			lastErr = err.Error()
		} else {
			lastErr = "solver returned an empty cookie"
		}
	}
	return SolveDecision{GiveUp: true, Reason: "solver failed after retries: " + lastErr}
}

// DataDomeBackoff is the site-level backoff for one directory site. A DataDome
// block is the site refusing the exit IP, so burning an enrichment attempt per
// blocked page would drain the whole queue during one block. The first block
// in a window may cost one attempt; every further block inside the window is
// requeued without one, exactly as a SearXNG suspension is.
type DataDomeBackoff struct {
	streak int
	until  time.Time
}

// RecordBlock registers one unsolved block and returns the new deadline plus
// whether this block is the first in its window. Only the first costs an
// enrichment attempt; the caller requeues the rest with their attempt counter
// untouched.
func (b *DataDomeBackoff) RecordBlock(now time.Time) (deadline time.Time, firstInWindow bool) {
	firstInWindow = b.until.IsZero() || now.After(b.until)
	if firstInWindow {
		b.streak = 1
	} else {
		b.streak++
	}
	b.until = now.Add(datadomeBackoffDelay(b.streak))
	return b.until, firstInWindow
}

// RecordSolved clears the window: a solved challenge proves the exit IP is
// accepted again, so the next block starts a fresh window.
func (b *DataDomeBackoff) RecordSolved() {
	b.streak = 0
	b.until = time.Time{}
}

// BackedOff reports whether the site is still inside its block window.
func (b *DataDomeBackoff) BackedOff(now time.Time) bool {
	return !b.until.IsZero() && now.Before(b.until)
}

// datadomeBackoffDelay is 30s, 60s, 120s, … capped at DataDomeBackoffCap.
func datadomeBackoffDelay(streak int) time.Duration {
	if streak <= 0 {
		return 0
	}
	d := DataDomeBackoffBase
	for i := 1; i < streak; i++ {
		d *= 2
		if d >= DataDomeBackoffCap {
			return DataDomeBackoffCap
		}
	}
	return d
}

// ModuleActive reports whether the DataDome directory module should actually
// run. It is on only when the flag is enabled AND a proxy URL is configured;
// an enabled flag with an empty proxy is a misconfiguration and must fall
// open to "off" (the caller logs that once). This is the single decision the
// enrich and SERP skip paths share, so the two cannot drift.
func ModuleActive(enabled bool, proxyURL string) bool {
	return enabled && strings.TrimSpace(proxyURL) != ""
}

// ShouldSkipDomain is the ONE DataDome-aware blocklist verdict, shared by the
// SERP pre-INSERT filter and the enrich early skip so the two sites can never
// disagree: a blocklisted domain is skipped exactly as before unless the
// DataDome module is active AND the domain is one of its directory sites.
// datadomeActive should be directory.ModuleActive(flag, proxy). With the
// shipped default (flag off) this is byte-for-byte the plain isSkipDomain
// verdict used before the module existed.
//
// NOTE: isSkipDomain itself lives in the stage package (test-gated); this
// helper takes its verdict so the DataDome half stays pure and untagged.
func ShouldSkipDomain(blocklisted, datadomeActive bool, domain string) bool {
	if !blocklisted {
		return false
	}
	return !(datadomeActive && IsDataDomeDirectorySite(domain))
}

// IsDataDomeDirectorySite reports whether a host is one of the directory
// sites this module exists for.
func IsDataDomeDirectorySite(domain string) bool {
	domain = strings.ToLower(strings.TrimSpace(domain))
	for _, d := range []string{"yelp.com", "tripadvisor.com"} {
		if domain == d || strings.HasSuffix(domain, "."+d) {
			return true
		}
	}
	return false
}

// UnwrapRedirectURL extracts the business's own website from a directory
// redirect link such as Yelp's /biz_redir?url=<encoded>. A URL that is not a
// redirect, or whose target is missing or not http(s), is returned unchanged
// so the caller can decide what to do with it.
func UnwrapRedirectURL(rawURL string, param string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	target := u.Query().Get(param)
	if target == "" {
		return rawURL
	}
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		return rawURL
	}
	return target
}
