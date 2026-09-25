package stage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SearXNG suspension handling.
//
// A SearXNG instance that has suspended its upstream engines answers with HTTP
// 200 and valid JSON carrying zero results plus a non-empty
// `unresponsive_engines` list (e.g. [["google cse","Suspended: too many
// requests"]]). That is SearXNG's own capacity failing, not the job: burning a
// serp_jobs attempt on it would permanently fail healthy work during any
// suspension window, because prod caps serp_jobs at max_attempts = 3.
//
// So the two failure kinds are separated here: a suspension requeues the job
// with its attempt_count untouched (not incrementing is not resetting —
// Invariant #1 holds), while a genuine failure (non-JSON garbage, connection
// refused, DNS error, timeout) keeps the normal incrementing retry path.
//
// This file carries no build tag: the decision is pure stdlib logic so it can
// be unit-tested by both the tagged and untagged builds.

const (
	// SuspensionBackoffBase is the tab backoff after the first suspension.
	SuspensionBackoffBase = 30 * time.Second
	// SuspensionBackoffCap caps the escalating tab backoff at 10 minutes.
	SuspensionBackoffCap = 10 * time.Minute
)

// SearXNGOutcome classifies one SearXNG fetch.
type SearXNGOutcome int

const (
	// OutcomeSuccess: a usable page was returned.
	OutcomeSuccess SearXNGOutcome = iota
	// OutcomeSuspended: SearXNG is rate-limiting us or an upstream is down.
	OutcomeSuspended
	// OutcomeGenuineFailure: a real fault worth spending an attempt on.
	OutcomeGenuineFailure
)

// SuspensionDecision is the outcome of ClassifySearXNGOutcome.
type SuspensionDecision struct {
	Outcome SearXNGOutcome
	// Engines lists the upstream engines SearXNG reported as unresponsive.
	Engines []string
	// IncrementAttempt is false only for OutcomeSuspended.
	IncrementAttempt bool
	// ErrMsg is the error_msg to store on the job.
	ErrMsg string
}

// searxngSuspensionProbe is the subset of the SearXNG response that decides
// whether the instance is rate-limiting us.
type searxngSuspensionProbe struct {
	Results             []json.RawMessage `json:"results"`
	UnresponsiveEngines [][]string        `json:"unresponsive_engines"`
}

// ClassifySearXNGOutcome decides how a SearXNG fetch result should be treated.
//
// statusCode is the HTTP status (0 when the request never completed), body is
// whatever was read (nil on a transport error), and fetchErr is the transport
// error, if any.
//
// A status of 429 or 503 is SearXNG refusing service, not a fault in the job.
// A 200 carrying valid JSON with zero results and a non-empty
// unresponsive_engines list is the same condition expressed in the body.
// Anything else that failed — a
// non-JSON body, a dead connection, a DNS error, a timeout — is genuine.
func ClassifySearXNGOutcome(statusCode int, body []byte, fetchErr error) SuspensionDecision {
	// SearXNG itself refusing service.
	if statusCode == 429 || statusCode == 503 {
		return SuspensionDecision{
			Outcome:          OutcomeSuspended,
			IncrementAttempt: false,
			ErrMsg:           fmt.Sprintf("searxng upstream suspended: HTTP %d", statusCode),
		}
	}

	// A transport error with nothing to inspect: DNS, connection refused,
	// timeout, TLS error. Genuine — but note a 429/503 that FetchPlain turned
	// into an error was already handled above, so this is a real fault.
	if fetchErr != nil && len(body) == 0 {
		return SuspensionDecision{
			Outcome:          OutcomeGenuineFailure,
			IncrementAttempt: true,
			ErrMsg:           fetchErr.Error(),
		}
	}

	var probe searxngSuspensionProbe
	if err := json.Unmarshal(body, &probe); err != nil {
		// Not JSON at all: an error page, a proxy interception, a truncated
		// body. Worth an attempt to see whether it was transient.
		msg := "searxng returned a non-JSON body"
		if fetchErr != nil {
			msg = fmt.Sprintf("%s: %v", msg, fetchErr)
		}
		return SuspensionDecision{
			Outcome:          OutcomeGenuineFailure,
			IncrementAttempt: true,
			ErrMsg:           msg,
		}
	}

	// An unresponsive list alone is NOT a suspension: SearXNG routinely
	// reports one engine down (e.g. duckduckgo CAPTCHA) while the others
	// still return results. Suspension is zero results AND somebody missing.
	if len(probe.Results) == 0 && len(probe.UnresponsiveEngines) > 0 {
		names := make([]string, 0, len(probe.UnresponsiveEngines))
		for _, pair := range probe.UnresponsiveEngines {
			if len(pair) == 0 {
				continue
			}
			if len(pair) == 1 {
				names = append(names, pair[0])
				continue
			}
			names = append(names, pair[0]+" ("+pair[1]+")")
		}
		if len(names) == 0 {
			names = []string{"unknown"}
		}
		return SuspensionDecision{
			Outcome:          OutcomeSuspended,
			Engines:          names,
			IncrementAttempt: false,
			ErrMsg:           "searxng upstream suspended: " + strings.Join(names, ", "),
		}
	}

	return SuspensionDecision{Outcome: OutcomeSuccess, IncrementAttempt: false}
}

// SuspensionBackoff returns the tab backoff after `streak` consecutive
// suspensions: 30s, 60s, 120s, … capped at SuspensionBackoffCap. A streak of
// zero or less yields zero, meaning "no backoff owed".
func SuspensionBackoff(streak int) time.Duration {
	if streak <= 0 {
		return 0
	}
	d := SuspensionBackoffBase
	for i := 1; i < streak; i++ {
		d *= 2
		if d >= SuspensionBackoffCap {
			return SuspensionBackoffCap
		}
	}
	return d
}
