package stage

// MaxIrrelevantAttempts caps retries for poisoned/irrelevant SERP pages.
// Poisoning is query-deterministic (same query -> same irrelevant results),
// so retrying repeatedly wastes crawl budget; failing after 3 attempts allows
// transient engine/proxy routing glitches to recover while capping wasted work.
const MaxIrrelevantAttempts = 3

// RetryDecision calculates next status, attempt count, and backoff delay.
type RetryDecision struct {
	NewAttempt int
	Status     string // "new" or "failed"
	BackoffSec int    // 0 if failed
}

// EvaluateRetryDecision calculates the transition for a failing or soft-blocked job.
// It is the reference model for the retry SQL in serp.go: newAttempt = current + 1,
// status flips to 'failed' once newAttempt reaches the cap, and the backoff is
// 30*2^shift seconds with shift = min(currentAttempt, 6) — the same rule the SQL
// encodes as `30 * power(2, LEAST(6, attempt_count))` (attempt_count there is the
// pre-increment value). capOverride lowers the cap for a specific failure reason;
// MaxIrrelevantAttempts (3) is passed for poisoned pages. Note the serp_jobs
// max_attempts column defaults to 3, so the generic cap and the irrelevant cap are
// equal in production today; the override still matters if the column default is raised.
func EvaluateRetryDecision(currentAttempt, maxAttempts, capOverride int) RetryDecision {
	newAttempt := currentAttempt + 1
	effectiveCap := maxAttempts
	if capOverride > 0 && (effectiveCap <= 0 || capOverride < effectiveCap) {
		effectiveCap = capOverride
	}
	if effectiveCap <= 0 {
		effectiveCap = 3
	}

	if newAttempt >= effectiveCap {
		return RetryDecision{
			NewAttempt: newAttempt,
			Status:     "failed",
			BackoffSec: 0,
		}
	}

	shift := currentAttempt
	if shift < 0 {
		shift = 0
	}
	if shift > 6 {
		shift = 6
	}
	return RetryDecision{
		NewAttempt: newAttempt,
		Status:     "new",
		BackoffSec: 30 * (1 << shift),
	}
}
