# DataDome Directory Module (Yelp, TripAdvisor)

The enrich pipeline can route Yelp and TripAdvisor pages through a residential
proxy with DataDome challenge handling. **This module ships DISABLED**
(`DIRECTORY_DATADOME_ENABLED=0` in every compose file) and has **never run
against a live challenge**: as of 2026-09-25 both sites return a DataDome 403
to every fetch method available (plain HTTP, foxhound stealth HTTP through a
datacenter proxy, and a Camoufox browser with a captcha extension). Nothing in
this document claims live-verified behavior.

## How to enable

Set these on the **enrich** workers only (the SERP stage never touches these
sites):

| Variable | Meaning | Default |
|---|---|---|
| `DIRECTORY_DATADOME_ENABLED` | `"1"` turns the module on | `"0"` |
| `DIRECTORY_PROXY_URL` | residential proxy URL for the module's pooled browser | `""` |
| `DIRECTORY_PROXY_STICKY` | `"1"` reuses one exit IP per session, so a solved `datadome` cookie stays valid | `"1"` |
| `DATADOME_SOLVER` | `"none"` or `"capsolver"` | `"none"` |
| `CAPSOLVER_API_KEY` | solver key. Secret: never logged, never printed in error paths | `""` |

Enabling the flag without a proxy is a misconfiguration and falls open to
"off" with a one-time `slog.Warn` (`Invariant #7`). With the flag on and a
proxy configured, the two sites are no longer skipped at SERP insert or in
enrich, and their pages are fetched through one pooled Camoufox browser per
enrich worker — never through the shared browser or the no-proxy stealth path.

## What it does once enabled

- `internal/directory/datadome.go` detects a DataDome challenge
  (`captcha-delivery.com` / `geo.captcha-delivery.com`, the `datadome` cookie
  challenge form, or a 403 carrying the `dd` marker).
- When a solver is configured, the same proxy/session that fetched the page is
  used to solve, and the resulting `datadome` cookie rides the sticky session.
  `DATADOME_SOLVER=none` (the default) means no solving is attempted: the job
  is backed off immediately.
- A DataDome block burns at most **one** enrichment attempt per window; the
  rest of that site's jobs requeue with their `attempt_count` untouched, on a
  site-level backoff (30s → 10min), mirroring how a SearXNG suspension works.
- Extractors queue only the business's **own website** for enrichment — never a
  yelp.com or tripadvisor.com URL (the same rule YellowPages follows since
  #52). Businesses with no external website are enriched from the directory
  page only; nothing extra is queued.

## Cost drivers

- **Residential GB per page.** One browser fetch loads the page plus its
  assets through the residential exit. Estimate volume at activation time
  before committing the whole queue to it: the queue depth times a per-page
  byte count measured in the first hour.
- **Solver calls per challenge.** CapSolver bills per solved challenge, not
  per page. The site-level backoff bounds how often one exit can challenge,
  but a badly-solved session can challenge on every page — watch the
  solver-call rate, not just the page rate.

## Activation checklist

Run these against **live** pages before flipping the flag everywhere:

1. From a throwaway container that uses `DIRECTORY_PROXY_URL`, fetch one Yelp
   business page and one TripAdvisor listing page: assert HTTP 200 with no
   `captcha-delivery.com` content and no `datadome` challenge cookie.
2. If a challenge appears, assert `DecideSolve` returns a cookie that a
   subsequent fetch accepts (no second challenge on the same session).
3. Assert the extractors against the *real* HTML: `Name`, `Phone`, `Address`,
   `Category`, `Rating` populated, and `Listing.URL` is the business's own
   website (not yelp.com / tripadvisor.com). Compare against the synthetic
   fixtures in `internal/directory/testdata/*_SYNTHETIC.html`; if selectors
   drift, fix them here before activating.
4. Assert the job-row accounting on a challenged page: first block costs one
   attempt, the next blocks in the window requeue with `attempt_count`
   untouched.
5. Watch the first hour: `datadome site in backoff window` log rate, solver
   calls/min, residential GB used, and listings queued per hour. Tune
   `DIRECTORY_PROXY_STICKY` and worker count from those numbers.

## Rollback

Flip the flag:

```
DIRECTORY_DATADOME_ENABLED=0
```

That is the whole rollback: the two sites are skipped exactly as they are
today (the shipped default), and any `datadome site in backoff window` rows
drain through the normal reconciler path. No schema change, no migration, no
code deploy needed.
