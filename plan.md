# web-crawler plan

## Current milestone: split precheck bottleneck timing

Cycle-local timing summaries are now deployed and show that the current crawl-cycle body cost is
mostly `fetch` and `precheck`, not lease selection. The next priority is to split `precheck` into
robots admission and host rate-limit reservation so the next speed fix can target the right subsystem.

## Completed

- Removed `url_ledger.stats()` from the daemon cycle-complete log path.
- Stopped the daemon from running a second `url_ledger.readiness()` / scheduler snapshot after each
  crawl cycle.
- Made daemon runtime scheduler views use the already-read readiness object instead of live full
  queue diagnostics.
- Stopped pre-cycle blocked-retry promotion from calling `blocked_reason_counts()` and full durable
  scheduler snapshots.
- Changed daemon cycle-start readiness to use the lightweight `host_runnable_heads` read model
  instead of the live full scheduler readiness query.
- Changed `/stats/diagnostics` to return runtime-snapshot-only degraded diagnostics.
- Added tests that fail if cycle completion calls live scheduler stats or snapshots.
- Added cycle-local timing summaries and compact cycle-complete p95 timing logs.
- Confirmed production exposes `runtime.payload.timing_summary` through `/stats`.

## Current slice

- Add `robots_ms` and `rate_limit_ms` to per-result timings.
- Include both fields in `timing_summary.stages` and compact cycle-complete p95 logs.
- Keep `precheck_ms` as the existing aggregate for compatibility.

## Acceptance

- Runtime snapshots include `robots_ms` and `rate_limit_ms` stage summaries.
- Cycle-complete logs include `robots_p95` and `rate_limit_p95`.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Confirm `/stats` exposes `runtime.payload.timing_summary.stages.robots_ms` and
  `runtime.payload.timing_summary.stages.rate_limit_ms`.
- Observe at least two production cycles and decide whether `precheck` is dominated by robots
  admission or host rate-limit reservation.
- If `fetch_request_ms` remains the largest stage, move next to slow-host deprioritization or timeout
  tuning.
