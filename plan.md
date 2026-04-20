# web-crawler plan

## Current milestone: remove live scheduler scans from the daemon hot loop

The current priority is to make measured crawler throughput match wall-clock throughput. The daemon
must not run full scheduler diagnostics or readiness snapshots at cycle boundaries, because those
queries scan multi-million-row scheduler queues and delay the next crawl cycle.

## Completed in this slice

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

## Acceptance

- Cycle completion does not call `url_ledger.stats()`.
- Cycle completion does not call `scheduler_state_snapshot()`.
- Cycle start does not call the full `url_ledger.readiness()` query when lightweight daemon
  readiness is available.
- Pre-cycle retry promotion uses cheap runnable-surface and blocked-queue counts.
- `/stats` stays backed by the persisted runtime snapshot.
- `/stats/diagnostics` does not execute the expensive `pending_entries` full scheduler scan.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Confirm crawler `pg_stat_activity` no longer shows daemon-origin long-running
  `WITH pending_entries AS (...)` queries at cycle boundaries.
- Confirm `Cycle complete` to next `Cycle N:` gap drops from about 60-70 seconds to a few seconds.
- Compare start-to-next-start effective pages/sec against the recent production baseline of about
  0.95-1.2 pages/sec.
- If the gap is fixed but crawl body speed is still low, investigate finalizer serialization and
  fetch/precheck latency next.
