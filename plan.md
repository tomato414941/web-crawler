# web-crawler plan

## Current milestone: harden crawl pipeline boundaries

The crawler is now past the migration cleanup and host-first read-model deployment work. Speed is
not the immediate focus for this slice. The current focus is to prevent another silent pipeline
stall by making crawl pipeline stages explicit, observable, and testable:

- `CrawlerEngine` should orchestrate stages, not own every stage policy.
- Pipeline queues should be bounded and owned by a dedicated runtime object.
- Finalize should own post-parse scheduler mutation.
- Publish should own blocking storage/output writes.
- Stage liveness should be visible in runtime stats.
- Stage workers should survive item-level errors and record failures.

## Completed in this slice

- Replaced the stale deployment-oriented plan with the current design-simplification milestone.
- Moved cycle-local host-first lease telemetry out of `UrlLedger`.
- Moved retry failure transition policy out of `UrlLedger`.
- Split durable URL value from live queue ordering:
  - `url_ledger.discovery_value` records how valuable a URL is to discover.
  - scheduler queues use `scheduler_score` for execution ordering.
  - `host_runnable_heads.head_scheduler_score` reflects the derived head row ordering.
- Moved scheduler admission projection into `SchedulerMembershipStore`:
  - `UrlLedger` still reads and updates durable URL rows.
  - scheduler membership now owns queue-row projection and queue replacement.
- Moved active execution lease storage into `ExecutionLeaseStore`:
  - `UrlLedger` still selects candidates and updates durable URL rows.
  - active lease token matching, lease row upsert/delete, and lease recovery deletion now live outside
    the URL ledger.
- Added bounded crawl pipeline queues and runtime liveness for finalizer/publisher workers.
- Extracted pipeline queue metrics, finalizer stage, and publish stage out of `CrawlerEngine`.
- Added pipeline contract tests for queue metrics, liveness, finalizer error survival, and publisher
  error survival.
- Preserved existing public scheduler telemetry methods and runtime payload behavior.

## Verification

- `UrlLedger` no longer directly owns host-first fallback counters or last lease diagnostic fields.
- `UrlLedger` no longer computes retry backoff, retry score decay, or terminal/retry failure
  transitions inline.
- Retry failure decay changes queue `scheduler_score`; it no longer rewrites URL discovery value.
- Admission projection now uses membership-store APIs instead of ledger-local queue projection helpers.
- Active lease storage now has direct unit coverage and is no longer implemented inline in `UrlLedger`.
- `CrawlerEngine` still exposes existing runtime queue and liveness keys.
- Finalizer and publisher item-level failures are counted and do not kill the queue worker.
- Pipeline boundaries are documented in `docs/system-architecture.md` and
  `docs/system-architecture.ja.md`.
- Existing tests covering scheduler stats, lease diagnostics, retry transitions, and runtime stats pass.
- No production speed change was required for this slice.

## Next candidates

- Keep `host_runnable_heads` as a derived read model, but consider moving ranking policy out of the
  read-model store.
- Continue reducing `UrlLedger` facade breadth around requeue, lease selection, and host-head
  operations.
- Continue slimming `CrawlerEngine` by extracting parse-stage orchestration after the finalizer and
  publisher boundary is stable in production.
- Keep detailed speed investigation separate from this design-simplification milestone.
