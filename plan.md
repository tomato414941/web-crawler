# web-crawler plan

## Current milestone: harden crawl pipeline and read-model boundaries

The crawler is now past the migration cleanup and host-first read-model deployment work. Speed is
not the immediate focus for this slice. The current focus is to prevent another silent pipeline
stall and read-model responsibility creep by making crawl pipeline stages and host runnable
read-model boundaries explicit, observable, and testable:

- `CrawlerEngine` should orchestrate stages, not own every stage policy.
- Pipeline queues should be bounded and owned by a dedicated runtime object.
- Parse should own fetched-page parsing and parse-failure conversion.
- Finalize should own post-parse scheduler mutation.
- Publish should own blocking storage/output writes.
- Stage liveness should be visible in runtime stats.
- Stage workers should survive item-level errors and record failures.
- Finalizer bottlenecks should be explained by operation-level timing, not guessed from queue depth.
- Host runnable capability and host runnable head should be documented as related but separate
  runtime execution concepts.
- `host_runnable_heads` should remain a derived read model, not a second durable source of truth.

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
- Extracted parser stage orchestration out of `CrawlerEngine` and added parser liveness to runtime
  stats.
- Added finalizer operation timing breakdown for discovery, admission, host updates, and scheduler
  state transitions.
- Added admission operation timing breakdown for intent updates, row fetches, queue membership
  replacement, host-head updates, lease cleanup, and commit time.
- Moved admission host-head maintenance out of the synchronous admission path:
  - admission now refreshes affected host-head rows with set-based differential updates.
  - dirty host-head rows are reserved for head deletion repair, not normal admission.
  - daemon maintenance refreshes dirty host-head rows in bounded bulk batches.
- Extracted fetch-stage queue handoff orchestration out of `CrawlerEngine`.
- Added pipeline contract tests for queue metrics, liveness, finalizer error survival, and publisher
  error survival.
- Added parser-stage contract tests for success, parse-error conversion, and worker survival.
- Added fetch-stage contract tests for success, skipped, and failed queue routing.
- Added dirty host-head refresh tests and runtime payload coverage.
- Added a separate dirty-refresh limit and elapsed-time telemetry.
- Preserved existing public scheduler telemetry methods and runtime payload behavior.
- Clarified crawler concepts so host runnable capability means whether a host can produce work and
  roughly how much, while host runnable head means the representative next URL for that host.
- Clarified scheduler execution docs so `runnable_url_count` is an ordering/readiness signal, not an
  exact source-of-truth count.
- Moved host runnable-head ranking and runnable-time SQL policy into `HostRunnableHeadPolicy`.
- Added `HostRunnableHeadMaintenance` as the named maintenance facade for rebuild, dirty refresh,
  repair, and stale-candidate deletion without changing schema or external callers.

## Verification

- `UrlLedger` no longer directly owns host-first fallback counters or last lease diagnostic fields.
- `UrlLedger` no longer computes retry backoff, retry score decay, or terminal/retry failure
  transitions inline.
- Retry failure decay changes queue `scheduler_score`; it no longer rewrites URL discovery value.
- Admission projection now uses membership-store APIs instead of ledger-local queue projection helpers.
- Active lease storage now has direct unit coverage and is no longer implemented inline in `UrlLedger`.
- `CrawlerEngine` still exposes existing runtime queue and liveness keys.
- `CrawlerEngine` now exposes `parser_liveness` alongside finalizer and publisher liveness.
- Finalizer and publisher item-level failures are counted and do not kill the queue worker.
- Parser exceptions are converted into finalizer failures without killing the parser worker.
- Fetch workers now delegate queue handoff to a pipeline stage while keeping HTTP and scheduler
  details in `CrawlerEngine` callbacks.
- `timing_summary["finalizer"]` now exposes finalizer sub-stage p50/p95/max values.
- `timing_summary["finalizer"]` now exposes admission sub-stage p50/p95/max values.
- `admit_host_heads_ms` now measures set-based host-head differential refresh for admission.
- Runtime stats expose `host_head_dirty_refresh` alongside bounded host-head repair.
  `remaining_hosts` shows whether dirty refresh is keeping up.
- Pipeline boundaries are documented in `docs/system-architecture.md` and
  `docs/system-architecture.ja.md`.
- Host runnable capability/head boundaries are documented in `docs/crawler-concepts.md` and
  `docs/crawler-concepts.ja.md`.
- Host runnable-head execution semantics are aligned in `docs/scheduler-execution.md` and
  `docs/scheduler-execution.ja.md`.
- `HostRunnableHeadStore` still owns the public API and SQL primitives, but ranking policy and
  maintenance responsibility now have explicit names.
- No schema migration or caller migration was required for this slice.
- Existing tests covering scheduler stats, lease diagnostics, retry transitions, and runtime stats pass.
- No production speed change was required for this slice.

## Next candidates

- If `HostRunnableHeadStore` grows again, split actual maintenance SQL into a dedicated module while
  preserving the current public API.
- Continue reducing `UrlLedger` facade breadth around requeue, lease selection, and host-head
  operations.
- Continue slimming `CrawlerEngine` by extracting reusable fetch failure classification if it starts
  obscuring HTTP behavior changes.
- Evaluate whether dirty host-head repair remains exceptional under production crawl load before
  adding any larger scheduler projection.
- Keep detailed speed investigation separate from this design-simplification milestone.
