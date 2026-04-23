# web-crawler plan

## Current milestone: production observation and bounded growth

The crawler is now past the migration cleanup, host-first read-model deployment, first
pipeline-stage extraction, scheduler service decomposition, bounded page-content storage, and
discovery breadth cap work. The production DB was reset after unbounded page-body storage and URL
frontier growth filled the disk. The crawler has since been redeployed with bounded storage and
bounded discovery admission. The next bottleneck observed in production was finalizer backpressure:
fetch and parse could feed work faster than the finalizer could apply discovery admission, host
success, and URL completion mutations one page at a time.

The current focus is to keep the crawler honest under production load:

- `CrawlerEngine` should orchestrate stages, not own every stage policy.
- Pipeline queues should be bounded and owned by a dedicated runtime object.
- Parse should own fetched-page parsing and parse-failure conversion.
- Finalize should own post-parse scheduler mutation.
- Publish should own blocking storage/output writes.
- Stage liveness should be visible in runtime stats.
- Stage workers should survive item-level errors and record failures.
- Finalizer bottlenecks should be explained by operation-level timing, not guessed from queue depth.
- Success finalization should use bounded batches for set-shaped scheduler mutations.
- Host runnable capability and host runnable head should be documented as related but separate
  runtime execution concepts.
- `host_runnable_heads` should remain a derived read model, not a second durable source of truth.
- `UrlLedger` should remain the public facade for URL state, not the owner of every scheduling
  strategy and mutation body.
- Lease selection, scheduler admission, and requeue/recovery should be named scheduler services.
- `pages` should be a lightweight page index, not an unbounded page-body archive.
- Stored page text should live behind explicit storage tiers.
- Link extraction should not imply admitting every discovered URL into the frontier.
- Production observation should distinguish "growth is bounded per page" from "the total frontier
  will stop growing"; the latter still depends on crawl scope, host policy, and value filtering.
- DB growth should be watched as stored bytes, relation size, and URL ledger size separately.
- Discovery value should explain both ranking and admission decisions; rejected links need
  operator-visible reasons.

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
- Moved host runnable-head maintenance SQL implementation into `HostRunnableHeadMaintenance`:
  - `HostRunnableHeadStore` keeps the public API, normal read/write path, and shared SQL primitives.
  - maintenance now owns rebuild, dirty refresh, repair, and stale-candidate deletion bodies.
- Added `SchedulerLeaseSelector` and moved lease selection strategy out of `UrlLedger`:
  - host-first read-model leasing, bounded fallback leasing, URL-order leasing, batch leasing, and
    runnable-head SQL are now owned by the selector.
  - `UrlLedger` still exposes the existing public lease API and compatibility private wrappers.
- Added `SchedulerAdmissionService` and moved scheduler membership admission out of `UrlLedger`:
  - known-URL admission, discovered-task admission, candidate-row selection, and admission
    diagnostics now live behind a named service.
- Added `SchedulerRequeueService` and moved requeue/recovery mutations out of `UrlLedger`:
  - lease recovery, failed-URL retry requeue, refresh requeue, blocked-host backoff insertion, and
    seed upsert/requeue now live behind a named service.
- Kept database schema, public API, runtime telemetry, and deployment behavior unchanged for the
  scheduler-decomposition refactor slice.
- Split stored page text out of `pages` into `page_content`:
  - `pages` now records page metadata, storage tier, stored byte count, truncation, and outlink
    counts.
  - `page_content` stores only the bounded text payload for pages that keep text.
- Added storage tiering for crawled text:
  - `metadata_only` stores no text body.
  - `summary` stores a small sample for low-value or oversized pages.
  - `standard` stores normal text pages within a bounded budget.
  - `extended` stores larger samples only for high-discovery-value pages.
- Added discovery breadth caps:
  - minimum discovery value before admission.
  - maximum admitted links per page.
  - maximum admitted links per target host per page.
  - `outlink_count` records extracted links while stored `outlinks` records admitted links.
- Deployed bounded storage and frontier caps to production in `d3225c9`.
- Restarted the production crawler after applying `004_page_content_storage.sql`.
- Added discovery admission policy visibility:
  - discovered URLs now carry URL archetype and parent-context signals with their discovery value.
  - low-value URL archetypes require a stronger value threshold before scheduler admission.
  - parser telemetry records extracted, admitted, cap-rejected, and value-rejected link counts.
- Added bounded success-finalizer batching:
  - finalizer workers now coalesce nearby successful parsed pages before applying scheduler mutations.
  - discovery insertion and admission run once per batch.
  - host success updates run through `HostStore.record_success_many`.
  - URL completion runs through `UrlLedger.mark_done_many`.
  - `timing_summary["counts"]["finalizer_batch_size"]` exposes actual batch sizes.

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
- `HostRunnableHeadMaintenance` is no longer only a delegating facade; it owns maintenance control
  flow and SQL while preserving existing external behavior.
- `UrlLedger` no longer owns the implementation bodies for lease selection, scheduler admission,
  lease recovery, requeue, blocked-backoff insertion, or seed requeue.
- Scheduler services are constructed lazily for tests that instantiate `UrlLedger` with `__new__`,
  while normal runtime construction wires the services eagerly.
- Existing tests covering scheduler stats, lease diagnostics, retry transitions, and runtime stats pass.
- No production speed change was required for this slice.
- Fresh and migrated schemas converge on `pages` plus `page_content`.
- `/pages/{url_hash}` still returns `content` through a join, preserving API shape.
- Storage policy and discovery cap behavior have direct unit coverage.
- Discovery admission reasons have direct unit coverage and are exposed through runtime timing
  summary counts.
- Success-finalizer batching has direct coverage at the pipeline, engine, host store, URL ledger, and
  telemetry layers.
- Production smoke checks passed after deploy:
  - API health returned `ok`.
  - no recent critical crawler/API log errors were observed.
  - storage tiers were populated and stayed within configured per-page byte caps.
  - extracted outlinks and stored/admitted outlinks diverged as expected, proving the admission cap is
    active.

## Production observation

The bounded-storage and bounded-discovery changes removed the old disk-filling failure mode: `pages`
is no longer an unbounded text archive, `page_content` is tier-capped, and extracted links are not
automatically admitted into the scheduler. The remaining production questions are operational:

- whether `finalize_queue_wait_ms` falls after success-finalizer batching is deployed.
- whether actual `finalizer_batch_size` values are high enough to reduce DB round trips.
- whether URL frontier growth remains appropriate for the intended broad-WWW crawl scope.
- whether stored bytes, relation sizes, and disk usage remain bounded during sustained crawling.
- whether fetch/robots latency, not DB mutation throughput, becomes the dominant speed limit after
  finalizer backpressure is reduced.

## Next candidates

- Evaluate the success-finalizer batching deployment with:
  - API health and recent error logs.
  - `finalize_queue_wait_ms` p50/p95/max.
  - `finalizer_batch_size` count/avg/p95/max.
  - page throughput and active queue depths.
- Add a repeatable production observation command for:
  - URL ledger growth over time.
  - stored content growth over time.
  - tier distribution.
  - extracted vs admitted outlinks.
  - admission reason counts.
  - queue backpressure.
- Decide the crawl scope policy explicitly:
  - keep broad WWW discovery and rely on value scoring/caps.
  - restrict by host/domain allowlist.
  - add stronger admission thresholds for low-value hosts or pages.
- Tune discovery admission if production URL growth remains too fast:
  - lower `max_discovered_urls_per_page`.
  - lower `max_discovered_urls_per_target_host_per_page`.
  - raise `min_discovery_value`.
  - raise `low_value_archetype_min_discovery_value`.
  - add host-level or registrable-domain-level budgets only if URL-level and host-level caps are not
    enough.
- Watch `stored_content_bytes`, storage tier distribution, relation sizes, and URL ledger growth
  during production crawl cycles.
- Keep detailed speed investigation separate unless queue backpressure or fetch latency becomes the
  dominant production risk.
- Remove compatibility private wrappers from `UrlLedger` only after tests and internal callers stop
  patching or calling those private methods directly.
- Continue reducing `UrlLedger` facade breadth around host-head operations and URL discovery/update
  mutations.
- Continue slimming `CrawlerEngine` by extracting reusable fetch failure classification if it starts
  obscuring HTTP behavior changes.
- Evaluate whether dirty host-head repair remains exceptional under production crawl load before
  adding any larger scheduler projection.
