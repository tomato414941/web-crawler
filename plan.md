# web-crawler plan

## Current milestone: scheduler responsibility split

The current priority is to keep the crawler fast while making scheduler ownership explicit.
The URL ledger should own durable URL facts. Scheduler membership, host runnable-head read models,
quarantine, and leases should be separate implementation units.

## Completed in this slice

- Renamed the normal scheduler surfaces to `runnable` and `scheduled`.
- Added migration support for existing databases that still have the previous scheduler table names
  or queue values.
- Moved scheduler membership table/surface operations into `SchedulerMembershipStore`.
- Moved the `host_runnable_heads` projection into `HostRunnableHeadStore`.
- Kept the existing host-first lease path behavior: read model first, source-of-truth revalidation,
  stale candidate cleanup, and derived-query fallback.

## Remaining near-term work

- Move active lease insert/delete/recovery into a dedicated lease store.
- Move the remaining admission/requeue methods out of `UrlLedger` and into scheduler membership.
- Split heavy diagnostic stats out of `PgStorage` so normal storage and operator diagnostics have
  separate ownership.
- Re-measure production lease timings and crawler throughput after deploy.

## Acceptance

- New databases use `scheduler_queue_runnable` and `scheduler_queue_scheduled`.
- Existing databases migrate to the new scheduler queue names and queue values.
- `/stats` reports scheduler surfaces as `runnable`, `scheduled`, and `refresh`.
- Normal host-first leasing remains read-model-first and does not add hot-path global rebuilds.
- Related tests and lint pass before deploy.

## Next checks after deploy

- Confirm migration `030_rename_scheduler_surfaces.sql` is applied.
- Confirm `/health` and `/stats` are healthy.
- Check production logs for read-model refresh latency and `lease=` timings.
- Compare pages/sec and PostgreSQL CPU against the previous deployment.
