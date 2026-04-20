# web-crawler plan

## Current milestone: improve crawl throughput

The project now has a single greenfield schema baseline and no known active migration bridge.
The immediate priority is to raise production crawl speed without adding durable schema or broad
scheduler abstractions until runtime evidence says they are needed.

## Completed

- Added host latency observations: EWMA, last observed latency, observed timestamp, and sample count.
- Deployed the host latency observation fields to production.
- Reset database migrations to a single `001_schema.sql` baseline.
- Reset production `schema_migrations` to the new baseline.
- Removed archived migration history from the repo.
- Removed the one-time migration baseline bridge.
- Removed test cleanup for obsolete scheduler table names.
- Renamed the remaining internal refresh queue constant to refresh vocabulary.
- Split cycle timing enough to show that current production latency is mostly robots/precheck and
  fetch cost, not only lease selection.
- Shortened the robots fetch timeout and verified production still runs.
- Exposed host-first read-model fallback counters in runtime stats.
- Added a per-host robots fetch lock so concurrent workers share one robots.txt check.
- Deployed commit `88359ef` and verified `/health`, `/stats`, and `/stats/diagnostics`.

## Current slice

- Observe one or more production cycles with the new fallback counters.
- Decide whether high fallback misses are just idle worker polling or a scheduler supply issue.
- If speed remains low, target fetch transport behavior before adding scheduler abstractions.

## Acceptance

- Production `/stats` continues to expose `host_first_fallback`.
- Production cycle timing identifies the next dominant cost after robots/fetch/scheduler/persist.
- Related tests and lint pass before the next deploy.

## Next checks after deploy

- Recheck production crawl timing after another full cycle.
- Compare active-cycle timing against completed-cycle throughput.
- Keep changes schema-free unless runtime evidence shows a durable model is needed.
