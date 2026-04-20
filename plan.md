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

## Current slice

- Expose host-first read-model fallback counters in runtime stats.
- Prevent concurrent workers for the same host from repeatedly fetching the same robots.txt.
- Keep this slice schema-free: no migration, no new durable table, no queue policy rewrite.
- Commit, push, deploy, and evaluate production `/stats`.

## Acceptance

- Runtime stats include `host_first_fallback` counters.
- Repeated unavailable robots checks for one host use the runtime cache instead of repeating HTTP.
- Concurrent robots checks for one host share one fetch.
- Related tests, full tests, lint, and diff checks pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Confirm production crawler and API containers are running.
- Recheck production crawl timing and `host_first_fallback` after a full cycle.
- If speed remains low, target fetch transport behavior before adding scheduler abstractions.
