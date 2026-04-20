# web-crawler plan

## Current milestone: remove hidden historical debt

The project now uses a single greenfield schema baseline. The immediate priority is to keep the
repo aligned with that baseline by removing old migration bridges, old queue vocabulary, and stale
planning text.

## Completed

- Added host latency observations: EWMA, last observed latency, observed timestamp, and sample count.
- Deployed the host latency observation fields to production.
- Reset database migrations to a single `001_schema.sql` baseline.
- Reset production `schema_migrations` to the new baseline.
- Removed archived migration history from the repo.
- Split cycle timing enough to show that current production latency is mostly robots/precheck and
  fetch cost, not only lease selection.
- Shortened the robots fetch timeout and verified production still runs.

## Current slice

- Remove the one-time migration baseline bridge now that production has been reset.
- Remove test cleanup for obsolete scheduler table names.
- Rename the remaining internal refresh queue constant to refresh vocabulary.
- Keep this cleanup behavior-neutral: no scheduler policy or speed logic changes.

## Acceptance

- `sql_migrations/` contains only the current baseline and package marker.
- `schema_migrations` on production remains `001_schema.sql`.
- Repo search has no old migration bridge, old scheduler table cleanup, or old refresh queue vocabulary.
- Related tests, full tests, lint, and diff checks pass before deploy.

## Next checks after deploy

- Confirm `/health`, `/stats`, and `/stats/diagnostics` are healthy.
- Confirm production crawler and API containers are running.
- Recheck production crawl timing after a full cycle.
- If speed remains low, target fetch/robots behavior next rather than adding scheduler abstractions.
