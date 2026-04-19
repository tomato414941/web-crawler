# web-crawler plan

## Current milestone: scheduler execution design

The current priority is to make the scheduler execution layer explicit before the next
performance change. The crawler now has host-first normal execution, but the lease hot path is too
expensive at production concurrency.

The documentation split is:

- `crawler-concepts`: abstract model and naming principles
- `scheduler-state-model`: source-of-truth boundaries and invariants
- `scheduler-execution`: runtime lease strategy, read models, and hot-path constraints
- `system-architecture`: project-wide subsystem boundaries

## Active work

- Add `docs/scheduler-execution.md` and `docs/scheduler-execution.ja.md`.
- Keep `crawler-concepts` abstract by moving runtime execution details out of it.
- Cross-link `scheduler-state-model` and `system-architecture` to the new execution document.
- Record the current production bottleneck: host-first lease selection is the main hot path.
- Keep the next implementation candidate narrow: optimize the host-first lease query before adding
  a new durable projection.

## Not in this slice

- No code change.
- No database schema migration.
- No production configuration change.
- No new durable scheduler projection.

## Acceptance

- Scheduler execution has a dedicated document in English and Japanese.
- Existing docs clearly point to the right layer instead of repeating execution details.
- `crawler-concepts` stays abstract and does not carry SQL or worker-lane details.
- The next code task is clear: optimize host-first lease candidate selection.
