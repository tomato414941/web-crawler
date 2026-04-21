# web-crawler plan

## Current milestone: simplify scheduler boundaries

The crawler is now past the migration cleanup and host-first read-model deployment work. Speed is
not the immediate priority. The current priority is to keep the design understandable while moving
closer to the crawler concepts:

- URL ledger should keep durable URL facts.
- Scheduler membership should own live queue membership.
- Execution ownership should stay explicit in leases.
- Runtime read models should remain derived and repairable.
- Policy and telemetry should not keep accumulating inside `UrlLedger`.

## Completed in this slice

- Replaced the stale deployment-oriented plan with the current design-simplification milestone.
- Moved cycle-local host-first lease telemetry out of `UrlLedger`.
- Moved retry failure transition policy out of `UrlLedger`.
- Preserved existing public scheduler telemetry methods and runtime payload behavior.

## Verification

- `UrlLedger` no longer directly owns host-first fallback counters or last lease diagnostic fields.
- `UrlLedger` no longer computes retry backoff, retry priority, or terminal/retry failure
  transitions inline.
- Existing tests covering scheduler stats, lease diagnostics, retry transitions, and runtime stats pass.
- No production speed change was required for this slice.

## Next candidates

- Clarify whether `priority` is value, urgency, retry decay, or queue ranking.
- Keep `host_runnable_heads` as a derived read model, but consider moving ranking policy out of the
  read-model store.
- Keep detailed speed investigation separate from this design-simplification milestone.
