# Agent Boundary

The AI browser agent is an experimental acquisition tool. It is not part of the crawler core.

The crawler core discovers URLs, fetches resources under explicit policy, records observations,
and keeps scheduler state. The agent asks a model to operate a browser toward a task. That makes
it useful for exceptional acquisition work, but it also gives it a different safety and operations
model from normal crawling.

## Position

The agent is outside these core paths:

- daemon crawl loop
- scheduler frontier
- automatic discovery admission
- broad-web exploration
- page storage and crawl observation model
- production API behavior

Agent output should be treated as a task result plus an action trace, not as a normal crawl event.
Do not mix agent runs into broad crawl metrics unless a dedicated observation model is added.

## Allowed Use

Use the agent only for explicit, bounded tasks where simpler fetchers are insufficient.

Appropriate examples:

- a known public page needs a small interaction to reveal text
- a high-value public source needs manual-style inspection
- a downstream evidence workflow needs an action trace for one target

Prefer `HttpFetcher` or `BrowserFetcher` for normal acquisition. JavaScript rendering alone is not
a reason to use the agent.

## Default Constraints

The CLI requires `--experimental-agent`.

By default, the agent:

- applies the crawler egress guard before navigation
- applies the crawler egress guard to browser subresource requests
- blocks private, loopback, link-local, multicast, reserved, unresolved, and unsupported targets
- stays on the starting host for main-frame navigation
- blocks form input
- stays out of daemon and scheduler workflows

Operators may explicitly allow external navigation or form input for a run, but those flags should
be treated as risk escalations.

## Prohibited Use

Do not use the agent for:

- broad frontier exploration
- automatic processing of large URL sets
- credentialed browsing
- administrative interfaces
- local or private network targets
- form submission workflows
- irreversible actions
- production automation without separate network containment

## Future Work

If the agent remains useful, it should eventually move to a separate package or process with its
own network policy, action schema, audit log, rate limits, and storage model.
