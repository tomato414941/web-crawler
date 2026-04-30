# Seed Catalog

Seeds are bootstrap points for discovery. They do not define the crawler's full target scope.

The committed seed catalog lives in `config/seeds.json`. Treat it as the operator-facing source
of truth for which URLs are seeds and why they exist. Runtime `.env` files should only contain
the rendered `CRAWL_SEED_URLS` string for the currently enabled subset.

## Render Runtime Seeds

```bash
PYTHONPATH=src python scripts/render_seed_env.py
```

The output is a shell-compatible assignment:

```bash
CRAWL_SEED_URLS="https://example.com/ https://example.org/"
```

## Entry Fields

Each catalog entry stores:

- `url` — the seed URL itself
- `enabled` — whether it should appear in rendered runtime seed lists
- `tags` — operator metadata such as `tech`, `media`, `culture`, or `public-sector`
- `notes` — short rationale for why the seed exists

Tags are for operator understanding and seed-set maintenance. They are not currently used by
runtime scheduling policy.

## Current Production Notes

The current recommended private deployment seeds are:

```bash
https://www.iana.org/
https://datatracker.ietf.org/
https://www.rfc-editor.org/
```

These avoid `www.icann.org`, which is currently hostile to the crawler, and reduce stale-page
churn so the daemon does not spend cycles requeueing dead scheduled work too aggressively.
