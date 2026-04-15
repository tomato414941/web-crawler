"""Seed catalog helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SeedEntry:
    """Seed URL with operator metadata."""

    url: str
    enabled: bool
    tags: tuple[str, ...]
    notes: str


def load_seed_catalog(path: str | Path) -> list[SeedEntry]:
    """Load seed entries from a JSON catalog."""
    catalog_path = Path(path)
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    entries: list[SeedEntry] = []
    for raw_entry in payload.get("seeds", []):
        entries.append(
            SeedEntry(
                url=str(raw_entry["url"]),
                enabled=bool(raw_entry.get("enabled", True)),
                tags=tuple(str(tag) for tag in raw_entry.get("tags", [])),
                notes=str(raw_entry.get("notes", "")),
            )
        )
    return entries


def enabled_seed_urls(entries: list[SeedEntry]) -> list[str]:
    """Return enabled seed URLs in catalog order."""
    return [entry.url for entry in entries if entry.enabled]


def render_seed_env(entries: list[SeedEntry], env_name: str = "CRAWL_SEED_URLS") -> str:
    """Render a shell-compatible env assignment for enabled seeds."""
    return f'{env_name}="{" ".join(enabled_seed_urls(entries))}"'
