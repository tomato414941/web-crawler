#!/usr/bin/env python3
"""Render CRAWL_SEED_URLS from the seed catalog."""

from __future__ import annotations

from pathlib import Path

from crawler.seed_catalog import load_seed_catalog, render_seed_env


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    catalog_path = repo_root / "config" / "seeds.json"
    print(render_seed_env(load_seed_catalog(catalog_path)))


if __name__ == "__main__":
    main()
