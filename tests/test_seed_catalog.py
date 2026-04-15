from pathlib import Path

from crawler.seed_catalog import enabled_seed_urls, load_seed_catalog, render_seed_env


def test_load_seed_catalog_reads_entries():
    catalog_path = Path(__file__).resolve().parents[1] / "config" / "seeds.json"

    entries = load_seed_catalog(catalog_path)

    assert entries
    assert entries[0].url == "https://www.wikipedia.org/"
    assert entries[0].enabled is True
    assert "reference" in entries[0].tags


def test_enabled_seed_urls_preserves_enabled_order():
    catalog_path = Path(__file__).resolve().parents[1] / "config" / "seeds.json"

    urls = enabled_seed_urls(load_seed_catalog(catalog_path))

    assert urls[0] == "https://www.wikipedia.org/"
    assert urls[-1] == "https://www.goodreads.com/"
    assert "https://www.npmjs.com/" not in urls


def test_render_seed_env_outputs_shell_assignment():
    catalog_path = Path(__file__).resolve().parents[1] / "config" / "seeds.json"

    rendered = render_seed_env(load_seed_catalog(catalog_path))

    assert rendered.startswith('CRAWL_SEED_URLS="https://www.wikipedia.org/')
    assert rendered.endswith('https://www.goodreads.com/"')
