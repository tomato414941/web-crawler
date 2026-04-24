"""Repeatable operator observation snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_TABLES,
    URL_LEDGER_TABLE,
)

OBSERVED_RELATIONS = (
    "pages",
    "page_content",
    URL_LEDGER_TABLE,
    *PHYSICAL_QUEUE_TABLES.values(),
    BLOCKED_HOST_BACKOFF_TABLE,
    LEASE_TABLE,
    "host_runnable_heads",
    "host_runnable_head_dirty_hosts",
    "host_ledger",
    "host_state",
    "crawler_runtime_stats",
)


def read_operator_observation(storage: Any) -> dict[str, object]:
    """Read a compact, repeatable production observation from Postgres."""
    stats = storage.get_runtime_stats_summary()
    storage_shape = _read_storage_shape(storage.conn)
    return build_operator_observation(stats, storage_shape)


def build_operator_observation(
    stats: Mapping[str, object],
    storage_shape: Mapping[str, object],
) -> dict[str, object]:
    """Build a stable operator-facing observation from detailed stats."""
    operator_summary = _mapping(stats.get("operator_summary"))
    scheduler = _mapping(operator_summary.get("scheduler_readiness_states"))
    throughput = _mapping(operator_summary.get("throughput"))
    backpressure = _mapping(operator_summary.get("backpressure"))
    runtime = _mapping(stats.get("runtime"))
    storage_totals = _mapping(storage_shape.get("totals"))

    return {
        "crawl": {
            "total_pages": _int(stats.get("total_pages")),
            "hosts": _int(stats.get("hosts")),
            "oldest_crawl": stats.get("oldest_crawl"),
            "newest_crawl": stats.get("newest_crawl"),
            "total_bytes": _int(stats.get("total_bytes")),
            "total_stored_bytes": _int(stats.get("total_stored_bytes")),
        },
        "scheduler": {
            "pending": _int(scheduler.get("pending")),
            "runnable": _int(scheduler.get("runnable")),
            "scheduled": _int(scheduler.get("scheduled")),
            "retry_quarantine": _int(scheduler.get("retry_quarantine")),
            "blocked_host_next_request": _int(scheduler.get("blocked_host_next_request")),
            "blocked_host_backoff": _int(scheduler.get("blocked_host_backoff")),
            "leased": _int(scheduler.get("leased")),
        },
        "throughput": {
            "pages_per_second": throughput.get("pages_per_second"),
            "cycle_pages": throughput.get("cycle_pages"),
            "active_hosts": _int(throughput.get("active_hosts")),
            "errors": dict(_mapping(throughput.get("errors"))),
        },
        "backpressure": {
            "parse_queue_size": _int(backpressure.get("parse_queue_size")),
            "finalize_queue_size": _int(backpressure.get("finalize_queue_size")),
            "publish_queue_size": _int(backpressure.get("publish_queue_size")),
            "parse_queue_wait_max_ms": _float(backpressure.get("parse_queue_wait_max_ms")),
            "finalize_queue_wait_max_ms": _float(
                backpressure.get("finalize_queue_wait_max_ms")
            ),
            "publish_queue_wait_max_ms": _float(backpressure.get("publish_queue_wait_max_ms")),
        },
        "storage": {
            "tiers": list(storage_shape.get("tiers", [])),
            "relations": list(storage_shape.get("relations", [])),
            "outlinks": {
                "extracted": _int(storage_totals.get("outlink_count")),
                "stored": _int(storage_totals.get("stored_outlink_count")),
                "stored_ratio": _ratio(
                    storage_totals.get("stored_outlink_count"),
                    storage_totals.get("outlink_count"),
                ),
            },
            "url_ledger": dict(_mapping(storage_shape.get("url_ledger"))),
        },
        "runtime": {
            "updated_at": runtime.get("updated_at"),
            "stats_source": stats.get("stats_source"),
            "diagnostics_endpoint": stats.get("diagnostics_endpoint"),
        },
    }


def format_operator_observation(observation: Mapping[str, object]) -> str:
    """Format an observation snapshot for terminal use."""
    crawl = _mapping(observation.get("crawl"))
    scheduler = _mapping(observation.get("scheduler"))
    throughput = _mapping(observation.get("throughput"))
    backpressure = _mapping(observation.get("backpressure"))
    storage = _mapping(observation.get("storage"))
    runtime = _mapping(observation.get("runtime"))

    lines = [
        "Crawler Observation",
        f"Runtime: source={runtime.get('stats_source') or 'unknown'} updated_at={runtime.get('updated_at') or 'n/a'}",
        "",
        "Crawl",
        f"  pages={_format_int(crawl.get('total_pages'))} hosts={_format_int(crawl.get('hosts'))}",
        f"  stored={_format_bytes(crawl.get('total_stored_bytes'))} raw={_format_bytes(crawl.get('total_bytes'))}",
        "",
        "Scheduler",
        (
            "  "
            f"pending={_format_int(scheduler.get('pending'))} "
            f"runnable={_format_int(scheduler.get('runnable'))} "
            f"scheduled={_format_int(scheduler.get('scheduled'))} "
            f"retry={_format_int(scheduler.get('retry_quarantine'))} "
            f"leased={_format_int(scheduler.get('leased'))}"
        ),
        "",
        "Throughput",
        (
            "  "
            f"pages_per_second={_format_optional(throughput.get('pages_per_second'))} "
            f"cycle_pages={_format_optional(throughput.get('cycle_pages'))} "
            f"active_hosts={_format_int(throughput.get('active_hosts'))}"
        ),
        f"  errors={dict(_mapping(throughput.get('errors')))}",
        "",
        "Backpressure",
        (
            "  "
            f"parse={_format_int(backpressure.get('parse_queue_size'))} "
            f"finalize={_format_int(backpressure.get('finalize_queue_size'))} "
            f"publish={_format_int(backpressure.get('publish_queue_size'))}"
        ),
        (
            "  "
            f"wait_max_ms parse={_format_float(backpressure.get('parse_queue_wait_max_ms'))} "
            f"finalize={_format_float(backpressure.get('finalize_queue_wait_max_ms'))} "
            f"publish={_format_float(backpressure.get('publish_queue_wait_max_ms'))}"
        ),
        "",
        "Storage Tiers",
    ]

    tiers = list(storage.get("tiers", []))
    if tiers:
        for tier in tiers:
            tier_map = _mapping(tier)
            lines.append(
                "  "
                f"{tier_map.get('storage_tier')}: "
                f"pages={_format_int(tier_map.get('pages'))} "
                f"stored={_format_bytes(tier_map.get('stored_content_bytes'))} "
                f"raw={_format_bytes(tier_map.get('content_length'))}"
            )
    else:
        lines.append("  none")

    outlinks = _mapping(storage.get("outlinks"))
    lines.extend(
        [
            "",
            "Outlinks",
            (
                "  "
                f"extracted={_format_int(outlinks.get('extracted'))} "
                f"stored={_format_int(outlinks.get('stored'))} "
                f"stored_ratio={_format_ratio(outlinks.get('stored_ratio'))}"
            ),
            "",
            "Relation Sizes",
        ]
    )
    relations = list(storage.get("relations", []))
    if relations:
        for relation in relations:
            relation_map = _mapping(relation)
            lines.append(
                "  "
                f"{relation_map.get('relation')}: "
                f"{_format_bytes(relation_map.get('total_bytes'))}"
            )
    else:
        lines.append("  none")
    return "\n".join(lines)


def _read_storage_shape(conn: Any) -> dict[str, object]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT
                 storage_tier,
                 COUNT(*) AS pages,
                 COALESCE(SUM(stored_content_bytes), 0) AS stored_content_bytes,
                 COALESCE(SUM(content_length), 0) AS content_length,
                 COALESCE(SUM(outlink_count), 0) AS outlink_count,
                 COALESCE(SUM(stored_outlink_count), 0) AS stored_outlink_count
               FROM pages
               GROUP BY storage_tier
               ORDER BY storage_tier ASC"""
        )
        tier_rows = [
            {
                "storage_tier": storage_tier,
                "pages": int(pages or 0),
                "stored_content_bytes": int(stored_content_bytes or 0),
                "content_length": int(content_length or 0),
                "outlink_count": int(outlink_count or 0),
                "stored_outlink_count": int(stored_outlink_count or 0),
            }
            for (
                storage_tier,
                pages,
                stored_content_bytes,
                content_length,
                outlink_count,
                stored_outlink_count,
            ) in cur.fetchall()
        ]

        cur.execute(
            """SELECT
                 COALESCE(SUM(outlink_count), 0) AS outlink_count,
                 COALESCE(SUM(stored_outlink_count), 0) AS stored_outlink_count
               FROM pages"""
        )
        outlink_count, stored_outlink_count = cur.fetchone()

        cur.execute(f"SELECT to_regclass('public.{URL_LEDGER_TABLE}')")
        url_ledger_exists = cur.fetchone()[0] is not None
        url_ledger: dict[str, int] = {}
        if url_ledger_exists:
            cur.execute(
                f"""SELECT
                     COUNT(*) AS urls,
                     COUNT(DISTINCT host) AS hosts,
                     COUNT(*) FILTER (WHERE terminal_reason IS NOT NULL) AS terminal,
                     COUNT(*) FILTER (WHERE last_error IS NOT NULL) AS with_errors
                   FROM public.{URL_LEDGER_TABLE}"""
            )
            urls, hosts, terminal, with_errors = cur.fetchone()
            url_ledger = {
                "urls": int(urls or 0),
                "hosts": int(hosts or 0),
                "terminal": int(terminal or 0),
                "with_errors": int(with_errors or 0),
            }

        relations = []
        for relation in OBSERVED_RELATIONS:
            cur.execute("SELECT to_regclass(%s)", (f"public.{relation}",))
            exists = cur.fetchone()[0] is not None
            if not exists:
                continue
            cur.execute("SELECT pg_total_relation_size(%s::regclass)", (f"public.{relation}",))
            total_bytes = cur.fetchone()[0]
            relations.append({"relation": relation, "total_bytes": int(total_bytes or 0)})

    conn.commit()
    relations.sort(key=lambda item: (-int(item["total_bytes"]), str(item["relation"])))
    return {
        "tiers": tier_rows,
        "totals": {
            "outlink_count": int(outlink_count or 0),
            "stored_outlink_count": int(stored_outlink_count or 0),
        },
        "url_ledger": url_ledger,
        "relations": relations,
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _ratio(numerator: object, denominator: object) -> float | None:
    denominator_int = _int(denominator)
    if denominator_int == 0:
        return None
    return _int(numerator) / denominator_int


def _format_int(value: object) -> str:
    return f"{_int(value):,}"


def _format_float(value: object) -> str:
    return f"{_float(value):.1f}"


def _format_ratio(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.1%}"


def _format_optional(value: object) -> object:
    return "n/a" if value is None else value


def _format_bytes(value: object) -> str:
    size = float(_int(value))
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TiB"
