"""Repeatable operator observation snapshots."""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_TABLES,
    URL_LEDGER_TABLE,
)
from .scheduler_invariants import SchedulerInvariantChecker
from .host_ledger import HOST_LEDGER_TABLE

logger = logging.getLogger(__name__)

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
OPERATOR_OBSERVATION_STATEMENT_TIMEOUT_MS = 15000
_FALLBACK_ERROR_MESSAGE = "Observation failed; check service logs for details."
_URL_WITH_CREDENTIALS_PATTERN = re.compile(
    r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s]+",
)


def read_operator_observation(
    storage: Any,
    *,
    include_scheduler_invariants: bool = False,
) -> dict[str, object]:
    """Read a compact, repeatable production observation from Postgres."""
    stats = storage.get_runtime_stats_summary()
    storage_shape = _read_storage_shape(storage.conn)
    scheduler_invariants = None
    if include_scheduler_invariants:
        scheduler_invariants = SchedulerInvariantChecker(storage.conn).check(sample_limit=0).to_dict()
    return build_operator_observation(stats, storage_shape, scheduler_invariants)


def build_operator_observation(
    stats: Mapping[str, object],
    storage_shape: Mapping[str, object],
    scheduler_invariants: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build a stable operator-facing observation from detailed stats."""
    operator_summary = _mapping(stats.get("operator_summary"))
    scheduler = _mapping(operator_summary.get("scheduler_readiness_states"))
    throughput = _mapping(operator_summary.get("throughput"))
    backpressure = _mapping(operator_summary.get("backpressure"))
    admission_control = _mapping(operator_summary.get("admission_control"))
    discovery_admission = _mapping(operator_summary.get("discovery_admission"))
    runtime = _mapping(stats.get("runtime"))
    storage_totals = _mapping(storage_shape.get("totals"))
    invariants = _mapping(scheduler_invariants)
    invariants_checked = scheduler_invariants is not None

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
            "invariants": {
                "checked": invariants_checked,
                "ok": bool(invariants.get("ok", True)) if invariants_checked else None,
                "violations_total": _int(invariants.get("violations_total")),
                "duplicate_memberships": _int(invariants.get("duplicate_memberships")),
                "terminal_in_live_queue": _int(invariants.get("terminal_in_live_queue")),
                "expired_leases": _int(invariants.get("expired_leases")),
                "orphan_host_heads": _int(invariants.get("orphan_host_heads")),
                "host_head_mismatches": _int(invariants.get("host_head_mismatches")),
                "checked_at": invariants.get("checked_at"),
            },
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
        "admission_control": {
            "mode": admission_control.get("mode"),
            "target_pending": _int(admission_control.get("target_pending")),
            "pending": _int(admission_control.get("pending")),
            "min_score": _float(admission_control.get("min_score")),
            "per_page_cap": _int(admission_control.get("per_page_cap")),
            "per_target_host_cap": _int(admission_control.get("per_target_host_cap")),
            "new_external_host_cap": _int(
                admission_control.get("new_external_host_cap")
            ),
        },
        "discovery_admission": {
            "extracted": _int(discovery_admission.get("extracted")),
            "admitted": _int(discovery_admission.get("admitted")),
            "rejected": _int(discovery_admission.get("rejected")),
            "admit_ratio": discovery_admission.get("admit_ratio"),
            "rejection_reasons": dict(
                _mapping(discovery_admission.get("rejection_reasons"))
            ),
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
    invariants = _mapping(scheduler.get("invariants"))
    throughput = _mapping(observation.get("throughput"))
    backpressure = _mapping(observation.get("backpressure"))
    admission_control = _mapping(observation.get("admission_control"))
    discovery_admission = _mapping(observation.get("discovery_admission"))
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
        (
            "  "
            "invariants "
            + (
                f"ok={str(bool(invariants.get('ok'))).lower()} "
                f"violations={_format_int(invariants.get('violations_total'))} "
                f"duplicates={_format_int(invariants.get('duplicate_memberships'))} "
                f"terminal={_format_int(invariants.get('terminal_in_live_queue'))} "
                f"expired_leases={_format_int(invariants.get('expired_leases'))} "
                f"orphan_heads={_format_int(invariants.get('orphan_host_heads'))}"
                if invariants.get("checked")
                else "not_checked"
            )
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
        "Admission Control",
        (
            "  "
            f"mode={admission_control.get('mode') or 'unknown'} "
            f"target_pending={_format_int(admission_control.get('target_pending'))} "
            f"pending={_format_int(admission_control.get('pending'))} "
            f"min_score={_format_float(admission_control.get('min_score'))}"
        ),
        (
            "  "
            f"caps page={_format_int(admission_control.get('per_page_cap'))} "
            f"target_host={_format_int(admission_control.get('per_target_host_cap'))} "
            f"new_external_host={_format_int(admission_control.get('new_external_host_cap'))}"
        ),
        (
            "  "
            f"discovery extracted={_format_int(discovery_admission.get('extracted'))} "
            f"admitted={_format_int(discovery_admission.get('admitted'))} "
            f"rejected={_format_int(discovery_admission.get('rejected'))} "
            f"admit_ratio={_format_ratio(discovery_admission.get('admit_ratio'))}"
        ),
        f"  rejection_reasons={dict(_mapping(discovery_admission.get('rejection_reasons')))}",
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


def serialize_operator_observation(observation: Mapping[str, object]) -> str:
    """Serialize an observation for machine-readable CLI output."""
    return json.dumps(observation, indent=2, ensure_ascii=False)


def build_observation_record(
    observation: Mapping[str, object],
    *,
    observed_at: float | None = None,
) -> dict[str, object]:
    """Build one JSONL-safe observation record."""
    return {
        "ok": True,
        "observed_at": time.time() if observed_at is None else observed_at,
        "observation": dict(observation),
    }


def build_observation_error_record(
    error: BaseException,
    *,
    observed_at: float | None = None,
) -> dict[str, object]:
    """Build one JSONL-safe error record without leaking connection details."""
    return {
        "ok": False,
        "observed_at": time.time() if observed_at is None else observed_at,
        "error_type": type(error).__name__,
        "error": sanitize_observation_error(error),
    }


def sanitize_observation_error(error: BaseException) -> str:
    """Return a short operator-safe error message."""
    message = str(error).strip()
    if not message:
        return _FALLBACK_ERROR_MESSAGE
    message = _URL_WITH_CREDENTIALS_PATTERN.sub("<redacted-url>", message)
    message = re.sub(r"\b\S+:\S+@", "<redacted-credentials>@", message)
    message = " ".join(message.split())
    return message[:240] if message else _FALLBACK_ERROR_MESSAGE


@dataclass(frozen=True, slots=True)
class ObservationWatchConfig:
    """Runtime settings for periodic observation."""

    output: Path
    interval: float = 300.0
    limit: int | None = None
    max_bytes: int = 10_485_760
    max_files: int = 7
    max_failures: int = 5


class ObservationWatchFailed(RuntimeError):
    """Raised when periodic observation reaches its failure policy."""


class ObservationWatcher:
    """Run periodic observations and persist JSONL records."""

    def __init__(
        self,
        *,
        storage_factory: Callable[[], Any],
        config: ObservationWatchConfig,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
        log: logging.Logger = logger,
    ) -> None:
        self._storage_factory = storage_factory
        self._config = config
        self._sleep = sleep
        self._clock = clock
        self._log = log
        self._consecutive_failures = 0

    def run(self) -> int:
        count = 0
        while self._config.limit is None or count < self._config.limit:
            record = self._observe_once()
            append_observation_record(
                self._config.output,
                record,
                max_bytes=self._config.max_bytes,
                max_files=self._config.max_files,
            )
            count += 1
            self._log_record(record)

            if (
                not record["ok"]
                and self._config.max_failures
                and self._consecutive_failures >= self._config.max_failures
            ):
                self._log.error(
                    "exiting after %d consecutive observation failures",
                    self._config.max_failures,
                )
                raise ObservationWatchFailed(
                    f"{self._config.max_failures} consecutive observation failures"
                )

            if self._config.limit is not None and count >= self._config.limit:
                break
            self._sleep(self._config.interval)
        return count

    def _observe_once(self) -> dict[str, object]:
        observed_at = self._clock()
        try:
            with self._storage_factory() as storage:
                observation = read_operator_observation(storage)
            self._consecutive_failures = 0
            return build_observation_record(observation, observed_at=observed_at)
        except Exception as exc:  # noqa: BLE001
            self._consecutive_failures += 1
            return build_observation_error_record(exc, observed_at=observed_at)

    def _log_record(self, record: Mapping[str, object]) -> None:
        if record["ok"]:
            self._log_success(record)
            return
        self._log.warning(
            "failed observation attempt=%d error_type=%s error=%s output=%s",
            self._consecutive_failures,
            record.get("error_type"),
            record.get("error"),
            self._config.output,
        )

    def _log_success(self, record: Mapping[str, object]) -> None:
        observation = record.get("observation")
        if not isinstance(observation, Mapping):
            self._log.info("observed ok output=%s", self._config.output)
            return
        crawl = _mapping(observation.get("crawl"))
        scheduler = _mapping(observation.get("scheduler"))
        throughput = _mapping(observation.get("throughput"))
        self._log.info(
            "observed ok pages=%s pending=%s pps=%s output=%s",
            crawl.get("total_pages", "n/a"),
            scheduler.get("pending", "n/a"),
            throughput.get("pages_per_second", "n/a"),
            self._config.output,
        )


def append_observation_record(
    path: str | Path,
    record: Mapping[str, object],
    *,
    max_bytes: int = 0,
    max_files: int = 7,
) -> None:
    """Append one JSON object as a JSON Lines record.

    This writer assumes a single process writes to a given path.
    """
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if max_bytes > 0:
        _rotate_jsonl(output_path, max_bytes=max_bytes, max_files=max_files)
    with output_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(dict(record), ensure_ascii=False, sort_keys=True))
        file.write("\n")


def _rotate_jsonl(path: Path, *, max_bytes: int, max_files: int) -> None:
    if max_files <= 0 or not path.exists() or path.stat().st_size < max_bytes:
        return

    oldest = path.with_name(f"{path.name}.{max_files}")
    if oldest.exists():
        oldest.unlink()

    for index in range(max_files - 1, 0, -1):
        source = path.with_name(f"{path.name}.{index}")
        if source.exists():
            source.replace(path.with_name(f"{path.name}.{index + 1}"))

    path.replace(path.with_name(f"{path.name}.1"))


def _read_storage_shape(conn: Any) -> dict[str, object]:
    with conn.cursor() as cur:
        cur.execute(
            "SET LOCAL statement_timeout = %s",
            (OPERATOR_OBSERVATION_STATEMENT_TIMEOUT_MS,),
        )
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
                "SELECT GREATEST(COALESCE(reltuples, 0), 0)::bigint FROM pg_class WHERE oid = %s::regclass",
                (f"public.{URL_LEDGER_TABLE}",),
            )
            urls = cur.fetchone()[0]
            cur.execute(f"SELECT to_regclass('public.{HOST_LEDGER_TABLE}')")
            host_ledger_exists = cur.fetchone()[0] is not None
            hosts = 0
            if host_ledger_exists:
                cur.execute(
                    "SELECT GREATEST(COALESCE(reltuples, 0), 0)::bigint FROM pg_class WHERE oid = %s::regclass",
                    (f"public.{HOST_LEDGER_TABLE}",),
                )
                hosts = cur.fetchone()[0]
            url_ledger = {
                "urls": int(urls or 0),
                "hosts": int(hosts or 0),
                "estimated": True,
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
