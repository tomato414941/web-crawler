"""Continuous web crawling daemon."""

import asyncio
import contextlib
import logging
import signal
import threading
import time

import psycopg2

from .config import settings
from .crawl import CrawlerEngine
from .daemon_policy import DaemonSchedulerPolicy
from .host_manager import HostManager
from .host_store import HostStore
from .discovery import seed_hosts_from_urls
from .storage import PgStorage
from .scheduler_membership import SCHEDULER_SURFACE_RUNNABLE
from .scheduler import Scheduler

logger = logging.getLogger(__name__)

_MAX_RECONNECT_ATTEMPTS = 5
_RECONNECT_DELAY = 5.0
_PIPELINE_LIVENESS_KEYS = (
    "parser_liveness",
    "finalizer_liveness",
)


def _format_error_breakdown(error_breakdown: dict[str, int]) -> str:
    """Render a stable one-line error summary for cycle logs."""
    if not error_breakdown:
        return "none"
    ordered = (
        "http_4xx",
        "http_5xx",
        "timeout",
        "connection_error",
        "http_other",
        "other",
    )
    parts = [f"{name}={error_breakdown[name]}" for name in ordered if error_breakdown.get(name)]
    return ", ".join(parts) if parts else "none"


class CrawlDaemon:
    """Runs CrawlerEngine in cycles, refreshing stale pages and re-seeding."""

    def __init__(
        self,
        seeds: list[str],
        postgres_dsn: str,
        cycle_pages: int = 500,
        refresh_ttl: int = 86400,
        concurrency: int = 5,
        delay: float = 1.0,
        cycle_pause: float = 5.0,
        idle_sleep: float = 60.0,
        scheduled_runnable_per_host: int | None = None,
        scheduled_runnable_per_branch: int | None = None,
        scheduled_surface_delay_seconds: float | None = None,
        min_runnable_sleep: float | None = None,
    ):
        self._seeds = seeds
        self._seed_hosts = sorted(seed_hosts_from_urls(seeds))
        self._postgres_dsn = postgres_dsn
        self._cycle_pages = cycle_pages
        self._refresh_ttl = refresh_ttl
        self._concurrency = concurrency
        self._delay = delay
        self._cycle_pause = cycle_pause
        self._idle_sleep = idle_sleep
        self._scheduled_runnable_per_host = (
            settings.daemon_keep_runnable_per_host
            if scheduled_runnable_per_host is None
            else scheduled_runnable_per_host
        )
        self._scheduled_runnable_per_branch = (
            settings.daemon_keep_runnable_per_branch
            if scheduled_runnable_per_branch is None
            else scheduled_runnable_per_branch
        )
        self._scheduled_surface_delay_seconds = (
            settings.daemon_scheduled_surface_delay_seconds
            if scheduled_surface_delay_seconds is None
            else scheduled_surface_delay_seconds
        )
        self._scheduled_surface_delay_limit = max(
            0,
            settings.daemon_scheduled_surface_delay_limit,
        )
        self._min_runnable_sleep = (
            settings.daemon_min_runnable_sleep if min_runnable_sleep is None else min_runnable_sleep
        )
        self._min_runnable_supply_count = max(
            1, min(len(self._seeds), settings.daemon_min_runnable_supply_count)
        )
        self._min_runnable_supply_hosts = max(
            1,
            min(
                len(self._seed_hosts) or len(self._seeds) or 1,
                settings.daemon_min_runnable_supply_hosts,
            ),
        )
        self._blocked_retry_budget = max(0, settings.daemon_blocked_retry_budget)
        self._blocked_retry_per_host = max(1, settings.daemon_blocked_retry_per_host)
        self._blocked_retry_max_consecutive_failures = (
            settings.daemon_blocked_retry_max_consecutive_failures
        )
        self._quarantine_retire_min_consecutive_failures = (
            settings.daemon_quarantine_retire_min_consecutive_failures
        )
        self._quarantine_retire_after_seconds = max(
            0.0, settings.daemon_quarantine_retire_after_seconds
        )
        self._host_head_repair_limit = max(0, settings.daemon_host_head_repair_limit)
        self._host_head_dirty_refresh_limit = max(
            0,
            settings.daemon_host_head_dirty_refresh_limit,
        )
        self._shutdown = False
        self._engine: CrawlerEngine | None = None
        self._last_runtime_snapshot: dict[str, object] = {}
        self._last_host_head_repair: dict[str, int] = {
            "checked_heads": 0,
            "orphan_heads": 0,
            "stale_heads": 0,
            "missing_heads": 0,
            "repaired_hosts": 0,
        }
        self._last_host_head_dirty_refresh: dict[str, int | float] = {
            "selected_hosts": 0,
            "refreshed_hosts": 0,
            "remaining_hosts": 0,
            "elapsed_ms": 0,
        }
        self._host_store: HostStore | None = None
        self._host_manager = HostManager(
            user_agent=settings.user_agent,
            default_delay=delay,
        )
        self._policy = DaemonSchedulerPolicy(
            cycle_pages=self._cycle_pages,
            min_runnable_supply_count=self._min_runnable_supply_count,
            min_runnable_supply_hosts=self._min_runnable_supply_hosts,
            blocked_retry_budget=self._blocked_retry_budget,
            blocked_retry_per_host=self._blocked_retry_per_host,
            blocked_retry_max_consecutive_failures=self._blocked_retry_max_consecutive_failures,
            quarantine_retire_min_consecutive_failures=self._quarantine_retire_min_consecutive_failures,
            quarantine_retire_after_seconds=self._quarantine_retire_after_seconds,
            scheduled_runnable_per_host=self._scheduled_runnable_per_host,
            scheduled_runnable_per_branch=self._scheduled_runnable_per_branch,
            scheduled_surface_delay_limit=self._scheduled_surface_delay_limit,
            scheduled_surface_delay_seconds=self._scheduled_surface_delay_seconds,
        )

    async def run(self):
        """Main daemon loop."""
        self._install_signals()
        logger.info(
            "Daemon starting: seeds=%s, cycle_pages=%d, refresh_ttl=%ds",
            self._seeds,
            self._cycle_pages,
            self._refresh_ttl,
        )

        storage = None
        scheduler = None
        cycle = 0

        try:
            while not self._shutdown:
                # Ensure DB connection
                if storage is None:
                    storage, scheduler = await self._connect()
                    if storage is None:
                        await self._interruptible_sleep(self._idle_sleep)
                        continue

                try:
                    bootstrapped = self._bootstrap_scheduler(scheduler)
                    if bootstrapped:
                        logger.info("Bootstrapped scheduler with %d seed URLs", bootstrapped)
                    maintenance = self._policy.prepare_scheduler(
                        scheduler,
                        refresh_stale=lambda: self._refresh_stale(storage, scheduler),
                    )
                    if maintenance["admitted"]:
                        logger.info(
                            "Admitted %d discovered URLs into the scheduled surface",
                            maintenance["admitted"],
                        )
                    if maintenance["rebalanced_before"]:
                        logger.info(
                            "Rebalanced blocked-host-backoff queue: quarantined=%d restored=0",
                            maintenance["rebalanced_before"],
                        )
                    if maintenance["scheduled"]:
                        logger.info(
                            "Delayed %d low-score scheduled-surface URLs",
                            maintenance["scheduled"],
                        )
                    if maintenance["rebalanced_after"]:
                        logger.info(
                            "Rebalanced blocked-host-backoff queue: quarantined=%d restored=0",
                            maintenance["rebalanced_after"],
                        )
                    if maintenance["restored"]:
                        logger.info(
                            "Restored %d recovered blocked-host-backoff URLs",
                            maintenance["restored"],
                        )
                    if maintenance["retired"]:
                        logger.info("Retired %d blocked-host-backoff URLs", maintenance["retired"])
                    if maintenance["promoted"]:
                        logger.info(
                            "Promoted %d blocked-host-backoff URLs for retry",
                            maintenance["promoted"],
                        )
                    self._refresh_dirty_host_runnable_heads(scheduler)
                    self._repair_host_runnable_heads(scheduler)

                    readiness = scheduler.daemon_readiness()
                    pending = readiness.pending
                    if pending == 0:
                        logger.info("No URLs to crawl, sleeping %ds", self._idle_sleep)
                        self._persist_runtime_payload(
                            storage,
                            self._idle_runtime_payload(
                                state="idle_no_pending",
                                pending=pending,
                                runnable=0,
                                cycle=cycle,
                            ),
                        )
                        await self._interruptible_sleep(self._idle_sleep)
                        continue

                    runnable = readiness.runnable
                    if runnable == 0:
                        next_runnable_delay = readiness.next_runnable_delay
                        sleep_seconds = self._idle_sleep
                        if next_runnable_delay is not None:
                            sleep_seconds = min(
                                self._idle_sleep,
                                max(self._min_runnable_sleep, next_runnable_delay),
                            )
                        logger.info(
                            "No runnable URLs (pending=%d), sleeping %.1fs",
                            pending,
                            sleep_seconds,
                        )
                        self._persist_runtime_payload(
                            storage,
                            self._idle_runtime_payload(
                                state="idle_waiting_runnable",
                                pending=pending,
                                runnable=runnable,
                                cycle=cycle,
                            )
                            | {
                                "next_runnable_delay": next_runnable_delay,
                            }
                            | self._scheduler_runtime_views(
                                scheduler,
                                readiness=readiness,
                            ),
                        )
                        await self._interruptible_sleep(sleep_seconds)
                        continue

                    cycle += 1
                    logger.info("Cycle %d: %d runnable / %d pending URLs", cycle, runnable, pending)
                    start = time.time()
                    cycle_result = await self._run_cycle(storage, scheduler)
                    host_first_fallback = {
                        "attempts": 0,
                        "hits": 0,
                        "misses": 0,
                        "read_model_hits": 0,
                        "read_model_stale": 0,
                        "read_model_misses": 0,
                        "read_model_errors": 0,
                    }
                    if len(cycle_result) == 2:
                        pages, error_breakdown = cycle_result
                        timing_summary = {}
                        timing_summary_log = "timings=unavailable"
                    elif len(cycle_result) == 4:
                        pages, error_breakdown, timing_summary, timing_summary_log = cycle_result
                    else:
                        (
                            pages,
                            error_breakdown,
                            timing_summary,
                            timing_summary_log,
                            host_first_fallback,
                        ) = cycle_result
                    elapsed = time.time() - start
                    rate = pages / elapsed if elapsed > 0 else 0
                    last_completed_cycle = {
                        "running": False,
                        "state": "cycle_complete",
                        "cycle": cycle,
                        "pages": pages,
                        "elapsed_seconds": round(elapsed, 3),
                        "pages_per_second": round(rate, 3),
                        "errors": error_breakdown,
                        "timing_summary": timing_summary,
                        "host_first_fallback": host_first_fallback,
                    }
                    logger.info(
                        "Cycle %d complete: %d pages in %.1fs (%.1f pages/s) | errors=%s | pending=%d runnable=%d | timings=%s",
                        cycle,
                        pages,
                        elapsed,
                        rate,
                        _format_error_breakdown(error_breakdown),
                        pending,
                        runnable,
                        timing_summary_log,
                    )
                    cycle_payload = self._idle_runtime_payload(
                        state="cycle_complete",
                        pending=pending,
                        runnable=runnable,
                        cycle=cycle,
                    )
                    cycle_payload.update(
                        {
                            "pages": pages,
                            "elapsed_seconds": round(elapsed, 3),
                            "pages_per_second": round(rate, 3),
                            "errors": error_breakdown,
                            "timing_summary": timing_summary,
                            "host_first_fallback": host_first_fallback,
                            "active_cycle": {
                                "running": False,
                                "state": "cycle_complete",
                                "cycle": cycle,
                                "pages_crawled": pages,
                                "claimed_pages": 0,
                                "active_hosts": 0,
                                "parse_queue_size": 0,
                                "finalize_queue_size": 0,
                                **self._last_pipeline_liveness(),
                            },
                            "last_completed_cycle": last_completed_cycle,
                        }
                    )
                    cycle_payload.update(
                        self._scheduler_runtime_views(
                            scheduler,
                            readiness=readiness,
                        )
                    )
                    self._persist_runtime_payload(storage, cycle_payload)

                    if not self._shutdown:
                        await self._interruptible_sleep(self._cycle_pause)

                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    logger.error("Database connection lost: %s", e)
                    storage = self._close_storage(storage)
                    scheduler = None
                    await self._interruptible_sleep(_RECONNECT_DELAY)

        finally:
            self._close_storage(storage)
            await self._host_manager.close()

        logger.info("Daemon shutdown complete")

    def _persist_runtime_payload(self, storage: object, payload: dict[str, object]) -> None:
        """Persist runtime snapshots when the storage backend supports it."""
        payload = dict(payload)
        payload.setdefault("host_head_repair", dict(self._last_host_head_repair))
        payload.setdefault(
            "host_head_dirty_refresh",
            dict(self._last_host_head_dirty_refresh),
        )
        active_cycle = payload.get("active_cycle")
        if isinstance(active_cycle, dict):
            active_cycle.setdefault("host_head_repair", dict(self._last_host_head_repair))
            active_cycle.setdefault(
                "host_head_dirty_refresh",
                dict(self._last_host_head_dirty_refresh),
            )
        self._last_runtime_snapshot.update(payload)
        if hasattr(storage, "upsert_runtime_stats"):
            storage.upsert_runtime_stats("crawler", dict(self._last_runtime_snapshot))

    def _idle_runtime_payload(
        self,
        *,
        state: str,
        pending: int,
        runnable: int,
        cycle: int,
    ) -> dict[str, object]:
        """Build daemon-level runtime stats outside active crawl cycles."""
        payload = {
            "running": False,
            "state": state,
            "cycle": cycle,
            "pending": pending,
            "runnable": runnable,
            "concurrency": self._concurrency,
            "cycle_pages": self._cycle_pages,
            "parse_queue_size": 0,
            "finalize_queue_size": 0,
            "host_head_repair": dict(self._last_host_head_repair),
            "host_head_dirty_refresh": dict(self._last_host_head_dirty_refresh),
        }
        for key in (
            "pages_crawled",
            "claimed_pages",
            "max_pages",
            "parse_queue_wait_last_ms",
            "finalize_queue_wait_last_ms",
            "parse_queue_wait_max_ms",
            "finalize_queue_wait_max_ms",
            "parse_queue_depth_max",
            "finalize_queue_depth_max",
            "failure_breakdown",
            "timing_summary",
            "host_first_fallback",
            "last_completed_cycle",
            *_PIPELINE_LIVENESS_KEYS,
        ):
            if key in self._last_runtime_snapshot:
                payload[key] = self._last_runtime_snapshot[key]
        payload["active_cycle"] = {
            "running": False,
            "state": state,
            "cycle": cycle,
            "pending": pending,
            "runnable": runnable,
            "parse_queue_size": 0,
            "finalize_queue_size": 0,
            "host_head_dirty_refresh": dict(self._last_host_head_dirty_refresh),
            **self._last_pipeline_liveness(),
        }
        return payload

    def _last_pipeline_liveness(self) -> dict[str, object]:
        """Return the latest stage liveness payloads for idle/cycle-complete snapshots."""
        return {
            key: self._last_runtime_snapshot[key]
            for key in _PIPELINE_LIVENESS_KEYS
            if key in self._last_runtime_snapshot
        }

    def _scheduler_runtime_views(
        self,
        _url_ledger: Scheduler,
        *,
        readiness,
        _now: float | None = None,
    ) -> dict[str, object]:
        """Build runtime-facing scheduler views without live full-queue diagnostics."""
        blocked_reason_counts = dict(getattr(readiness, "blocked", {}) or {})
        readiness_state_counts = dict(getattr(readiness, "state_counts", {}) or {})
        effective_scheduler_states = {
            "scheduled": int(readiness_state_counts.get("scheduled", 0) or 0),
            "runnable": int(readiness_state_counts.get("runnable", 0) or 0),
            "blocked": sum(int(value or 0) for value in blocked_reason_counts.values()),
        }

        return {
            "scheduler_state_snapshot": {
                "readiness_state_counts": dict(readiness_state_counts),
                "effective_state_counts": dict(effective_scheduler_states),
                "blocked_reason_counts": dict(blocked_reason_counts),
            },
            "blocked_reason_counts": blocked_reason_counts,
            "readiness_blocked": blocked_reason_counts,
            "readiness_blocked_reasons": blocked_reason_counts,
            "readiness_state_counts": readiness_state_counts,
            "scheduler_state": readiness_state_counts,
            "scheduler_readiness_states": readiness_state_counts,
            "effective_scheduler_states": effective_scheduler_states,
        }

    def _add_scheduler_runtime_views(
        self,
        payload: dict[str, object],
        scheduler: Scheduler,
    ) -> dict[str, object]:
        """Attach scheduler readiness views to a runtime payload when available."""
        try:
            readiness = scheduler.daemon_readiness()
            payload.update(self._scheduler_runtime_views(scheduler, readiness=readiness))
        except Exception:
            logger.debug("Failed to attach scheduler runtime views", exc_info=True)
        return payload

    def _repair_host_runnable_heads(self, scheduler: Scheduler) -> None:
        """Run bounded host-head repair before daemon cycle gating."""
        if self._host_head_repair_limit <= 0:
            return
        try:
            summary = scheduler.repair_host_runnable_heads(limit=self._host_head_repair_limit)
        except Exception:
            logger.debug("Host runnable-head repair failed", exc_info=True)
            return
        self._last_host_head_repair = summary.as_dict()

    def _refresh_dirty_host_runnable_heads(self, scheduler: Scheduler) -> None:
        """Refresh bounded dirty host-head rows before daemon cycle gating."""
        if self._host_head_dirty_refresh_limit <= 0:
            return
        try:
            summary = scheduler.refresh_dirty_host_runnable_heads(
                limit=self._host_head_dirty_refresh_limit
            )
        except Exception:
            logger.debug("Dirty host runnable-head refresh failed", exc_info=True)
            return
        self._last_host_head_dirty_refresh = summary.as_dict()

    def _report_runtime_stats(self, stop_event: threading.Event, engine: CrawlerEngine) -> None:
        """Persist crawler runtime stats for API consumers.

        Run outside the main event loop so long synchronous scheduler/DB work inside crawl
        workers does not stall runtime visibility.
        """
        storage = PgStorage(self._postgres_dsn)
        scheduler = Scheduler(storage.conn) if hasattr(storage, "conn") else None
        try:
            while not stop_event.is_set():
                payload = engine.snapshot_runtime_stats()
                if payload["running"]:
                    if scheduler is not None:
                        payload = self._add_scheduler_runtime_views(payload, scheduler)
                    self._persist_runtime_payload(storage, payload)
                stop_event.wait(1.0)
        finally:
            with contextlib.suppress(Exception):
                payload = engine.snapshot_runtime_stats()
                if scheduler is not None:
                    payload = self._add_scheduler_runtime_views(payload, scheduler)
                self._persist_runtime_payload(storage, payload)
            storage.close()

    def _flush_runtime_stats(
        self,
        storage: PgStorage,
        engine: CrawlerEngine,
        scheduler: Scheduler | None = None,
    ) -> None:
        """Store one last runtime snapshot on cycle boundaries."""
        payload = engine.snapshot_runtime_stats()
        if scheduler is not None:
            payload = self._add_scheduler_runtime_views(payload, scheduler)
        self._persist_runtime_payload(storage, payload)

    async def _connect(self) -> tuple[PgStorage | None, Scheduler | None]:
        """Connect to Postgres and initialize scheduler state."""
        for attempt in range(1, _MAX_RECONNECT_ATTEMPTS + 1):
            try:
                storage = PgStorage(self._postgres_dsn)
                scheduler = Scheduler(storage.conn)
                self._host_store = HostStore(storage.conn, default_delay=self._delay)
                scheduler.attach_host_store(self._host_store)
                self._host_manager.attach_store(self._host_store)
                self._host_manager.attach_host_ledger_store(scheduler.host_ledger_store)
                count = scheduler.recover_leased(expired_only=False)
                if count:
                    logger.info("Recovered %d leased URLs", count)
                prime = self._policy.prime_scheduler(scheduler)
                if prime["admitted"]:
                    logger.info(
                        "Admitted %d discovered URLs into the scheduled surface", prime["admitted"]
                    )
                if prime["rebalanced"]:
                    logger.info(
                        "Rebalanced blocked-host-backoff queue: quarantined=%d restored=0",
                        prime["rebalanced"],
                    )
                if prime["scheduled"]:
                    logger.info("Delayed %d low-score scheduled-surface URLs", prime["scheduled"])
                if prime["promoted"]:
                    logger.info(
                        "Promoted %d blocked-host-backoff URLs for retry", prime["promoted"]
                    )
                logger.info("Database connected (attempt %d)", attempt)
                return storage, scheduler
            except psycopg2.OperationalError as e:
                logger.error(
                    "Connection attempt %d/%d failed: %s", attempt, _MAX_RECONNECT_ATTEMPTS, e
                )
                if attempt < _MAX_RECONNECT_ATTEMPTS:
                    await self._interruptible_sleep(_RECONNECT_DELAY)
        logger.error("All %d connection attempts failed", _MAX_RECONNECT_ATTEMPTS)
        return None, None

    def _close_storage(self, storage: PgStorage | None) -> None:
        if storage:
            try:
                storage.close()
            except Exception:
                pass
        return None

    async def _run_cycle(
        self, storage: PgStorage, scheduler: Scheduler
    ) -> tuple[int, dict[str, int], dict[str, object], str, dict[str, int]]:
        """Run one crawl cycle."""
        runtime_storage = PgStorage(self._postgres_dsn)
        try:
            async with CrawlerEngine(
                max_pages=self._cycle_pages,
                same_host=False,
                delay=self._delay,
                concurrency=self._concurrency,
                pg_storage=storage,
                scheduler=scheduler,
                host_manager=self._host_manager,
                host_store=self._host_store,
                seed_urls=self._seeds,
            ) as engine:
                self._engine = engine
                self._flush_runtime_stats(runtime_storage, engine, scheduler)
                reporter_stop = threading.Event()
                reporter = threading.Thread(
                    target=self._report_runtime_stats,
                    args=(reporter_stop, engine),
                    daemon=True,
                )
                reporter.start()
                try:
                    await engine.crawl()
                finally:
                    reporter_stop.set()
                    reporter.join(timeout=2.0)
                    self._flush_runtime_stats(runtime_storage, engine, scheduler)
                    self._engine = None
                return (
                    engine.pages_crawled,
                    engine.failure_breakdown,
                    engine.timing_summary(),
                    engine.timing_summary_log(),
                    engine._host_first_fallback_stats(),
                )
        finally:
            runtime_storage.close()

    def _ensure_runnable_supply(self, scheduler: Scheduler):
        """Keep the runnable scheduler surface supplied from existing scheduler state."""
        before_pending = scheduler.pending_count()
        before_runnable = scheduler.runnable_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        before_runnable_pending = scheduler.pending_count(
            runnable_surface=SCHEDULER_SURFACE_RUNNABLE
        )
        self._policy.ensure_runnable_supply(scheduler)
        after_pending = scheduler.pending_count()
        after_runnable = scheduler.runnable_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        after_runnable_pending = scheduler.pending_count(
            runnable_surface=SCHEDULER_SURFACE_RUNNABLE
        )
        if after_pending == before_pending and after_runnable == before_runnable:
            return
        logger.info(
            "Ensured runnable supply: pending_total=%d->%d runnable=%d->%d pending_runnable=%d->%d target_runnable=%d",
            before_pending,
            after_pending,
            before_runnable,
            after_runnable,
            before_runnable_pending,
            after_runnable_pending,
            self._min_runnable_supply_count,
        )

    def _bootstrap_scheduler(self, scheduler: Scheduler) -> int:
        """Seed an empty scheduler through a dedicated bootstrap path."""
        if scheduler.pending_count() != 0:
            return 0
        return scheduler.upsert_seeds(self._seeds, discovery_value=2.0)

    def _promote_blocked_retry(self, scheduler: Scheduler) -> int:
        """Restore a small cooled-down subset from blocked retry queue when runnable work is thin."""
        return self._policy.promote_blocked_retry(scheduler)

    def _retire_blocked_retry(self, scheduler: Scheduler) -> int:
        """Retire long-stuck blocked retry URLs out of pending scheduler state."""
        return self._policy.retire_blocked_retry(scheduler)

    def _restore_recovered_blocked_retry(self, scheduler: Scheduler) -> int:
        """Restore healthy blocked retry hosts before using bounded retry promotion."""
        return self._policy.restore_recovered_blocked_retry(scheduler)

    def _refresh_stale(self, storage: PgStorage, scheduler: Scheduler):
        """Re-queue stale pages for refresh intent."""
        pending = scheduler.pending_count()
        if pending >= self._cycle_pages:
            return

        batch_size = self._cycle_pages - pending
        if batch_size <= 0:
            return

        cutoff = time.time() - self._refresh_ttl
        now = time.time()
        with storage.conn.cursor() as cur:
            cur.execute(
                """SELECT url_ledger.url
                   FROM url_ledger
                   JOIN pages ON url_ledger.url = pages.url
                   WHERE pages.crawled_at < %s
                     AND url_ledger.last_success_at IS NOT NULL
                   ORDER BY pages.crawled_at ASC
                   LIMIT %s""",
                (cutoff, batch_size),
            )
            candidate_urls = [url for (url,) in cur.fetchall()]

        count = scheduler.requeue_refresh_urls(
            candidate_urls,
            next_fetch_at=now,
        )
        if count:
            logger.info(
                "Re-queued %d stale pages for refresh (TTL=%ds, pending=%d, target=%d)",
                count,
                self._refresh_ttl,
                pending,
                self._cycle_pages,
            )

    def _install_signals(self):
        """Register signal handlers for graceful shutdown."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_signal, sig)

    def _handle_signal(self, sig):
        logger.info("Received %s, shutting down after current cycle...", sig.name)
        self._shutdown = True
        if self._engine:
            self._engine.stop()

    async def _interruptible_sleep(self, seconds: float):
        """Sleep that exits early on shutdown."""
        end = time.time() + seconds
        while not self._shutdown and time.time() < end:
            await asyncio.sleep(min(1.0, end - time.time()))
