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
from .domain_manager import DomainManager
from .domain_store import DomainStore
from .discovery import seed_hosts_from_urls
from .storage import PgStorage
from .url_ledger import RUNNABLE_SURFACE_FRONTLINE, UrlLedger

logger = logging.getLogger(__name__)

_MAX_RECONNECT_ATTEMPTS = 5
_RECONNECT_DELAY = 5.0


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
        deferred_runnable_per_domain: int | None = None,
        deferred_runnable_per_branch: int | None = None,
        deferred_surface_defer_seconds: float | None = None,
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
        self._deferred_runnable_per_domain = (
            settings.daemon_keep_runnable_per_domain
            if deferred_runnable_per_domain is None
            else deferred_runnable_per_domain
        )
        self._deferred_runnable_per_branch = (
            settings.daemon_keep_runnable_per_branch
            if deferred_runnable_per_branch is None
            else deferred_runnable_per_branch
        )
        self._deferred_surface_defer_seconds = (
            settings.daemon_deferred_surface_defer_seconds
            if deferred_surface_defer_seconds is None
            else deferred_surface_defer_seconds
        )
        self._min_runnable_sleep = (
            settings.daemon_min_runnable_sleep if min_runnable_sleep is None else min_runnable_sleep
        )
        self._min_frontline_runnable = max(
            1, min(len(self._seeds), settings.daemon_min_frontline_runnable)
        )
        self._min_frontline_hosts = max(
            1,
            min(
                len(self._seed_hosts) or len(self._seeds) or 1,
                settings.daemon_min_frontline_hosts,
            ),
        )
        self._blocked_retry_budget = max(0, settings.daemon_blocked_retry_budget)
        self._blocked_retry_per_domain = max(1, settings.daemon_blocked_retry_per_domain)
        self._blocked_retry_max_consecutive_failures = settings.daemon_blocked_retry_max_consecutive_failures
        self._quarantine_retire_min_consecutive_failures = (
            settings.daemon_quarantine_retire_min_consecutive_failures
        )
        self._quarantine_retire_after_seconds = max(0.0, settings.daemon_quarantine_retire_after_seconds)
        self._shutdown = False
        self._engine: CrawlerEngine | None = None
        self._last_runtime_snapshot: dict[str, object] = {}
        self._domain_store: DomainStore | None = None
        self._domain_manager = DomainManager(
            user_agent=settings.user_agent,
            default_delay=delay,
        )
        self._policy = DaemonSchedulerPolicy(
            cycle_pages=self._cycle_pages,
            min_frontline_runnable=self._min_frontline_runnable,
            min_frontline_hosts=self._min_frontline_hosts,
            blocked_retry_budget=self._blocked_retry_budget,
            blocked_retry_per_domain=self._blocked_retry_per_domain,
            blocked_retry_max_consecutive_failures=self._blocked_retry_max_consecutive_failures,
            quarantine_retire_min_consecutive_failures=self._quarantine_retire_min_consecutive_failures,
            quarantine_retire_after_seconds=self._quarantine_retire_after_seconds,
            deferred_runnable_per_domain=self._deferred_runnable_per_domain,
            deferred_runnable_per_branch=self._deferred_runnable_per_branch,
            deferred_surface_defer_seconds=self._deferred_surface_defer_seconds,
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
        url_ledger = None
        cycle = 0

        try:
            while not self._shutdown:
                # Ensure DB connection
                if storage is None:
                    storage, url_ledger = await self._connect()
                    if storage is None:
                        await self._interruptible_sleep(self._idle_sleep)
                        continue

                try:
                    bootstrapped = self._bootstrap_scheduler(url_ledger)
                    if bootstrapped:
                        logger.info("Bootstrapped scheduler with %d seed URLs", bootstrapped)
                    maintenance = self._policy.prepare_scheduler(
                        url_ledger,
                        refresh_stale=lambda: self._refresh_stale(storage, url_ledger),
                    )
                    if maintenance["admitted"]:
                        logger.info("Admitted %d discovered URLs into the deferred surface", maintenance["admitted"])
                    if maintenance["rebalanced_before"]:
                        logger.info(
                            "Rebalanced blocked-domain-backoff queue: quarantined=%d restored=0",
                            maintenance["rebalanced_before"],
                        )
                    if maintenance["deferred"]:
                        logger.info("Deferred %d low-priority deferred-surface URLs", maintenance["deferred"])
                    if maintenance["rebalanced_after"]:
                        logger.info(
                            "Rebalanced blocked-domain-backoff queue: quarantined=%d restored=0",
                            maintenance["rebalanced_after"],
                        )
                    if maintenance["restored"]:
                        logger.info("Restored %d recovered blocked-domain-backoff URLs", maintenance["restored"])
                    if maintenance["retired"]:
                        logger.info("Retired %d blocked-domain-backoff URLs", maintenance["retired"])
                    if maintenance["promoted"]:
                        logger.info("Promoted %d blocked-domain-backoff URLs for retry", maintenance["promoted"])

                    readiness = url_ledger.readiness()
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
                            ) | {
                                "next_runnable_delay": next_runnable_delay,
                                "readiness_blocked": dict(readiness.blocked),
                                "scheduler_state": dict(readiness.state_counts),
                            },
                        )
                        await self._interruptible_sleep(sleep_seconds)
                        continue

                    cycle += 1
                    logger.info("Cycle %d: %d runnable / %d pending URLs", cycle, runnable, pending)
                    start = time.time()
                    pages, error_breakdown = await self._run_cycle(storage, url_ledger)
                    elapsed = time.time() - start
                    rate = pages / elapsed if elapsed > 0 else 0
                    logger.info(
                        "Cycle %d complete: %d pages in %.1fs (%.1f pages/s) | errors=%s | %s",
                        cycle,
                        pages,
                        elapsed,
                        rate,
                        _format_error_breakdown(error_breakdown),
                        url_ledger.stats(),
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
                        }
                    )
                    self._persist_runtime_payload(storage, cycle_payload)

                    if not self._shutdown:
                        await self._interruptible_sleep(self._cycle_pause)

                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    logger.error("Database connection lost: %s", e)
                    storage = self._close_storage(storage)
                    url_ledger = None
                    await self._interruptible_sleep(_RECONNECT_DELAY)

        finally:
            self._close_storage(storage)
            await self._domain_manager.close()

        logger.info("Daemon shutdown complete")

    def _persist_runtime_payload(self, storage: object, payload: dict[str, object]) -> None:
        """Persist runtime snapshots when the storage backend supports it."""
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
            "publish_queue_size": 0,
        }
        for key in (
            "pages_crawled",
            "claimed_pages",
            "max_pages",
            "parse_queue_wait_last_ms",
            "finalize_queue_wait_last_ms",
            "publish_queue_wait_last_ms",
            "parse_queue_wait_max_ms",
            "finalize_queue_wait_max_ms",
            "publish_queue_wait_max_ms",
            "parse_queue_depth_max",
            "finalize_queue_depth_max",
            "publish_queue_depth_max",
            "failure_breakdown",
        ):
            if key in self._last_runtime_snapshot:
                payload[key] = self._last_runtime_snapshot[key]
        return payload

    def _report_runtime_stats(self, stop_event: threading.Event, engine: CrawlerEngine) -> None:
        """Persist crawler runtime stats for API consumers.

        Run outside the main event loop so long synchronous scheduler/DB work inside crawl
        workers does not stall runtime visibility.
        """
        storage = PgStorage(self._postgres_dsn)
        try:
            while not stop_event.is_set():
                if engine._running:
                    self._persist_runtime_payload(storage, engine.snapshot_runtime_stats())
                stop_event.wait(1.0)
        finally:
            with contextlib.suppress(Exception):
                self._persist_runtime_payload(storage, engine.snapshot_runtime_stats())
            storage.close()

    def _flush_runtime_stats(self, storage: PgStorage, engine: CrawlerEngine) -> None:
        """Store one last runtime snapshot on cycle boundaries."""
        self._persist_runtime_payload(storage, engine.snapshot_runtime_stats())

    async def _connect(self) -> tuple[PgStorage | None, UrlLedger | None]:
        """Connect to Postgres and initialize scheduler state."""
        for attempt in range(1, _MAX_RECONNECT_ATTEMPTS + 1):
            try:
                storage = PgStorage(self._postgres_dsn)
                url_ledger = UrlLedger(storage.conn)
                self._domain_store = DomainStore(storage.conn, default_delay=self._delay)
                url_ledger.attach_domain_store(self._domain_store)
                self._domain_manager.attach_store(self._domain_store)
                count = url_ledger.recover_leased(expired_only=False)
                if count:
                    logger.info("Recovered %d leased URLs", count)
                prime = self._policy.prime_scheduler(url_ledger)
                if prime["admitted"]:
                    logger.info("Admitted %d discovered URLs into the deferred surface", prime["admitted"])
                if prime["rebalanced"]:
                    logger.info(
                        "Rebalanced blocked-domain-backoff queue: quarantined=%d restored=0",
                        prime["rebalanced"],
                    )
                if prime["deferred"]:
                    logger.info("Deferred %d low-priority deferred-surface URLs", prime["deferred"])
                if prime["promoted"]:
                    logger.info("Promoted %d blocked-domain-backoff URLs for retry", prime["promoted"])
                logger.info("Database connected (attempt %d)", attempt)
                return storage, url_ledger
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
        self, storage: PgStorage, url_ledger: UrlLedger
    ) -> tuple[int, dict[str, int]]:
        """Run one crawl cycle."""
        runtime_storage = PgStorage(self._postgres_dsn)
        try:
            async with CrawlerEngine(
                max_pages=self._cycle_pages,
                same_domain=False,
                delay=self._delay,
                concurrency=self._concurrency,
                pg_storage=storage,
url_ledger=url_ledger,
                domain_manager=self._domain_manager,
                domain_store=self._domain_store,
                seed_urls=self._seeds,
            ) as engine:
                self._engine = engine
                self._flush_runtime_stats(runtime_storage, engine)
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
                    self._flush_runtime_stats(runtime_storage, engine)
                    self._engine = None
                return engine.pages_crawled, engine.failure_breakdown
        finally:
            runtime_storage.close()

    def _ensure_frontline_supply(self, url_ledger: UrlLedger):
        """Keep the frontline runnable surface supplied from existing scheduler state."""
        before_pending = url_ledger.pending_count()
        before_runnable = url_ledger.runnable_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        before_frontline_pending = url_ledger.pending_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        self._policy.ensure_frontline_supply(url_ledger)
        after_pending = url_ledger.pending_count()
        after_runnable = url_ledger.runnable_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        after_frontline_pending = url_ledger.pending_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        if after_pending == before_pending and after_runnable == before_runnable:
            return
        logger.info(
            "Ensured frontline supply: pending_total=%d->%d runnable_frontline=%d->%d pending_frontline=%d->%d target_runnable=%d",
            before_pending,
            after_pending,
            before_runnable,
            after_runnable,
            before_frontline_pending,
            after_frontline_pending,
            self._min_frontline_runnable,
        )

    def _bootstrap_scheduler(self, url_ledger: UrlLedger) -> int:
        """Seed an empty scheduler through a dedicated bootstrap path."""
        if url_ledger.pending_count() != 0:
            return 0
        return url_ledger.upsert_seeds(self._seeds, priority=2.0)

    def _promote_blocked_retry(self, url_ledger: UrlLedger) -> int:
        """Restore a small cooled-down subset from blocked retry queue when runnable work is thin."""
        return self._policy.promote_blocked_retry(url_ledger)

    def _retire_blocked_retry(self, url_ledger: UrlLedger) -> int:
        """Retire long-stuck blocked retry URLs out of pending scheduler state."""
        return self._policy.retire_blocked_retry(url_ledger)

    def _restore_recovered_blocked_retry(self, url_ledger: UrlLedger) -> int:
        """Restore healthy blocked retry domains before using bounded retry promotion."""
        return self._policy.restore_recovered_blocked_retry(url_ledger)

    def _refresh_stale(self, storage: PgStorage, url_ledger: UrlLedger):
        """Re-queue stale pages for refresh intent."""
        pending = url_ledger.pending_count()
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

        count = url_ledger.requeue_refresh_urls(
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
