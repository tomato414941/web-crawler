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
from .domain_manager import DomainManager
from .domain_store import DomainStore
from .frontier import Frontier
from .storage import PgStorage

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
    """Runs CrawlerEngine in cycles, re-crawling stale pages and re-seeding."""

    def __init__(
        self,
        seeds: list[str],
        postgres_dsn: str,
        cycle_pages: int = 500,
        recrawl_ttl: int = 86400,
        max_depth: int = 3,
        concurrency: int = 5,
        delay: float = 1.0,
        cycle_pause: float = 5.0,
        idle_sleep: float = 60.0,
        backlog_ready_per_domain: int | None = None,
        backlog_low_priority: float | None = None,
        backlog_defer_seconds: float | None = None,
        min_ready_sleep: float | None = None,
    ):
        self._seeds = seeds
        self._postgres_dsn = postgres_dsn
        self._cycle_pages = cycle_pages
        self._recrawl_ttl = recrawl_ttl
        self._max_depth = max_depth
        self._concurrency = concurrency
        self._delay = delay
        self._cycle_pause = cycle_pause
        self._idle_sleep = idle_sleep
        self._backlog_ready_per_domain = (
            settings.daemon_keep_ready_per_domain
            if backlog_ready_per_domain is None
            else backlog_ready_per_domain
        )
        self._backlog_low_priority = (
            settings.daemon_backlog_low_priority
            if backlog_low_priority is None
            else backlog_low_priority
        )
        self._backlog_defer_seconds = (
            settings.daemon_backlog_defer_seconds
            if backlog_defer_seconds is None
            else backlog_defer_seconds
        )
        self._min_ready_sleep = (
            settings.daemon_min_ready_sleep if min_ready_sleep is None else min_ready_sleep
        )
        self._shutdown = False
        self._engine: CrawlerEngine | None = None
        self._last_runtime_snapshot: dict[str, object] = {}
        self._domain_store: DomainStore | None = None
        self._domain_manager = DomainManager(
            user_agent=settings.user_agent,
            default_delay=delay,
        )

    async def run(self):
        """Main daemon loop."""
        self._install_signals()
        logger.info(
            "Daemon starting: seeds=%s, cycle_pages=%d, recrawl_ttl=%ds",
            self._seeds,
            self._cycle_pages,
            self._recrawl_ttl,
        )

        storage = None
        frontier = None
        cycle = 0

        try:
            while not self._shutdown:
                # Ensure DB connection
                if storage is None:
                    storage, frontier = await self._connect()
                    if storage is None:
                        await self._interruptible_sleep(self._idle_sleep)
                        continue

                try:
                    self._ensure_seeds(frontier)
                    self._recrawl_stale(storage, frontier)
                    deferred = frontier.defer_overcrowded_backlog(
                        keep_ready_per_domain=self._backlog_ready_per_domain,
                        low_priority_threshold=self._backlog_low_priority,
                        defer_seconds=self._backlog_defer_seconds,
                    )
                    if deferred:
                        logger.info("Deferred %d low-priority backlog URLs", deferred)

                    readiness = frontier.readiness()
                    pending = readiness.pending
                    if pending == 0:
                        logger.info("No URLs to crawl, sleeping %ds", self._idle_sleep)
                        self._persist_runtime_payload(
                            storage,
                            self._idle_runtime_payload(
                                state="idle_no_pending",
                                pending=pending,
                                ready=0,
                                cycle=cycle,
                            ),
                        )
                        await self._interruptible_sleep(self._idle_sleep)
                        continue

                    ready = readiness.ready
                    if ready == 0:
                        next_ready_delay = readiness.next_ready_delay
                        sleep_seconds = self._idle_sleep
                        if next_ready_delay is not None:
                            sleep_seconds = min(
                                self._idle_sleep,
                                max(self._min_ready_sleep, next_ready_delay),
                            )
                        logger.info(
                            "No ready URLs (pending=%d), sleeping %.1fs",
                            pending,
                            sleep_seconds,
                        )
                        self._persist_runtime_payload(
                            storage,
                            self._idle_runtime_payload(
                                state="idle_waiting_ready",
                                pending=pending,
                                ready=ready,
                                cycle=cycle,
                            ),
                        )
                        await self._interruptible_sleep(sleep_seconds)
                        continue

                    cycle += 1
                    logger.info("Cycle %d: %d ready / %d pending URLs", cycle, ready, pending)
                    start = time.time()
                    pages, error_breakdown = await self._run_cycle(storage, frontier)
                    elapsed = time.time() - start
                    rate = pages / elapsed if elapsed > 0 else 0
                    logger.info(
                        "Cycle %d complete: %d pages in %.1fs (%.1f pages/s) | errors=%s | %s",
                        cycle,
                        pages,
                        elapsed,
                        rate,
                        _format_error_breakdown(error_breakdown),
                        frontier.stats(),
                    )
                    cycle_payload = self._idle_runtime_payload(
                        state="cycle_complete",
                        pending=pending,
                        ready=ready,
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
                    frontier = None
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
        ready: int,
        cycle: int,
    ) -> dict[str, object]:
        """Build daemon-level runtime stats outside active crawl cycles."""
        payload = {
            "running": False,
            "state": state,
            "cycle": cycle,
            "pending": pending,
            "ready": ready,
            "concurrency": self._concurrency,
            "cycle_pages": self._cycle_pages,
            "parse_queue_size": 0,
            "publish_queue_size": 0,
        }
        for key in (
            "pages_crawled",
            "claimed_pages",
            "max_pages",
            "parse_queue_wait_last_ms",
            "publish_queue_wait_last_ms",
            "parse_queue_wait_max_ms",
            "publish_queue_wait_max_ms",
            "parse_queue_depth_max",
            "publish_queue_depth_max",
            "failure_breakdown",
        ):
            if key in self._last_runtime_snapshot:
                payload[key] = self._last_runtime_snapshot[key]
        return payload

    def _report_runtime_stats(self, stop_event: threading.Event, engine: CrawlerEngine) -> None:
        """Persist crawler runtime stats for API consumers.

        Run outside the main event loop so long synchronous frontier/DB work inside crawl
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

    async def _connect(self) -> tuple[PgStorage | None, Frontier | None]:
        """Connect to Postgres and initialize frontier."""
        for attempt in range(1, _MAX_RECONNECT_ATTEMPTS + 1):
            try:
                storage = PgStorage(self._postgres_dsn)
                frontier = Frontier(storage.conn)
                self._domain_store = DomainStore(storage.conn, default_delay=self._delay)
                frontier.attach_domain_store(self._domain_store)
                self._domain_manager.attach_store(self._domain_store)
                count = frontier.recover_leased(expired_only=False)
                if count:
                    logger.info("Recovered %d leased URLs", count)
                deferred = frontier.defer_overcrowded_backlog(
                    keep_ready_per_domain=self._backlog_ready_per_domain,
                    low_priority_threshold=self._backlog_low_priority,
                    defer_seconds=self._backlog_defer_seconds,
                )
                if deferred:
                    logger.info("Deferred %d low-priority backlog URLs", deferred)
                logger.info("Database connected (attempt %d)", attempt)
                return storage, frontier
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
        self, storage: PgStorage, frontier: Frontier
    ) -> tuple[int, dict[str, int]]:
        """Run one crawl cycle."""
        runtime_storage = PgStorage(self._postgres_dsn)
        try:
            async with CrawlerEngine(
                max_pages=self._cycle_pages,
                max_depth=self._max_depth,
                same_domain=False,
                delay=self._delay,
                concurrency=self._concurrency,
                pg_storage=storage,
                frontier=frontier,
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

    def _ensure_seeds(self, frontier: Frontier):
        """Re-seed frontier when empty."""
        if frontier.pending_count() > 0:
            return

        count = frontier.upsert_seeds(self._seeds, priority=2.0)
        logger.info("Re-seeded %d URLs", count)

    def _recrawl_stale(self, storage: PgStorage, frontier: Frontier):
        """Re-queue pages older than recrawl_ttl."""
        pending = frontier.pending_count()
        if pending >= self._cycle_pages:
            return

        batch_size = self._cycle_pages - pending
        if batch_size <= 0:
            return

        cutoff = time.time() - self._recrawl_ttl
        now = time.time()
        with storage.conn.cursor() as cur:
            cur.execute(
                """WITH candidates AS (
                       SELECT frontier.url
                       FROM frontier
                       JOIN pages ON frontier.url = pages.url
                       WHERE pages.crawled_at < %s
                         AND frontier.status = 'done'
                       ORDER BY pages.crawled_at ASC
                       LIMIT %s
                   )
                   UPDATE frontier
                   SET status = 'pending',
                       queue_class = 'recrawl',
                       next_fetch_at = %s,
                       lease_token = NULL,
                       lease_expires_at = NULL
                   WHERE url IN (SELECT url FROM candidates)""",
                (cutoff, batch_size, now),
            )
            count = cur.rowcount
        storage.conn.commit()
        if count:
            logger.info(
                "Re-queued %d stale pages (TTL=%ds, pending=%d, target=%d)",
                count,
                self._recrawl_ttl,
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
