"""Continuous web crawling daemon."""

import asyncio
import logging
import signal
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
        self._shutdown = False
        self._engine: CrawlerEngine | None = None
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
            self._seeds, self._cycle_pages, self._recrawl_ttl,
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
                    frontier.requeue_failed()

                    pending = frontier.pending_count()
                    if pending == 0:
                        logger.info("No URLs to crawl, sleeping %ds", self._idle_sleep)
                        await self._interruptible_sleep(self._idle_sleep)
                        continue

                    cycle += 1
                    logger.info("Cycle %d: %d pending URLs", cycle, pending)
                    start = time.time()
                    pages = await self._run_cycle(storage, frontier)
                    elapsed = time.time() - start
                    rate = pages / elapsed if elapsed > 0 else 0
                    logger.info(
                        "Cycle %d complete: %d pages in %.1fs (%.1f pages/s) | %s",
                        cycle, pages, elapsed, rate, frontier.stats(),
                    )

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
                logger.info("Database connected (attempt %d)", attempt)
                return storage, frontier
            except psycopg2.OperationalError as e:
                logger.error("Connection attempt %d/%d failed: %s", attempt, _MAX_RECONNECT_ATTEMPTS, e)
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

    async def _run_cycle(self, storage: PgStorage, frontier: Frontier) -> int:
        """Run one crawl cycle."""
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
            await engine.crawl()
            self._engine = None
            return engine.pages_crawled

    def _ensure_seeds(self, frontier: Frontier):
        """Re-seed frontier when empty."""
        if frontier.pending_count() > 0:
            return

        count = frontier.upsert_seeds(self._seeds, priority=2.0)
        logger.info("Re-seeded %d URLs", count)

    def _recrawl_stale(self, storage: PgStorage, frontier: Frontier):
        """Re-queue pages older than recrawl_ttl."""
        cutoff = time.time() - self._recrawl_ttl
        now = time.time()
        with storage.conn.cursor() as cur:
            cur.execute(
                """UPDATE frontier
                   SET status = 'pending',
                       next_fetch_at = %s,
                       lease_token = NULL,
                       lease_expires_at = NULL
                   FROM pages
                   WHERE frontier.url = pages.url
                     AND pages.crawled_at < %s
                     AND frontier.status = 'done'""",
                (now, cutoff),
            )
            count = cur.rowcount
        storage.conn.commit()
        if count:
            logger.info("Re-queued %d stale pages (TTL=%ds)", count, self._recrawl_ttl)

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
