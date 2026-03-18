"""Continuous web crawling daemon."""

import asyncio
import logging
import signal
import time
from urllib.parse import urlparse

from .crawl import CrawlerEngine
from .frontier import Frontier
from .storage import PgStorage
from .urls import normalize_url

logger = logging.getLogger(__name__)


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

    async def run(self):
        """Main daemon loop."""
        self._install_signals()
        logger.info(
            "Daemon starting: seeds=%s, cycle_pages=%d, recrawl_ttl=%ds",
            self._seeds, self._cycle_pages, self._recrawl_ttl,
        )

        with PgStorage(self._postgres_dsn) as storage:
            frontier = Frontier(storage.conn)
            self._recover_processing(storage)
            cycle = 0

            while not self._shutdown:
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
                pages = await self._run_cycle(storage, frontier)
                logger.info("Cycle %d complete: %d pages crawled", cycle, pages)

                if not self._shutdown:
                    await self._interruptible_sleep(self._cycle_pause)

        logger.info("Daemon shutdown complete")

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
        ) as engine:
            self._engine = engine
            await engine.crawl()
            self._engine = None
            return engine.pages_crawled

    def _ensure_seeds(self, frontier: Frontier):
        """Re-seed frontier when empty."""
        if frontier.pending_count() > 0:
            return

        conn = frontier._conn
        for url in self._seeds:
            normalized = normalize_url(url)
            domain = urlparse(normalized).netloc
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO frontier (url, domain, depth, priority, source_url, added_at, status)
                       VALUES (%s, %s, 0, 2.0, NULL, %s, 'pending')
                       ON CONFLICT (url) DO UPDATE SET status = 'pending', added_at = EXCLUDED.added_at""",
                    (normalized, domain, time.time()),
                )
        conn.commit()
        logger.info("Re-seeded %d URLs", len(self._seeds))

    def _recrawl_stale(self, storage: PgStorage, frontier: Frontier):
        """Re-queue pages older than recrawl_ttl."""
        cutoff = time.time() - self._recrawl_ttl
        with storage.conn.cursor() as cur:
            cur.execute(
                """UPDATE frontier SET status = 'pending'
                   FROM pages
                   WHERE frontier.url = pages.url
                     AND pages.crawled_at < %s
                     AND frontier.status = 'done'""",
                (cutoff,),
            )
            count = cur.rowcount
        storage.conn.commit()
        if count:
            logger.info("Re-queued %d stale pages (TTL=%ds)", count, self._recrawl_ttl)

    def _recover_processing(self, storage: PgStorage):
        """Reset URLs stuck in processing state from a previous crash."""
        with storage.conn.cursor() as cur:
            cur.execute("UPDATE frontier SET status = 'pending' WHERE status = 'processing'")
            count = cur.rowcount
        storage.conn.commit()
        if count:
            logger.info("Recovered %d URLs from processing state", count)

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
