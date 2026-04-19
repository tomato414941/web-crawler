"""Persistent host identity and history ledger."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import ipaddress
import time
from urllib.parse import urlsplit

import psycopg2.extras

from .schema import assert_public_table_columns

HOST_LEDGER_TABLE = "host_ledger"
HOST_LEDGER_REQUIRED_COLUMNS = {
    "host",
    "registrable_domain",
    "first_seen_at",
    "last_seen_at",
    "last_success_at",
    "last_failure_at",
    "known_url_count",
    "success_count",
    "failure_count",
    "robots_last_checked_at",
    "robots_status",
    "created_at",
    "updated_at",
}


@dataclass(frozen=True, slots=True)
class HostLedgerRecord:
    """Durable host identity and history."""

    host: str
    registrable_domain: str | None
    first_seen_at: float
    last_seen_at: float
    last_success_at: float | None
    last_failure_at: float | None
    known_url_count: int
    success_count: int
    failure_count: int
    robots_last_checked_at: float | None
    robots_status: str | None
    created_at: float
    updated_at: float


def registrable_domain_for_host(host: str) -> str | None:
    """Return a best-effort registrable-domain label without adding a PSL dependency."""
    hostname = urlsplit(f"//{host}").hostname
    if not hostname:
        return None
    normalized = hostname.strip(".").lower()
    if not normalized:
        return None
    try:
        ipaddress.ip_address(normalized)
        return normalized
    except ValueError:
        pass
    labels = [label for label in normalized.split(".") if label]
    if len(labels) <= 2:
        return normalized
    return ".".join(labels[-2:])


class HostLedgerStore:
    """Postgres-backed storage for durable host identity and history."""

    def __init__(self, conn):
        self._conn = conn
        assert_public_table_columns(self._conn, HOST_LEDGER_TABLE, HOST_LEDGER_REQUIRED_COLUMNS)

    def _row_to_record(self, row: tuple) -> HostLedgerRecord:
        return HostLedgerRecord(
            host=row[0],
            registrable_domain=row[1],
            first_seen_at=row[2],
            last_seen_at=row[3],
            last_success_at=row[4],
            last_failure_at=row[5],
            known_url_count=row[6],
            success_count=row[7],
            failure_count=row[8],
            robots_last_checked_at=row[9],
            robots_status=row[10],
            created_at=row[11],
            updated_at=row[12],
        )

    def get(self, host: str) -> HostLedgerRecord | None:
        """Return one host ledger row if present."""
        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT host,
                           registrable_domain,
                           first_seen_at,
                           last_seen_at,
                           last_success_at,
                           last_failure_at,
                           known_url_count,
                           success_count,
                           failure_count,
                           robots_last_checked_at,
                           robots_status,
                           created_at,
                           updated_at
                    FROM {HOST_LEDGER_TABLE}
                    WHERE host = %s""",
                (host,),
            )
            row = cur.fetchone()
        self._conn.commit()
        if row is None:
            return None
        return self._row_to_record(row)

    def record_discovered_urls(
        self,
        host_counts: Mapping[str, int],
        *,
        seen_at: float | None = None,
    ) -> None:
        """Record host discovery and newly known URL counts."""
        with self._conn.cursor() as cur:
            self.record_discovered_urls_in_tx(cur, host_counts, seen_at=seen_at)
        self._conn.commit()

    def record_discovered_urls_in_tx(
        self,
        cur,
        host_counts: Mapping[str, int],
        *,
        seen_at: float | None = None,
    ) -> None:
        """Record host discovery inside an existing transaction."""
        timestamp = time.time() if seen_at is None else seen_at
        rows = [
            (
                host,
                registrable_domain_for_host(host),
                timestamp,
                timestamp,
                max(0, int(count)),
                timestamp,
                timestamp,
            )
            for host, count in sorted(host_counts.items())
            if host
        ]
        if not rows:
            return
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO {HOST_LEDGER_TABLE} (
                   host,
                   registrable_domain,
                   first_seen_at,
                   last_seen_at,
                   known_url_count,
                   created_at,
                   updated_at
               )
               VALUES %s
               ON CONFLICT (host) DO UPDATE SET
                   registrable_domain = COALESCE(
                       {HOST_LEDGER_TABLE}.registrable_domain,
                       EXCLUDED.registrable_domain
                   ),
                   first_seen_at = LEAST(
                       {HOST_LEDGER_TABLE}.first_seen_at,
                       EXCLUDED.first_seen_at
                   ),
                   last_seen_at = GREATEST(
                       {HOST_LEDGER_TABLE}.last_seen_at,
                       EXCLUDED.last_seen_at
                   ),
                   known_url_count = {HOST_LEDGER_TABLE}.known_url_count
                       + EXCLUDED.known_url_count,
                   updated_at = EXCLUDED.updated_at""",
            rows,
            page_size=200,
        )

    def record_success(self, host: str, *, at: float | None = None) -> None:
        """Record a successful crawl for a host."""
        with self._conn.cursor() as cur:
            self.record_success_in_tx(cur, host, at=at)
        self._conn.commit()

    def record_success_in_tx(self, cur, host: str, *, at: float | None = None) -> None:
        """Record a successful crawl inside an existing transaction."""
        timestamp = time.time() if at is None else at
        self._record_outcome_in_tx(
            cur,
            host,
            at=timestamp,
            timestamp_column="last_success_at",
            count_column="success_count",
        )

    def record_failure(self, host: str, *, at: float | None = None) -> None:
        """Record a failed crawl for a host."""
        with self._conn.cursor() as cur:
            self.record_failure_in_tx(cur, host, at=at)
        self._conn.commit()

    def record_failure_in_tx(self, cur, host: str, *, at: float | None = None) -> None:
        """Record a failed crawl inside an existing transaction."""
        timestamp = time.time() if at is None else at
        self._record_outcome_in_tx(
            cur,
            host,
            at=timestamp,
            timestamp_column="last_failure_at",
            count_column="failure_count",
        )

    def _record_outcome_in_tx(
        self,
        cur,
        host: str,
        *,
        at: float,
        timestamp_column: str,
        count_column: str,
    ) -> None:
        if not host:
            return
        cur.execute(
            f"""INSERT INTO {HOST_LEDGER_TABLE} (
                   host,
                   registrable_domain,
                   first_seen_at,
                   last_seen_at,
                   {timestamp_column},
                   {count_column},
                   created_at,
                   updated_at
               )
               VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
               ON CONFLICT (host) DO UPDATE SET
                   registrable_domain = COALESCE(
                       {HOST_LEDGER_TABLE}.registrable_domain,
                       EXCLUDED.registrable_domain
                   ),
                   last_seen_at = GREATEST({HOST_LEDGER_TABLE}.last_seen_at, EXCLUDED.last_seen_at),
                   {timestamp_column} = GREATEST(
                       COALESCE({HOST_LEDGER_TABLE}.{timestamp_column}, 0),
                       EXCLUDED.{timestamp_column}
                   ),
                   {count_column} = {HOST_LEDGER_TABLE}.{count_column} + 1,
                   updated_at = EXCLUDED.updated_at""",
            (host, registrable_domain_for_host(host), at, at, at, at, at),
        )

    def record_robots_check(
        self,
        host: str,
        *,
        status: str,
        checked_at: float | None = None,
    ) -> None:
        """Record a robots.txt check summary for a host."""
        with self._conn.cursor() as cur:
            self.record_robots_check_in_tx(cur, host, status=status, checked_at=checked_at)
        self._conn.commit()

    def record_robots_check_in_tx(
        self,
        cur,
        host: str,
        *,
        status: str,
        checked_at: float | None = None,
    ) -> None:
        """Record a robots.txt check summary inside an existing transaction."""
        if not host:
            return
        timestamp = time.time() if checked_at is None else checked_at
        cur.execute(
            f"""INSERT INTO {HOST_LEDGER_TABLE} (
                   host,
                   registrable_domain,
                   first_seen_at,
                   last_seen_at,
                   robots_last_checked_at,
                   robots_status,
                   created_at,
                   updated_at
               )
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (host) DO UPDATE SET
                   registrable_domain = COALESCE(
                       {HOST_LEDGER_TABLE}.registrable_domain,
                       EXCLUDED.registrable_domain
                   ),
                   last_seen_at = GREATEST({HOST_LEDGER_TABLE}.last_seen_at, EXCLUDED.last_seen_at),
                   robots_last_checked_at = EXCLUDED.robots_last_checked_at,
                   robots_status = EXCLUDED.robots_status,
                   updated_at = EXCLUDED.updated_at""",
            (
                host,
                registrable_domain_for_host(host),
                timestamp,
                timestamp,
                timestamp,
                status,
                timestamp,
                timestamp,
            ),
        )
