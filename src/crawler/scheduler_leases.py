"""Execution lease storage for active scheduler work."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Callable

import psycopg2.extras

from .urls import normalize_url

ACTIVE_LEASES_TABLE = "active_leases"
LEASE_REQUIRED_COLUMNS = {
    "url",
    "host",
    "physical_queue",
    "lease_token",
    "lease_expires_at",
}


@dataclass(frozen=True)
class ExecutionLeaseRow:
    url: str
    host: str
    physical_queue: str
    lease_token: str
    lease_expires_at: float

    def as_tuple(self) -> tuple[str, str, str, str, float]:
        return (
            self.url,
            self.host,
            self.physical_queue,
            self.lease_token,
            self.lease_expires_at,
        )


ExecutionLeaseRowInput = ExecutionLeaseRow | tuple[str, str, str, str, float]


class ExecutionLeaseStore:
    """Owns active execution lease rows."""

    def __init__(
        self,
        conn,
        *,
        table_name: str = ACTIVE_LEASES_TABLE,
        normalize_physical_queue: Callable[[str | None], str] | None = None,
    ):
        self._conn = conn
        self._table_name = table_name
        self._normalize_physical_queue = normalize_physical_queue or (lambda value: value or "")

    def new_token(self) -> str:
        return uuid.uuid4().hex

    def match_sql(self, table_alias: str, lease_token: str | None) -> tuple[str, tuple[str, ...]]:
        if lease_token is None:
            return "", ()
        return (
            " AND EXISTS ("
            f"SELECT 1 FROM {self._table_name} AS active "
            f"WHERE active.url = {table_alias}.url AND active.lease_token = %s"
            ")",
            (lease_token,),
        )

    def _row_tuple(self, row: ExecutionLeaseRowInput) -> tuple[str, str, str, str, float]:
        if isinstance(row, ExecutionLeaseRow):
            return row.as_tuple()
        return row

    def _row_tuples(
        self, rows: list[ExecutionLeaseRowInput]
    ) -> list[tuple[str, str, str, str, float]]:
        return [self._row_tuple(row) for row in rows]

    def delete(self, cur, urls: list[str]) -> None:
        normalized_urls = sorted({normalize_url(url) for url in urls if url})
        if not normalized_urls:
            return
        cur.execute(f"DELETE FROM {self._table_name} WHERE url = ANY(%s)", (normalized_urls,))

    def acquire(self, cur, rows: list[ExecutionLeaseRowInput]) -> set[str]:
        """Lease URLs that hold no lease yet and return the URLs leased now.

        An existing lease stays with its worker until completion or expiry recovery ends it.
        """
        lease_rows = [
            (
                normalize_url(url),
                host,
                self._normalize_physical_queue(physical_queue),
                lease_token,
                lease_expires_at,
            )
            for url, host, physical_queue, lease_token, lease_expires_at in self._row_tuples(rows)
            if url
        ]
        if not lease_rows:
            return set()
        leased = psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO {self._table_name}
                    (url, host, physical_queue, lease_token, lease_expires_at)
                VALUES %s
                ON CONFLICT (url) DO NOTHING
                RETURNING url""",
            lease_rows,
            page_size=200,
            fetch=True,
        )
        return {url for (url,) in leased}

    def recover_rows(self, cur, *, now: float, expired_only: bool) -> list[tuple[str, str, str]]:
        if expired_only:
            where = "lease_expires_at <= %s"
            params = (now,)
        else:
            where = "TRUE"
            params = ()

        cur.execute(
            f"""DELETE FROM {self._table_name}
                WHERE {where}
                RETURNING url, host, physical_queue""",
            params,
        )
        return list(cur.fetchall())
