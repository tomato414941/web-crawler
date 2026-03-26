"""Schema validation helpers."""

from collections.abc import Collection


def get_public_table_columns(conn, table_name: str) -> set[str]:
    """Return public-column names for a table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return {column_name for (column_name,) in cur.fetchall()}


def assert_public_table_columns(
    conn,
    table_name: str,
    required_columns: Collection[str],
) -> None:
    """Raise when a public table is missing required columns."""
    columns = get_public_table_columns(conn, table_name)
    if not columns:
        raise RuntimeError(f"{table_name} schema is missing; run `crawler migrate`")

    missing = sorted(set(required_columns) - columns)
    if missing:
        missing_columns = ", ".join(missing)
        raise RuntimeError(
            f"{table_name} schema is outdated; missing columns: {missing_columns}. "
            "Run `crawler migrate`."
        )
