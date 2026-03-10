"""SQLite writer: DDL generation, executemany ingest, index building.

Uses Python's built-in ``sqlite3`` module for all operations. Tables are created with PRIMARY KEY inline (unlike the PG
writer's post-load approach) because SQLite cannot add PKs after creation, and INTEGER PRIMARY KEY is an alias for rowid
with no insert penalty.

List-type Arrow columns are stored as JSON text (e.g. '["Rock","Pop"]'). SQLite's built-in ``json_each()`` enables
querying them::

    SELECT * FROM releases, json_each(genres) WHERE value = 'Rock';

Bulk loading uses WAL journal mode, synchronous=OFF, and one transaction per chunk for maximum throughput.
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from discogskit._console import status
from discogskit.writers._ipc import deserialize_batches

if TYPE_CHECKING:
    from discogskit.entities import EntityDef

# ------------------------------------------------------------------------------------------------------------------------
# Arrow → SQLite DDL generation
# ------------------------------------------------------------------------------------------------------------------------

_ARROW_TO_SQLITE: dict[pa.DataType, str] = {
    pa.bool_(): "INTEGER",
    pa.int32(): "INTEGER",
    pa.int64(): "INTEGER",
    pa.utf8(): "TEXT",
}

_SQLITE_DEFAULTS: dict[str, str] = {
    "INTEGER": "0",
    "TEXT": "''",
}


def _arrow_to_sqlite_type(arrow_type: pa.DataType) -> str:
    if isinstance(arrow_type, pa.ListType):
        return "TEXT"  # JSON-encoded arrays
    sql = _ARROW_TO_SQLITE.get(arrow_type)
    if sql is None:
        raise ValueError(f"Unsupported Arrow type: {arrow_type}")
    return sql


def generate_ddl(
    table_name: str,
    schema: pa.Schema,
    *,
    fk_column: str | None = None,
    fk_ref_table: str | None = None,
    pk_column: str | None = None,
) -> str:
    """Generate CREATE TABLE DDL from an Arrow schema."""
    lines = []
    for field in schema:
        sql_type = _arrow_to_sqlite_type(field.type)
        parts = [f"    {field.name:<20s} {sql_type}"]
        if field.name == pk_column:
            parts.append("PRIMARY KEY")
        elif not field.nullable:
            parts.append("NOT NULL")
            if isinstance(field.type, pa.ListType):
                parts.append("DEFAULT '[]'")
            else:
                parts.append(f"DEFAULT {_SQLITE_DEFAULTS[sql_type]}")
        if field.name == fk_column and fk_ref_table is not None:
            parts.append(f"REFERENCES {fk_ref_table}({pk_column})")
        lines.append(" ".join(parts))
    cols = ",\n".join(lines)
    return f"CREATE TABLE {table_name} (\n{cols}\n)"


# ------------------------------------------------------------------------------------------------------------------------
# SQLiteWriter
# ------------------------------------------------------------------------------------------------------------------------


class SQLiteWriter:
    """Writer implementation for SQLite via the stdlib sqlite3 module."""

    def __init__(self, path: str, *, fk: bool = False, overwrite: bool = False) -> None:
        self._path = path
        self._conn = sqlite3.connect(path)
        self._fk = fk
        self._overwrite = overwrite
        self._insert_sql: dict[str, str] = {}
        self._list_columns: dict[str, set[int]] = {}

    def setup(self, entity: EntityDef) -> None:
        """Apply PRAGMAs, drop/create tables."""
        cur = self._conn.cursor()

        # Check for existing tables before any destructive operations
        if not self._overwrite:
            existing = {
                row[0]
                for row in cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            conflict = existing & set(entity.table_order)
            if conflict:
                from discogskit.writers import OutputExistsError

                example = sorted(conflict)[0]
                raise OutputExistsError(
                    f"Tables already exist in {self._path} "
                    f"(e.g. {example}). Use --overwrite to replace them."
                )

        # Performance PRAGMAs
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA cache_size=-64000")
        cur.execute("PRAGMA temp_store=MEMORY")

        if self._fk:
            cur.execute("PRAGMA foreign_keys=ON")

        # Drop tables in reverse order
        for t in reversed(entity.table_order):
            cur.execute(f"DROP TABLE IF EXISTS {t}")

        # Create tables
        t_ddl = time.perf_counter()
        root = entity.table_order[0]
        pk_col = entity.pk_column
        fk_col = entity.fk_column

        for t in entity.table_order:
            ddl = generate_ddl(
                t,
                entity.schemas[t],
                pk_column=pk_col,
                fk_column=fk_col if t != root and self._fk else None,
                fk_ref_table=root if t != root and self._fk else None,
            )
            cur.execute(ddl)

        self._conn.commit()

        n_tables = len(entity.table_order)
        status("Create", f"{n_tables} tables", f"[{time.perf_counter() - t_ddl:.2f}s]")

        # Precompute INSERT SQL and list column indices
        for t in entity.table_order:
            schema = entity.schemas[t]
            n_cols = len(schema)
            placeholders = ", ".join("?" * n_cols)
            self._insert_sql[t] = f"INSERT INTO {t} VALUES ({placeholders})"

            list_cols = set()
            for i, field in enumerate(schema):
                if isinstance(field.type, pa.ListType):
                    list_cols.add(i)
            if list_cols:
                self._list_columns[t] = list_cols

    def write_chunk(
        self,
        ipc_dict: dict[str, bytes],
        entity: EntityDef,
        table_timings: dict[str, float] | None = None,
    ) -> int:
        """Write one chunk. Returns root table row count."""
        count = 0
        root_table = entity.table_order[0]
        cur = self._conn.cursor()

        for tname in entity.table_order:
            t0 = time.perf_counter() if table_timings is not None else 0
            batches = deserialize_batches(ipc_dict[tname])
            if not batches or batches[0].num_rows == 0:
                if table_timings is not None:
                    table_timings[tname] = table_timings.get(tname, 0.0) + (
                        time.perf_counter() - t0
                    )
                continue
            if tname == root_table:
                count = sum(b.num_rows for b in batches)

            insert_sql = self._insert_sql[tname]
            list_cols = self._list_columns.get(tname)

            for batch in batches:
                columns = [
                    batch.column(i).to_pylist() for i in range(batch.num_columns)
                ]
                if list_cols:
                    for i in list_cols:
                        columns[i] = [
                            json.dumps(v) if v is not None else "[]" for v in columns[i]
                        ]
                rows = list(zip(*columns))
                cur.executemany(insert_sql, rows)

            if table_timings is not None:
                table_timings[tname] = table_timings.get(tname, 0.0) + (
                    time.perf_counter() - t0
                )

        if table_timings is not None:
            t_commit = time.perf_counter()
        self._conn.commit()
        if table_timings is not None:
            table_timings["_commit"] = table_timings.get("_commit", 0.0) + (
                time.perf_counter() - t_commit
            )
        return count

    def finalize(self, entity: EntityDef) -> None:
        """Create indexes on FK columns and verify FK integrity."""
        fk_col = entity.fk_column

        if fk_col and len(entity.table_order) > 1:
            t0 = time.perf_counter()
            cur = self._conn.cursor()
            for t in entity.table_order[1:]:
                cur.execute(f"CREATE INDEX {t}_{fk_col}_idx ON {t}({fk_col})")
            self._conn.commit()
            n_idx = len(entity.table_order) - 1
            status("Index", f"{n_idx} indexes", f"[{time.perf_counter() - t0:.2f}s]")

        if self._fk:
            t0 = time.perf_counter()
            violations = self._conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                status("Verify", f"[red]{len(violations)} FK violations[/]")
            else:
                status(
                    "Verify", "foreign keys OK", f"[{time.perf_counter() - t0:.2f}s]"
                )

    def close(self) -> None:
        """Release resources."""
        self._conn.close()
