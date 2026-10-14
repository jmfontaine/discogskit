"""SQLite writer: DDL generation, executemany ingest, index building.

Uses Python's built-in ``sqlite3`` module for all operations. Tables are created with PRIMARY KEY inline (unlike the PG
writer's post-load approach) because SQLite cannot add PKs after creation, and INTEGER PRIMARY KEY is an alias for rowid
with no insert penalty.

List-type Arrow columns are stored as compact JSON text, with non-ASCII characters as-is rather than ``\\uXXXX``
escapes (e.g. '["Rock","Pop"]', '["Björk","ビョーク"]'), so ``LIKE`` matches them. SQLite's built-in ``json_each()``
enables querying them::

    SELECT * FROM releases, json_each(genres) WHERE value = 'Rock';

Bulk loading uses WAL journal mode, synchronous=OFF, and one transaction per chunk for maximum throughput. Both are
temporary: ``finalize()`` switches to synchronous=FULL, checkpoints the WAL into the database file, syncs the file,
and restores the journal mode the file had before the load, since WAL mode persists in the file. ``setup()`` drops
and creates the tables in one transaction, so a failing drop leaves every table in place.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import suppress
from typing import TYPE_CHECKING

import pyarrow as pa

from discogskit._console import status

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


def _arrow_to_sqlite_type(arrow_type: pa.DataType) -> str:
    if isinstance(arrow_type, pa.ListType):
        return "TEXT"  # JSON-encoded arrays
    sql = _ARROW_TO_SQLITE.get(arrow_type)
    if sql is None:
        raise ValueError(f"Unsupported Arrow type: {arrow_type}")
    return sql


def _quote(name: str) -> str:
    """Quote an SQL identifier, so names like ``join`` aren't parsed as keywords."""
    return '"' + name.replace('"', '""') + '"'


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
        parts = [f"    {_quote(field.name):<22s} {sql_type}"]
        if field.name == pk_column:
            parts.append("PRIMARY KEY")
        elif not field.nullable:
            parts.append("NOT NULL")
        if (
            field.name == fk_column
            and fk_ref_table is not None
            and pk_column is not None
        ):
            parts.append(f"REFERENCES {_quote(fk_ref_table)}({_quote(pk_column)})")
        lines.append(" ".join(parts))
    cols = ",\n".join(lines)
    return f"CREATE TABLE {_quote(table_name)} (\n{cols}\n)"


def _index_ddl(table_name: str, column: str) -> str:
    """CREATE INDEX on a child table's link column, run after the bulk load."""
    return (
        f"CREATE INDEX {_quote(f'{table_name}_{column}_idx')}"
        f" ON {_quote(table_name)}({_quote(column)})"
    )


# ------------------------------------------------------------------------------------------------------------------------
# SQLiteWriter
# ------------------------------------------------------------------------------------------------------------------------


class SQLiteWriter:
    """Writer implementation for SQLite via the stdlib sqlite3 module."""

    def __init__(self, path: str, *, fk: bool = False, overwrite: bool = False) -> None:
        self._path = path
        # pipeline.run calls write_chunk on its writer thread, while setup/finalize/
        # close run on the caller's thread. Uses never overlap: the writer thread is
        # idle whenever the caller touches the connection.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._fk = fk
        self._overwrite = overwrite
        self._insert_sql: dict[str, str] = {}
        # The file's journal mode before setup() switched it to WAL, until finalize() restores it.
        self._journal_mode: str | None = None
        self._list_columns: dict[str, set[int]] = {}

    def cancel(self) -> None:
        """Do nothing: a write to a local SQLite file can't stall the way a database server can."""

    def check(self, entity: EntityDef) -> None:
        """Raise ``OutputExistsError`` if any of ``entity``'s tables exists and overwrite is off."""
        if self._overwrite:
            return
        existing = {
            row[0]
            for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        conflict = existing & set(entity.table_order)
        if conflict:
            from discogskit.writers import OutputExistsError

            example = min(conflict)
            raise OutputExistsError(
                f"Tables already exist in {self._path} "
                f"(e.g. {example}). Use --overwrite to replace them."
            )

    def setup(self, entity: EntityDef) -> None:
        """Apply PRAGMAs, drop/create tables."""
        # Checked again here, before any destructive operation: tables can appear after the caller's own check().
        self.check(entity)
        cur = self._conn.cursor()

        # Performance PRAGMAs, outside the transaction: journal_mode can't change inside one.
        self._journal_mode = cur.execute("PRAGMA journal_mode").fetchone()[0]
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA cache_size=-64000")
        cur.execute("PRAGMA temp_store=MEMORY")

        if self._fk:
            cur.execute("PRAGMA foreign_keys=ON")

        # Drop and recreate tables in one transaction, so a refused drop (e.g. a user table referencing the root
        # table, with --fk) leaves every table as it was.
        t_ddl = time.perf_counter()
        root = entity.table_order[0]
        fk_col = entity.fk_column
        cur.execute("BEGIN")
        try:
            for t in reversed(entity.table_order):
                cur.execute(f"DROP TABLE IF EXISTS {_quote(t)}")
            for t in entity.table_order:
                ddl = generate_ddl(
                    t,
                    entity.schemas[t],
                    fk_column=fk_col if t != root and self._fk else None,
                    fk_ref_table=root if t != root and self._fk else None,
                    pk_column="id",
                )
                cur.execute(ddl)
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

        n_tables = len(entity.table_order)
        status("Create", f"{n_tables} tables", f"[{time.perf_counter() - t_ddl:.2f}s]")

        # Precompute INSERT SQL and list column indices
        for t in entity.table_order:
            schema = entity.schemas[t]
            n_cols = len(schema)
            placeholders = ", ".join("?" * n_cols)
            self._insert_sql[t] = f"INSERT INTO {_quote(t)} VALUES ({placeholders})"

            list_cols = set()
            for i, field in enumerate(schema):
                if isinstance(field.type, pa.ListType):
                    list_cols.add(i)
            if list_cols:
                self._list_columns[t] = list_cols

    def write_chunk(self, tables: dict[str, pa.RecordBatch]) -> dict[str, float]:
        """Write one chunk; returns per-table flush time in seconds."""
        timings: dict[str, float] = {}
        cur = self._conn.cursor()

        for table_name, batch in tables.items():
            t0 = time.perf_counter()
            if batch.num_rows:
                insert_sql = self._insert_sql[table_name]
                list_cols = self._list_columns.get(table_name)

                columns = [
                    batch.column(i).to_pylist() for i in range(batch.num_columns)
                ]
                if list_cols:
                    for i in list_cols:
                        columns[i] = [
                            None
                            if v is None
                            else json.dumps(
                                v, ensure_ascii=False, separators=(",", ":")
                            )
                            for v in columns[i]
                        ]
                cur.executemany(insert_sql, zip(*columns))
            timings[table_name] = time.perf_counter() - t0

        t_commit = time.perf_counter()
        self._conn.commit()
        timings["_commit"] = time.perf_counter() - t_commit
        return timings

    def finalize(self, entity: EntityDef) -> None:
        """Create indexes on FK columns, then make the database durable and restore its journal mode."""
        fk_col = entity.fk_column

        if len(entity.table_order) > 1:
            t0 = time.perf_counter()
            cur = self._conn.cursor()
            for t in entity.table_order[1:]:
                cur.execute(_index_ddl(t, fk_col))
            self._conn.commit()
            n_idx = len(entity.table_order) - 1
            status("Index", f"{n_idx} indexes", f"[{time.perf_counter() - t0:.2f}s]")

        self._restore_journal_mode()

    def _restore_journal_mode(self) -> None:
        """Sync the load to disk and switch the file back to the journal mode it had before ``setup()``."""
        assert self._journal_mode is not None, "finalize() called before setup()"
        # With synchronous=FULL, the checkpoint syncs the database file after copying the WAL's frames into it. But
        # it skips that sync when no frame is left to copy, as when automatic checkpoints already copied them all,
        # with synchronous=OFF, so without syncing. Rewriting user_version unchanged adds a frame, so the checkpoint
        # always has one to copy and syncs the file. (Not os.fsync() on a second descriptor: closing it would drop
        # the connection's POSIX locks, https://www.sqlite.org/howtocorrupt.html#posix_close_bug.)
        self._conn.execute("PRAGMA synchronous=FULL")
        user_version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        self._conn.execute(f"PRAGMA user_version={int(user_version)}")
        busy, log, checkpointed = self._conn.execute(
            "PRAGMA wal_checkpoint(TRUNCATE)"
        ).fetchone()
        if busy or log != checkpointed:
            raise sqlite3.OperationalError(
                f"Couldn't checkpoint the WAL of {self._path} ({checkpointed} of {log} frames): another "
                "connection is using the database."
            )
        # The mode came from SQLite itself, a bare keyword such as "delete" or "wal".
        mode = self._conn.execute(
            f"PRAGMA journal_mode={self._journal_mode}"
        ).fetchone()[0]
        if mode != self._journal_mode:
            raise sqlite3.OperationalError(
                f"Couldn't restore the journal mode of {self._path} to {self._journal_mode} (it's {mode}): another "
                "connection is using the database."
            )
        self._journal_mode = None

    def close(self) -> None:
        """Release resources. After a failed load, still try to restore the journal mode, without replacing the
        error that aborted it: a file left in WAL mode would otherwise keep it, even after a successful rerun.
        """
        if self._journal_mode is not None:
            with suppress(sqlite3.Error):
                self._conn.rollback()
                self._restore_journal_mode()
        self._conn.close()
