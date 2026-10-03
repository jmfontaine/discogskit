"""PostgreSQL writer: DDL generation, ADBC ingest, index building.

Uses two PostgreSQL client libraries for different purposes:

- **ADBC** (``adbc_driver_postgresql``): bulk data writes via ``adbc_ingest``, which uses the COPY protocol under the
  hood and accepts Arrow RecordBatches directly — the fastest path from Arrow to PostgreSQL, avoiding row-by-row
  serialization.

- **psycopg**: DDL operations (CREATE TABLE, CREATE INDEX) because ADBC's DBAPI layer doesn't support arbitrary SQL
  well.

Tables are created bare (no PK, no FK, no indexes) and constraints are added AFTER bulk load. This is a standard
PostgreSQL bulk-loading optimization: inserting into indexed tables triggers per-row index maintenance, which is far
slower than building indexes once after all data is loaded.

UNLOGGED tables skip WAL (Write-Ahead Log) writes for ~2x speedup on parse+load. The tradeoff: tables remain unlogged
permanently — data is not recovered after a crash unless the user converts them back with
``ALTER TABLE … SET LOGGED``. This is acceptable for imports where the source .xml.gz is the durable copy and can be
re-loaded quickly.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from discogskit._console import status
from discogskit.writers._ipc import deserialize_batches

if TYPE_CHECKING:
    from discogskit.entities import EntityDef

# ------------------------------------------------------------------------------------------------------------------------
# Arrow → PostgreSQL DDL generation
#
# DDL is generated from the Arrow schemas (defined in entity modules) so that column types and nullability stay in sync
# with the parsing code. The Arrow schemas are the single source of truth.
# ------------------------------------------------------------------------------------------------------------------------

_ARROW_TO_PG: dict[pa.DataType, sql.SQL] = {
    pa.bool_(): sql.SQL("BOOLEAN"),
    pa.int32(): sql.SQL("INTEGER"),
    pa.int64(): sql.SQL("BIGINT"),
    pa.utf8(): sql.SQL("TEXT"),
}


def _arrow_to_pg_type(arrow_type: pa.DataType) -> sql.Composable:
    if isinstance(arrow_type, pa.ListType):
        inner = _ARROW_TO_PG.get(arrow_type.value_type)
        if inner is None:
            raise ValueError(f"Unsupported list value type: {arrow_type.value_type}")
        return inner + sql.SQL("[]")
    pg = _ARROW_TO_PG.get(arrow_type)
    if pg is None:
        raise ValueError(f"Unsupported Arrow type: {arrow_type}")
    return pg


def generate_ddl(
    table_name: str, schema: pa.Schema, *, unlogged: bool = False
) -> sql.Composed:
    """Generate CREATE TABLE DDL from an Arrow schema."""
    prefix = sql.SQL("CREATE UNLOGGED TABLE") if unlogged else sql.SQL("CREATE TABLE")
    col_defs: list[sql.Composable] = []
    for field in schema:
        pg_type = _arrow_to_pg_type(field.type)
        col: sql.Composable = sql.Identifier(field.name) + sql.SQL(" ") + pg_type
        if not field.nullable:
            col += sql.SQL(" NOT NULL")
        col_defs.append(col)
    return sql.SQL("{} {} (\n    {}\n)").format(
        prefix,
        sql.Identifier(table_name),
        sql.SQL(",\n    ").join(col_defs),
    )


# ------------------------------------------------------------------------------------------------------------------------
# Table group splitting (for multi-writer parallelism)
#
# When using multiple write workers, each worker gets its own ADBC connection and a subset of tables to flush
# concurrently. Naive round-robin distributes tables without regard for cost; instead we use profiled flush weights
# (TABLE_WEIGHTS in entity modules) and greedy bin-packing: sort tables heaviest-first, assign each to the lightest
# group. The weights are stable across runs because the Discogs data distribution is consistent.
# ------------------------------------------------------------------------------------------------------------------------


def _split_table_groups(n: int, entity: EntityDef) -> list[list[str]]:
    """Distribute tables across n groups, balanced by profiled flush weight."""
    sorted_tables = sorted(
        entity.table_order,
        key=lambda t: entity.table_weights.get(t, 0),
        reverse=True,
    )
    groups: list[list[str]] = [[] for _ in range(n)]
    group_weights = [0.0] * n
    for tname in sorted_tables:
        lightest = min(range(n), key=lambda i: group_weights[i])
        groups[lightest].append(tname)
        group_weights[lightest] += entity.table_weights.get(tname, 0)
    return groups


# ------------------------------------------------------------------------------------------------------------------------
# ADBC flush helpers
# ------------------------------------------------------------------------------------------------------------------------


def _flush_group(
    adbc_conn,
    ipc_dict: dict,
    tables: list[str],
    schemas: dict,
    root_table: str,
    table_timings: dict[str, float] | None = None,
) -> int:
    """Flush a subset of tables. Returns root table count (if present)."""
    count = 0
    with adbc_conn.cursor() as cur:
        for tname in tables:
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
            reader = pa.RecordBatchReader.from_batches(schemas[tname], iter(batches))
            cur.adbc_ingest(tname, reader, mode="append")
            if table_timings is not None:
                table_timings[tname] = table_timings.get(tname, 0.0) + (
                    time.perf_counter() - t0
                )
    if table_timings is not None:
        t_commit = time.perf_counter()
    adbc_conn.commit()
    if table_timings is not None:
        table_timings["_commit"] = table_timings.get("_commit", 0.0) + (
            time.perf_counter() - t_commit
        )
    return count


# ------------------------------------------------------------------------------------------------------------------------
# Schema targeting
#
# Without --pg-schema, every statement is unqualified and lands wherever the DSN's search_path resolves it (unchanged
# behavior). With --pg-schema, self._conn gets an explicit SET search_path so current_schema() and every unqualified
# statement on it resolve to the chosen schema; self._dsn is rewritten with the equivalent libpq ``options`` so every
# later connection (ADBC, including multi-writer, and the per-index connections finalize() opens) resolves the same
# way, with no changes needed to the statements themselves. __init__ verifies a fresh ADBC connection agrees with
# self._conn on current_schema() before any work starts: the schema name goes through a different parser (libpq's
# ``options`` splitting) there than in self._conn's SET, so an unquoted or unescaped name could silently resolve
# differently and land data in the wrong place.
# ------------------------------------------------------------------------------------------------------------------------


def _dsn_with_schema(dsn: str, schema: str) -> str:
    """Append ``-csearch_path=<schema>`` to the DSN's libpq ``options``, keeping existing ones."""
    ident = '"' + schema.replace('"', '""') + '"'
    opt = "-csearch_path=" + "".join(
        f"\\{ch}" if ch.isspace() or ch == "\\" else ch for ch in ident
    )
    existing = conninfo_to_dict(dsn).get("options")
    return make_conninfo(dsn, options=f"{existing} {opt}" if existing else opt)


# ------------------------------------------------------------------------------------------------------------------------
# PostgreSQLWriter
# ------------------------------------------------------------------------------------------------------------------------


class _Closeable(Protocol):
    def close(self) -> None: ...


def _close_with_timeout(conn: _Closeable, timeout: float, log: logging.Logger) -> None:
    """Call ``conn.close()`` in a thread, abandoning it after *timeout* seconds."""
    t = Thread(daemon=True, target=conn.close)
    t.start()
    t.join(timeout)
    if t.is_alive():
        log.debug("close() timed out after %.1fs for %s", timeout, type(conn).__name__)


class PostgreSQLWriter:
    """Writer implementation for PostgreSQL via psycopg (DDL) and ADBC (ingest)."""

    def __init__(
        self,
        dsn: str,
        *,
        create_schema: bool = False,
        fk: bool = False,
        index_workers: int = 2,
        overwrite: bool = False,
        schema: str | None = None,
        unlogged: bool = False,
        write_workers: int = 1,
    ) -> None:
        self._fk = fk
        self._index_workers = index_workers
        self._overwrite = overwrite
        self._unlogged = unlogged
        self._write_workers = write_workers

        import psycopg

        self._conn = psycopg.connect(dsn, autocommit=True)

        if schema is not None:
            try:
                exists = self._conn.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,)
                ).fetchone()
                if exists is None:
                    if not create_schema:
                        raise ValueError(
                            f"Schema {schema!r} does not exist. Create it first, "
                            "or pass --pg-create-schema to create it."
                        )
                    try:
                        self._conn.execute(
                            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
                        )
                    except psycopg.errors.InsufficientPrivilege as exc:
                        raise ValueError(
                            f"Can't create schema {schema!r}: {exc}. Grant CREATE "
                            "on the database to this role, or create the schema "
                            "yourself."
                        ) from None

                has_usage = self._conn.execute(
                    "SELECT has_schema_privilege(current_user, %s, 'USAGE')",
                    (schema,),
                ).fetchone()
                if not (has_usage and has_usage[0]):
                    raise ValueError(
                        f"Can't target schema {schema!r}: this role lacks USAGE "
                        "on it. Grant USAGE (and CREATE, to create tables) on "
                        "the schema to this role."
                    )

                self._conn.execute(
                    sql.SQL("SET search_path TO {}").format(sql.Identifier(schema))
                )
                ddl_row = self._conn.execute("SELECT current_schema()").fetchone()
                ddl_schema = ddl_row[0] if ddl_row else None
                if ddl_schema != schema:
                    raise ValueError(
                        f"Can't target schema {schema!r}: current_schema() "
                        f"resolved to {ddl_schema!r} instead."
                    )

                dsn = _dsn_with_schema(dsn, schema)
                import adbc_driver_postgresql.dbapi as adbc_pg

                check_conn = adbc_pg.connect(dsn)
                try:
                    with check_conn.cursor() as cur:
                        cur.execute("SELECT current_schema()")
                        adbc_row = cur.fetchone()
                        adbc_schema = adbc_row[0] if adbc_row else None
                finally:
                    check_conn.close()
                if adbc_schema != schema:
                    raise ValueError(
                        f"Can't target schema {schema!r}: PostgreSQL write "
                        f"connections resolved {adbc_schema!r} instead."
                    )
            except Exception:
                self._conn.close()
                raise

        self._dsn = dsn

        # ADBC connections + table groups + executor (set up lazily in setup())
        self._adbc_conn = None
        self._adbc_conns: list = []
        self._executor: ThreadPoolExecutor | None = None
        self._group_timings: list[dict] | None = None
        self._groups: list[list[str]] = []
        self._setup_called = False

    def setup(self, entity: EntityDef) -> None:
        """Drop/create tables in the current schema, set up ADBC."""
        import adbc_driver_postgresql.dbapi as adbc_pg
        import psycopg

        from discogskit.writers import OutputExistsError

        # Unqualified CREATE TABLE lands in current_schema(); every check and drop
        # names it explicitly, so a same-named table elsewhere on search_path is
        # never mistaken for ours.
        row = self._conn.execute("SELECT current_schema()").fetchone()
        schema = row[0] if row else None
        if schema is None:
            raise ValueError(
                "No schema to create tables in: search_path names no existing schema"
            )

        if not self._overwrite:
            existing = {
                row[0]
                for row in self._conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                    (schema,),
                )
            }
            conflict = existing & set(entity.table_order)
            if conflict:
                example = min(conflict)
                raise OutputExistsError(
                    f"Tables already exist in schema {schema} "
                    f"(e.g. {example}). Use --overwrite to replace them."
                )

        # Drop and recreate tables in one transaction, so a refused drop leaves
        # every table as it was. One DROP for all tables: foreign keys between
        # them don't block it, and PostgreSQL lists every outside dependent.
        t_ddl = time.perf_counter()
        tables = sql.SQL(", ").join(
            sql.Identifier(schema, t) for t in entity.table_order
        )
        with self._conn.transaction():
            try:
                self._conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(tables))
            except psycopg.errors.DependentObjectsStillExist as exc:
                detail = exc.diag.message_detail or ""
                dependents = "".join(f"\n  {line}" for line in detail.splitlines())
                raise OutputExistsError(
                    f"Can't overwrite tables in schema {schema}: other objects "
                    f"depend on them.{dependents}\n"
                    "Drop or change those objects first, or load into another schema."
                ) from None
            for t in entity.table_order:
                self._conn.execute(
                    generate_ddl(t, entity.schemas[t], unlogged=self._unlogged)
                )

        n_tables = len(entity.table_order)
        mode_label = "unlogged " if self._unlogged else ""
        status(
            "Create",
            f"{n_tables} {mode_label}tables",
            f"[{time.perf_counter() - t_ddl:.2f}s]",
        )

        # Set up ADBC connections
        if self._write_workers <= 1:
            self._adbc_conn = adbc_pg.connect(self._dsn)
        else:
            self._groups = _split_table_groups(self._write_workers, entity)
            self._adbc_conns = [
                adbc_pg.connect(self._dsn) for _ in range(self._write_workers)
            ]
            self._executor = ThreadPoolExecutor(max_workers=self._write_workers)
        self._setup_called = True

    def write_chunk(
        self,
        ipc_dict: dict[str, bytes],
        entity: EntityDef,
        table_timings: dict[str, float] | None = None,
    ) -> int:
        root_table = entity.table_order[0]

        if self._write_workers <= 1:
            return _flush_group(
                self._adbc_conn,
                ipc_dict,
                entity.table_order,
                entity.schemas,
                root_table,
                table_timings,
            )

        # Multi-writer path: dispatch to thread pool
        if table_timings is not None and self._group_timings is None:
            self._group_timings = [{} for _ in range(self._write_workers)]
        assert self._executor is not None
        futures = [
            self._executor.submit(
                _flush_group,
                conn,
                ipc_dict,
                tables,
                entity.schemas,
                root_table,
                self._group_timings[i] if self._group_timings is not None else None,
            )
            for i, (conn, tables) in enumerate(zip(self._adbc_conns, self._groups))
        ]
        return sum(f.result() for f in futures)

    def get_table_timings(self) -> dict[str, float]:
        """Merge per-group timings into a single dict. Call after all chunks."""
        if self._group_timings is None:
            return {}
        merged = {}
        for gt in self._group_timings:
            for k, v in gt.items():
                merged[k] = merged.get(k, 0.0) + v
        return merged

    def finalize(self, entity: EntityDef) -> None:
        """Build PK, indexes, and optional FK constraints."""
        import psycopg

        # Shut down write executor and ADBC connections before index building
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None
        if self._adbc_conn is not None:
            self._adbc_conn.close()
            self._adbc_conn = None
        for conn in self._adbc_conns:
            conn.close()
        self._adbc_conns = []

        table_order = entity.table_order

        pk_col = entity.pk_column
        fk_col = entity.fk_column
        root = table_order[0]

        if not self._setup_called:
            # indexes-only path: tables already exist, drop old indexes first
            t_drop = time.perf_counter()
            if fk_col:
                for t in table_order[1:]:
                    self._conn.execute(
                        sql.SQL("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}").format(
                            sql.Identifier(t),
                            sql.Identifier(f"{t}_{fk_col}_fkey"),
                        )
                    )
            self._conn.execute(
                sql.SQL("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}").format(
                    sql.Identifier(root), sql.Identifier(f"{root}_pkey")
                )
            )
            if fk_col:
                for t in table_order[1:]:
                    self._conn.execute(
                        sql.SQL("DROP INDEX IF EXISTS {}").format(
                            sql.Identifier(f"{t}_{fk_col}_idx")
                        )
                    )
            status(
                "Drop",
                "indexes + constraints",
                f"[{time.perf_counter() - t_drop:.2f}s]",
            )

        t0 = time.perf_counter()
        self._conn.execute(
            sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
                sql.Identifier(root), sql.Identifier(pk_col)
            )
        )
        status("Index", f"primary key on {root}", f"[{time.perf_counter() - t0:.2f}s]")

        if fk_col and len(table_order) > 1:
            t1 = time.perf_counter()
            if self._index_workers <= 1:
                for t in table_order[1:]:
                    self._conn.execute(
                        sql.SQL("CREATE INDEX ON {} ({})").format(
                            sql.Identifier(t), sql.Identifier(fk_col)
                        )
                    )
            else:

                def _create_index(table_name):
                    with psycopg.connect(self._dsn, autocommit=True) as idx_conn:
                        idx_conn.execute(
                            sql.SQL("CREATE INDEX ON {} ({})").format(
                                sql.Identifier(table_name), sql.Identifier(fk_col)
                            )
                        )
                    return table_name

                with ThreadPoolExecutor(max_workers=self._index_workers) as executor:
                    futures = {
                        executor.submit(_create_index, t): t for t in table_order[1:]
                    }
                    for f in as_completed(futures):
                        f.result()

            n_idx = len(table_order) - 1
            workers_label = (
                f", {self._index_workers} workers" if self._index_workers > 1 else ""
            )
            status(
                "Index",
                f"{n_idx} indexes{workers_label}",
                f"[{time.perf_counter() - t1:.2f}s]",
            )

        if self._fk and fk_col and len(table_order) > 1:
            t2 = time.perf_counter()
            for t in table_order[1:]:
                self._conn.execute(
                    sql.SQL(
                        "ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}({})"
                    ).format(
                        sql.Identifier(t),
                        sql.Identifier(fk_col),
                        sql.Identifier(root),
                        sql.Identifier(pk_col),
                    )
                )
            status(
                "Constrain",
                f"{len(table_order) - 1} foreign keys",
                f"[{time.perf_counter() - t2:.2f}s]",
            )

    def close(self, *, timeout: float = 5.0) -> None:
        """Release all resources.

        Each network operation is guarded by *timeout* seconds so that
        ``close()`` never blocks indefinitely (e.g. after Ctrl-C when the
        server is unresponsive).
        """
        log = logging.getLogger(__name__)

        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None

        for closeable in [self._adbc_conn, *self._adbc_conns]:
            if closeable is not None:
                _close_with_timeout(closeable, timeout, log)
        self._adbc_conn = None
        self._adbc_conns = []

        _close_with_timeout(self._conn, timeout, log)
