"""Integration tests for PostgreSQLWriter full lifecycle."""

from __future__ import annotations

import dataclasses
import gzip
import os
import signal
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from unittest.mock import MagicMock, patch
from urllib.parse import urlsplit

import click
import pytest
from typer.testing import CliRunner

from discogskit.entities import ENTITIES
from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from tests.conftest import ARTISTS_XML, LABELS_XML, empty_ipc, ipc_to_record_batches


def _load(dsn, entity, ipc_dict, **options):
    from discogskit.writers.postgresql import PostgreSQLWriter

    writer = PostgreSQLWriter(dsn, **options)
    try:
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)
    finally:
        writer.close()


@pytest.mark.integration
class TestPostgreSQLWriter:
    def test_full_lifecycle(self, pg_dsn, entity, ipc_dict):
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            # Verify tables exist
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'"
                ).fetchall()
            }
            for t in entity.table_order:
                assert t in tables

            # Verify row counts
            row = conn.execute("SELECT COUNT(*) FROM artists").fetchone()
            assert row is not None and row[0] == 2
            row = conn.execute("SELECT COUNT(*) FROM artist_aliases").fetchone()
            assert row is not None and row[0] == 1

            # Verify PK exists
            pk = conn.execute(
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_name = 'artists' AND constraint_type = 'PRIMARY KEY'"
            ).fetchone()
            assert pk is not None

            # Verify FK indexes exist
            indexes = {
                row[0]
                for row in conn.execute(
                    "SELECT indexname FROM pg_indexes WHERE tablename = 'artist_aliases'"
                ).fetchall()
            }
            assert any("artist_id" in idx for idx in indexes)

    def test_unlogged_mode(self, pg_dsn, entity, ipc_dict):
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, unlogged=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            # Verify UNLOGGED: relpersistence = 'u'
            row = conn.execute(
                "SELECT relpersistence FROM pg_class WHERE relname = 'artists'"
            ).fetchone()
            assert row is not None and row[0] == "u"

    def test_write_chunk_records_table_timings(self, pg_dsn, entity, ipc_dict):
        """write_chunk returns per-table flush timing, plus ``_commit``."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        assert "artists" in timings
        assert "_commit" in timings
        assert all(v >= 0 for v in timings.values())

    def test_empty_batch_records_table_timings(self, pg_dsn, entity):
        """Empty batches still get a timing entry."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(empty_ipc("artists")))
            writer.finalize(entity)
        finally:
            writer.close()

        assert "artists" in timings

    def test_single_index_worker(self, pg_dsn, entity, ipc_dict):
        """Single index worker uses sequential index creation."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, index_workers=1, overwrite=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            indexes = {
                row[0]
                for row in conn.execute(
                    "SELECT indexname FROM pg_indexes WHERE tablename = 'artist_aliases'"
                ).fetchall()
            }
            assert any("artist_id" in idx for idx in indexes)

    def test_close_without_finalize(self, pg_dsn, entity, ipc_dict):
        """close() without finalize() should not error."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.close()

    def test_multi_writer(self, pg_dsn, entity, ipc_dict):
        """Multi-writer mode distributes tables across write workers."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            row = conn.execute("SELECT COUNT(*) FROM artists").fetchone()
            assert row is not None and row[0] == 2

    def test_multi_writer_records_group_timings(self, pg_dsn, entity, ipc_dict):
        """Multi-writer mode merges per-group timings into one dict."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        assert set(timings) == set(entity.table_order) | {"_commit"}
        assert all(v >= 0 for v in timings.values())

    def test_multi_writer_close_without_finalize(self, pg_dsn, entity, ipc_dict):
        """Multi-writer close() without finalize() cleans up executor and connections."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.close()  # close without finalize

    def test_fk_constraints(self, pg_dsn, entity, ipc_dict):
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, fk=True, overwrite=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            fks = conn.execute(
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE constraint_type = 'FOREIGN KEY' AND table_schema = 'public'"
            ).fetchall()
            assert len(fks) == len(entity.table_order) - 1

    def test_load_does_not_need_superuser(self, pg_dsn, entity, ipc_dict):
        """A role with USAGE/CREATE on its schema, but not superuser, can load (#26)."""
        import psycopg
        from psycopg import sql

        role = f"loader_{uuid.uuid4().hex[:8]}"
        parts = urlsplit(pg_dsn)
        role_dsn = parts._replace(
            netloc=f"{role}:pw@{parts.hostname}:{parts.port}"
        ).geturl()
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'pw'").format(
                    sql.Identifier(role)
                )
            )
            conn.execute(
                sql.SQL("GRANT USAGE, CREATE ON SCHEMA public TO {}").format(
                    sql.Identifier(role)
                )
            )
            # Tables may already exist, owned by whatever role an earlier test used;
            # dropping them here lets the new role create (and own) them fresh.
            tables = sql.SQL(", ").join(sql.Identifier(t) for t in entity.table_order)
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(tables))
            is_superuser = conn.execute(
                "SELECT rolsuper FROM pg_roles WHERE rolname = %s", (role,)
            ).fetchone()
        assert is_superuser == (False,)

        try:
            _load(role_dsn, entity, ipc_dict, overwrite=True)

            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute("SELECT COUNT(*) FROM artists").fetchone()
            assert count == (2,)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


@pytest.mark.integration
class TestOverwriteSafety:
    """``--overwrite`` replaces only discogskit's own tables, in the schema it loads into."""

    @pytest.fixture()
    def schemas(self, pg_dsn):
        """Fresh ``target`` and ``other`` schemas, and a DSN with search_path = target, other."""
        import psycopg
        from psycopg import sql

        suffix = uuid.uuid4().hex[:8]
        target, other = f"target_{suffix}", f"other_{suffix}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            for name in (target, other):
                conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        yield target, other, f"{pg_dsn}?options=-csearch_path%3D{target}%2C{other}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            for name in (target, other):
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name))
                )

    def test_refuses_to_drop_tables_other_objects_depend_on(
        self, entity, ipc_dict, schemas
    ):
        import psycopg
        from psycopg import sql

        from discogskit.writers import OutputExistsError

        target, _, dsn = schemas
        _load(dsn, entity, ipc_dict, overwrite=True)
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("CREATE VIEW artist_names AS SELECT name FROM artists")
            conn.execute("CREATE VIEW alias_names AS SELECT * FROM artist_aliases")

        with pytest.raises(OutputExistsError) as excinfo:
            _load(dsn, entity, ipc_dict, overwrite=True)
        # Every blocker is listed, not just the first table's.
        assert "artist_names" in str(excinfo.value)
        assert "alias_names" in str(excinfo.value)

        # All or nothing: no table was dropped, not even ones the view doesn't use.
        with psycopg.connect(dsn) as conn:
            counts = {
                t: conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(target, t))
                ).fetchone()
                for t in entity.table_order
            }
            view = conn.execute("SELECT COUNT(*) FROM artist_names").fetchone()
        assert counts["artists"] == (2,)
        assert counts["artist_aliases"] == (1,)
        assert view == (2,)

    def test_loads_into_current_schema_leaving_same_names_elsewhere_alone(
        self, entity, ipc_dict, schemas
    ):
        """search_path = target, other: unqualified DROP would hit other.artists."""
        import psycopg
        from psycopg import sql

        target, other, dsn = schemas
        other_artists = sql.Identifier(other, "artists")
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE TABLE {} (id integer)").format(other_artists))
            conn.execute(sql.SQL("INSERT INTO {} VALUES (42)").format(other_artists))

        _load(dsn, entity, ipc_dict, overwrite=False)

        with psycopg.connect(dsn) as conn:
            loaded = conn.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(target, "artists")
                )
            ).fetchone()
            untouched = conn.execute(
                sql.SQL("SELECT id FROM {}").format(other_artists)
            ).fetchall()
        assert loaded == (2,)
        assert untouched == [(42,)]

    def test_existing_tables_in_current_schema_need_overwrite(
        self, entity, ipc_dict, schemas
    ):
        from discogskit.writers import OutputExistsError

        _, _, dsn = schemas
        _load(dsn, entity, ipc_dict, overwrite=False)

        with pytest.raises(OutputExistsError, match="--overwrite"):
            _load(dsn, entity, ipc_dict, overwrite=False)

    def test_failed_write_connection_leaves_tables_untouched(
        self, entity, ipc_dict, schemas, monkeypatch
    ):
        """A write connection that can't open stops setup() before any drop (#108)."""
        import adbc_driver_postgresql.dbapi as adbc_pg
        import psycopg
        from psycopg import sql

        from discogskit.writers.postgresql import PostgreSQLWriter

        target, _, dsn = schemas
        _load(dsn, entity, ipc_dict, overwrite=True)

        real_connect = adbc_pg.connect
        opened = []

        def connect_once(*args, **kwargs):
            if opened:
                raise RuntimeError("too many connections")
            opened.append(real_connect(*args, **kwargs))
            return opened[-1]

        monkeypatch.setattr(adbc_pg, "connect", connect_once)
        app_name = f"discogskit_{uuid.uuid4().hex[:8]}"
        writer = PostgreSQLWriter(
            f"{dsn}&application_name={app_name}", overwrite=True, write_workers=2
        )
        try:
            with pytest.raises(RuntimeError, match="too many connections"):
                writer.setup(entity)
            # Only the writer's own DDL connection is left; a closed backend can
            # take a moment to leave pg_stat_activity.
            deadline = time.monotonic() + 5
            with psycopg.connect(dsn, autocommit=True) as conn:
                while True:
                    open_conns = conn.execute(
                        "SELECT COUNT(*) FROM pg_stat_activity "
                        "WHERE application_name = %s",
                        (app_name,),
                    ).fetchone()
                    if open_conns == (1,) or time.monotonic() > deadline:
                        break
                    time.sleep(0.05)
        finally:
            writer.close()
        assert open_conns == (1,)

        with psycopg.connect(dsn) as conn:
            counts = {
                t: conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(target, t))
                ).fetchone()
                for t in entity.table_order
            }
        assert counts["artists"] == (2,)
        assert counts["artist_aliases"] == (1,)


@pytest.mark.integration
class TestWriteGroupFailure:
    """With several write workers, one group's error waits for the others (#108)."""

    @pytest.fixture()
    def groups(self, monkeypatch):
        """Make the group holding ``artists`` fail at once and the other wait for a gate.

        The failing group comes first, so collecting results in order hits its
        error while the other group is still writing. Yields the gate, an event
        set once the sibling group waits on it, and the list of events the
        sibling group records.
        """
        from discogskit.writers import postgresql

        real_flush = postgresql._flush_group
        gate = threading.Event()
        waiting = threading.Event()
        events = []

        def flush(conn, tables, schemas):
            if "artists" in tables:
                raise RuntimeError("boom")
            waiting.set()
            gate.wait(30)
            timings = real_flush(conn, tables, schemas)
            events.append("sibling committed")
            return timings

        monkeypatch.setattr(postgresql, "_flush_group", flush)
        yield gate, waiting, events
        gate.set()

    def test_write_chunk_raises_after_sibling_groups_finish(
        self, pg_dsn, artists_xml_file, groups
    ):
        from discogskit.writers.postgresql import PostgreSQLWriter

        gate, _, events = groups
        entity = ENTITIES["artists"]
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=size, entity="artists", file_path=str(artists_xml_file), start=0
            )
        ).tables
        errors = []
        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)

        def write():
            try:
                writer.write_chunk(ipc_to_record_batches(ipc_dict))
            except RuntimeError as exc:
                errors.append(exc)
                events.append("raised")

        try:
            writer.setup(entity)
            thread = threading.Thread(target=write)
            thread.start()
            thread.join(1)
            returned_early = not thread.is_alive()
            gate.set()
            thread.join(30)
        finally:
            writer.close()

        assert not returned_early
        assert [str(e) for e in errors] == ["boom"]
        assert events == ["sibling committed", "raised"]

    def test_cli_reports_the_group_error_after_siblings_finish(
        self, pg_dsn, tmp_path, groups, monkeypatch
    ):
        from discogskit.cli import app
        from discogskit.writers.postgresql import PostgreSQLWriter

        gate, waiting, events = groups
        real_close = PostgreSQLWriter.close

        def close(self):
            events.append("close")
            real_close(self)

        monkeypatch.setattr(PostgreSQLWriter, "close", close)
        gz = tmp_path / "discogs_20260301_artists.xml.gz"
        with gzip.open(gz, "wb") as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(ARTISTS_XML.encode())
            f.write(b"</artists>")

        # The CLI runs on this thread, so another opens the gate a second after
        # the sibling group reaches it: long enough for a close() that doesn't
        # wait for the sibling to come first.
        def open_gate():
            if waiting.wait(30):
                time.sleep(1)
            gate.set()

        opener = threading.Thread(target=open_gate)
        opener.start()
        try:
            result = CliRunner().invoke(
                app,
                [
                    "load",
                    str(gz),
                    "--dsn",
                    pg_dsn,
                    "--no-progress",
                    "--overwrite",
                    "--parse-workers",
                    "1",
                    "--pg-write-workers",
                    "2",
                ],
            )
        finally:
            waiting.set()
            opener.join()

        assert result.exit_code == 1, click.unstyle(result.output)
        assert "Error: boom" in click.unstyle(result.output)
        assert events == ["sibling committed", "close"]


class TestWriteWorkerCap:
    def test_workers_capped_at_table_count(self, monkeypatch):
        """Three workers on labels (two tables) open two write connections, not three."""
        import adbc_driver_postgresql.dbapi as adbc_pg
        import psycopg

        from discogskit.writers.postgresql import PostgreSQLWriter

        ddl_conn = MagicMock()
        ddl_conn.execute.return_value.fetchone.return_value = ("public",)
        monkeypatch.setattr(psycopg, "connect", MagicMock(return_value=ddl_conn))
        adbc_connect = MagicMock()
        monkeypatch.setattr(adbc_pg, "connect", adbc_connect)

        writer = PostgreSQLWriter(
            "postgresql://unused", overwrite=True, write_workers=3
        )
        try:
            writer.setup(ENTITIES["labels"])
        finally:
            writer.close()

        assert adbc_connect.call_count == 2


class TestCancelRequests:
    """cancel() never blocks its caller, and close() never frees a connection a cancel request still uses (#110)."""

    @staticmethod
    def _writer(monkeypatch, *conns):
        import adbc_driver_postgresql.dbapi as adbc_pg
        import psycopg

        from discogskit.writers import postgresql
        from discogskit.writers.postgresql import PostgreSQLWriter

        ddl_conn = MagicMock()
        ddl_conn.execute.return_value.fetchone.return_value = ("public",)
        monkeypatch.setattr(psycopg, "connect", MagicMock(return_value=ddl_conn))
        monkeypatch.setattr(adbc_pg, "connect", MagicMock(side_effect=conns))
        monkeypatch.setattr(postgresql, "_CLOSE_TIMEOUT", 0.2)
        writer = PostgreSQLWriter(
            "postgresql://unused", overwrite=True, write_workers=len(conns)
        )
        writer.setup(ENTITIES["artists"])
        return writer

    @staticmethod
    def _wait_until(condition):
        deadline = time.monotonic() + 5
        while not condition():
            assert time.monotonic() < deadline
            time.sleep(0.01)

    def test_every_write_connection_is_cancelled_then_closed(self, monkeypatch):
        conns = MagicMock(), MagicMock()
        writer = self._writer(monkeypatch, *conns)
        try:
            writer.cancel()
            self._wait_until(lambda: all(c.adbc_cancel.called for c in conns))
        finally:
            writer.close()
        for conn in conns:
            conn.close.assert_called_once()

    def test_unanswered_cancel_request_blocks_neither_cancel_nor_close(
        self, monkeypatch
    ):
        """A server that never answers a cancel request mustn't hang the interrupt.

        close() leaves the write connections open rather than free the cancel handle the request still uses.
        """
        release = threading.Event()
        conn = MagicMock()
        conn.adbc_cancel = MagicMock(side_effect=lambda: release.wait(10))
        writer = self._writer(monkeypatch, conn)
        try:
            t0 = time.monotonic()
            writer.cancel()
            writer.cancel()
            elapsed = time.monotonic() - t0
            self._wait_until(lambda: conn.adbc_cancel.called)
            writer.close()
        finally:
            release.set()

        assert elapsed < 1
        conn.close.assert_not_called()


@contextmanager
def _locked(dsn, tables, *, hold):
    """Hold ACCESS EXCLUSIVE locks on *tables* from another session for up to *hold* seconds."""
    import psycopg
    from psycopg import sql

    locked = threading.Event()
    release = threading.Event()

    def run():
        with psycopg.connect(dsn) as conn:
            conn.execute(
                sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(
                    sql.SQL(", ").join(sql.Identifier(t) for t in tables)
                )
            )
            locked.set()
            release.wait(hold)
            conn.rollback()

    holder = threading.Thread(target=run)
    holder.start()
    try:
        assert locked.wait(10)
        yield
    finally:
        release.set()
        holder.join()


@pytest.mark.integration
class TestIndexAbort:
    """An index error or an interrupt stops parallel index creation promptly (#109).

    Child tables are locked from another session so their CREATE INDEX waits;
    the lock lifts after ``_HOLD`` seconds, so a finalize() that waits for
    every index still ends, just late.
    """

    _HOLD = 15

    @pytest.fixture()
    def dsn(self, pg_dsn):
        import psycopg
        from psycopg import sql

        schema = f"idx_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        yield f"{pg_dsn}?options=-csearch_path%3D{schema}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )

    @staticmethod
    def _indexed_tables(dsn):
        import psycopg

        with psycopg.connect(dsn) as conn:
            rows = conn.execute(
                "SELECT tablename FROM pg_indexes WHERE schemaname = current_schema()"
            ).fetchall()
        return {r[0] for r in rows}

    def _assert_failed_index_skips_queued_ones(self, dsn, monkeypatch):
        import psycopg
        from psycopg import sql

        from discogskit.writers.postgresql import PostgreSQLWriter

        entity = ENTITIES["releases"]
        root, failing, *waiting = entity.table_order

        # The failing CREATE INDEX runs only once a sibling is about to start
        # its own, so there is a running build to cancel.
        real_execute = psycopg.Connection.execute
        sibling_started = threading.Event()

        def execute(conn, query, *args, **kwargs):
            if isinstance(query, sql.Composable):
                text = query.as_string(conn)
                if text.startswith("CREATE INDEX"):
                    if f'"{failing}"' in text:
                        assert sibling_started.wait(10)
                    else:
                        sibling_started.set()
            return real_execute(conn, query, *args, **kwargs)

        monkeypatch.setattr(psycopg.Connection, "execute", execute)
        writer = PostgreSQLWriter(dsn, overwrite=True, index_workers=2)
        try:
            writer.setup(entity)
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(failing)))
            with _locked(dsn, waiting, hold=self._HOLD):
                t0 = time.monotonic()
                with pytest.raises(psycopg.errors.UndefinedTable):
                    writer.finalize(entity)
                elapsed = time.monotonic() - t0
        finally:
            writer.close()

        assert elapsed < self._HOLD / 2
        # Only the primary key exists: queued indexes never ran, and the ones
        # waiting on the lock were cancelled.
        assert self._indexed_tables(dsn) == {root}

    def test_failed_index_skips_queued_ones(self, dsn, monkeypatch):
        self._assert_failed_index_skips_queued_ones(dsn, monkeypatch)

    def test_interrupt_during_cancel_neither_stops_it_nor_replaces_the_error(
        self, dsn, monkeypatch
    ):
        """A second Ctrl+C while cancelling: builds still stop, the first error wins."""
        import psycopg

        real_cancel = psycopg.Connection.cancel_safe
        interrupted = []

        def cancel_safe(conn, *args, **kwargs):
            if not interrupted:
                interrupted.append(conn)
                raise KeyboardInterrupt
            return real_cancel(conn, *args, **kwargs)

        monkeypatch.setattr(psycopg.Connection, "cancel_safe", cancel_safe)
        self._assert_failed_index_skips_queued_ones(dsn, monkeypatch)
        assert interrupted

    def test_interrupt_while_submitting_cancels_started_indexes(self, dsn, monkeypatch):
        """Ctrl+C before every index is queued still cancels the ones already running."""
        import psycopg
        from psycopg import sql

        from discogskit.writers import postgresql
        from discogskit.writers.postgresql import PostgreSQLWriter

        entity = ENTITIES["releases"]
        root, *children = entity.table_order
        workers = 2

        # Each started CREATE INDEX waits on the lock; the interrupt lands on the
        # submit after the workers' tasks, once both have reached execute.
        real_execute = psycopg.Connection.execute
        started = threading.Semaphore(0)

        def execute(conn, query, *args, **kwargs):
            if isinstance(query, sql.Composable) and query.as_string(conn).startswith(
                "CREATE INDEX"
            ):
                started.release()
            return real_execute(conn, query, *args, **kwargs)

        class InterruptingExecutor(postgresql.ThreadPoolExecutor):
            submitted = 0

            def submit(self, *args, **kwargs):
                if self.submitted == workers:
                    for _ in range(workers):
                        assert started.acquire(timeout=10)
                    raise KeyboardInterrupt
                self.submitted += 1
                return super().submit(*args, **kwargs)

        monkeypatch.setattr(psycopg.Connection, "execute", execute)
        monkeypatch.setattr(postgresql, "ThreadPoolExecutor", InterruptingExecutor)
        writer = PostgreSQLWriter(dsn, overwrite=True, index_workers=workers)
        try:
            writer.setup(entity)
            with _locked(dsn, children, hold=self._HOLD):
                t0 = time.monotonic()
                with pytest.raises(KeyboardInterrupt):
                    writer.finalize(entity)
                elapsed = time.monotonic() - t0
        finally:
            writer.close()

        assert elapsed < self._HOLD / 2
        assert self._indexed_tables(dsn) == {root}

    def test_interrupt_cancels_running_index(self, dsn):
        import psycopg

        from discogskit.writers.postgresql import PostgreSQLWriter

        entity = ENTITIES["artists"]
        root, *children = entity.table_order
        main = threading.main_thread().ident
        assert main is not None
        sent = threading.Event()

        def interrupt_once_an_index_waits():
            deadline = time.monotonic() + self._HOLD
            with psycopg.connect(dsn, autocommit=True) as conn:
                while time.monotonic() < deadline:
                    row = conn.execute(
                        "SELECT COUNT(*) FROM pg_stat_activity "
                        "WHERE query LIKE 'CREATE INDEX%' AND wait_event_type = 'Lock'"
                    ).fetchone()
                    if row and row[0]:
                        sent.set()
                        signal.pthread_kill(main, signal.SIGINT)
                        return
                    time.sleep(0.05)

        writer = PostgreSQLWriter(dsn, overwrite=True, index_workers=2)
        try:
            writer.setup(entity)
            with _locked(dsn, children, hold=self._HOLD):
                interrupter = threading.Thread(target=interrupt_once_an_index_waits)
                interrupter.start()
                t0 = time.monotonic()
                try:
                    with pytest.raises(KeyboardInterrupt):
                        writer.finalize(entity)
                finally:
                    elapsed = time.monotonic() - t0
                    interrupter.join()
        finally:
            writer.close()

        assert sent.is_set()
        assert elapsed < self._HOLD / 2
        assert self._indexed_tables(dsn) == {root}


@pytest.mark.integration
class TestInterruptCancelsCopy:
    """Ctrl+C cancels a COPY the server blocks, instead of waiting for it (#110).

    The tables are locked from another session once ``setup()`` has created
    them, so the first COPY waits; the lock lifts after ``_HOLD`` seconds, so a
    run that waits for the COPY still ends, just late.
    """

    _HOLD = 15

    @pytest.fixture()
    def dsn(self, pg_dsn):
        import psycopg
        from psycopg import sql

        schema = f"copy_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        yield f"{pg_dsn}?options=-csearch_path%3D{schema}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )

    @staticmethod
    def _backends(conn, app_name, *, copy_waiting_on_lock=False):
        query = (
            "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = %s "
            "AND query LIKE 'COPY%%' AND wait_event_type = 'Lock'"
            if copy_waiting_on_lock
            else "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = %s"
        )
        row = conn.execute(query, (app_name,)).fetchone()
        assert row is not None
        return row[0]

    @pytest.mark.parametrize("write_workers", [1, 2])
    def test_interrupt_cancels_blocked_copy(
        self, dsn, tmp_path, capsys, monkeypatch, write_workers
    ):
        import psycopg

        from discogskit import pipeline
        from discogskit.writers.postgresql import PostgreSQLWriter

        gz = tmp_path / "discogs_20260301_artists.xml.gz"
        with gzip.open(gz, "wb") as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(ARTISTS_XML.encode())
            f.write(b"</artists>")
        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=gz,
            keep_xml=False,
            parse_workers=1,
            profile=False,
            progress=False,
            strict=False,
            write_queue=1,
        )
        main = threading.main_thread().ident
        assert main is not None
        sent = threading.Event()
        app_name = f"discogskit_{uuid.uuid4().hex[:8]}"

        def interrupt_once_a_copy_waits():
            deadline = time.monotonic() + self._HOLD
            with psycopg.connect(dsn, autocommit=True) as conn:
                while time.monotonic() < deadline:
                    if self._backends(conn, app_name, copy_waiting_on_lock=True):
                        sent.set()
                        signal.pthread_kill(main, signal.SIGINT)
                        return
                    time.sleep(0.05)

        interrupter = threading.Thread(target=interrupt_once_a_copy_waits)
        real_setup = PostgreSQLWriter.setup
        stack = ExitStack()

        def setup(writer, entity):
            real_setup(writer, entity)
            stack.enter_context(_locked(dsn, entity.table_order, hold=self._HOLD))
            interrupter.start()

        monkeypatch.setattr(PostgreSQLWriter, "setup", setup)
        writer = PostgreSQLWriter(
            f"{dsn}&application_name={app_name}",
            overwrite=True,
            write_workers=write_workers,
        )
        try:
            with stack:
                t0 = time.monotonic()
                try:
                    with pytest.raises(KeyboardInterrupt):
                        pipeline.run(config, writer)
                finally:
                    elapsed = time.monotonic() - t0
                    if interrupter.is_alive():
                        interrupter.join()
        finally:
            writer.close()

        # A closed connection's backend can take a moment to leave pg_stat_activity.
        with psycopg.connect(dsn, autocommit=True) as conn:
            deadline = time.monotonic() + 5
            while (left := self._backends(conn, app_name)) and (
                time.monotonic() < deadline
            ):
                time.sleep(0.05)
        assert left == 0
        assert sent.is_set()
        assert elapsed < self._HOLD / 2
        assert "Interrupted" in capsys.readouterr().out


@pytest.mark.integration
class TestPgSchema:
    """``--pg-schema``/``--pg-create-schema``: target a specific schema (#53)."""

    def test_rejects_nonexistent_schema_without_create_flag(self, pg_dsn):
        from discogskit.writers.postgresql import PostgreSQLWriter

        schema = f"missing_{uuid.uuid4().hex[:8]}"
        with pytest.raises(ValueError, match=f"Schema '{schema}' does not exist"):
            PostgreSQLWriter(pg_dsn, schema=schema)

    def test_create_schema_creates_and_loads_into_it(self, pg_dsn, entity, ipc_dict):
        """A schema that doesn't exist yet is created with --pg-create-schema."""
        import psycopg
        from psycopg import sql

        schema = f"new_{uuid.uuid4().hex[:8]}"
        try:
            _load(pg_dsn, entity, ipc_dict, create_schema=True, schema=schema)

            with psycopg.connect(pg_dsn) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,)
                ).fetchone()
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
                # The PK index and every child table's FK index (built over the
                # per-index connections, not self._conn) must land in the
                # chosen schema, not just the PK alone.
                indexes = conn.execute(
                    "SELECT indexname FROM pg_indexes WHERE schemaname = %s",
                    (schema,),
                ).fetchall()
            assert exists is not None
            assert count == (2,)
            assert len(indexes) == len(entity.table_order)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )

    def test_loads_into_existing_schema_without_create_flag(
        self, pg_dsn, entity, ipc_dict
    ):
        import psycopg
        from psycopg import sql

        schema = f"existing_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            _load(pg_dsn, entity, ipc_dict, schema=schema)

            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
            assert count == (2,)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    def test_mixed_case_schema_name(self, pg_dsn, entity, ipc_dict):
        """A name outside [a-z_][a-z0-9_]* must resolve the same on every connection."""
        import psycopg
        from psycopg import sql

        schema = f"MixedCase_{uuid.uuid4().hex[:8]}"
        try:
            _load(pg_dsn, entity, ipc_dict, create_schema=True, schema=schema)

            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
                # Nothing silently landed in the lowercase-folded twin.
                lower_exists = conn.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname = %s",
                    (schema.lower(),),
                ).fetchone()
            assert count == (2,)
            assert lower_exists is None
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    def test_pgoptions_reach_every_connection(self, pg_dsn, tmp_path, monkeypatch):
        """``load --pg-schema`` runs its write and index connections with PGOPTIONS too, not only its DDL one
        (#223)."""
        import adbc_driver_postgresql.dbapi as adbc_pg
        import psycopg
        from psycopg import sql

        from discogskit.cli import app

        adbc_connect = adbc_pg.connect
        psycopg_connect = psycopg.connect
        # (library, maintenance_work_mem, current_schema()) of each connection the load opens.
        seen: list[tuple[str, str, str]] = []

        def probe_adbc(*args, **kwargs):
            conn = adbc_connect(*args, **kwargs)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_setting('maintenance_work_mem'), current_schema()"
                )
                row = cur.fetchone()
            assert row is not None
            seen.append(("adbc", row[0], row[1]))
            return conn

        def probe_psycopg(*args, **kwargs):
            conn = psycopg_connect(*args, **kwargs)
            row = conn.execute(
                "SELECT current_setting('maintenance_work_mem'), current_schema()"
            ).fetchone()
            assert row is not None
            seen.append(("psycopg", row[0], row[1]))
            return conn

        gz = tmp_path / "discogs_20260301_artists.xml.gz"
        with gzip.open(gz, "wt") as f:
            f.write(
                f"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n{ARTISTS_XML}</artists>"
            )
        schema = f"pgoptions_{uuid.uuid4().hex[:8]}"
        args = [
            "load",
            str(gz),
            "--dsn",
            pg_dsn,
            "--no-progress",
            "--parse-workers",
            "1",
        ]
        args += ["--pg-create-schema", "--pg-schema", schema]
        args += ["--pg-index-workers", "2", "--pg-write-workers", "2"]
        monkeypatch.setenv("PGOPTIONS", "-c maintenance_work_mem=77MB")
        try:
            with (
                patch.object(adbc_pg, "connect", probe_adbc),
                patch.object(psycopg, "connect", probe_psycopg),
            ):
                result = CliRunner().invoke(app, args)
            assert result.exit_code == 0, click.unstyle(result.output)
        finally:
            monkeypatch.delenv("PGOPTIONS")
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )

        assert {setting for _, setting, _ in seen} == {"77MB"}
        children = len(ENTITIES["artists"].table_order) - 1
        # The schema check's connection and the two write connections.
        assert seen.count(("adbc", "77MB", schema)) == 3
        # One connection per child table's index; the DDL connection is opened before search_path is set.
        assert seen.count(("psycopg", "77MB", schema)) == children

    def test_schema_name_with_a_space(self, pg_dsn, entity, ipc_dict):
        import psycopg
        from psycopg import sql

        schema = f"my schema {uuid.uuid4().hex[:8]}"
        try:
            _load(pg_dsn, entity, ipc_dict, create_schema=True, schema=schema)

            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
            assert count == (2,)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    def test_preserves_existing_options_in_dsn(self, pg_dsn):
        """--pg-schema must not discard other -c settings already in the DSN's options."""
        import psycopg
        from psycopg import sql

        from discogskit.writers.postgresql import PostgreSQLWriter

        schema = f"opts_{uuid.uuid4().hex[:8]}"
        dsn = f"{pg_dsn}?options=-c%20work_mem%3D7MB"
        writer = PostgreSQLWriter(dsn, create_schema=True, schema=schema)
        try:
            with psycopg.connect(writer._dsn, autocommit=True) as conn:
                work_mem = conn.execute("SHOW work_mem").fetchone()
                current = conn.execute("SELECT current_schema()").fetchone()
            assert work_mem == ("7MB",)
            assert current == (schema,)
        finally:
            writer.close()
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    def test_authority_less_uri(self, pg_dsn, entity, ipc_dict):
        """``postgresql:///db?host=...`` (no ``@host:port``) must still resolve the schema."""
        import psycopg
        from psycopg import sql

        parts = urlsplit(pg_dsn)
        dbname = parts.path.lstrip("/")
        dsn = (
            f"postgresql:///{dbname}?host={parts.hostname}&port={parts.port}"
            f"&user={parts.username}&password={parts.password}"
        )
        schema = f"noauth_{uuid.uuid4().hex[:8]}"
        try:
            _load(dsn, entity, ipc_dict, create_schema=True, schema=schema)

            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
            assert count == (2,)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    def test_overwrite_safety_inside_chosen_schema(self, pg_dsn, entity, ipc_dict):
        """Same #52 safety (no CASCADE, all-or-nothing, blockers listed), scoped to --pg-schema."""
        import psycopg
        from psycopg import sql

        from discogskit.writers import OutputExistsError

        schema = f"safety_{uuid.uuid4().hex[:8]}"
        try:
            _load(
                pg_dsn,
                entity,
                ipc_dict,
                create_schema=True,
                overwrite=True,
                schema=schema,
            )
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("CREATE VIEW {} AS SELECT name FROM {}").format(
                        sql.Identifier(schema, "artist_names"),
                        sql.Identifier(schema, "artists"),
                    )
                )

            with pytest.raises(OutputExistsError) as excinfo:
                _load(pg_dsn, entity, ipc_dict, overwrite=True, schema=schema)
            assert "artist_names" in str(excinfo.value)

            # All or nothing: the table the view depends on was not dropped.
            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
            assert count == (2,)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    def test_multi_writer_lands_in_chosen_schema(self, pg_dsn, entity, ipc_dict):
        import psycopg
        from psycopg import sql

        schema = f"multi_{uuid.uuid4().hex[:8]}"
        try:
            _load(
                pg_dsn,
                entity,
                ipc_dict,
                create_schema=True,
                overwrite=True,
                schema=schema,
                write_workers=2,
            )
            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
            assert count == (2,)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )

    def test_missing_create_privilege_is_a_clear_error(self, pg_dsn):
        """--pg-create-schema needs CREATE on the database; a role without it fails clearly."""
        import psycopg
        from psycopg import sql

        from discogskit.writers.postgresql import PostgreSQLWriter

        role = f"loader_{uuid.uuid4().hex[:8]}"
        schema = f"needs_create_{uuid.uuid4().hex[:8]}"
        parts = urlsplit(pg_dsn)
        dbname = parts.path.lstrip("/")
        role_dsn = parts._replace(
            netloc=f"{role}:pw@{parts.hostname}:{parts.port}"
        ).geturl()
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'pw'").format(
                    sql.Identifier(role)
                )
            )
            conn.execute(
                sql.SQL("REVOKE CREATE ON DATABASE {} FROM {}").format(
                    sql.Identifier(dbname), sql.Identifier(role)
                )
            )
        try:
            with pytest.raises(ValueError, match="Can't create schema"):
                PostgreSQLWriter(role_dsn, create_schema=True, schema=schema)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))

    def test_create_schema_flag_works_without_database_create(
        self, pg_dsn, entity, ipc_dict
    ):
        """A role with rights on an already-existing schema, but not database CREATE,
        must still be able to load with --pg-create-schema (#53, finding on PR #74).
        """
        import psycopg
        from psycopg import sql

        role = f"loader_{uuid.uuid4().hex[:8]}"
        schema = f"existing_create_{uuid.uuid4().hex[:8]}"
        parts = urlsplit(pg_dsn)
        dbname = parts.path.lstrip("/")
        role_dsn = parts._replace(
            netloc=f"{role}:pw@{parts.hostname}:{parts.port}"
        ).geturl()
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'pw'").format(
                    sql.Identifier(role)
                )
            )
            conn.execute(
                sql.SQL("GRANT ALL ON SCHEMA {} TO {}").format(
                    sql.Identifier(schema), sql.Identifier(role)
                )
            )
            conn.execute(
                sql.SQL("REVOKE CREATE ON DATABASE {} FROM {}").format(
                    sql.Identifier(dbname), sql.Identifier(role)
                )
            )
        try:
            _load(role_dsn, entity, ipc_dict, create_schema=True, schema=schema)

            with psycopg.connect(pg_dsn) as conn:
                count = conn.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(
                        sql.Identifier(schema, "artists")
                    )
                ).fetchone()
            assert count == (2,)
        finally:
            with psycopg.connect(pg_dsn, autocommit=True) as conn:
                conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )


@pytest.mark.integration
class TestExistingTablesCheckedUpFront:
    """Existing tables for any job fail the load before the first job starts (#102)."""

    @pytest.fixture()
    def dsn(self, pg_dsn):
        import psycopg
        from psycopg import sql

        schema = f"upfront_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        yield f"{pg_dsn}?options=-csearch_path%3D{schema}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )

    @staticmethod
    def _gz(tmp_path, entity, xml):
        gz = tmp_path / f"discogs_20260301_{entity}.xml.gz"
        with gzip.open(gz, "wb") as f:
            f.write(f"<?xml version='1.0' encoding='UTF-8'?>\n<{entity}>\n".encode())
            f.write(xml.encode())
            f.write(f"</{entity}>".encode())
        return gz

    @staticmethod
    def _load_cli(dsn, *paths, overwrite=False):
        from discogskit.cli import app

        args = ["load", *map(str, paths), "--dsn", dsn, "--no-progress"]
        args += ["--parse-workers", "1"]
        if overwrite:
            args.append("--overwrite")
        return CliRunner().invoke(app, args)

    @staticmethod
    def _tables(dsn):
        import psycopg

        with psycopg.connect(dsn) as conn:
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = current_schema()"
            ).fetchall()
        return {r[0] for r in rows}

    def test_existing_second_entity_fails_before_first_job(self, dsn, tmp_path):
        artists = self._gz(tmp_path, "artists", ARTISTS_XML)
        labels = self._gz(tmp_path, "labels", LABELS_XML)
        first = self._load_cli(dsn, labels)
        assert first.exit_code == 0, click.unstyle(first.output)
        labels_tables = set(ENTITIES["labels"].table_order)
        assert self._tables(dsn) == labels_tables

        with patch("discogskit.cli.pipeline.run") as run:
            result = self._load_cli(dsn, artists, labels)
        output = click.unstyle(result.output)
        assert result.exit_code == 1, output
        # Rich may wrap the message at the terminal width.
        message = " ".join(output.split())
        assert message.startswith("Error: Tables already exist in schema upfront_")
        assert message.endswith(
            "(e.g. label_sublabels). Use --overwrite to replace them."
        )
        run.assert_not_called()
        assert self._tables(dsn) == labels_tables

    def test_overwrite_replaces_existing_tables(self, dsn, tmp_path):
        artists = self._gz(tmp_path, "artists", ARTISTS_XML)
        labels = self._gz(tmp_path, "labels", LABELS_XML)
        first = self._load_cli(dsn, labels)
        assert first.exit_code == 0, click.unstyle(first.output)

        result = self._load_cli(dsn, artists, labels, overwrite=True)
        assert result.exit_code == 0, click.unstyle(result.output)
        assert self._tables(dsn) == {
            *ENTITIES["artists"].table_order,
            *ENTITIES["labels"].table_order,
        }


def _libpq_options(options: str) -> list[str]:
    """Split a libpq ``options`` value as the server does: whitespace separates arguments, and a
    backslash makes the next character literal."""
    args: list[str] = []
    arg: list[str] | None = None
    chars = iter(options)
    for ch in chars:
        if ch.isspace():
            if arg is not None:
                args.append("".join(arg))
                arg = None
            continue
        if arg is None:
            arg = []
        arg.append(next(chars) if ch == "\\" else ch)
    if arg is not None:
        args.append("".join(arg))
    return args


class TestDsnWithSchema:
    @pytest.mark.parametrize(
        "dsn, pgoptions, existing",
        [
            (
                "postgresql://u@h/db",
                "-c maintenance_work_mem=777MB -cstatement_timeout=0",
                ["-c", "maintenance_work_mem=777MB", "-cstatement_timeout=0"],
            ),
            # libpq reads PGOPTIONS only when the DSN sets no options, even empty ones.
            (
                "postgresql://u@h/db?options=-cstatement_timeout%3D12345",
                "-c maintenance_work_mem=777MB",
                ["-cstatement_timeout=12345"],
            ),
            ("postgresql://u@h/db?options=", "-c maintenance_work_mem=777MB", []),
            ("host=h dbname=db", None, []),
        ],
        ids=["pgoptions", "dsn-options", "empty-dsn-options", "neither"],
    )
    def test_pgoptions_kept_unless_the_dsn_has_options(
        self, dsn, pgoptions, existing, monkeypatch
    ):
        """Setting options in the DSN stops libpq reading PGOPTIONS, so they're carried over (#223)."""
        from psycopg.conninfo import conninfo_to_dict

        from discogskit.writers.postgresql import _dsn_with_schema

        if pgoptions is None:
            monkeypatch.delenv("PGOPTIONS", raising=False)
        else:
            monkeypatch.setenv("PGOPTIONS", pgoptions)
        options = conninfo_to_dict(_dsn_with_schema(dsn, "discogs")).get("options")
        assert isinstance(options, str)
        assert _libpq_options(options) == [*existing, '-csearch_path="discogs"']

    @pytest.mark.parametrize(
        "schema",
        ["discogs", 'say "hi"', "back\\slash", "tab\tand space", 'all "\\ \t'],
        ids=["plain", "double-quote", "backslash", "whitespace", "everything"],
    )
    @pytest.mark.parametrize(
        "dsn, existing",
        [
            ("postgresql://u@h:5432/db", []),
            (
                "host=h dbname=db options='-cstatement_timeout=0'",
                ["-cstatement_timeout=0"],
            ),
            (
                "postgresql://u@h/db?options=-cwork_mem%3D64MB%20-cgeqo%3Doff",
                ["-cwork_mem=64MB", "-cgeqo=off"],
            ),
        ],
        ids=["url", "key-value-with-options", "url-with-options"],
    )
    def test_search_path_is_the_quoted_schema(self, dsn, existing, schema, monkeypatch):
        """The server reads back exactly the quoted identifier, after any options already set."""
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict

        from discogskit.writers.postgresql import _dsn_with_schema

        monkeypatch.delenv("PGOPTIONS", raising=False)
        params = conninfo_to_dict(_dsn_with_schema(dsn, schema))
        options = params.pop("options")
        assert isinstance(options, str)

        assert _libpq_options(options) == [
            *existing,
            "-csearch_path=" + sql.Identifier(schema).as_string(None),
        ]
        original = conninfo_to_dict(dsn)
        original.pop("options", None)
        assert params == original


class TestSplitTableGroups:
    def test_balances_by_weight(self):
        """Heaviest table first, each into the lightest group: round-robin would pair 4+2 and 3+1."""
        from discogskit.writers.postgresql import _split_table_groups

        entity = dataclasses.replace(
            ENTITIES["artists"],
            table_weights={
                "artist_aliases": 3,
                "artist_groups": 2,
                "artist_members": 1,
                "artists": 4,
            },
        )

        assert _split_table_groups(2, entity) == [
            ["artists", "artist_members"],
            ["artist_aliases", "artist_groups"],
        ]

    def test_distributes_all_tables(self):
        from discogskit.writers.postgresql import _split_table_groups

        entity = ENTITIES["releases"]
        groups = _split_table_groups(3, entity)

        assert len(groups) == 3
        all_tables = [t for g in groups for t in g]
        assert sorted(all_tables) == sorted(entity.table_order)

    def test_more_groups_than_tables(self):
        """Each table gets a group of its own; the extra groups stay empty (the writer caps workers
        at the table count, so it never asks for them)."""
        from discogskit.writers.postgresql import _split_table_groups

        entity = ENTITIES["artists"]
        groups = _split_table_groups(len(entity.table_order) + 2, entity)

        assert sorted(groups) == [[], [], *sorted([t] for t in entity.table_order)]

    def test_single_group(self):
        from discogskit.writers.postgresql import _split_table_groups

        entity = ENTITIES["artists"]
        groups = _split_table_groups(1, entity)

        assert len(groups) == 1
        assert sorted(groups[0]) == sorted(entity.table_order)
