"""Integration tests for PostgreSQLWriter full lifecycle."""

from __future__ import annotations

import os

import pytest

from discogskit.entities import get
from discogskit.entities import ChunkArgs
from discogskit.entities.artists import extract_chunk_to_ipc as artists_extract


@pytest.mark.integration
class TestPostgreSQLWriter:
    @pytest.fixture()
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return artists_extract(ChunkArgs(str(artists_xml_file), 0, size))

    def test_full_lifecycle(self, pg_dsn, entity, ipc_dict):
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        try:
            writer.setup(entity)
            count = writer.write_chunk(ipc_dict, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2

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
            writer.write_chunk(ipc_dict, entity)
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

    def test_write_chunk_with_table_timings(self, pg_dsn, entity, ipc_dict):
        """write_chunk records per-table timing when table_timings is passed."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(ipc_dict, entity, table_timings=timings)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2
        assert "artists" in timings
        assert "_commit" in timings
        assert all(v >= 0 for v in timings.values())

    def test_empty_batch_with_table_timings(self, pg_dsn, entity):
        """Empty batches are timed correctly when table_timings is passed."""
        from discogskit.entities.artists import _cols_to_ipc, _new_cols
        from discogskit.writers.postgresql import PostgreSQLWriter

        empty_ipc = _cols_to_ipc(_new_cols())

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(empty_ipc, entity, table_timings=timings)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 0
        assert "artists" in timings

    def test_single_index_worker(self, pg_dsn, entity, ipc_dict):
        """Single index worker uses sequential index creation."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, index_workers=1, overwrite=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_dict, entity)
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
        writer.write_chunk(ipc_dict, entity)
        writer.close()

    def test_multi_writer(self, pg_dsn, entity, ipc_dict):
        """Multi-writer mode distributes tables across write workers."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        try:
            writer.setup(entity)
            count = writer.write_chunk(ipc_dict, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2

        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            row = conn.execute("SELECT COUNT(*) FROM artists").fetchone()
            assert row is not None and row[0] == 2

    def test_multi_writer_with_timings(self, pg_dsn, entity, ipc_dict):
        """Multi-writer mode with table_timings records per-group timings."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(ipc_dict, entity, table_timings=timings)
            # get_table_timings merges per-group timings
            merged = writer.get_table_timings()
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2
        assert len(merged) > 0

    def test_get_table_timings_without_profile(self, pg_dsn, entity, ipc_dict):
        """get_table_timings returns empty dict when no timings were requested."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_dict, entity)  # no table_timings
            merged = writer.get_table_timings()
            writer.finalize(entity)
        finally:
            writer.close()

        assert merged == {}

    def test_multi_writer_close_without_finalize(self, pg_dsn, entity, ipc_dict):
        """Multi-writer close() without finalize() cleans up executor and connections."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        writer.setup(entity)
        writer.write_chunk(ipc_dict, entity)
        writer.close()  # close without finalize

    def test_tuning(self, pg_dsn, entity, ipc_dict):
        """tune=True applies and resets PostgreSQL tuning parameters."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, tune=True)
        try:
            writer.setup(entity)
            count = writer.write_chunk(ipc_dict, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2

    def test_indexes_only(self, pg_dsn, entity, ipc_dict):
        """finalize() without setup() rebuilds indexes on existing tables."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        # First load data normally
        writer1 = PostgreSQLWriter(pg_dsn, index_workers=1, overwrite=True)
        try:
            writer1.setup(entity)
            writer1.write_chunk(ipc_dict, entity)
            writer1.finalize(entity)
        finally:
            writer1.close()

        # Now rebuild indexes with a fresh writer (no setup)
        writer2 = PostgreSQLWriter(pg_dsn, index_workers=1, overwrite=True)
        try:
            writer2.finalize(entity)
        finally:
            writer2.close()

        import psycopg

        with psycopg.connect(pg_dsn) as conn:
            pk = conn.execute(
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_name = 'artists' AND constraint_type = 'PRIMARY KEY'"
            ).fetchone()
            assert pk is not None

    def test_fk_constraints(self, pg_dsn, entity, ipc_dict):
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, fk=True, overwrite=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_dict, entity)
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


class TestSplitTableGroups:
    def test_distributes_all_tables(self):
        from discogskit.entities import get
        from discogskit.writers.postgresql import _split_table_groups

        entity = get("releases")
        groups = _split_table_groups(3, entity)

        assert len(groups) == 3
        all_tables = [t for g in groups for t in g]
        assert sorted(all_tables) == sorted(entity.table_order)

    def test_single_group(self):
        from discogskit.entities import get
        from discogskit.writers.postgresql import _split_table_groups

        entity = get("artists")
        groups = _split_table_groups(1, entity)

        assert len(groups) == 1
        assert sorted(groups[0]) == sorted(entity.table_order)
