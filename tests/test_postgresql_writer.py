"""Integration tests for PostgreSQLWriter full lifecycle."""

from __future__ import annotations

import os
import uuid
from urllib.parse import urlsplit

import pytest

from discogskit.entities import ChunkArgs, get
from discogskit.entities._worker import extract_chunk_to_ipc
from tests.conftest import empty_ipc


def _load(dsn, entity, ipc_dict, **options):
    from discogskit.writers.postgresql import PostgreSQLWriter

    writer = PostgreSQLWriter(dsn, **options)
    try:
        writer.setup(entity)
        writer.write_chunk(ipc_dict, entity)
        writer.finalize(entity)
    finally:
        writer.close()


@pytest.mark.integration
class TestPostgreSQLWriter:
    @pytest.fixture()
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        )

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
        from discogskit.writers.postgresql import PostgreSQLWriter

        writer = PostgreSQLWriter(pg_dsn, overwrite=True)
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(
                empty_ipc("artists"), entity, table_timings=timings
            )
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
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        )

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
