"""Integration tests for PostgreSQLWriter full lifecycle."""

from __future__ import annotations

import os
import uuid
from urllib.parse import urlsplit

import pytest

from discogskit.entities import ChunkArgs, get
from discogskit.entities._worker import extract_chunk_to_ipc
from tests.conftest import empty_ipc, ipc_to_record_batches


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


@pytest.mark.integration
class TestPgSchema:
    """``--pg-schema``/``--pg-create-schema``: target a specific schema (#53)."""

    @pytest.fixture()
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        )

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
