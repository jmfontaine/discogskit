"""Tests for get_writer() DSN routing."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from discogskit.writers import get_writer


class TestGetWriter:
    def test_postgresql_dsn(self):
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = mock_connect
            writer = get_writer("postgresql://user:pass@localhost/db")
        from discogskit.writers.postgresql import PostgreSQLWriter

        assert isinstance(writer, PostgreSQLWriter)

    def test_postgres_scheme_dsn(self):
        """``postgres://`` is accepted alongside ``postgresql://``."""
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = mock_connect
            writer = get_writer("postgres://user:pass@localhost/db")
        from discogskit.writers.postgresql import PostgreSQLWriter

        assert isinstance(writer, PostgreSQLWriter)

    def test_sqlite_dsn(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        writer = get_writer(f"sqlite:///{db_path}")
        from discogskit.writers.sqlite import SQLiteWriter

        assert isinstance(writer, SQLiteWriter)
        writer.close()

    def test_sqlite_file_extension(self, tmp_path):
        db_path = str(tmp_path / "test.sqlite3")
        writer = get_writer(db_path)
        from discogskit.writers.sqlite import SQLiteWriter

        assert isinstance(writer, SQLiteWriter)
        writer.close()

    def test_sqlite_accepts_cli_defaults_for_pg_only_options(self, tmp_path):
        """CLI defaults the user never touched must not trigger the PostgreSQL-only check."""
        db_path = str(tmp_path / "test.db")
        writer = get_writer(
            db_path,
            create_schema=False,
            index_workers=2,
            schema=None,
            unlogged=False,
            write_workers=1,
        )
        from discogskit.writers.sqlite import SQLiteWriter

        assert isinstance(writer, SQLiteWriter)
        writer.close()

    def test_sqlite_accepts_universal_options(self, tmp_path):
        """fk and overwrite apply to both targets, so SQLite accepts them."""
        db_path = str(tmp_path / "test.db")
        writer = get_writer(db_path, fk=True, overwrite=True)
        from discogskit.writers.sqlite import SQLiteWriter

        assert isinstance(writer, SQLiteWriter)
        writer.close()

    @pytest.mark.parametrize(
        "kwargs, flag",
        [
            ({"create_schema": True}, "--pg-create-schema"),
            ({"index_workers": 4}, "--pg-index-workers"),
            ({"schema": "custom"}, "--pg-schema"),
            ({"unlogged": True}, "--pg-unlogged"),
            ({"write_workers": 2}, "--pg-write-workers"),
        ],
        ids=["create-schema", "index-workers", "schema", "unlogged", "write-workers"],
    )
    def test_sqlite_rejects_postgresql_only_options(self, tmp_path, kwargs, flag):
        db_path = str(tmp_path / "test.db")
        with pytest.raises(ValueError, match=flag):
            get_writer(db_path, **kwargs)

    def test_sqlite_rejects_multiple_postgresql_only_options_together(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        with pytest.raises(ValueError) as excinfo:
            get_writer(db_path, unlogged=True, write_workers=3)
        assert "--pg-unlogged" in str(excinfo.value)
        assert "--pg-write-workers" in str(excinfo.value)

    def test_parquet_dsn_raises(self):
        with pytest.raises(ValueError, match="Unsupported database DSN"):
            get_writer("parquet:///tmp/output")

    def test_jsonl_dsn_raises(self):
        with pytest.raises(ValueError, match="Unsupported database DSN"):
            get_writer("jsonl:///tmp/output")

    def test_unknown_dsn_raises(self):
        with pytest.raises(ValueError, match="Unsupported database DSN"):
            get_writer("unknown://something")

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql+psycopg://user:s3cr3t@localhost/discogs",
            # Unencoded "@", "/" and "?" in the password: everything up to the last "@" is userinfo.
            "mysql://user:s3/cr@3t@localhost/discogs",
            "mysql://user:s3?cr3t@localhost/discogs?sslmode=require",
            "postgresql+psycopg://user@localhost/discogs?password=s3cr3t&sslmode=require",
            # A percent-encoded parameter name, which libpq decodes.
            "postgresql+psycopg://user@localhost/discogs?%70assword=s3cr3t",
            "host=localhost password=s3cr3t dbname=discogs",
            "host=localhost password = s3&cr3t dbname=discogs",
            "host=localhost password=s3\\ cr3t dbname=discogs",
            "host=localhost password='s3 cr3t' dbname=discogs",
            "host=localhost password='s3\\' cr3t' dbname=discogs",
            # Unterminated quote: not valid keyword/value syntax, so the whole DSN is hidden.
            "host=localhost password='s3 cr3t",
            "host=localhost password='s3 x=cr3t",
            # "://" in a keyword/value password doesn't make the DSN a URL.
            "host=localhost password='https://s3 cr3t' dbname=discogs",
            "host=localhost password=http://s3cr3t dbname=discogs",
            # "password=" inside another keyword's value isn't the password keyword.
            "application_name='password=' password='s3 cr3t' dbname=discogs",
        ],
    )
    def test_unsupported_dsn_error_hides_the_password(self, dsn):
        with pytest.raises(ValueError, match="Unsupported database DSN") as excinfo:
            get_writer(dsn)
        message = str(excinfo.value)
        assert "s3" not in message
        assert "cr3t" not in message
        assert "***" in message

    @pytest.mark.parametrize(
        "dsn, shown",
        [
            (
                "postgresql+psycopg://user:s3cr3t@localhost:5432/discogs",
                "postgresql+psycopg://user:***@localhost:5432/discogs",
            ),
            (
                "postgresql+psycopg://user@localhost/discogs?sslmode=require",
                "postgresql+psycopg://user@localhost/discogs?***",
            ),
            (
                "application_name='password=' password='s3 cr3t' dbname=discogs",
                "application_name='password=' password=*** dbname=discogs",
            ),
            ("host=localhost password='s3 cr3t", "***"),
            ("output/discogs.txt", "output/discogs.txt"),
        ],
    )
    def test_unsupported_dsn_error_keeps_the_rest_of_the_dsn(self, dsn, shown):
        with pytest.raises(ValueError) as excinfo:
            get_writer(dsn)
        assert f"Unsupported database DSN: {shown!r}." in str(excinfo.value)
