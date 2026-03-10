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

    def test_parquet_dsn_raises(self):
        with pytest.raises(ValueError, match="Unsupported database DSN"):
            get_writer("parquet:///tmp/output")

    def test_jsonl_dsn_raises(self):
        with pytest.raises(ValueError, match="Unsupported database DSN"):
            get_writer("jsonl:///tmp/output")

    def test_unknown_dsn_raises(self):
        with pytest.raises(ValueError, match="Unsupported database DSN"):
            get_writer("unknown://something")
