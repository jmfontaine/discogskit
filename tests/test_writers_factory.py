"""Tests for the writer factories: get_writer() DSN routing and get_file_writer()."""

from __future__ import annotations

import bz2
import gzip
import json
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import discogskit
from discogskit.writers import get_writer
from tests.conftest import ARTISTS_XML


def _create_tables(writer):
    """Create the artists tables, and so the database file, which a SQLite writer creates only in ``setup()``."""
    try:
        writer.setup(discogskit.ENTITIES["artists"])
    finally:
        writer.close()


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

    def test_sqlite_dsn_relative_path(self, monkeypatch, tmp_path):
        """``sqlite:///out.db`` is relative to the working directory, as in SQLAlchemy."""
        monkeypatch.chdir(tmp_path)
        _create_tables(get_writer("sqlite:///out.db"))
        assert [p.name for p in tmp_path.iterdir()] == ["out.db"]

    @pytest.mark.parametrize(
        "dsn, reason",
        [
            ("sqlite:out.db", "Use sqlite:///<path>"),
            ("sqlite://out.db", "Use sqlite:///<path>"),
            ("SQLite://out.db", "Use sqlite:///<path>"),
            ("sqlite:///", "no path"),
            ("sqlite:///out.db?mode=ro", "contains '?'"),
            ("out.db?mode=ro.db", "contains '?'"),
        ],
    )
    def test_malformed_sqlite_dsn_creates_no_file(
        self, monkeypatch, tmp_path, dsn, reason
    ):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ValueError, match="Invalid SQLite DSN") as excinfo:
            get_writer(dsn)
        assert reason in str(excinfo.value)
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize(
        "dsn",
        [
            "sqlite:///:memory:",
            "sqlite:///:MEMORY:",
            "SQLITE:///:Memory:",
            "sqlite:///file::memory:",
            "sqlite:///FILE:%3Amemory%3A",
            "file::memory:#discogs.db",
            "file::memory:%00.db",
        ],
    )
    def test_in_memory_sqlite_dsn_is_rejected(self, monkeypatch, tmp_path, dsn):
        """An in-memory database would load the whole dump, then discard it when the run ends."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ValueError, match="Invalid SQLite DSN") as excinfo:
            get_writer(dsn)
        assert "in-memory database" in str(excinfo.value)
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize("dsn", [":memory:.db", "sqlite:///memory.db"])
    def test_file_named_like_memory_is_accepted(self, monkeypatch, tmp_path, dsn):
        """Only the exact in-memory names are rejected, not files whose name merely contains one."""
        monkeypatch.chdir(tmp_path)
        _create_tables(get_writer(dsn))
        assert len(list(tmp_path.iterdir())) == 1

    def test_other_url_with_sqlite_suffix_is_unsupported(self, monkeypatch, tmp_path):
        """A URL isn't a SQLite path just because it ends in .db."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ValueError, match="Unsupported database DSN"):
            get_writer("mysql://localhost/discogs.db")
        assert list(tmp_path.iterdir()) == []

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


_ARTISTS_XML = (
    b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n"
    b"<artist>\n  <id>1</id>\n  <name>Test</name>\n  <data_quality>Correct</data_quality>\n</artist>\n"
    b"<artist>\n  <id>2</id>\n  <name>Other</name>\n  <data_quality>Correct</data_quality>\n</artist>\n"
    b"</artists>"
)

# Output file and reader per format and codec, for reading converted output back.
_ARTISTS_OUTPUT = {
    ("jsonl", "bzip2"): "artists/artists.jsonl.bz2",
    ("jsonl", "gzip"): "artists/artists.jsonl.gz",
    ("jsonl", "none"): "artists/artists.jsonl",
    ("parquet", "brotli"): "artists/artists.parquet",
    ("parquet", "gzip"): "artists/artists.parquet",
    ("parquet", "none"): "artists/artists.parquet",
    ("parquet", "snappy"): "artists/artists.parquet",
    ("parquet", "zstd"): "artists/artists.parquet",
}


def _read_rows(path, output_format, compression):
    if output_format == "parquet":
        return pq.read_table(path).to_pylist()
    opener = {"bzip2": bz2.open, "gzip": gzip.open, "none": open}[compression]
    with opener(path, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def _convert(tmp_path, output_format, *, xml=_ARTISTS_XML, **writer_kwargs):
    """Convert a dump (two artists by default) with ``get_file_writer()``; return the run's result and output directory."""
    gz_path = tmp_path / "discogs_20260301_artists.xml.gz"
    gz_path.write_bytes(gzip.compress(xml))
    out = tmp_path / "out"
    config = discogskit.PipelineConfig(
        chunk_mb=1,
        entity=discogskit.detect_entity(gz_path.name),
        gz_path=gz_path,
        keep_xml=False,
        parse_workers=1,
        profile=False,
        progress=False,
        strict=False,
        write_queue=2,
    )
    writer = discogskit.get_file_writer(out, output_format, **writer_kwargs)
    try:
        result = discogskit.run(config, writer)
    finally:
        writer.close()
    return result, out


class TestGetFileWriter:
    """get_file_writer() through the declared API only: names exported by ``discogskit``."""

    def test_brotli_reads_back_the_same_data_as_uncompressed(self, tmp_path):
        # Records filling every artists table.
        xml = f"<artists>{ARTISTS_XML}</artists>".encode()
        (tmp_path / "brotli").mkdir()
        (tmp_path / "none").mkdir()
        _, brotli_out = _convert(
            tmp_path / "brotli", "parquet", compression="brotli", xml=xml
        )
        _, none_out = _convert(
            tmp_path / "none", "parquet", compression="none", xml=xml
        )

        for table in discogskit.ENTITIES["artists"].table_order:
            brotli = pq.ParquetFile(brotli_out / "artists" / f"{table}.parquet")
            assert brotli.metadata.row_group(0).column(0).compression == "BROTLI"
            expected = pq.read_table(none_out / "artists" / f"{table}.parquet")
            assert expected.num_rows > 0, table
            assert brotli.read().equals(expected), table

    @pytest.mark.parametrize(
        "output_format, compression",
        sorted(_ARTISTS_OUTPUT),
        ids=[f"{fmt}-{codec}" for fmt, codec in sorted(_ARTISTS_OUTPUT)],
    )
    def test_converts_a_dump(self, tmp_path, output_format, compression):
        result, out = _convert(tmp_path, output_format, compression=compression)

        assert result.total_records == 2
        rows = _read_rows(
            out / _ARTISTS_OUTPUT[(output_format, compression)],
            output_format,
            compression,
        )
        assert [(row["id"], row["name"]) for row in rows] == [
            (1, "Test"),
            (2, "Other"),
        ]

    def test_jsonl_defaults_to_uncompressed(self, tmp_path):
        _, out = _convert(tmp_path, "jsonl")
        rows = _read_rows(out / "artists" / "artists.jsonl", "jsonl", "none")
        assert [row["id"] for row in rows] == [1, 2]

    def test_parquet_defaults_to_zstd(self, tmp_path):
        _, out = _convert(tmp_path, "parquet")
        metadata = pq.ParquetFile(out / "artists" / "artists.parquet").metadata
        assert metadata.row_group(0).column(0).compression == "ZSTD"

    @pytest.mark.parametrize(
        "output_format, compression, match",
        [
            ("jsonl", "brotli", "unsupported compression 'brotli' for jsonl"),
            ("jsonl", "snappy", "unsupported compression 'snappy' for jsonl"),
            ("jsonl", "zstd", "unsupported compression 'zstd' for jsonl"),
            ("parquet", "bzip2", "unsupported compression 'bzip2' for parquet"),
            ("parquet", "bogus", "unsupported compression 'bogus' for parquet"),
        ],
    )
    def test_rejects_codec_the_format_cannot_use(
        self, tmp_path, output_format, compression, match
    ):
        out = tmp_path / "out"
        with pytest.raises(ValueError, match=match):
            discogskit.get_file_writer(out, output_format, compression=compression)
        assert not out.exists()

    @pytest.mark.parametrize("level", [1.5, "3", True], ids=["float", "str", "bool"])
    @pytest.mark.parametrize(
        "output_format, compression",
        [("jsonl", "bzip2"), ("jsonl", "gzip"), ("parquet", None)],
    )
    def test_rejects_level_that_is_not_an_integer(
        self, tmp_path, output_format, compression, level
    ):
        """Checked before the range, which would let 1.5 and True through and fail on "3" with a TypeError."""
        out = tmp_path / "out"
        with pytest.raises(ValueError) as excinfo:
            discogskit.get_file_writer(
                out, output_format, compression=compression, compression_level=level
            )
        assert str(excinfo.value) == (
            f"compression level must be an integer, not {level!r}."
        )
        assert not out.exists()

    @pytest.mark.parametrize(
        "output_format, compression, level, match",
        [
            (
                "jsonl",
                "bzip2",
                0,
                r"compression level 0 is out of range for bzip2 \(1 to 9\)",
            ),
            ("jsonl", "bzip2", 10, "out of range for bzip2"),
            (
                "jsonl",
                "gzip",
                -1,
                r"compression level -1 is out of range for gzip \(0 to 9\)",
            ),
            ("jsonl", "gzip", 10, "out of range for gzip"),
            ("jsonl", "none", 5, "none does not support a compression level"),
            (
                "parquet",
                "brotli",
                pa.Codec.minimum_compression_level("brotli") - 1,
                "out of range for brotli",
            ),
            (
                "parquet",
                "brotli",
                pa.Codec.maximum_compression_level("brotli") + 1,
                "out of range for brotli",
            ),
            (
                "parquet",
                "gzip",
                pa.Codec.maximum_compression_level("gzip") + 1,
                "out of range for gzip",
            ),
            (
                "parquet",
                "zstd",
                pa.Codec.minimum_compression_level("zstd") - 1,
                "out of range for zstd",
            ),
            (
                "parquet",
                "zstd",
                pa.Codec.maximum_compression_level("zstd") + 1,
                "out of range for zstd",
            ),
            ("parquet", "none", 5, "none does not support a compression level"),
            ("parquet", "snappy", 5, "snappy does not support a compression level"),
        ],
    )
    def test_rejects_level_the_codec_cannot_use(
        self, tmp_path, output_format, compression, level, match
    ):
        out = tmp_path / "out"
        with pytest.raises(ValueError, match=match):
            discogskit.get_file_writer(
                out, output_format, compression=compression, compression_level=level
            )
        assert not out.exists()

    def test_rejects_unknown_format(self, tmp_path):
        out = tmp_path / "out"
        with pytest.raises(
            ValueError, match="unsupported output format 'csv'. Valid: jsonl, parquet."
        ):
            discogskit.get_file_writer(out, "csv")
        assert not out.exists()
