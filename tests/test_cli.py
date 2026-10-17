"""Tests for CLI error handling and option validation."""

from __future__ import annotations

import gzip
import json
import random
import re
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from rich.text import Text
from typer.testing import CliRunner, Result

from discogskit import pipeline
from discogskit.cli import app
from discogskit.decompress import DecompressError
from discogskit.entities import ENTITIES
from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.writers import OutputExistsError
from tests.conftest import (
    ARTISTS_XML,
    LABELS_XML,
    UNICODE_ARTISTS_ROWS,
    UNICODE_ARTISTS_XML,
)

runner = CliRunner()

# (min, max) --compression-level pyarrow's Codec accepts for each Parquet codec under test — read from
# pyarrow itself, not hard-coded: zstd's range (-131072 to 22) is nothing like gzip's (1 to 9).
_PARQUET_LEVEL_RANGE = {
    codec: (
        pa.Codec.minimum_compression_level(codec),
        pa.Codec.maximum_compression_level(codec),
    )
    for codec in ("brotli", "gzip", "zstd")
}

# Each Parquet codec's own default --compression-level, likewise read from pyarrow rather than hard-coded —
# zstd's Arrow default (1) differs from libzstd's own default (3), and either could change with pyarrow.
_PARQUET_LEVEL_DEFAULT = {
    codec: pa.Codec.default_compression_level(codec)
    for codec in ("brotli", "gzip", "zstd")
}

# A valid-looking .xml.gz filename so detect_entity() succeeds.
_ENTITY_FILE = "discogs_20260301_artists.xml.gz"

# entity -> (a record with an id, one without, one with an empty id)
_RECORDS_WITHOUT_ID = {
    "artists": (
        "<artist><id>1</id></artist>",
        "<artist><name>A</name></artist>",
        "<artist><id/></artist>",
    ),
    "labels": (
        "<label><id>1</id></label>",
        "<label><name>A</name></label>",
        "<label><id/></label>",
    ),
    "masters": (
        '<master id="1"/>',
        "<master><title>A</title></master>",
        '<master id=""/>',
    ),
    "releases": (
        '<release id="1" status="Accepted"/>',
        '<release status="Accepted"/>',
        '<release id="" status="Accepted"/>',
    ),
}


def _make_gz(tmp_path: Path) -> Path:
    """Create a dummy .xml.gz file so path validation passes."""
    gz = tmp_path / _ENTITY_FILE
    gz.write_bytes(b"not a real gz")
    return gz


_COMPRESSIBLE_WORDS = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
]  # fmt: skip


def _compressible_artists_gz(tmp_path: Path, n: int = 3000) -> Path:
    """A real .xml.gz with enough varied, repetitive text for --compression-level to produce
    measurably different output sizes once converted."""
    rng = random.Random(42)
    gz = tmp_path / _ENTITY_FILE
    with gzip.open(gz, "wb") as f:
        f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
        for i in range(1, n + 1):
            profile = " ".join(rng.choices(_COMPRESSIBLE_WORDS, k=30))
            f.write(
                f"<artist><id>{i}</id><name>Artist {i}</name>"
                f"<profile>{profile}</profile></artist>\n".encode()
            )
        f.write(b"</artists>")
    return gz


def _unstyle(output: str) -> str:
    """Strip ANSI styling from CLI output (Typer colors its error panels when it detects CI)."""
    return Text.from_ansi(output).plain


def _widen_console(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stop Rich wrapping a long path mid-name, so assertions can match it whole.

    The shared console reads ``COLUMNS`` each time it prints; ``tests/__init__.py`` drops an inherited one, which it
    would keep instead.
    """
    monkeypatch.setenv("COLUMNS", "1000")


_WRITER_CLOSE_FAILURES = [
    (KeyboardInterrupt(), 130, "Interrupted"),
    (OutputExistsError("x"), 1, "Error: x"),
    (
        DecompressError(Path("discogs_20260301_artists.xml.gz")),
        1,
        "Error: failed to decompress",
    ),
    (RuntimeError("x"), 1, "Error: x"),
]
_WRITER_CLOSE_FAILURE_IDS = [
    "KeyboardInterrupt",
    "OutputExistsError",
    "DecompressError",
    "RuntimeError",
]


class TestNoTracebacks:
    """Every unhandled exception from the pipeline must produce a clean error message."""

    @pytest.mark.parametrize("command", ["convert", "load"])
    @pytest.mark.parametrize(
        "exc",
        [
            RuntimeError("something broke"),
            TypeError("bad type"),
            ValueError("bad value"),
        ],
        ids=["RuntimeError", "TypeError", "ValueError"],
    )
    def test_unexpected_exception(
        self, tmp_path: Path, command: str, exc: Exception
    ) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run", side_effect=exc),
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, [command, str(gz)])
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        # CliRunner turns an escaped exception into result.exception with empty output, so
        # SystemExit (from typer.Exit) is what proves the handler caught it.
        assert type(result.exception) is SystemExit, repr(result.exception)
        assert f"Error: {exc}" in output
        assert "Traceback" not in output


class TestRecordErrorOutput:
    def test_record_error_reaches_output_intact(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A record error keeps its file, chunk and line through the parse worker process and Rich (#93).

        The id looks like Rich markup, which Rich would otherwise swallow.
        """
        _widen_console(monkeypatch)
        gz = tmp_path / _ENTITY_FILE
        with gzip.open(gz, "wb") as f:
            f.write(
                b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n"
                b"<artist>\n  <id>1</id>\n</artist>\n"
                b"<artist>\n  <id>[b]</id>\n</artist>\n"
                b"</artists>"
            )
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "--format",
                "jsonl",
                "--keep-xml",
                "--no-progress",
                "--output",
                str(tmp_path / "out"),
                "--parse-workers",
                "2",
            ],
        )

        # The same chunk parsed in this process gives the message the CLI must print unchanged.
        xml = str(gz.with_suffix(""))
        [(start, end)] = ENTITIES["artists"].find_split_points(xml, 1 << 20)
        with pytest.raises(ValueError) as info:
            extract_chunk_to_ipc(
                ChunkArgs(end=end, entity="artists", file_path=xml, start=start)
            )
        message = str(info.value)
        assert message.startswith(
            f"Invalid <artist> record in {xml} at chunk line 4 (chunk bytes {start}-{end}, id='[b]'): "
        )
        assert result.exit_code == 1
        assert f"Error: {message}" in _unstyle(result.output).splitlines()

    @pytest.mark.parametrize("variant", ["empty", "missing"])
    @pytest.mark.parametrize("entity", sorted(_RECORDS_WITHOUT_ID))
    def test_record_without_id_fails(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entity: str, variant: str
    ) -> None:
        """A record without an id stops `convert` with its line instead of vanishing (#94)."""
        _widen_console(monkeypatch)
        good, missing, empty = _RECORDS_WITHOUT_ID[entity]
        gz = tmp_path / f"discogs_20260301_{entity}.xml.gz"
        with gzip.open(gz, "wt") as f:
            f.write(
                f"<?xml version='1.0' encoding='UTF-8'?>\n<{entity}>\n"
                f"{good}\n{missing if variant == 'missing' else empty}\n</{entity}>"
            )
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "--format",
                "jsonl",
                "--no-progress",
                "--output",
                str(tmp_path / "out"),
                "--parse-workers",
                "1",
            ],
        )
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert re.search(
            rf"^Error: Invalid <{entity[:-1]}> record in .* at chunk line 2 .*: missing or empty id$",
            output,
            re.MULTILINE,
        ), output


class TestWriterAlwaysClosed:
    """writer.close() must run on every exit path: success, user error, or crash."""

    @pytest.mark.parametrize(
        "exc, expected_exit_code, expected_message",
        _WRITER_CLOSE_FAILURES,
        ids=_WRITER_CLOSE_FAILURE_IDS,
    )
    def test_convert_closes_writer_once(
        self,
        tmp_path: Path,
        exc: BaseException,
        expected_exit_code: int,
        expected_message: str,
    ) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run", side_effect=exc),
            patch("discogskit.writers.parquet.ParquetWriter.close") as close,
        ):
            result = runner.invoke(app, ["convert", str(gz), "-f", "parquet"])
        assert result.exit_code == expected_exit_code
        assert expected_message in _unstyle(result.output)
        close.assert_called_once()

    def test_convert_closes_writer_once_on_success(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with (
            patch("discogskit.cli.pipeline.run", return_value=pipeline_result),
            patch("discogskit.writers.parquet.ParquetWriter.close") as close,
        ):
            result = runner.invoke(app, ["convert", str(gz), "-f", "parquet"])
        assert result.exit_code == 0
        assert "0 artists converted" in _unstyle(result.output)
        close.assert_called_once()

    @pytest.mark.parametrize(
        "exc, expected_exit_code, expected_message",
        _WRITER_CLOSE_FAILURES,
        ids=_WRITER_CLOSE_FAILURE_IDS,
    )
    def test_load_closes_writer_once(
        self,
        tmp_path: Path,
        exc: BaseException,
        expected_exit_code: int,
        expected_message: str,
    ) -> None:
        gz = _make_gz(tmp_path)
        mock_writer = MagicMock()
        with (
            patch("discogskit.cli.pipeline.run", side_effect=exc),
            patch("discogskit.cli.get_writer", return_value=mock_writer),
        ):
            result = runner.invoke(app, ["load", str(gz)])
        assert result.exit_code == expected_exit_code
        assert expected_message in _unstyle(result.output)
        mock_writer.close.assert_called_once()

    def test_load_closes_writer_once_on_success(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        mock_writer = MagicMock()
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with (
            patch("discogskit.cli.pipeline.run", return_value=pipeline_result),
            patch("discogskit.cli.get_writer", return_value=mock_writer),
        ):
            result = runner.invoke(app, ["load", str(gz)])
        assert result.exit_code == 0
        assert "0 artists loaded" in _unstyle(result.output)
        mock_writer.close.assert_called_once()


class TestUncompressedXmlInput:
    @pytest.mark.parametrize("command", ["convert", "load"])
    def test_rejected_before_any_work(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command: str
    ) -> None:
        """`.xml` used to be accepted, then failed decompression (issue #15)."""
        gz = _make_gz(tmp_path)
        xml = tmp_path / "discogs_20260301_labels.xml"
        xml.write_bytes(b"<labels></labels>")
        _widen_console(monkeypatch)
        with (
            patch("discogskit.cli.pipeline.run") as run,
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, [command, str(gz), str(xml)])
        assert result.exit_code == 1
        assert "uncompressed .xml input is not supported" in _unstyle(result.output)
        run.assert_not_called()
        assert xml.read_bytes() == b"<labels></labels>"


class TestUnrecognizedFileExtension:
    def test_directory_still_only_globs_xml_gz(self, tmp_path: Path) -> None:
        """Directory arguments keep globbing *.xml.gz; other files there are ignored."""
        gz = _make_gz(tmp_path)
        (tmp_path / "discogs_20260301_labels.xml.bz2").write_bytes(b"not xml")
        with patch("discogskit.cli.pipeline.run") as run:
            runner.invoke(app, ["convert", str(tmp_path), "-f", "parquet"])
        run.assert_called_once()
        [(config, _writer)] = [call.args for call in run.call_args_list]
        assert config.gz_path == gz

    @pytest.mark.parametrize("command", ["convert", "load"])
    def test_rejected_before_any_work(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command: str
    ) -> None:
        """A typo'd or unsupported extension (e.g. .bz2) used to be silently skipped."""
        gz = _make_gz(tmp_path)
        # Spaces in the name: the message must reproduce the path exactly, not just unwrapped.
        other = tmp_path / "discogs  20260301 labels.xml.bz2"
        other.write_bytes(b"not xml")
        _widen_console(monkeypatch)
        with (
            patch("discogskit.cli.pipeline.run") as run,
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, [command, str(gz), str(other)])
        assert result.exit_code == 1
        assert f"{other}: not a .xml.gz dump" in _unstyle(result.output)
        run.assert_not_called()


def _dump_gz(directory: Path, entity: str, xml: str, date: str = "20260301") -> Path:
    """A real ``discogs_<date>_<entity>.xml.gz`` holding ``xml`` (records without their envelope)."""
    gz = directory / f"discogs_{date}_{entity}.xml.gz"
    with gzip.open(gz, "wb") as f:
        f.write(f"<?xml version='1.0' encoding='UTF-8'?>\n<{entity}>\n".encode())
        f.write(xml.encode())
        f.write(f"</{entity}>".encode())
    return gz


def _two_months(tmp_path: Path) -> tuple[list[Path], str]:
    dumps = tmp_path / "dumps"
    dumps.mkdir()
    older = _dump_gz(dumps, "artists", ARTISTS_XML, date="20260201")
    newer = _dump_gz(dumps, "artists", ARTISTS_XML)
    return [dumps], f"two inputs for artists: {older} and {newer}"


def _directory_and_its_file(tmp_path: Path) -> tuple[list[Path], str]:
    dumps = tmp_path / "dumps"
    dumps.mkdir()
    gz = _dump_gz(dumps, "artists", ARTISTS_XML)
    return [dumps, gz], f"two inputs for artists: {gz} and {gz}"


def _file_twice(tmp_path: Path) -> tuple[list[Path], str]:
    gz = _dump_gz(tmp_path, "artists", ARTISTS_XML)
    return [gz, gz], f"two inputs for artists: {gz} and {gz}"


def _file_by_another_path(tmp_path: Path) -> tuple[list[Path], str]:
    dumps = tmp_path / "dumps"
    dumps.mkdir()
    gz = _dump_gz(dumps, "artists", ARTISTS_XML)
    other = tmp_path / "dumps" / ".." / "dumps" / gz.name
    return [gz, other], f"two inputs for artists: {gz} and {other}"


def _file_under_another_name(tmp_path: Path) -> tuple[list[Path], str]:
    """A symlink named for another entity still points at the same dump."""
    gz = _dump_gz(tmp_path, "artists", ARTISTS_XML)
    link = tmp_path / "discogs_20260301_labels.xml.gz"
    link.symlink_to(gz)
    return [gz, link], f"same file given twice: {gz} and {link}"


class TestDuplicateInputs:
    """Two inputs for one entity, or one file given twice, fail before any job runs (#102)."""

    @pytest.mark.parametrize("command", ["convert", "load"])
    @pytest.mark.parametrize(
        "case",
        [
            _two_months,
            _directory_and_its_file,
            _file_twice,
            _file_by_another_path,
            _file_under_another_name,
        ],
        ids=lambda case: case.__name__.lstrip("_"),
    )
    def test_rejected_before_any_work(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        command: str,
        case: Callable[[Path], tuple[list[Path], str]],
    ) -> None:
        paths, message = case(tmp_path)
        db = tmp_path / "out.db"
        extra = (
            ["--dsn", str(db)]
            if command == "load"
            else ["--output", str(tmp_path / "out")]
        )
        _widen_console(monkeypatch)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, [command, *map(str, paths), *extra])
        assert result.exit_code == 1, _unstyle(result.output)
        assert f"Error: {message}\n" in _unstyle(result.output)
        run.assert_not_called()
        assert not db.exists()
        assert not (tmp_path / "out").exists()


class TestExistingOutputCheckedUpFront:
    """Existing output for any job fails the run before the first job starts (#102)."""

    @staticmethod
    def _args(target: str, tmp_path: Path) -> list[str]:
        if target == "sqlite":
            return ["load", "--dsn", str(tmp_path / "out.db")]
        return ["convert", "-f", target, "--output", str(tmp_path / "out")]

    @staticmethod
    def _run(args: list[str], *paths: Path) -> Result:
        return runner.invoke(
            app, [*args, *map(str, paths), "--parse-workers", "1", "--no-progress"]
        )

    @pytest.mark.parametrize("target", ["jsonl", "parquet", "sqlite"])
    def test_existing_second_entity_fails_before_first_job(
        self, monkeypatch: pytest.MonkeyPatch, target: str, tmp_path: Path
    ) -> None:
        args = self._args(target, tmp_path)
        artists = _dump_gz(tmp_path, "artists", ARTISTS_XML)
        labels = _dump_gz(tmp_path, "labels", LABELS_XML)
        first = self._run(args, labels)
        assert first.exit_code == 0, _unstyle(first.output)

        _widen_console(monkeypatch)
        with patch("discogskit.cli.pipeline.run") as run:
            result = self._run(args, artists, labels)
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        [message] = [line for line in output.splitlines() if line.strip()]
        assert message.startswith("Error: ")
        assert message.endswith("Use --overwrite to replace them.")
        # Every labels table name starts with "label"; none of the artists tables' does.
        assert "(e.g. label" in message
        run.assert_not_called()

    @pytest.mark.parametrize("target", ["jsonl", "parquet", "sqlite"])
    def test_overwrite_replaces_existing_output(
        self, target: str, tmp_path: Path
    ) -> None:
        args = self._args(target, tmp_path)
        artists = _dump_gz(tmp_path, "artists", ARTISTS_XML)
        labels = _dump_gz(tmp_path, "labels", LABELS_XML)
        first = self._run(args, labels)
        assert first.exit_code == 0, _unstyle(first.output)

        result = self._run([*args, "--overwrite"], artists, labels)
        output = _unstyle(result.output)
        assert result.exit_code == 0, output
        assert "artists converted" in output or "artists loaded" in output
        assert "labels converted" in output or "labels loaded" in output


# Discogs text is full of BBCode-style tags that Rich reads as markup: an unmatched closer
# raised MarkupError inside the error handler, and matched tags or a bracketed word vanished.
# Rich also turns emoji codes into emoji, and markup.escape() doubles a trailing backslash.
_MARKUP_TEXT = (
    "[b]Bold[/b] [url=https://example.com]link[/url] stray [/b] in [backup] :cd: C:\\"
)


class TestLiteralText:
    """Exception and path text prints verbatim, never as Rich markup (#103)."""

    @staticmethod
    def _assert_clean_error(result: Result, message: str) -> None:
        """Exit 1 through typer.Exit with ``message`` intact, not a MarkupError from the handler."""
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert type(result.exception) is SystemExit, repr(result.exception)
        assert f"Error: {message}\n" in output

    @pytest.mark.parametrize("command", ["convert", "load"])
    @pytest.mark.parametrize("exc_type", [OutputExistsError, RuntimeError])
    def test_pipeline_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        command: str,
        exc_type: type[Exception],
    ) -> None:
        gz = _make_gz(tmp_path)
        _widen_console(monkeypatch)
        with (
            patch("discogskit.cli.pipeline.run", side_effect=exc_type(_MARKUP_TEXT)),
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, [command, str(gz)])
        self._assert_clean_error(result, _MARKUP_TEXT)

    def test_writer_construction_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        gz = _make_gz(tmp_path)
        _widen_console(monkeypatch)
        with (
            patch("discogskit.cli.pipeline.run") as run,
            patch("discogskit.cli.get_writer", side_effect=RuntimeError(_MARKUP_TEXT)),
        ):
            result = runner.invoke(app, ["load", str(gz)])
        self._assert_clean_error(result, _MARKUP_TEXT)
        run.assert_not_called()

    def test_undetectable_entity_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        gz = tmp_path / "discogs_20260301_[backup].xml.gz"
        gz.write_bytes(b"not a real gz")
        _widen_console(monkeypatch)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["convert", str(gz)])
        self._assert_clean_error(
            result,
            f"Cannot detect entity from '{gz.name}'. Expected format: "
            "discogs_YYYYMMDD_ENTITY.xml.gz (known entities: artists, labels, masters, releases)",
        )
        run.assert_not_called()

    @pytest.mark.parametrize(
        "name, kind, message",
        [
            ("", "dir", "no .xml.gz files found in {p}"),
            ("discogs_20260301_artists.xml.gz", "missing", "file not found: {p}"),
            (
                "discogs_20260301_artists.xml",
                "file",
                "{p}: uncompressed .xml input is not supported; pass the original .xml.gz dump",
            ),
            ("discogs_20260301_artists.xml.bz2", "file", "{p}: not a .xml.gz dump"),
        ],
        ids=["empty-dir", "missing-file", "uncompressed-xml", "not-xml-gz"],
    )
    def test_path_in_input_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        name: str,
        kind: str,
        message: str,
    ) -> None:
        parent = tmp_path / "dumps [backup]"
        parent.mkdir()
        p = parent / name
        if kind == "file":
            p.write_bytes(b"not xml")
        _widen_console(monkeypatch)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["convert", str(p)])
        self._assert_clean_error(result, message.format(p=p))
        run.assert_not_called()

    def test_path_in_status_output(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        parent = tmp_path / "dumps [backup]"
        parent.mkdir()
        gz = parent / "discogs_20260301 [backup]_artists.xml.gz"
        with gzip.open(gz, "wb") as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(ARTISTS_XML.encode())
            f.write(b"</artists>")
        _widen_console(monkeypatch)
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "-f",
                "jsonl",
                "--output",
                str(tmp_path / "out"),
                "--parse-workers",
                "1",
                "--no-progress",
            ],
        )
        output = _unstyle(result.output)
        assert result.exit_code == 0, output
        lines = output.splitlines()
        assert f"Artists  {gz.name}" in lines
        assert f"  {'Source':<14s} {gz}" in lines


class TestPreflight:
    @pytest.mark.parametrize(
        "name, kind, extra, message",
        [
            ("", "dir", [], "no .xml.gz files found in {p}"),
            (_ENTITY_FILE, "missing", [], "file not found: {p}"),
            (
                "discogs_20260301_unknown.xml.gz",
                "file",
                [],
                (
                    "Cannot detect entity from 'discogs_20260301_unknown.xml.gz'. Expected format: "
                    "discogs_YYYYMMDD_ENTITY.xml.gz (known entities: artists, labels, masters, releases)"
                ),
            ),
            (
                _ENTITY_FILE,
                "file",
                ["--pg-create-schema"],
                "--pg-create-schema requires --pg-schema.",
            ),
        ],
        ids=[
            "empty-dir",
            "missing-file",
            "unknown-entity",
            "create-schema-without-schema",
        ],
    )
    def test_load_rejects_bad_input_before_opening_the_database(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        name: str,
        kind: str,
        extra: list[str],
        message: str,
    ) -> None:
        """Bad input fails with exit 1 before the writer is built, so no SQLite file is left behind."""
        inputs = tmp_path / "in"
        inputs.mkdir()
        p = inputs / name
        if kind == "file":
            p.write_bytes(b"not a real gz")
        db = tmp_path / "out.db"
        _widen_console(monkeypatch)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["load", str(p), "--dsn", str(db), *extra])
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert type(result.exception) is SystemExit, repr(result.exception)
        assert f"Error: {message.format(p=p)}\n" in output
        run.assert_not_called()
        assert not db.exists()

    def test_version(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0, result.output
        assert _unstyle(result.output) == f"discogskit {version('discogskit')}\n"


class TestFormatAndCompressionEnums:
    @pytest.mark.parametrize(
        "fmt, compression",
        [
            ("jsonl", "brotli"),
            ("jsonl", "snappy"),
            ("jsonl", "zstd"),
            ("parquet", "bzip2"),
        ],
        ids=["jsonl-brotli", "jsonl-snappy", "jsonl-zstd", "parquet-bzip2"],
    )
    def test_compression_not_valid_for_format_is_rejected(
        self, tmp_path: Path, fmt: str, compression: str
    ) -> None:
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(
                app, ["convert", str(gz), "-f", fmt, "--compression", compression]
            )
        assert result.exit_code == 1
        output = _unstyle(result.output)
        assert f"unsupported compression '{compression}' for {fmt}" in output
        run.assert_not_called()

    def test_compression_value_is_passed_through_to_writer(
        self, tmp_path: Path
    ) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run"),
            patch("discogskit.writers.parquet.ParquetWriter") as writer_cls,
        ):
            runner.invoke(
                app, ["convert", str(gz), "-f", "parquet", "--compression", "gzip"]
            )
        assert writer_cls.call_args.kwargs["compression"] == "gzip"

    def test_format_is_case_insensitive(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run"),
            patch("discogskit.writers.parquet.ParquetWriter") as parquet_cls,
            patch("discogskit.writers.jsonl.JSONLWriter") as jsonl_cls,
        ):
            runner.invoke(app, ["convert", str(gz), "-f", "PARQUET"])
        parquet_cls.assert_called_once()
        jsonl_cls.assert_not_called()

    def test_invalid_compression_value_is_a_usage_error(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["convert", str(gz), "--compression", "bogus"])
        assert result.exit_code == 2
        assert "'bogus'" in _unstyle(result.output)
        run.assert_not_called()

    def test_invalid_format_is_a_usage_error(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["convert", str(gz), "-f", "bogus"])
        assert result.exit_code == 2
        output = _unstyle(result.output)
        assert "'bogus'" in output
        assert "'parquet'" in output
        assert "'jsonl'" in output
        run.assert_not_called()


_COUNT_OPTIONS = {
    "convert": ["--chunk-mb", "--parse-workers", "--write-queue"],
    "load": [
        "--chunk-mb",
        "--parse-workers",
        "--pg-index-workers",
        "--pg-write-workers",
        "--write-queue",
    ],
}


class TestOptionMinimums:
    @pytest.mark.parametrize(
        "command, option, value",
        [
            (command, option, value)
            for command, options in _COUNT_OPTIONS.items()
            for option in options
            for value in ("0", "-1")
        ],
    )
    def test_below_one_is_a_usage_error(
        self, tmp_path: Path, command: str, option: str, value: str
    ) -> None:
        """Rejected before any work: a negative --chunk-mb used to loop forever."""
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, [command, str(gz), option, value])
        assert result.exit_code == 2
        output = _unstyle(result.output)
        # Typer colors the error panel when it detects CI (GITHUB_ACTIONS).
        assert option in output
        assert "is not in the range x>=1" in output
        run.assert_not_called()

    @pytest.mark.parametrize(
        "option", ["--pg-fk", "--write-workers", "--index-workers"]
    )
    def test_renamed_options_are_no_such_option(
        self, tmp_path: Path, option: str
    ) -> None:
        """Clean cutover (#38): the old pre-rename flag names are gone, not aliased."""
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["load", str(gz), option, "1"])
        assert result.exit_code == 2
        assert "No such option" in _unstyle(result.output)
        run.assert_not_called()


class TestFkOption:
    def test_fk_declared_in_sqlite(self, tmp_path: Path) -> None:
        """--fk (renamed from --pg-fk, #38) reaches SQLite: the child tables declare their keys.

        Enforcement is per connection, so it can't be checked once the CLI has closed the database;
        ``test_sqlite_writer.py``'s ``test_fk_rejects_orphan_child_row`` covers it.
        """
        gz = tmp_path / "discogs_20260301_artists.xml.gz"
        with gzip.open(gz, "wb") as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(ARTISTS_XML.encode())
            f.write(b"</artists>")

        db = tmp_path / "out.db"
        result = runner.invoke(
            app,
            [
                "load",
                str(gz),
                "--dsn",
                str(db),
                "--fk",
                "--parse-workers",
                "1",
                "--no-progress",
            ],
        )
        assert result.exit_code == 0, _unstyle(result.output)

        conn = sqlite3.connect(db)
        fks = conn.execute("PRAGMA foreign_key_list(artist_aliases)").fetchall()
        conn.close()
        assert len(fks) > 0


class TestProfile:
    """--profile lists a commit row only for writers that time a commit (#118)."""

    @staticmethod
    def _flush_table_rows(output: str) -> list[str]:
        """First cell of each row in the "Per-table flush" table."""
        lines = _unstyle(output).splitlines()
        start = next(i for i, line in enumerate(lines) if "Per-table flush" in line)
        # Skip the title, the header row and the rule under it; the table ends at the first blank line.
        rows = []
        for line in lines[start + 3 :]:
            if not line.strip():
                break
            rows.append(line.split()[0])
        return rows

    @staticmethod
    def _artists_gz(tmp_path: Path) -> Path:
        gz = tmp_path / _ENTITY_FILE
        with gzip.open(gz, "wb") as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(ARTISTS_XML.encode())
            f.write(b"</artists>")
        return gz

    @pytest.mark.parametrize("fmt", ["jsonl", "parquet"])
    def test_convert_has_no_commit_row(
        self, fmt: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        gz = self._artists_gz(tmp_path)
        _widen_console(monkeypatch)
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "-f",
                fmt,
                "--output",
                str(tmp_path / "out"),
                "--parse-workers",
                "1",
                "--profile",
                "--no-progress",
            ],
        )
        assert result.exit_code == 0, _unstyle(result.output)
        assert self._flush_table_rows(result.output) == ENTITIES["artists"].table_order

    def test_sqlite_load_has_commit_row(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        gz = self._artists_gz(tmp_path)
        _widen_console(monkeypatch)
        result = runner.invoke(
            app,
            [
                "load",
                str(gz),
                "--dsn",
                str(tmp_path / "out.db"),
                "--parse-workers",
                "1",
                "--profile",
                "--no-progress",
            ],
        )
        assert result.exit_code == 0, _unstyle(result.output)
        assert self._flush_table_rows(result.output) == [
            *ENTITIES["artists"].table_order,
            "commit",
        ]


class TestStrict:
    """--strict fails the run once it has reported every unhandled element, before any output is published (#179)."""

    _TARGETS: ClassVar[list[str]] = ["jsonl", "parquet", "sqlite"]
    _UNHANDLED: ClassVar[str] = (
        "<artist><id>99</id><name>X</name><new_tag/><other_tag/></artist>\n"
    )

    @staticmethod
    def _args(target: str, gz: Path, tmp_path: Path) -> list[str]:
        if target == "sqlite":
            args = ["load", str(gz), "--dsn", str(tmp_path / "out.db")]
        else:
            args = ["convert", str(gz), "-f", target, "--output", str(tmp_path / "out")]
        return [*args, "--parse-workers", "1", "--no-progress"]

    @staticmethod
    def _artists_gz(tmp_path: Path, *, unhandled: bool) -> Path:
        gz = tmp_path / _ENTITY_FILE
        with gzip.open(gz, "wt", encoding="utf-8") as f:
            f.write("<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(ARTISTS_XML)
            if unhandled:
                f.write(TestStrict._UNHANDLED)
            f.write("</artists>")
        return gz

    @staticmethod
    def _published(target: str, tmp_path: Path) -> bool:
        """Whether the run published its output: the SQLite indexes ``finalize()`` builds, or the entity's directory
        of files."""
        if target != "sqlite":
            return (tmp_path / "out" / "artists").exists()
        conn = sqlite3.connect(tmp_path / "out.db")
        try:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        finally:
            conn.close()
        return bool(rows)

    @pytest.mark.parametrize("target", _TARGETS)
    def test_clean_input_with_strict_succeeds(
        self, target: str, tmp_path: Path
    ) -> None:
        gz = self._artists_gz(tmp_path, unhandled=False)
        result = runner.invoke(app, [*self._args(target, gz, tmp_path), "--strict"])
        output = _unstyle(result.output)
        assert result.exit_code == 0, output
        assert "Strict" not in output
        assert self._published(target, tmp_path)

    @pytest.mark.parametrize("target", _TARGETS)
    def test_unhandled_elements_fail_the_run_before_publishing(
        self, monkeypatch: pytest.MonkeyPatch, target: str, tmp_path: Path
    ) -> None:
        _widen_console(monkeypatch)
        gz = self._artists_gz(tmp_path, unhandled=True)
        result = runner.invoke(app, [*self._args(target, gz, tmp_path), "--strict"])
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert type(result.exception) is SystemExit, repr(result.exception)
        lines = [line.strip() for line in output.splitlines()]
        reported = [line for line in lines if line.startswith(("Strict", "Error"))]
        assert reported == [
            "Strict         unhandled XML element <new_tag> in <artist>",
            "Strict         unhandled XML element <other_tag> in <artist>",
            "Error: --strict: 2 unhandled XML element paths in <artist>; see the Strict lines above",
        ]
        assert not self._published(target, tmp_path)

    @pytest.mark.parametrize("target", _TARGETS)
    def test_unhandled_elements_without_strict_succeed(
        self, target: str, tmp_path: Path
    ) -> None:
        gz = self._artists_gz(tmp_path, unhandled=True)
        result = runner.invoke(app, self._args(target, gz, tmp_path))
        output = _unstyle(result.output)
        assert result.exit_code == 0, output
        assert "Strict" not in output
        assert self._published(target, tmp_path)


class TestFailedRunLeavesNoNewOutput:
    """A failed run removes the database file or output directory it created and left empty (#194), never one that
    existed before."""

    @staticmethod
    def _existing_db(db: Path, existing: str) -> bytes | None:
        """Create ``db`` as ``existing`` says (``"none"``, ``"empty"`` or ``"database"``); return its bytes."""
        if existing == "empty":
            db.touch()
        elif existing == "database":
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE mine (x)")
            conn.commit()
            conn.close()
        return db.read_bytes() if db.exists() else None

    @staticmethod
    def _truncated_gz(tmp_path: Path) -> Path:
        records = "".join(
            f"<artist><id>{i}</id><name>Artist {i}</name></artist>\n"
            for i in range(1, 5001)
        )
        data = gzip.compress(f"<artists>\n{records}</artists>".encode())
        gz = tmp_path / _ENTITY_FILE
        gz.write_bytes(data[: len(data) // 2])
        return gz

    @pytest.mark.parametrize("existing", ["none", "empty", "database"])
    def test_ctrl_c_before_load(self, tmp_path: Path, existing: str) -> None:
        db = tmp_path / "out.db"
        before = self._existing_db(db, existing)
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run", side_effect=KeyboardInterrupt):
            result = runner.invoke(app, ["load", str(gz), "--dsn", str(db)])
        assert result.exit_code == 130, _unstyle(result.output)
        assert (db.read_bytes() if db.exists() else None) == before

    @pytest.mark.parametrize("output_format", ["jsonl", "parquet"])
    @pytest.mark.parametrize("existing", [False, True])
    def test_failed_convert(
        self, tmp_path: Path, existing: bool, output_format: str
    ) -> None:
        """A run failing after setup() created ``<output>`` (and its missing parents) leaves none of them."""
        out = tmp_path / "parent" / "out"
        if existing:
            out.mkdir(parents=True)
        gz = TestStrict._artists_gz(tmp_path, unhandled=True)
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "-f",
                output_format,
                "--no-progress",
                "--output",
                str(out),
                "--parse-workers",
                "1",
                "--strict",
            ],
        )
        assert result.exit_code == 1, _unstyle(result.output)
        if existing:
            assert list(out.iterdir()) == []
        else:
            assert not (tmp_path / "parent").exists()

    @pytest.mark.parametrize("existing", ["none", "empty", "database"])
    def test_truncated_gz(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, existing: str
    ) -> None:
        _widen_console(monkeypatch)
        db = tmp_path / "out.db"
        before = self._existing_db(db, existing)
        gz = self._truncated_gz(tmp_path)
        result = runner.invoke(
            app, ["load", str(gz), "--dsn", str(db), "--no-progress"]
        )
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert f"Error: failed to decompress {gz.name}" in output
        assert (db.read_bytes() if db.exists() else None) == before


class TestSizesInMiB:
    def test_status_lines_report_mib(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Sizes are computed in units of 1,048,576 bytes, so they're labeled MiB (#195)."""
        _widen_console(monkeypatch)
        records = "".join(
            f"<artist><id>{i}</id><name>Artist number {i}</name></artist>\n"
            for i in range(1, 50001)
        )
        xml = f"<artists>\n{records}</artists>".encode()
        gz = tmp_path / _ENTITY_FILE
        gz.write_bytes(gzip.compress(xml))
        out = tmp_path / "out"
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "-f",
                "jsonl",
                "--output",
                str(out),
                "--no-progress",
                "--chunk-mb",
                "1",
            ],
        )
        output = _unstyle(result.output)
        assert result.exit_code == 0, output
        output_bytes = sum(p.stat().st_size for p in (out / "artists").iterdir())
        lines = [line.strip() for line in output.splitlines()]
        assert "Chunk size     1 MiB" in lines
        # A second "Decompress" line reports the time spent decompressing, among the timings.
        expected = f"Decompress     {len(xml) / 2**20:,.1f} MiB ["
        assert any(line.startswith(expected) for line in lines), output
        assert (
            f"Output         {len(ENTITIES['artists'].table_order)} files, "
            f"{output_bytes / 2**20:,.1f} MiB total"
        ) in lines


class TestUnicodeConvert:
    def test_gzip_jsonl_round_trips_unicode_text(self, tmp_path: Path) -> None:
        """Non-ASCII text, entity references and CDATA reach gzip-compressed JSONL intact."""
        gz = tmp_path / _ENTITY_FILE
        with gzip.open(gz, "wt", encoding="utf-8") as f:
            f.write("<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(UNICODE_ARTISTS_XML)
            f.write("</artists>")
        out = tmp_path / "out"

        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "--compression",
                "gzip",
                "-f",
                "jsonl",
                "--no-progress",
                "--output",
                str(out),
                "--parse-workers",
                "1",
            ],
        )

        assert result.exit_code == 0, _unstyle(result.output)
        for table_name in ENTITIES["artists"].table_order:
            with gzip.open(
                out / "artists" / f"{table_name}.jsonl.gz", "rt", encoding="utf-8"
            ) as f:
                rows = [json.loads(line) for line in f]
            assert rows == UNICODE_ARTISTS_ROWS.get(table_name, []), table_name


class TestCompressionLevel:
    """--compression-level (#32): the level must actually reach the codec through the real CLI."""

    _OUT_PATH: ClassVar[dict[tuple[str, str], str]] = {
        ("jsonl", "bzip2"): "artists/artists.jsonl.bz2",
        ("jsonl", "gzip"): "artists/artists.jsonl.gz",
        ("parquet", "brotli"): "artists/artists.parquet",
        ("parquet", "gzip"): "artists/artists.parquet",
        ("parquet", "zstd"): "artists/artists.parquet",
    }

    def _convert(self, gz: Path, out: Path, *extra_args: str) -> None:
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "--output",
                str(out),
                "--parse-workers",
                "1",
                "--no-progress",
                *extra_args,
            ],
        )
        assert result.exit_code == 0, _unstyle(result.output)

    @pytest.mark.parametrize(
        "fmt, compression, low, high",
        [
            ("jsonl", "gzip", 0, 9),
            ("jsonl", "bzip2", 1, 9),
            ("parquet", "brotli", *_PARQUET_LEVEL_RANGE["brotli"]),
            ("parquet", "gzip", *_PARQUET_LEVEL_RANGE["gzip"]),
            ("parquet", "zstd", *_PARQUET_LEVEL_RANGE["zstd"]),
        ],
        ids=[
            "jsonl-gzip",
            "jsonl-bzip2",
            "parquet-brotli",
            "parquet-gzip",
            "parquet-zstd",
        ],
    )
    def test_level_reaches_the_codec(
        self, tmp_path: Path, fmt: str, compression: str, low: int, high: int
    ) -> None:
        """Both boundary levels are accepted, and they produce different output: the level isn't
        accepted and then silently dropped before reaching the real writer."""
        gz = _compressible_artists_gz(tmp_path)
        sizes = {}
        for level in (low, high):
            out = tmp_path / f"out-{level}"
            self._convert(
                gz,
                out,
                "-f",
                fmt,
                "--compression",
                compression,
                "--compression-level",
                str(level),
            )
            sizes[level] = (out / self._OUT_PATH[(fmt, compression)]).stat().st_size
        assert sizes[low] != sizes[high]

    def test_default_jsonl_gzip_is_level_6(self, tmp_path: Path) -> None:
        """Omitting --compression-level keeps #32's gzip-6 default, not the library's level 9."""
        gz = _compressible_artists_gz(tmp_path)
        out_default = tmp_path / "out-default"
        out_level6 = tmp_path / "out-level6"
        self._convert(gz, out_default, "-f", "jsonl", "--compression", "gzip")
        self._convert(
            gz,
            out_level6,
            "-f",
            "jsonl",
            "--compression",
            "gzip",
            "--compression-level",
            "6",
        )
        default_size = (out_default / self._OUT_PATH[("jsonl", "gzip")]).stat().st_size
        level6_size = (out_level6 / self._OUT_PATH[("jsonl", "gzip")]).stat().st_size
        assert default_size == level6_size

    @pytest.mark.parametrize(
        "fmt, compression, library_default",
        [
            ("jsonl", "bzip2", 9),
            ("parquet", "brotli", _PARQUET_LEVEL_DEFAULT["brotli"]),
            ("parquet", "gzip", _PARQUET_LEVEL_DEFAULT["gzip"]),
            ("parquet", "zstd", _PARQUET_LEVEL_DEFAULT["zstd"]),
        ],
        ids=["jsonl-bzip2", "parquet-brotli", "parquet-gzip", "parquet-zstd"],
    )
    def test_default_other_codecs_match_their_own_library_default(
        self, tmp_path: Path, fmt: str, compression: str, library_default: int
    ) -> None:
        """Omitting --compression-level leaves every codec but JSONL gzip at its own library default."""
        gz = _compressible_artists_gz(tmp_path)
        out_default = tmp_path / "out-default"
        out_explicit = tmp_path / "out-explicit"
        self._convert(gz, out_default, "-f", fmt, "--compression", compression)
        self._convert(
            gz,
            out_explicit,
            "-f",
            fmt,
            "--compression",
            compression,
            "--compression-level",
            str(library_default),
        )
        default_size = (out_default / self._OUT_PATH[(fmt, compression)]).stat().st_size
        explicit_size = (
            (out_explicit / self._OUT_PATH[(fmt, compression)]).stat().st_size
        )
        assert default_size == explicit_size

    @pytest.mark.parametrize("fmt", ["jsonl", "parquet"])
    def test_rejects_level_for_none(self, tmp_path: Path, fmt: str) -> None:
        gz = _make_gz(tmp_path)
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "-f",
                fmt,
                "--compression",
                "none",
                "--compression-level",
                "5",
            ],
        )
        assert result.exit_code == 1
        output = _unstyle(result.output)
        assert "none does not support a compression level." in output

    def test_rejects_level_for_snappy(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "-f",
                "parquet",
                "--compression",
                "snappy",
                "--compression-level",
                "5",
            ],
        )
        assert result.exit_code == 1
        output = _unstyle(result.output)
        assert "snappy does not support a compression level." in output

    @pytest.mark.parametrize(
        "fmt, compression, level",
        [
            ("jsonl", "gzip", -1),
            ("jsonl", "gzip", 10),
            ("jsonl", "bzip2", 0),
            ("jsonl", "bzip2", 10),
            ("parquet", "brotli", _PARQUET_LEVEL_RANGE["brotli"][0] - 1),
            ("parquet", "brotli", _PARQUET_LEVEL_RANGE["brotli"][1] + 1),
            ("parquet", "gzip", _PARQUET_LEVEL_RANGE["gzip"][0] - 1),
            ("parquet", "gzip", _PARQUET_LEVEL_RANGE["gzip"][1] + 1),
            ("parquet", "zstd", _PARQUET_LEVEL_RANGE["zstd"][0] - 1),
            ("parquet", "zstd", _PARQUET_LEVEL_RANGE["zstd"][1] + 1),
        ],
        ids=[
            "jsonl-gzip-low",
            "jsonl-gzip-high",
            "jsonl-bzip2-low",
            "jsonl-bzip2-high",
            "parquet-brotli-low",
            "parquet-brotli-high",
            "parquet-gzip-low",
            "parquet-gzip-high",
            "parquet-zstd-low",
            "parquet-zstd-high",
        ],
    )
    def test_rejects_out_of_range_level(
        self, tmp_path: Path, fmt: str, compression: str, level: int
    ) -> None:
        gz = _make_gz(tmp_path)
        result = runner.invoke(
            app,
            [
                "convert",
                str(gz),
                "-f",
                fmt,
                "--compression",
                compression,
                "--compression-level",
                str(level),
            ],
        )
        assert result.exit_code == 1
        assert "is out of range for" in _unstyle(result.output)

    def test_format_compression_mismatch_error_still_wins(self, tmp_path: Path) -> None:
        """An invalid format/compression pairing is rejected before --compression-level is even looked at."""
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(
                app,
                [
                    "convert",
                    str(gz),
                    "-f",
                    "jsonl",
                    "--compression",
                    "zstd",
                    "--compression-level",
                    "5",
                ],
            )
        assert result.exit_code == 1
        output = _unstyle(result.output)
        assert "unsupported compression 'zstd' for jsonl" in output
        assert "compression-level" not in output
        run.assert_not_called()


class TestLoadRejectsPostgreSqlOnlyOptionsForSqlite:
    """get_writer()'s SQLite rejection must track the CLI's actual flag names, not copied literals."""

    def test_defaults_do_not_trigger_rejection(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        db = tmp_path / "out.db"
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with patch("discogskit.cli.pipeline.run", return_value=pipeline_result):
            result = runner.invoke(app, ["load", str(gz), "--dsn", str(db)])
        assert result.exit_code == 0, _unstyle(result.output)

    @pytest.mark.parametrize(
        "flag, value",
        [
            ("--pg-create-schema", None),
            ("--pg-index-workers", "4"),
            ("--pg-schema", "custom"),
            ("--pg-unlogged", None),
            ("--pg-write-workers", "3"),
        ],
        ids=["create-schema", "index-workers", "schema", "unlogged", "write-workers"],
    )
    def test_rejects_each_postgresql_only_flag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        flag: str,
        value: str | None,
    ) -> None:
        _widen_console(monkeypatch)
        gz = _make_gz(tmp_path)
        db = tmp_path / "out.db"
        args = ["load", str(gz), "--dsn", str(db), flag]
        if value is not None:
            args.append(value)
        if flag == "--pg-create-schema":
            # --pg-create-schema requires --pg-schema; add it so the rejection under test
            # (not the --pg-create-schema/--pg-schema pairing check) is the one that fires.
            args += ["--pg-schema", "custom"]
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with patch("discogskit.cli.pipeline.run", return_value=pipeline_result):
            result = runner.invoke(app, args)
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert flag in output
        assert "can only be used with PostgreSQL, not a SQLite DSN." in output

    def test_unknown_flag_is_a_typer_error_not_a_rejection(
        self, tmp_path: Path
    ) -> None:
        """A made-up flag must fail via Typer's own parsing (exit 2), not look like our exit-1 rejection."""
        gz = _make_gz(tmp_path)
        db = tmp_path / "out.db"
        result = runner.invoke(
            app, ["load", str(gz), "--dsn", str(db), "--pg-bogus-option"]
        )
        output = _unstyle(result.output)
        assert result.exit_code == 2, output
        assert "No such option" in output


class TestLoadRejectsInMemorySqlite:
    @pytest.mark.parametrize(
        "dsn",
        [
            "sqlite:///:memory:",
            "sqlite:///:MEMORY:",
            "sqlite:///file::memory:",
            "sqlite:///file::memory:%00.db",
            "file::memory:#discogs.db",
        ],
    )
    def test_fails_before_decompressing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, dsn: str
    ) -> None:
        """The whole load would go into a private database and be discarded when the process exits."""
        _widen_console(monkeypatch)
        monkeypatch.chdir(tmp_path)
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["load", str(gz), "--dsn", dsn])
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert f"Invalid SQLite DSN: {dsn!r} names an in-memory database" in output
        run.assert_not_called()
        assert list(tmp_path.iterdir()) == [gz]


class TestLoadHidesPasswordInMalformedPostgreSqlDsn:
    @pytest.mark.parametrize(
        "dsn, shown",
        [
            (
                "postgresql://user:s3%zzcr3t@localhost/discogs",
                "postgresql://user:***@localhost/discogs",
            ),
            (
                "postgresql://user:s3cr3t@[::1]x/discogs",
                "postgresql://user:***@[::1]x/discogs",
            ),
            (
                "postgresql://us#er:s3cr3t@[::1]x/discogs",
                "postgresql://us#er:***@[::1]x/discogs",
            ),
        ],
    )
    def test_fails_without_printing_the_password(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, dsn: str, shown: str
    ) -> None:
        """libpq quotes the token it can't parse, or the whole URI, in its error (#207)."""
        _widen_console(monkeypatch)
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["load", str(gz), "--dsn", dsn])
        output = _unstyle(result.output)
        assert result.exit_code == 1, output
        assert f"Invalid PostgreSQL DSN: {shown!r}." in output
        assert "s3" not in output
        assert "cr3t" not in output
        run.assert_not_called()


class TestConsoleScriptEntryPoint:
    def test_loads_in_a_fresh_interpreter(self) -> None:
        """Tools that load console scripts through importlib.metadata must get the Typer app.

        A fresh interpreter, because importing ``discogskit.cli`` here (as this module does) sets the
        package's ``cli`` attribute and hides a ``discogskit:cli.app`` entry point that fails elsewhere.
        """
        code = (
            "from importlib.metadata import entry_points\n"
            "import typer\n"
            "app = entry_points(group='console_scripts')['discogskit'].load()\n"
            "assert isinstance(app, typer.Typer), app\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, check=False, text=True
        )
        assert result.returncode == 0, result.stderr


# The real CLI in a fresh interpreter whose stdout is a pipe, with every chunk write slowed down.
_SLOW_WRITE_CLI = """\
import sys
import time

from discogskit.cli import app
from discogskit.writers.jsonl import JSONLWriter

real_write_chunk = JSONLWriter.write_chunk


def slow_write_chunk(self, tables):
    time.sleep(0.5)
    return real_write_chunk(self, tables)


JSONLWriter.write_chunk = slow_write_chunk
app(sys.argv[1:], prog_name="discogskit")
"""


class TestPipedOutput:
    def test_chunk_lines_arrive_as_each_chunk_is_written(self, tmp_path: Path) -> None:
        """Off a terminal, the default run prints per-chunk lines, each as its chunk is written, not all at exit
        (#105)."""
        gz = tmp_path / _ENTITY_FILE
        with gzip.open(gz, "wb", compresslevel=1) as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            for i in range(1, 50_001):
                f.write(
                    b"<artist><id>%d</id><name>Artist %d</name></artist>\n" % (i, i)
                )
            f.write(b"</artists>")
        args = ["convert", str(gz), "-f", "jsonl", "--output", str(tmp_path / "out")]
        args += ["--chunk-mb", "1", "--parse-workers", "1"]

        arrivals = []
        with subprocess.Popen(
            [sys.executable, "-c", _SLOW_WRITE_CLI, *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        ) as proc:
            assert proc.stdout is not None
            output = []
            for line in proc.stdout:
                output.append(line)
                if line.startswith("  chunk "):
                    arrivals.append((time.monotonic(), proc.poll()))
        assert proc.returncode == 0, "".join(output)

        # Each chunk write takes 0.5 s, so lines that arrive as written are spread out, the first while the run
        # is still going.
        assert len(arrivals) >= 3, "".join(output)
        assert arrivals[0][1] is None
        assert arrivals[-1][0] - arrivals[0][0] >= 0.4 * (len(arrivals) - 1)
