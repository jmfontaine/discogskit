"""Tests for JSONLWriter full lifecycle."""

from __future__ import annotations

import bz2
import gzip
import json
import re

import pytest

from discogskit.writers.jsonl import JSONLWriter
from tests.conftest import (
    empty_ipc,
    fail_after_first_move,
    ipc_to_record_batches,
    needs_permissions,
)


class TestJSONLWriter:
    def test_full_lifecycle(self, tmp_path, entity, ipc_dict):
        writer = JSONLWriter(str(tmp_path))
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        entity_dir = tmp_path / "artists"
        for table_name in entity.table_order:
            path = entity_dir / f"{table_name}.jsonl"
            assert path.exists()

        # Verify content
        artists_lines = (entity_dir / "artists.jsonl").read_text().strip().split("\n")
        assert len(artists_lines) == 2
        row = json.loads(artists_lines[0])
        assert row["id"] == 1
        assert row["name"] == "DJ Test"
        assert row["namevariations"] == ["DJ T", "Test"]

        aliases_lines = (
            (entity_dir / "artist_aliases.jsonl").read_text().strip().split("\n")
        )
        assert len(aliases_lines) == 1
        assert json.loads(aliases_lines[0])["alias_id"] == 10

    def test_gzip_compression(self, tmp_path, entity, ipc_dict):
        writer = JSONLWriter(str(tmp_path), compression="gzip")
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        entity_dir = tmp_path / "artists"
        for table_name in entity.table_order:
            path = entity_dir / f"{table_name}.jsonl.gz"
            assert path.exists()
            assert path.stat().st_size > 0

    def test_bzip2_compression(self, tmp_path, entity, ipc_dict):
        writer = JSONLWriter(str(tmp_path), compression="bzip2")
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        entity_dir = tmp_path / "artists"
        for table_name in entity.table_order:
            path = entity_dir / f"{table_name}.jsonl.bz2"
            assert path.exists()
            assert path.stat().st_size > 0

    @pytest.mark.parametrize("compression", ["gzip", "bzip2"])
    def test_multi_chunk_compressed_round_trip(
        self, tmp_path, entity, ipc_dict, compression
    ):
        """Two non-empty chunks across the write_chunk() boundary must decompress to valid JSONL.

        Catches a dropped or misplaced newline at the chunk boundary, which `.strip()`-based
        assertions on a single chunk's output cannot.
        """
        writer = JSONLWriter(str(tmp_path), compression=compression)
        tables = ipc_to_record_batches(ipc_dict)
        expected_rows = {name: batch.to_pylist() for name, batch in tables.items()}
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        ext = ".jsonl.gz" if compression == "gzip" else ".jsonl.bz2"
        opener = gzip.open if compression == "gzip" else bz2.open
        entity_dir = tmp_path / "artists"
        for table_name, rows in expected_rows.items():
            path = entity_dir / f"{table_name}{ext}"
            with opener(path, "rt", encoding="utf-8") as f:
                text = f.read()
            assert text.endswith("\n")
            lines = text[:-1].split("\n")
            assert len(lines) == len(rows) * 2
            assert [json.loads(line) for line in lines] == rows + rows

    def test_write_chunk_records_table_timings(self, tmp_path, entity, ipc_dict):
        """write_chunk returns per-table flush timing."""
        writer = JSONLWriter(str(tmp_path))
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        assert "artists" in timings
        assert all(v >= 0 for v in timings.values())

    def test_empty_batch_records_table_timings(self, tmp_path, entity):
        """Empty batches still get a timing entry."""
        writer = JSONLWriter(str(tmp_path))
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(empty_ipc("artists")))
            writer.finalize(entity)
        finally:
            writer.close()

        assert "artists" in timings

    @pytest.mark.parametrize("compression", ["bzip2", "gzip", "none"])
    def test_close_without_finalize_leaves_no_output(
        self, tmp_path, entity, ipc_dict, compression
    ):
        """A run that fails before finalize() leaves no final-named or staged files."""
        out = tmp_path / "out"
        writer = JSONLWriter(str(out), compression=compression)
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.close()

        assert list(out.iterdir()) == []

    @needs_permissions
    def test_unwritable_entity_dir_fails_in_setup(self, tmp_path, entity):
        """commit() couldn't move files into it, so setup() fails, before any parsing."""
        out = tmp_path / "out"
        entity_dir = out / "artists"
        entity_dir.mkdir(parents=True)
        entity_dir.chmod(0o500)
        writer = JSONLWriter(str(out))
        try:
            with pytest.raises(OSError, match=re.escape(str(entity_dir))):
                writer.setup(entity)
        finally:
            writer.close()
            entity_dir.chmod(0o700)
        assert [p.name for p in out.iterdir()] == ["artists"]
        assert list(entity_dir.iterdir()) == []

    def test_failed_commit_keeps_staged_files(
        self, monkeypatch, tmp_path, entity, ipc_dict
    ):
        """A move failing after setup() keeps the finished files and names their directory."""
        out = tmp_path / "out"
        writer = JSONLWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        fail_after_first_move(monkeypatch)
        try:
            with pytest.raises(OSError, match="completed files") as excinfo:
                writer.finalize(entity)
        finally:
            writer.close()

        root, *children = [f"{t}.jsonl" for t in entity.table_order]
        assert [p.name for p in (out / "artists").iterdir()] == [root]
        (staging,) = [p for p in out.iterdir() if p.name != "artists"]
        assert str(staging) in str(excinfo.value)
        assert sorted(p.name for p in staging.iterdir()) == sorted(children)
        aliases = (staging / "artist_aliases.jsonl").read_text().splitlines()
        assert json.loads(aliases[0])["alias_id"] == 10

    def test_failed_overwrite_keeps_previous_output(self, tmp_path, entity, ipc_dict):
        """With overwrite=True, a run that fails before finalize() keeps the old files."""
        out = tmp_path / "out"
        writer = JSONLWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)
        entity_dir = out / "artists"
        before = {p.name: p.read_bytes() for p in entity_dir.iterdir()}

        writer2 = JSONLWriter(str(out), overwrite=True)
        writer2.setup(entity)
        writer2.close()

        assert {p.name: p.read_bytes() for p in entity_dir.iterdir()} == before
        assert [p.name for p in out.iterdir()] == ["artists"]

    def test_overwrite_raises_when_output_exists(self, tmp_path, entity, ipc_dict):
        """setup() raises OutputExistsError when files exist and overwrite=False."""
        from discogskit.writers import OutputExistsError

        writer = JSONLWriter(str(tmp_path))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)

        writer2 = JSONLWriter(str(tmp_path))
        with pytest.raises(OutputExistsError, match="--overwrite"):
            writer2.setup(entity)

    def test_overwrite_succeeds_when_enabled(self, tmp_path, entity, ipc_dict):
        """setup() succeeds when files exist and overwrite=True."""
        writer = JSONLWriter(str(tmp_path))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)

        writer2 = JSONLWriter(str(tmp_path), overwrite=True)
        writer2.setup(entity)
        writer2.write_chunk(ipc_to_record_batches(ipc_dict))
        writer2.finalize(entity)


class TestJSONLWriterCompression:
    @pytest.mark.parametrize("compression", ["zstd", "snappy", "GZIP", ""])
    def test_unsupported_compression_raises(self, tmp_path, compression):
        """It used to write plain output while the status line claimed the codec."""
        with pytest.raises(ValueError, match="bzip2, gzip, none"):
            JSONLWriter(str(tmp_path), compression=compression)
