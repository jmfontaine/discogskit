"""Tests for ParquetWriter full lifecycle."""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from discogskit.writers.parquet import ParquetWriter
from tests.conftest import (
    empty_ipc,
    fail_after_first_move,
    ipc_to_record_batches,
    needs_permissions,
)


class TestParquetWriter:
    def test_full_lifecycle(self, tmp_path, entity, ipc_dict):
        writer = ParquetWriter(str(tmp_path))
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        entity_dir = tmp_path / "artists"
        for table_name in entity.table_order:
            path = entity_dir / f"{table_name}.parquet"
            assert path.exists()

        # Read back and verify content
        artists_table = pq.read_table(str(entity_dir / "artists.parquet"))
        assert artists_table.num_rows == 2
        assert artists_table.column("id").to_pylist() == [1, 2]
        assert artists_table.column("name").to_pylist() == ["DJ Test", "Minimal Artist"]

        # Every table's schema, nullability included, round-trips
        for table_name in entity.table_order:
            read_table = pq.read_table(str(entity_dir / f"{table_name}.parquet"))
            assert read_table.schema.equals(entity.schemas[table_name]), table_name

    def test_write_chunk_records_table_timings(self, tmp_path, entity, ipc_dict):
        """write_chunk returns per-table flush timing."""
        writer = ParquetWriter(str(tmp_path))
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
        writer = ParquetWriter(str(tmp_path))
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(empty_ipc("artists")))
            writer.finalize(entity)
        finally:
            writer.close()

        assert "artists" in timings

    def test_close_without_finalize_leaves_no_output(self, tmp_path, entity, ipc_dict):
        """A run that fails before finalize() leaves no final-named or staged files, nor the directory it created."""
        out = tmp_path / "out"
        writer = ParquetWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.close()

        assert not out.exists()

    def test_close_after_chdir_removes_only_the_directory_it_created(
        self, monkeypatch, tmp_path, entity
    ):
        """A relative output is removed where it was created, not under the new working directory."""
        (tmp_path / "a").mkdir()
        (tmp_path / "b" / "out").mkdir(parents=True)
        monkeypatch.chdir(tmp_path / "a")
        writer = ParquetWriter("out")
        writer.setup(entity)
        monkeypatch.chdir(tmp_path / "b")
        writer.close()

        assert list((tmp_path / "a").iterdir()) == []
        assert (tmp_path / "b" / "out").is_dir()

    def test_close_removes_only_directories_it_made(self, tmp_path, entity):
        """``missing/../existing`` names a directory that existed before: only ``missing`` was created."""
        (tmp_path / "existing").mkdir()
        writer = ParquetWriter(str(tmp_path / "missing" / ".." / "existing"))
        writer.setup(entity)
        writer.close()

        assert [p.name for p in tmp_path.iterdir() if p.is_dir()] == ["existing"]
        assert list((tmp_path / "existing").iterdir()) == []

    def test_setup_survives_another_run_removing_the_output_dir(
        self, monkeypatch, tmp_path, entity, ipc_dict
    ):
        """Another run that fails can remove the empty output directory between its creation and the staging one's."""
        out = tmp_path / "out"
        mkdtemp = tempfile.mkdtemp
        removed = []

        def remove_then_mkdtemp(*args, **kwargs):
            if not removed:
                out.rmdir()
                removed.append(True)
            return mkdtemp(*args, **kwargs)

        monkeypatch.setattr(tempfile, "mkdtemp", remove_then_mkdtemp)
        writer = ParquetWriter(str(out))
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        assert removed
        assert pq.read_table(str(out / "artists" / "artists.parquet")).num_rows == 2

    def test_setup_survives_another_run_removing_a_parent_dir(
        self, monkeypatch, tmp_path, entity
    ):
        """Another run that fails can remove an empty parent between its creation and the output directory's."""
        parent = tmp_path / "parent"
        out = parent / "out"
        mkdir = Path.mkdir
        removed = []

        def remove_parent_then_mkdir(self, *args, **kwargs):
            if self == out and not removed:
                parent.rmdir()
                removed.append(True)
            return mkdir(self, *args, **kwargs)

        monkeypatch.setattr(Path, "mkdir", remove_parent_then_mkdir)
        writer = ParquetWriter(str(out))
        writer.setup(entity)
        monkeypatch.undo()
        writer.close()

        assert removed
        assert list(tmp_path.iterdir()) == []

    def test_close_after_interrupted_commit_removes_the_dirs_it_created(
        self, monkeypatch, tmp_path, entity, ipc_dict
    ):
        """Ctrl+C before the first file moves leaves neither ``<output>`` nor ``<output>/<entity>``."""
        out = tmp_path / "out"
        writer = ParquetWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))

        def interrupt(src, dst):
            raise KeyboardInterrupt

        monkeypatch.setattr(os, "replace", interrupt)
        try:
            with pytest.raises(KeyboardInterrupt):
                writer.finalize(entity)
        finally:
            writer.close()

        assert not out.exists()

    def test_failed_staging_setup_removes_the_dirs_it_created(
        self, monkeypatch, tmp_path, entity
    ):
        """setup() failing after creating ``<output>`` leaves no writer state to clean up, so it removes it itself."""
        out = tmp_path / "parent" / "out"

        def no_space(*args, **kwargs):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(tempfile, "mkdtemp", no_space)
        writer = ParquetWriter(str(out))
        with pytest.raises(OSError, match="No space left"):
            writer.setup(entity)
        writer.close()

        assert not (tmp_path / "parent").exists()

    @needs_permissions
    def test_unwritable_entity_dir_fails_in_setup(self, tmp_path, entity):
        """commit() couldn't move files into it, so setup() fails, before any parsing."""
        out = tmp_path / "out"
        entity_dir = out / "artists"
        entity_dir.mkdir(parents=True)
        entity_dir.chmod(0o500)
        writer = ParquetWriter(str(out))
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
        writer = ParquetWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        fail_after_first_move(monkeypatch)
        try:
            with pytest.raises(OSError, match="completed files") as excinfo:
                writer.finalize(entity)
        finally:
            writer.close()

        root, *children = [f"{t}.parquet" for t in entity.table_order]
        assert [p.name for p in (out / "artists").iterdir()] == [root]
        (staging,) = [p for p in out.iterdir() if p.name != "artists"]
        assert str(staging) in str(excinfo.value)
        assert sorted(p.name for p in staging.iterdir()) == sorted(children)
        aliases = pq.read_table(str(staging / "artist_aliases.parquet"))
        assert aliases.column("alias_id").to_pylist() == [10]

    def test_failed_first_move_removes_the_entity_dir_it_created(
        self, monkeypatch, tmp_path, entity, ipc_dict
    ):
        """No file reached ``<output>/<entity>/``, so it goes; the staged files stay, outside it."""
        out = tmp_path / "out"
        writer = ParquetWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))

        def fail(src, dst):
            raise OSError(18, "Invalid cross-device link")

        monkeypatch.setattr(os, "replace", fail)
        try:
            with pytest.raises(OSError, match="after 0 of"):
                writer.finalize(entity)
        finally:
            writer.close()

        (staging,) = out.iterdir()
        assert staging.name.startswith("artists.partial-")

    def test_failed_run_does_not_disturb_concurrent_run(
        self, tmp_path, entity, ipc_dict
    ):
        """Two runs stage into one output dir: one failing before finalize() keeps the other's files."""
        out = tmp_path / "out"
        failing = ParquetWriter(str(out))
        succeeding = ParquetWriter(str(out))
        failing.setup(entity)
        succeeding.setup(entity)
        succeeding.write_chunk(ipc_to_record_batches(ipc_dict))
        failing.close()
        succeeding.finalize(entity)
        succeeding.close()

        assert pq.read_table(str(out / "artists" / "artists.parquet")).num_rows == 2
        assert [p.name for p in out.iterdir()] == ["artists"]

    def test_failed_overwrite_keeps_previous_output(self, tmp_path, entity, ipc_dict):
        """With overwrite=True, a run that fails before finalize() keeps the old files."""
        out = tmp_path / "out"
        writer = ParquetWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)
        entity_dir = out / "artists"
        before = {p.name: p.read_bytes() for p in entity_dir.iterdir()}

        writer2 = ParquetWriter(str(out), overwrite=True)
        writer2.setup(entity)
        writer2.close()

        assert {p.name: p.read_bytes() for p in entity_dir.iterdir()} == before
        assert [p.name for p in out.iterdir()] == ["artists"]

    def test_overwrite_raises_when_output_exists(self, tmp_path, entity, ipc_dict):
        """setup() raises OutputExistsError when files exist and overwrite=False."""
        from discogskit.writers import OutputExistsError

        writer = ParquetWriter(str(tmp_path))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)

        writer2 = ParquetWriter(str(tmp_path))
        with pytest.raises(OutputExistsError, match="--overwrite"):
            writer2.setup(entity)

    def test_overwrite_succeeds_when_enabled(self, tmp_path, entity, ipc_dict):
        """setup() succeeds when files exist and overwrite=True."""
        writer = ParquetWriter(str(tmp_path))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)

        writer2 = ParquetWriter(str(tmp_path), overwrite=True)
        writer2.setup(entity)
        writer2.write_chunk(ipc_to_record_batches(ipc_dict))
        writer2.finalize(entity)
