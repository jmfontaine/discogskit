"""Integration tests for ParquetWriter full lifecycle."""

from __future__ import annotations

import os

import pyarrow.parquet as pq
import pytest

from discogskit.entities import ChunkArgs, get
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.writers.parquet import ParquetWriter
from tests.conftest import empty_ipc, ipc_to_record_batches


@pytest.mark.integration
class TestParquetWriter:
    @pytest.fixture()
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        )

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

        # Verify schema round-trips correctly
        for table_name in entity.table_order:
            read_table = pq.read_table(str(entity_dir / f"{table_name}.parquet"))
            assert read_table.num_rows > 0 or table_name != entity.table_order[0]

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
        """A run that fails before finalize() leaves no final-named or staged files."""
        out = tmp_path / "out"
        writer = ParquetWriter(str(out))
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.close()

        assert list(out.iterdir()) == []

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
