"""Integration tests for ParquetWriter full lifecycle."""

from __future__ import annotations

import os

import pyarrow.parquet as pq
import pytest

from discogskit.entities import get
from discogskit.entities import ChunkArgs
from discogskit.entities.artists import extract_chunk_to_ipc as artists_extract
from discogskit.writers.parquet import ParquetWriter


@pytest.mark.integration
class TestParquetWriter:
    @pytest.fixture()
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return artists_extract(ChunkArgs(str(artists_xml_file), 0, size))

    def test_full_lifecycle(self, tmp_path, entity, ipc_dict):
        writer = ParquetWriter(str(tmp_path))
        try:
            writer.setup(entity)
            count = writer.write_chunk(ipc_dict, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2

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

    def test_write_chunk_with_table_timings(self, tmp_path, entity, ipc_dict):
        """write_chunk records per-table timing when table_timings is passed."""
        writer = ParquetWriter(str(tmp_path))
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(ipc_dict, entity, table_timings=timings)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2
        assert "artists" in timings
        assert all(v >= 0 for v in timings.values())

    def test_empty_batch_with_table_timings(self, tmp_path, entity):
        """Empty batches are timed correctly when table_timings is passed."""
        from discogskit.entities.artists import _cols_to_ipc, _new_cols

        empty_ipc = _cols_to_ipc(_new_cols())

        writer = ParquetWriter(str(tmp_path))
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(empty_ipc, entity, table_timings=timings)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 0
        assert "artists" in timings

    def test_close_without_finalize(self, tmp_path, entity, ipc_dict):
        """close() without finalize() should not error."""
        writer = ParquetWriter(str(tmp_path))
        writer.setup(entity)
        writer.write_chunk(ipc_dict, entity)
        writer.close()

    def test_overwrite_raises_when_output_exists(self, tmp_path, entity, ipc_dict):
        """setup() raises OutputExistsError when files exist and overwrite=False."""
        from discogskit.writers import OutputExistsError

        writer = ParquetWriter(str(tmp_path))
        writer.setup(entity)
        writer.write_chunk(ipc_dict, entity)
        writer.finalize(entity)

        writer2 = ParquetWriter(str(tmp_path))
        with pytest.raises(OutputExistsError, match="--overwrite"):
            writer2.setup(entity)

    def test_overwrite_succeeds_when_enabled(self, tmp_path, entity, ipc_dict):
        """setup() succeeds when files exist and overwrite=True."""
        writer = ParquetWriter(str(tmp_path))
        writer.setup(entity)
        writer.write_chunk(ipc_dict, entity)
        writer.finalize(entity)

        writer2 = ParquetWriter(str(tmp_path), overwrite=True)
        writer2.setup(entity)
        writer2.write_chunk(ipc_dict, entity)
        writer2.finalize(entity)
