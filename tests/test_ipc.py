"""Tests for IPC roundtrip serialization/deserialization."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.ipc as ipc

from discogskit.writers._ipc import deserialize_batches


def _serialize(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


class TestDeserializeBatches:
    def test_roundtrip(self):
        schema = pa.schema([pa.field("x", pa.int32()), pa.field("y", pa.utf8())])
        batch = pa.RecordBatch.from_pydict(
            {"x": [1, 2, 3], "y": ["a", "b", "c"]}, schema=schema
        )
        ipc_bytes = _serialize(batch)

        result = deserialize_batches(ipc_bytes)

        assert len(result) == 1
        assert result[0].num_rows == 3
        assert result[0].column("x").to_pylist() == [1, 2, 3]
        assert result[0].column("y").to_pylist() == ["a", "b", "c"]

    def test_empty_batch_preserves_schema(self):
        schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.utf8())])
        batch = pa.RecordBatch.from_pydict({"id": [], "name": []}, schema=schema)
        ipc_bytes = _serialize(batch)

        result = deserialize_batches(ipc_bytes)

        assert len(result) == 1
        assert result[0].num_rows == 0
        assert result[0].schema == schema
