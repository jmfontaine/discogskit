"""Tests for IPC roundtrip serialization/deserialization."""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyarrow import ipc

from discogskit.writers._ipc import deserialize_batch


def _serialize(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


class TestDeserializeBatch:
    def test_roundtrip(self):
        schema = pa.schema([pa.field("x", pa.int32()), pa.field("y", pa.utf8())])
        batch = pa.RecordBatch.from_pydict(
            {"x": [1, 2, 3], "y": ["a", "b", "c"]}, schema=schema
        )
        ipc_bytes = _serialize(batch)

        result = deserialize_batch(ipc_bytes)

        assert result.num_rows == 3
        assert result.column("x").to_pylist() == [1, 2, 3]
        assert result.column("y").to_pylist() == ["a", "b", "c"]

    def test_empty_batch_preserves_schema(self):
        schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.utf8())])
        batch = pa.RecordBatch.from_pydict({"id": [], "name": []}, schema=schema)
        ipc_bytes = _serialize(batch)

        result = deserialize_batch(ipc_bytes)

        assert result.num_rows == 0
        assert result.schema == schema

    def test_zero_batches_raises(self):
        schema = pa.schema([pa.field("id", pa.int32())])
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, schema)
        writer.close()
        ipc_bytes = sink.getvalue().to_pybytes()

        with pytest.raises(ValueError, match="found none"):
            deserialize_batch(ipc_bytes)

    def test_more_than_one_batch_raises(self):
        schema = pa.schema([pa.field("id", pa.int32())])
        batch = pa.RecordBatch.from_pydict({"id": [1]}, schema=schema)
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, schema)
        writer.write_batch(batch)
        writer.write_batch(batch)
        writer.close()
        ipc_bytes = sink.getvalue().to_pybytes()

        with pytest.raises(ValueError, match="found more than one"):
            deserialize_batch(ipc_bytes)
