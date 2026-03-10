"""Shared IPC deserialization for all writers."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.ipc as ipc


def deserialize_batches(ipc_bytes: bytes) -> list[pa.RecordBatch]:
    reader = ipc.open_stream(pa.BufferReader(ipc_bytes))
    return [batch for batch in reader]
