"""Arrow IPC serialization between the parse workers and the writer stage.

Each stream holds exactly one RecordBatch: the chunk worker serializes one batch per table per chunk, and the pipeline
deserializes each before handing it to the writer.
"""

from __future__ import annotations

import pyarrow as pa
from pyarrow import ipc


def deserialize_batch(ipc_bytes: bytes) -> pa.RecordBatch:
    """Deserialize one IPC stream into its single RecordBatch.

    Fails loudly if a stream holds no batch or more than one, instead of silently dropping extra batches or raising a
    bare StopIteration.
    """
    reader = ipc.open_stream(pa.BufferReader(ipc_bytes))
    try:
        batch = reader.read_next_batch()
    except StopIteration:
        raise ValueError("IPC stream must hold exactly one batch, found none") from None
    if next(reader, None) is not None:
        raise ValueError("IPC stream must hold exactly one batch, found more than one")
    return batch


def serialize_batch(batch: pa.RecordBatch, schema: pa.Schema) -> bytes:
    """Serialize ``batch`` as one IPC stream with ``schema``."""
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()
