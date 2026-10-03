"""Shared IPC deserialization for the pipeline's writer stage."""

from __future__ import annotations

import pyarrow as pa
from pyarrow import ipc


def deserialize_batch(ipc_bytes: bytes) -> pa.RecordBatch:
    """Deserialize one IPC stream into its single RecordBatch.

    Each stream holds exactly one batch: the chunk worker writes one RecordBatch per table per chunk. Fails loudly
    if that invariant ever breaks, instead of silently dropping extra batches or raising a bare StopIteration.
    """
    reader = ipc.open_stream(pa.BufferReader(ipc_bytes))
    try:
        batch = reader.read_next_batch()
    except StopIteration:
        raise ValueError("IPC stream must hold exactly one batch, found none") from None
    if next(reader, None) is not None:
        raise ValueError("IPC stream must hold exactly one batch, found more than one")
    return batch
