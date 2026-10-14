"""Non-ASCII text, entity references and CDATA survive parsing and every file writer."""

from __future__ import annotations

import json
import os
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from discogskit.entities import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.writers.jsonl import JSONLWriter
from discogskit.writers.parquet import ParquetWriter
from discogskit.writers.sqlite import SQLiteWriter
from tests.conftest import UNICODE_ARTISTS_ROWS, ipc_to_record_batches


def _read_jsonl(tmp_path, table_name, schema):
    lines = (tmp_path / "artists" / f"{table_name}.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in lines.splitlines()]


def _read_parquet(tmp_path, table_name, schema):
    return pq.read_table(tmp_path / "artists" / f"{table_name}.parquet").to_pylist()


def _read_sqlite(tmp_path, table_name, schema):
    list_columns = [f.name for f in schema if isinstance(f.type, pa.ListType)]
    conn = sqlite3.connect(tmp_path / "out.db")
    try:
        cursor = conn.execute(f'SELECT * FROM "{table_name}"')
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, row)) for row in cursor]
    finally:
        conn.close()
    for row in rows:
        for column in list_columns:
            if row[column] is not None:
                row[column] = json.loads(row[column])
    return rows


@pytest.mark.parametrize(
    "make_writer, read",
    [
        (lambda tmp_path: JSONLWriter(str(tmp_path)), _read_jsonl),
        (lambda tmp_path: ParquetWriter(str(tmp_path)), _read_parquet),
        (lambda tmp_path: SQLiteWriter(str(tmp_path / "out.db")), _read_sqlite),
    ],
    ids=["jsonl", "parquet", "sqlite"],
)
def test_unicode_text_round_trips(
    tmp_path, entity, unicode_artists_xml_file, make_writer, read
):
    size = os.path.getsize(unicode_artists_xml_file)
    ipc_dict = extract_chunk_to_ipc(
        ChunkArgs("artists", str(unicode_artists_xml_file), 0, size)
    ).tables
    writer = make_writer(tmp_path)
    try:
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)
    finally:
        writer.close()

    for table_name, schema in entity.schemas.items():
        expected = UNICODE_ARTISTS_ROWS.get(table_name, [])
        assert read(tmp_path, table_name, schema) == expected, table_name
