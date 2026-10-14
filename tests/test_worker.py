"""Tests for the context the chunk worker adds to record and table errors."""

from __future__ import annotations

import pickle

import pytest

from discogskit.entities import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc

# Bytes before the chunk, so the chunk doesn't start at byte 0 and its line numbers differ from the file's.
_PREFIX = b"<artist>\n  <id>1</id>\n</artist>\n"


def _extract_error(tmp_path, entity: str, chunk: bytes) -> tuple[str, int, int, str]:
    """Run the worker on ``chunk``, placed after ``_PREFIX``; return its file, byte range and error message."""
    f = tmp_path / f"{entity}.xml"
    f.write_bytes(_PREFIX + chunk)
    start, end = len(_PREFIX), len(_PREFIX) + len(chunk)
    with pytest.raises(ValueError) as info:
        extract_chunk_to_ipc(ChunkArgs(entity, str(f), start, end))
    # The pool pickles the error to send it to the parent; the message must survive.
    message = str(pickle.loads(pickle.dumps(info.value)))
    assert message == str(info.value)
    return str(f), start, end, message


class TestRecordErrorContext:
    @pytest.mark.parametrize(
        "entity, bad_record",
        [
            ("artists", b"<artist>\n  <id>abc</id>\n</artist>\n"),
            ("labels", b"<label>\n  <id>abc</id>\n</label>\n"),
            ("masters", b'<master id="abc">\n  <title>T</title>\n</master>\n'),
            ("releases", b'<release id="abc" status="Accepted">\n</release>\n'),
        ],
    )
    def test_non_integer_id_names_file_chunk_line_and_id(
        self, tmp_path, entity, bad_record
    ):
        tag = entity[:-1]
        good_record = {
            "artists": b"<artist>\n  <id>2</id>\n</artist>\n",
            "labels": b"<label>\n  <id>2</id>\n</label>\n",
            "masters": b'<master id="2">\n</master>\n',
            "releases": b'<release id="2" status="Accepted">\n</release>\n',
        }[entity]
        # The bad record starts on the chunk's 4th line, whatever the good record's length.
        chunk = good_record + b"\n" * (3 - good_record.count(b"\n")) + bad_record
        file, start, end, message = _extract_error(tmp_path, entity, chunk)
        assert message == (
            f"Invalid <{tag}> record in {file} at chunk line 4 (chunk bytes {start}-{end}, id='abc'):"
            " invalid literal for int() with base 10: 'abc'"
        )

    def test_line_past_libxml2_limit(self, tmp_path):
        """libxml2 keeps element lines in 16 bits and reports 65535 for this compact, deeply nested record."""
        chunk = (
            b'<release id="1"/>'
            + b"\n" * 70_000
            + b'<release id="abc"><tracklist><track><sub_tracks><track><artists><artist><id>2</id>'
            b"</artist></artists></track></sub_tracks></track></tracklist></release>\n"
        )
        file, start, end, message = _extract_error(tmp_path, "releases", chunk)
        assert message.startswith(
            f"Invalid <release> record in {file} at chunk line 70001 (chunk bytes {start}-{end}, id='abc'): "
        )

    @pytest.mark.parametrize(
        "before",
        [
            # lxml skips a namespaced <artist>, so it isn't a record.
            b'<artist xmlns="urn:x"><id>1</id></artist>\n',
            # lxml parses an <artist> under a nested <artists> as a record, before the record holding it.
            b"<artist><id>1</id><x><artists><artist><id>2</id></artist></artists></x></artist>\n",
            # expat rejects this attribute name, which lxml accepts; lxml's own line is exact this early.
            '<artist \U00010000="x"><id>1</id></artist>\n'.encode(),
        ],
        ids=["namespaced-record", "nested-container", "name-expat-rejects"],
    )
    def test_line_follows_the_records_the_worker_parses(self, tmp_path, before):
        chunk = before + b"<artist><id>abc</id></artist>\n"
        file, start, end, message = _extract_error(tmp_path, "artists", chunk)
        assert message.startswith(
            f"Invalid <artist> record in {file} at chunk line 2 (chunk bytes {start}-{end}, id='abc'): "
        )

    def test_element_line_unknown_in_a_record_holding_records(self, tmp_path):
        """The worker cleared the inner record, so counting elements would name the wrong line (#96)."""
        chunk = (
            b'<release id="7">\n'
            b'<x><releases><release id="8"><title>a</title></release></releases></x>\n'
            b"<tracklist><track><position>1</position><sub_tracks><track><position>1a</position>\n"
            b"<sub_tracks/></track></sub_tracks></track></tracklist></release>\n"
        )
        file, start, end, message = _extract_error(tmp_path, "releases", chunk)
        assert message == (
            f"Invalid <release> record in {file} at chunk line 1 (chunk bytes {start}-{end}, id='7'):"
            " <sub_tracks> nested in sub-track '1a' of track '1' at chunk line unknown"
        )


class TestTableErrorContext:
    @pytest.mark.parametrize(
        "chunk, table, detail",
        [
            (
                b"<artist>\n  <id>3000000000</id>\n</artist>\n",
                "artists",
                "Value 3000000000 too large to fit in C integer type",
            ),
            (
                b"<artist>\n  <id>2</id>\n  <aliases>\n"
                b'    <name id="%d">A</name>\n  </aliases>\n</artist>\n' % 2**70,
                "artist_aliases",
                "Python int too large to convert to C long",
            ),
        ],
        ids=["int32-overflow", "int64-overflow"],
    )
    def test_out_of_range_id_names_file_chunk_and_table(
        self, tmp_path, chunk, table, detail
    ):
        file, start, end, message = _extract_error(tmp_path, "artists", chunk)
        assert message == (
            f"Invalid {table} table values in {file} (chunk bytes {start}-{end}): {detail}"
        )
