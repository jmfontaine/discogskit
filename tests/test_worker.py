"""Tests for extract_chunk_to_ipc's XML envelope: built from the dump's own bytes, not guessed.

Regression tests for #79. Synthetic XML covering structural cases a valid document could present — a container
with a namespace declaration, a non-UTF-8 encoding, a DOCTYPE defining an entity — not claims about how real
Discogs dumps are formatted.
"""

from __future__ import annotations

import pytest

from discogskit.entities import ChunkArgs
from discogskit.entities._split import envelope_offsets, find_split_points
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.labels import SCHEMAS
from tests.conftest import ipc_to_tables


def _convert(tmp_path, xml_bytes: bytes, *, strict: bool = False):
    """Split and parse a whole synthetic labels dump, using the file's own envelope throughout."""
    f = tmp_path / "test.xml"
    f.write_bytes(xml_bytes)
    splits = find_split_points(str(f), 1 << 20, "label", container="labels")
    prolog_end, footer_start = envelope_offsets(splits)
    ipc_dicts = [
        extract_chunk_to_ipc(
            ChunkArgs(
                "labels",
                str(f),
                s,
                e,
                prolog_end=prolog_end,
                footer_start=footer_start,
                strict=strict,
            )
        )
        for s, e in splits
    ]
    tables = [ipc_to_tables(d, SCHEMAS) for d in ipc_dicts]
    return tables[0]["labels"] if len(tables) == 1 else tables


class TestEnvelopeFromFile:
    def test_plain_case_unchanged(self, tmp_path):
        """A plain UTF-8 file with no container attributes parses exactly as before."""
        xml = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<labels>\n"
            b"<label>\n"
            b"  <id>1</id>\n"
            b"  <name>Test Label</name>\n"
            b"</label>\n"
            b"</labels>\n"
        )
        labels = _convert(tmp_path, xml)
        assert labels.column("id").to_pylist() == [1]
        assert labels.column("name").to_pylist() == ["Test Label"]

    def test_namespace_declaration_on_container_is_used_by_a_record(self, tmp_path):
        """A record using a prefix declared on the container must parse, not fail with "Namespace prefix ...
        is not defined" — the hard-coded envelope dropped the container's own attributes entirely.
        """
        xml = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b'<labels xmlns:x="urn:example">\n'
            b"<label>\n"
            b"  <id>1</id>\n"
            b"  <name>Test Label</name>\n"
            b"  <x:marker>decoration</x:marker>\n"
            b"</label>\n"
            b"</labels>\n"
        )
        labels = _convert(tmp_path, xml)
        assert labels.column("id").to_pylist() == [1]
        assert labels.column("name").to_pylist() == ["Test Label"]

    def test_non_utf8_declared_encoding_round_trips_non_ascii_text(self, tmp_path):
        """A dump declaring an encoding other than UTF-8 must be decoded using that encoding, not UTF-8 —
        the hard-coded envelope always declared UTF-8 regardless of the file's own bytes.
        """
        text = "Café"
        xml = (
            "<?xml version='1.0' encoding='ISO-8859-1'?>\n"
            "<labels>\n"
            "<label>\n"
            "  <id>1</id>\n"
            f"  <name>{text}</name>\n"
            "</label>\n"
            "</labels>\n"
        ).encode("iso-8859-1")
        labels = _convert(tmp_path, xml)
        assert labels.column("name").to_pylist() == [text]

    def test_doctype_defined_entity_is_expanded(self, tmp_path):
        """An entity the dump's own DOCTYPE defines must expand — the hard-coded envelope dropped any DOCTYPE,
        so such a reference would fail with "Entity ... not defined".
        """
        xml = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<!DOCTYPE labels [\n"
            b'<!ENTITY greeting "Hello World">\n'
            b"]>\n"
            b"<labels>\n"
            b"<label>\n"
            b"  <id>1</id>\n"
            b"  <name>&greeting;</name>\n"
            b"</label>\n"
            b"</labels>\n"
        )
        labels = _convert(tmp_path, xml)
        assert labels.column("name").to_pylist() == ["Hello World"]


class TestEntityResolutionSecurity:
    """A DOCTYPE carried into the envelope from the dump could declare an external entity. iterparse must never
    resolve one — read a local file or reach the network — even though internal entities (defined inline in the
    same DOCTYPE) must still expand.
    """

    def test_external_system_entity_is_not_read(self, tmp_path):
        secret = tmp_path / "secret.txt"
        secret.write_text("TOP SECRET CONTENTS")
        xml = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<!DOCTYPE labels [\n"
            b'<!ENTITY xxe SYSTEM "file://' + str(secret).encode() + b'">\n'
            b"]>\n"
            b"<labels>\n"
            b"<label>\n"
            b"  <id>1</id>\n"
            b"  <name>&xxe;</name>\n"
            b"</label>\n"
            b"</labels>\n"
        )
        f = tmp_path / "xxe.xml"
        f.write_bytes(xml)
        splits = find_split_points(str(f), 1 << 20, "label", container="labels")
        prolog_end, footer_start = envelope_offsets(splits)
        [(s, e)] = splits

        with pytest.raises(ValueError) as exc_info:
            extract_chunk_to_ipc(
                ChunkArgs(
                    "labels",
                    str(f),
                    s,
                    e,
                    prolog_end=prolog_end,
                    footer_start=footer_start,
                )
            )

        assert "TOP SECRET CONTENTS" not in str(exc_info.value)

    def test_internal_entity_still_expands(self, tmp_path):
        """Confirms the security fix doesn't also block internal entities, which must keep working."""
        xml = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<!DOCTYPE labels [\n"
            b'<!ENTITY greeting "Hello World">\n'
            b"]>\n"
            b"<labels>\n"
            b"<label>\n"
            b"  <id>1</id>\n"
            b"  <name>&greeting;</name>\n"
            b"</label>\n"
            b"</labels>\n"
        )
        labels = _convert(tmp_path, xml)
        assert labels.column("name").to_pylist() == ["Hello World"]


class TestIncompatibleEncodingsRejected:
    def test_utf16_with_bom_is_rejected(self, tmp_path):
        """Splitting scans for literal ASCII tag bytes, which can't locate anything in UTF-16 (every ASCII
        character is widened with embedded NULs). A UTF-16 file — detected by its byte order mark — must be
        rejected up front with a clear error, not produce a confusing "no elements found".
        """
        xml = (
            "<?xml version='1.0' encoding='UTF-16'?>\n"
            "<labels>\n<label>\n  <id>1</id>\n</label>\n</labels>\n"
        ).encode("utf-16")  # the generic "utf-16" codec writes a leading BOM
        f = tmp_path / "utf16.xml"
        f.write_bytes(xml)

        with pytest.raises(ValueError, match="UTF-16"):
            find_split_points(str(f), 1 << 20, "label", container="labels")

    def test_iso_8859_1_is_accepted(self, tmp_path):
        """The ISO-8859-x family is ASCII-compatible byte-for-byte in its structural (tag) bytes, so it must not
        be rejected the way UTF-16/32 are.
        """
        xml = "<?xml version='1.0' encoding='ISO-8859-1'?>\n<labels>\n<label>\n  <id>1</id>\n  <name>Café</name>\n</label>\n</labels>\n".encode(
            "iso-8859-1"
        )
        f = tmp_path / "latin1.xml"
        f.write_bytes(xml)

        splits = find_split_points(str(f), 1 << 20, "label", container="labels")

        assert len(splits) == 1


class TestLineNumberNoteWithRealProlog:
    def test_multiline_prolog_offsets_the_reported_line_number(self, tmp_path):
        """With a single-line synthesized prolog, lxml's line 1 was always the chunk's own first line, so "line
        numbers count from byte {start}" was simply true. With a real, multi-line prolog (common once a DOCTYPE
        is involved), that's no longer line 1 — the message must say what's actually true instead.
        """
        prolog = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<!DOCTYPE labels [\n"
            b'<!ENTITY greeting "Hi">\n'
            b"]>\n"
            b"<labels>\n"
        )
        header_lines = prolog.count(b"\n")
        assert header_lines > 0, (
            "this test only means something with a multi-line prolog"
        )

        # A record with a child tag whose open/close names don't match ("name" vs "nam"); lxml catches this, but
        # the splitter doesn't, because it only tracks <label>/</label>, so this reaches extract_chunk_to_ipc.
        good_record = b"<label>\n  <id>1</id>\n</label>\n"
        bad_record = b"<label>\n  <id>2</id>\n  <name>X</nam>\n</label>\n"
        data = good_record + bad_record
        footer = b"\n</labels>\n"
        content = prolog + data + footer
        f = tmp_path / "multiline_prolog.xml"
        f.write_bytes(content)

        start = len(prolog)
        end = len(prolog) + len(data)

        with pytest.raises(ValueError) as exc_info:
            extract_chunk_to_ipc(
                ChunkArgs(
                    "labels",
                    str(f),
                    start,
                    end,
                    prolog_end=len(prolog),
                    footer_start=len(prolog) + len(data),
                )
            )

        message = str(exc_info.value)
        assert f"line {header_lines + 1} begins at byte {start}" in message
        assert f"subtract {header_lines}" in message
        # lxml's own message names the physical (post-prolog) line of the mismatch; within `data`, "</nam>" is on
        # line 6 (1-3: good_record, 4: "<label>", 5: "  <id>2</id>", 6: "  <name>X</nam>"), so lxml should report
        # header_lines + 6.
        assert f"line {header_lines + 6}" in message
