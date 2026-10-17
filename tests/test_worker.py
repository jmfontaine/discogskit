"""Tests for the shared chunk worker."""

from __future__ import annotations

import os
import pickle
import tracemalloc

import pytest

from discogskit.entities import ENTITIES, _worker
from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.artists import SCHEMAS
from tests.conftest import (
    ARTISTS_XML,
    LABELS_XML,
    MASTERS_XML,
    RELEASES_XML,
    ipc_to_tables,
)

PLAIN_RECORDS = {
    "artists": ARTISTS_XML,
    "labels": LABELS_XML,
    "masters": MASTERS_XML,
    "releases": RELEASES_XML,
}

_ARTIST_FIELDS = ["anv", "id", "join", "name"]
_EXTRAARTIST_FIELDS = ["anv", "id", "name", "role", "tracks"]


def _track_levels(track: str, *, sub_tracks: bool) -> list[str]:
    levels = [track]
    levels += [f"{track}/{tag}" for tag in ("duration", "position", "title")]
    levels += [f"{track}/artists", f"{track}/artists/artist"]
    levels += [f"{track}/artists/artist/{tag}" for tag in _ARTIST_FIELDS]
    levels += [f"{track}/extraartists", f"{track}/extraartists/artist"]
    levels += [f"{track}/extraartists/artist/{tag}" for tag in _EXTRAARTIST_FIELDS]
    if sub_tracks:
        levels += [f"{track}/sub_tracks"]
        levels += _track_levels(f"{track}/sub_tracks/track", sub_tracks=False)
    return levels


# Every element each parser walks, as its path relative to the record ("" for the record itself): an element injected
# into any of them is one no parser handles (#190).
_LEVELS = {
    "artists": [
        "",
        "aliases",
        "aliases/name",
        "data_quality",
        "groups",
        "groups/name",
        "id",
        "members",
        "members/name",
        "name",
        "namevariations",
        "namevariations/name",
        "profile",
        "realname",
        "urls",
        "urls/url",
    ],
    "labels": [
        "",
        "contactinfo",
        "data_quality",
        "id",
        "name",
        "parentLabel",
        "profile",
        "sublabels",
        "sublabels/label",
        "urls",
        "urls/url",
    ],
    "masters": [
        "",
        "artists",
        "artists/artist",
        *[f"artists/artist/{tag}" for tag in _ARTIST_FIELDS],
        "data_quality",
        "genres",
        "genres/genre",
        "main_release",
        "notes",
        "styles",
        "styles/style",
        "title",
        "videos",
        "videos/video",
        "videos/video/description",
        "videos/video/title",
        "year",
    ],
    "releases": [
        "",
        "artists",
        "artists/artist",
        *[f"artists/artist/{tag}" for tag in _ARTIST_FIELDS],
        "companies",
        "companies/company",
        *[
            f"companies/company/{tag}"
            for tag in ("catno", "entity_type", "entity_type_name", "id", "name")
        ],
        "country",
        "data_quality",
        "extraartists",
        "extraartists/artist",
        *[f"extraartists/artist/{tag}" for tag in _EXTRAARTIST_FIELDS],
        "formats",
        "formats/format",
        "formats/format/descriptions",
        "formats/format/descriptions/description",
        "genres",
        "genres/genre",
        "identifiers",
        "identifiers/identifier",
        "labels",
        "labels/label",
        "master_id",
        "notes",
        "released",
        "series",
        "series/series",
        "styles",
        "styles/style",
        "title",
        "tracklist",
        *_track_levels("tracklist/track", sub_tracks=True),
        "videos",
        "videos/video",
        "videos/video/description",
        "videos/video/title",
    ],
}
_RECORDS = {
    "artists": "<artist><id>1</id>{}</artist>",
    "labels": "<label><id>1</id>{}</label>",
    "masters": '<master id="1">{}</master>',
    "releases": '<release id="1" status="Accepted">{}</release>',
}


def _injected(entity_name: str, level: str) -> str:
    """One record whose element at ``level``, nested in its ancestors, has the attribute new_attr="x" and <new_tag/>
    as its only child.

    An artist's or label's <id> level is the record's own <id>: a second <id> would fail the record (#205).
    """
    record = _RECORDS[entity_name]
    if not level:
        end = record.index(">")
        return (record[:end] + ' new_attr="x"' + record[end:]).format("<new_tag/>")
    if level == "id" and "<id>1</id>" in record:
        return record.replace("<id>1</id>", '<id new_attr="x">1<new_tag/></id>').format(
            ""
        )
    *ancestors, tag = level.split("/")
    inner = "".join(f"<{ancestor}>" for ancestor in ancestors)
    inner += f'<{tag} new_attr="x"><new_tag/></{tag}>'
    inner += "".join(f"</{ancestor}>" for ancestor in reversed(ancestors))
    return record.format(inner)


@pytest.mark.parametrize(
    ("entity_name", "level"),
    [(entity, level) for entity, levels in _LEVELS.items() for level in levels],
    ids=[
        f"{entity}-{level or 'record'}"
        for entity, levels in _LEVELS.items()
        for level in levels
    ],
)
def test_strict_reports_unhandled_element_path(tmp_path, entity_name, level):
    """--strict reports an element and an attribute no parser handles at any level, by their paths relative to the
    record."""
    f = tmp_path / "chunk.xml"
    f.write_text(_injected(entity_name, level))
    args = ChunkArgs(
        end=os.path.getsize(f),
        entity=entity_name,
        file_path=str(f),
        start=0,
        strict=True,
    )

    path = f"{level}/" if level else ""
    assert extract_chunk_to_ipc(args).unknown == [f"{path}@new_attr", f"{path}new_tag"]


@pytest.mark.parametrize(
    ("entity_name", "record", "paths"),
    [
        ("artists", "<artist>HEAD<id>1</id></artist>", ["text()"]),
        ("artists", "<artist><id>1</id>TAIL</artist>", ["text()"]),
        # Not XML whitespace, so it's text.
        ("artists", "<artist>\u00a0<id>1</id></artist>", ["text()"]),
        (
            "artists",
            "<artist><id>1</id><urls><url>a</url>TAIL<url>b</url></urls></artist>",
            ["urls/text()"],
        ),
        (
            "artists",
            '<artist><id>1</id><aliases>HEAD<name id="2">A</name></aliases></artist>',
            ["aliases/text()"],
        ),
        (
            "labels",
            '<label><id>1</id><sublabels><label id="2">S</label>TAIL</sublabels></label>',
            ["sublabels/text()"],
        ),
        (
            "masters",
            '<master id="1"><artists><artist><id>2</id>TAIL</artist></artists></master>',
            ["artists/artist/text()"],
        ),
        (
            "masters",
            '<master id="1"><videos><video src="s">HEAD<title>t</title></video></videos></master>',
            ["videos/video/text()"],
        ),
        (
            "releases",
            '<release id="1"><labels><label id="2" name="L">TEXT</label></labels></release>',
            ["labels/label/text()"],
        ),
        (
            "releases",
            '<release id="1"><identifiers><identifier type="Barcode">TEXT</identifier></identifiers></release>',
            ["identifiers/identifier/text()"],
        ),
        (
            "releases",
            '<release id="1"><formats><format name="CD">TEXT</format></formats></release>',
            ["formats/format/text()"],
        ),
        (
            "releases",
            '<release id="1"><companies><company><id>2</id>TAIL</company></companies></release>',
            ["companies/company/text()"],
        ),
        (
            "releases",
            (
                '<release id="1"><tracklist><track><position>1</position>TAIL</track>'
                "<track><sub_tracks><track><title>t</title>TAIL</track></sub_tracks></track></tracklist></release>"
            ),
            ["tracklist/track/sub_tracks/track/text()", "tracklist/track/text()"],
        ),
        (
            "releases",
            '<release id="1"><extraartists><artist><id>2</id>TAIL</artist></extraartists></release>',
            ["extraartists/artist/text()"],
        ),
        # Text a parser reads, and whitespace, are not reported.
        ("artists", "<artist>\n\t <id>1</id>\r\n <name>A</name> </artist>", []),
        (
            "releases",
            '<release id="1"><master_id is_main_release="true">2</master_id></release>',
            [],
        ),
    ],
)
@pytest.mark.parametrize("strict", [True, False])
def test_strict_reports_unread_text(tmp_path, entity_name, record, paths, strict):
    """--strict reports text in an element whose text no parser reads, before its first child or after any; without
    --strict the record still loads and nothing is reported."""
    f = tmp_path / "chunk.xml"
    f.write_text(record + "\n")
    args = ChunkArgs(
        end=os.path.getsize(f),
        entity=entity_name,
        file_path=str(f),
        start=0,
        strict=strict,
    )

    result = extract_chunk_to_ipc(args)

    assert result.unknown == (paths if strict else [])
    tables = ipc_to_tables(result.tables, ENTITIES[entity_name].schemas)
    assert tables[entity_name].column("id").to_pylist() == [1]


@pytest.mark.parametrize("entity_name", sorted(PLAIN_RECORDS))
def test_strict_reports_nothing_on_known_xml(tmp_path, entity_name):
    """Every element of the fixtures is one the parsers handle, so --strict reports none of them."""
    f = tmp_path / "chunk.xml"
    f.write_text(PLAIN_RECORDS[entity_name])
    args = ChunkArgs(
        end=os.path.getsize(f),
        entity=entity_name,
        file_path=str(f),
        start=0,
        strict=True,
    )

    assert extract_chunk_to_ipc(args).unknown == []


@pytest.mark.parametrize("entity_name", sorted(PLAIN_RECORDS))
@pytest.mark.parametrize(
    "layout",
    [
        # Checked when the next record ends.
        "{ns}{plain}",
        # <other/> fails too (#215), but the namespaced record before it is the first problem in the chunk.
        "{ns}<other/>\n{plain}",
        # Checked once the chunk has been parsed.
        "{plain}{ns}",
        # libxml2 can't give exact element lines past 65535.
        "{plain}" + "\n" * 70_000 + "{ns}",
    ],
    ids=["first", "before-other", "last", "past-line-65535"],
)
def test_namespaced_record_fails(tmp_path, entity_name, layout):
    """A namespaced record element must fail, naming the file, the element and the line where its start tag begins,
    instead of being dropped."""
    tag = ENTITIES[entity_name].root_tag
    # A start tag over several lines: lxml's own line would be its last one.
    namespaced = f'<{tag}\n  xmlns="urn:x"\n  id="9">\n  <id>9</id>\n</{tag}>\n'
    plain = PLAIN_RECORDS[entity_name]
    xml = layout.format(ns=namespaced, plain=plain)
    line = xml[: xml.index(namespaced)].count("\n") + 1
    f = tmp_path / f"{entity_name}.xml"
    f.write_text(xml)

    with pytest.raises(ValueError) as info:
        extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f), entity=entity_name, file_path=str(f), start=0
            )
        )

    message = str(info.value)
    assert str(f) in message
    assert f"namespaced record element <{{urn:x}}{tag}> on line {line};" in message


def test_namespaced_line_past_65535_is_approximate_when_expat_fails(tmp_path):
    """Expat rejects some names libxml2 accepts (here U+0370); the line then can't be exact and mustn't claim to be."""
    xml = '<artist \u0370="x"><id>1</id></artist>' + "\n" * 70_000
    xml += '<artist xmlns="urn:x">\n  <id>9</id>\n</artist>\n'
    f = tmp_path / "artists.xml"
    f.write_text(xml, encoding="utf-8")

    with pytest.raises(ValueError, match=r"<\{urn:x\}artist> near line \d+;"):
        extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f), entity="artists", file_path=str(f), start=0
            )
        )


def test_namespaced_element_in_nested_container_is_not_a_record(tmp_path):
    """Only children of the chunk's container are records; one nested in a same-named element is record content."""
    xml = (
        "<artist><id>1</id><name>A</name>"
        '<artists><thing xmlns="urn:inner"/><artist><id>2</id></artist></artists>'
        "</artist>\n"
    )
    f = tmp_path / "artists.xml"
    f.write_text(xml)

    ipc_dict = extract_chunk_to_ipc(
        ChunkArgs(end=os.path.getsize(f), entity="artists", file_path=str(f), start=0)
    ).tables

    assert 1 in ipc_to_tables(ipc_dict, SCHEMAS)["artists"].column("id").to_pylist()


@pytest.mark.parametrize("entity_name", sorted(PLAIN_RECORDS))
@pytest.mark.parametrize(
    ("layout", "found"),
    [
        ("{plain}<other>data</other>\n{plain}", "element <other>"),
        ("{plain}<other>data</other>\n", "element <other>"),
        ("{plain}  STRAY TEXT\n{plain}", "text 'STRAY TEXT'"),
        ("\n STRAY TEXT\n{plain}", "text 'STRAY TEXT'"),
        ("{plain}STRAY TEXT\n", "text 'STRAY TEXT'"),
        # Of two problems, the first in the chunk is reported.
        ("{plain}STRAY TEXT\n<other/>{plain}", "text 'STRAY TEXT'"),
        ("{plain}<other/>STRAY TEXT\n{plain}", "element <other>"),
    ],
    ids=[
        "element-between",
        "element-after-last",
        "text-between",
        "text-before-first",
        "text-after-last",
        "text-then-element",
        "element-then-text",
    ],
)
@pytest.mark.parametrize("feed_bytes", [None, 3], ids=["one-feed", "split-feeds"])
def test_content_between_records_fails(
    tmp_path, monkeypatch, entity_name, layout, found, feed_bytes
):
    """Elements and text between records were dropped silently, even with --strict (#215); the splitter rejects them
    before the first record of the file and after its last."""
    if feed_bytes is not None:
        monkeypatch.setattr(_worker, "_FEED_BYTES", feed_bytes)
    entity = ENTITIES[entity_name]
    xml = layout.format(plain=PLAIN_RECORDS[entity_name])
    marker = "STRAY" if found.startswith("text") else "<other"
    line = xml[: xml.index(marker)].count("\n") + 1
    f = tmp_path / f"{entity_name}.xml"
    f.write_text(xml)
    size = os.path.getsize(f)

    with pytest.raises(ValueError) as info:
        extract_chunk_to_ipc(
            ChunkArgs(end=size, entity=entity_name, file_path=str(f), start=0)
        )

    assert str(info.value) == (
        f"Unsupported {entity_name} XML in {f}, chunk at bytes 0-{size} (line numbers count from byte 0): {found}"
        f" on line {line}; only <{entity.root_tag}> records may appear in <{entity_name}>"
    )


def test_stray_text_line_is_approximate_when_expat_fails(tmp_path):
    """Expat rejects some names libxml2 accepts (here U+0370); the line then comes from lxml and mustn't claim to be
    exact."""
    xml = '<artist \u0370="x"><id>1</id></artist>\nSTRAY\n<artist><id>2</id></artist>\n'
    f = tmp_path / "artists.xml"
    f.write_text(xml, encoding="utf-8")

    with pytest.raises(ValueError, match=r": text 'STRAY' near line 1; only <artist>"):
        extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f), entity="artists", file_path=str(f), start=0
            )
        )


def test_worker_never_holds_the_whole_chunk(tmp_path):
    """The worker read its whole byte range and copied it into the XML envelope, holding the chunk for the whole parse
    and twice while copying: 512 MB at the default 256 MB chunks (issue #30)."""
    # Elements no parser handles are dropped without --strict, so the records leave little in the column lists.
    record = (
        b"<artist><id>1</id><name>A</name><new_tag>"
        + b"x" * 4000
        + b"</new_tag></artist>\n"
    )
    chunk = record * 4096
    f = tmp_path / "artists.xml"
    f.write_bytes(chunk)
    args = ChunkArgs(end=len(chunk), entity="artists", file_path=str(f), start=0)

    tracemalloc.start()
    try:
        extract_chunk_to_ipc(args)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < len(chunk) // 2


@pytest.mark.parametrize("entity_name", sorted(PLAIN_RECORDS))
@pytest.mark.parametrize("feed_bytes", [1, 7, 64])
def test_feeding_in_slices_parses_the_same_tables(
    tmp_path, monkeypatch, entity_name, feed_bytes
):
    """Slices that end mid-tag, mid-attribute or mid-text must not change what the worker parses."""
    f = tmp_path / f"{entity_name}.xml"
    f.write_text(PLAIN_RECORDS[entity_name] * 3)
    args = ChunkArgs(
        end=os.path.getsize(f), entity=entity_name, file_path=str(f), start=0
    )
    expected = extract_chunk_to_ipc(args).tables
    monkeypatch.setattr(_worker, "_FEED_BYTES", feed_bytes)

    assert extract_chunk_to_ipc(args).tables == expected


def test_file_shorter_than_its_chunk_fails(tmp_path):
    """A file cut short between two records would still parse; the records past its end must not vanish silently."""
    f = tmp_path / "artists.xml"
    f.write_text(ARTISTS_XML)
    size = os.path.getsize(f)
    args = ChunkArgs(end=size + 100, entity="artists", file_path=str(f), start=0)

    with pytest.raises(
        ValueError,
        match=rf"ended at byte {size}, inside the chunk at bytes 0-{size + 100}",
    ):
        extract_chunk_to_ipc(args)


# Bytes before the chunk, so the chunk doesn't start at byte 0 and its line numbers differ from the file's.
_PREFIX = b"<artist>\n  <id>1</id>\n</artist>\n"


def _extract_error(tmp_path, entity: str, chunk: bytes) -> tuple[str, int, int, str]:
    """Run the worker on ``chunk``, placed after ``_PREFIX``; return its file, byte range and error message."""
    f = tmp_path / f"{entity}.xml"
    f.write_bytes(_PREFIX + chunk)
    start, end = len(_PREFIX), len(_PREFIX) + len(chunk)
    with pytest.raises(ValueError) as info:
        extract_chunk_to_ipc(
            ChunkArgs(end=end, entity=entity, file_path=str(f), start=start)
        )
    # The parse worker pickles the error to send it to the main process; the message must survive.
    message = str(pickle.loads(pickle.dumps(info.value)))
    assert message == str(info.value)
    return str(f), start, end, message


class TestRecordErrorContext:
    @pytest.mark.parametrize(
        "feed_bytes",
        [None, len(b"<artist><id>abc</id></artist>\n"), 5],
        ids=["same-slice", "slice-ends-after-bad-record", "small-slices"],
    )
    def test_bad_value_before_malformed_xml_is_reported_first(
        self, tmp_path, monkeypatch, feed_bytes
    ):
        """The first problem in the document is reported, whichever slices the worker feeds."""
        if feed_bytes is not None:
            monkeypatch.setattr(_worker, "_FEED_BYTES", feed_bytes)
        chunk = b"<artist><id>abc</id></artist>\n<artist><id>2</bad></artist>\n"
        file, start, end, message = _extract_error(tmp_path, "artists", chunk)
        assert message.startswith(
            f"Invalid <artist> record in {file} at chunk line 1 (chunk bytes {start}-{end}, id='abc'): "
        )

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


class TestMalformedXmlError:
    def test_reports_first_fatal_error(self, tmp_path):
        """libxml2 logs an undeclared entity, then a bogus follow-on error at the envelope's footer; the first is the
        cause and the only one in the chunk."""
        chunk = b"<artist><id>1</id><name>a &foo; b</name></artist>\n"
        file, start, end, message = _extract_error(tmp_path, "artists", chunk)
        # The header the worker puts before the chunk is on line 1 too, so columns there count it. libxml2 reports
        # the column just past the reference.
        header = b"<?xml version='1.0' encoding='UTF-8'?><artists>"
        column = len(header) + chunk.index(b"&foo;") + len(b"&foo;") + 1
        assert message == (
            f"Malformed artists XML in {file}, chunk at bytes {start}-{end} (line numbers count from byte {start}):"
            f" Entity 'foo' not defined, line 1, column {column}"
        )

    def test_reports_its_own_error_after_another_chunk(self, tmp_path):
        """libxml2's error log is shared by every parser in the thread; one chunk's errors mustn't leak into the next
        chunk's message."""
        _extract_error(
            tmp_path, "artists", b"<artist><id>1</id><name>&foo;</name></artist>\n"
        )
        _, _, _, message = _extract_error(
            tmp_path, "artists", b"<artist><id>2</bad></artist>\n"
        )
        assert message.endswith(
            ": Opening and ending tag mismatch: id line 1 and bad, line 1, column 67"
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
