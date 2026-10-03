"""Tests for find_split_points() — mmap-based XML splitting."""

from __future__ import annotations

from functools import partial

import pytest

from discogskit.entities import ChunkArgs
from discogskit.entities._split import find_split_points
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.labels import SCHEMAS as LABELS_SCHEMAS
from tests.conftest import LABELS_XML, ipc_to_tables


@pytest.fixture()
def find_splits():
    return partial(find_split_points, tag="item", container="items")


class TestSplitFinder:
    def test_single_chunk_small_file(self, tmp_path, find_splits):
        """Small file with large target → single chunk."""
        content = (
            b"<?xml version='1.0'?>\n<items>\n<item>1</item>\n<item>2</item>\n</items>"
        )
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        splits = find_splits(str(f), 1024 * 1024)

        assert len(splits) == 1
        start, end = splits[0]
        chunk = content[start:end]
        assert b"<item>1</item>" in chunk
        assert b"<item>2</item>" in chunk

    @pytest.mark.parametrize("first", [b'<item id="1">a</item>', b"<item>a</item>"])
    def test_data_starts_at_first_record_not_container(
        self, tmp_path, find_splits, first
    ):
        """The container <items> shares the "<item" prefix; the first record may have attributes."""
        content = (
            b"<?xml version='1.0'?>\n<items>\n" + first + b"\n<item>b</item>\n</items>"
        )
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        [(start, end)] = find_splits(str(f), 1024 * 1024)

        # The data region ends right after the last record's closing tag; the "\n" before "</items>" is outside
        # it, since a boundary no longer has to be followed by a newline (see TestSplitFinderDepthTracking).
        assert content[start:end] == first + b"\n<item>b</item>"

    def test_multiple_chunks(self, tmp_path, find_splits):
        """Many items with small target → multiple chunks."""
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(100))
        content = b"<?xml version='1.0'?>\n<items>\n" + items + b"</items>"
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        # Target ~50 bytes per chunk → many splits
        splits = find_splits(str(f), 50)

        assert len(splits) > 1

    def test_contiguous_no_gaps(self, tmp_path, find_splits):
        """Chunks must be contiguous with no gaps or overlaps."""
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(50))
        content = b"<?xml version='1.0'?>\n<items>\n" + items + b"</items>"
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        splits = find_splits(str(f), 100)

        for i in range(len(splits) - 1):
            assert splits[i][1] == splits[i + 1][0], "Chunks must be contiguous"

    def test_each_chunk_ends_at_closing_tag(self, tmp_path, find_splits):
        """Each chunk boundary must fall on a closing tag."""
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(50))
        content = b"<?xml version='1.0'?>\n<items>\n" + items + b"</items>"
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        splits = find_splits(str(f), 100)

        for start, end in splits:
            chunk = content[start:end]
            assert chunk.rstrip().endswith(b"</item>")

    def test_empty_file_raises(self, tmp_path, find_splits):
        f = tmp_path / "empty.xml"
        f.write_bytes(b"")

        with pytest.raises(ValueError):
            find_splits(str(f), 1024)

    def test_no_elements_raises(self, tmp_path, find_splits):
        """File with content but no matching elements."""
        f = tmp_path / "no_items.xml"
        f.write_bytes(b"<?xml version='1.0'?>\n<root>\n</root>")

        with pytest.raises(ValueError, match="No .* elements found"):
            find_splits(str(f), 1024)

    def test_no_end_tag_raises(self, tmp_path, find_splits):
        """File with start pattern but no closing end tag."""
        f = tmp_path / "no_end.xml"
        f.write_bytes(b"<?xml version='1.0'?>\n<items>\n<item>data")

        with pytest.raises(ValueError, match="unclosed"):
            find_splits(str(f), 1024)

    # 0 and -1 terminate even without the check (with wrong splits), so a
    # regression fails here instead of hanging like --chunk-mb -1 (-1 MiB) did.
    @pytest.mark.parametrize("target_chunk_bytes", [0, -1])
    def test_non_positive_chunk_size_raises(
        self, tmp_path, find_splits, target_chunk_bytes
    ):
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(3))
        f = tmp_path / "test.xml"
        f.write_bytes(b"<?xml version='1.0'?>\n<items>\n" + items + b"</items>")

        with pytest.raises(ValueError, match=f"got {target_chunk_bytes}$"):
            find_splits(str(f), target_chunk_bytes)


def _assert_splits_recover_all_labels(
    tmp_path,
    content: bytes,
    target_chunk_bytes: int,
    expected_records: int,
    *,
    multi_chunk: bool = True,
) -> None:
    """Split `content` (a bare stream of <label> records, wrapped here in a minimal <labels> container since a
    real closing tag is now required) and parse every chunk with the shared worker, checking that every record
    is accounted for exactly once.
    """
    f = tmp_path / "labels.xml"
    f.write_bytes(b"<?xml version='1.0'?>\n<labels>\n" + content + b"</labels>\n")

    splits = find_split_points(
        str(f), target_chunk_bytes, tag="label", container="labels"
    )
    if multi_chunk:
        assert len(splits) > 1, "test target_chunk_bytes should force multiple chunks"

    total = 0
    for start, end in splits:
        ipc_dict = extract_chunk_to_ipc(ChunkArgs("labels", str(f), start, end))
        tables = ipc_to_tables(ipc_dict, LABELS_SCHEMAS)
        total += tables["labels"].num_rows
    assert total == expected_records


class TestSplitFinderDepthTracking:
    """Regression tests for #17.

    A split is only safe where a closing tag brings the ``<label>`` nesting depth back to zero. Checking only
    "does the next tag start right here" breaks on unindented nested siblings (the next thing is another nested
    record, not a new top-level one) and on indented top-level records (nothing ever starts "right here", so the
    whole file becomes one chunk). A boundary also can't depend on any particular byte following the closing
    tag — not every valid XML document is formatted the same way.
    """

    def test_pretty_printed_nested_sublabels(self, tmp_path):
        """The original report: indented nested <sublabels><label>…</label></sublabels>, repeated into multiple
        chunks.
        """
        repeats = 2000
        _assert_splits_recover_all_labels(
            tmp_path,
            LABELS_XML.encode() * repeats,
            target_chunk_bytes=4096,
            expected_records=2 * repeats,
        )

    def test_unindented_sibling_nested_sublabels(self, tmp_path):
        """Two nested <label> siblings on their own unindented lines: the first nested closer is immediately
        followed by another "<label", which a simple "does the next tag start right here" check wrongly accepts
        as a top-level boundary.
        """
        repeats = 500

        def record(i: int) -> bytes:
            return (
                f"<label>\n"
                f"<id>{i}</id>\n"
                f"<sublabels>\n"
                f'<label id="{i}0">A</label>\n'
                f'<label id="{i}1">B</label>\n'
                f"</sublabels>\n"
                f"</label>\n"
            ).encode()

        records = [record(i) for i in range(repeats)]
        # Land the search a few records in, just before a record's first nested closer, so the naive fix's
        # literal end_tag search finds that one first instead of the real top-level closer further along.
        danger_zone_offset = record(0).find(b"</label>\n") - 10
        target_chunk_bytes = len(record(0)) * 3 + danger_zone_offset
        _assert_splits_recover_all_labels(
            tmp_path,
            b"".join(records),
            target_chunk_bytes=target_chunk_bytes,
            expected_records=repeats,
        )

    def test_self_closing_nested_sublabel(self, tmp_path):
        """A self-closing nested <label id=".."/> must count as a complete element, not an opening tag with no
        matching closer, or the depth counter would never return to zero.
        """
        repeats = 500
        records = [
            (
                f"<label>\n"
                f"  <id>{i}</id>\n"
                f"  <sublabels>\n"
                f'    <label id="{i}0"/>\n'
                f"  </sublabels>\n"
                f"</label>\n"
            ).encode()
            for i in range(repeats)
        ]
        _assert_splits_recover_all_labels(
            tmp_path,
            b"".join(records),
            target_chunk_bytes=300,
            expected_records=repeats,
        )

    def test_indented_top_level_records(self, tmp_path):
        """A top-level record's closing tag followed by whitespace before the next record's opening tag — not
        immediately adjacent to it — must still count as a boundary once depth returns to zero, and must still
        produce more than one chunk.
        """
        repeats = 500
        records = [
            f"  <label>\n    <id>{i}</id>\n  </label>\n".encode()
            for i in range(repeats)
        ]
        _assert_splits_recover_all_labels(
            tmp_path,
            b"".join(records),
            target_chunk_bytes=200,
            expected_records=repeats,
        )

    def test_crlf_line_endings(self, tmp_path):
        """A boundary must not depend on a bare "\\n" after the closing tag."""
        repeats = 500
        records = [
            f"<label>\r\n  <id>{i}</id>\r\n</label>\r\n".encode()
            for i in range(repeats)
        ]
        _assert_splits_recover_all_labels(
            tmp_path,
            b"".join(records),
            target_chunk_bytes=200,
            expected_records=repeats,
        )

    def test_no_separator_between_records(self, tmp_path):
        """Records packed back-to-back, with no whitespace at all between one's closing tag and the next's
        opening tag.
        """
        repeats = 500
        records = [f"<label><id>{i}</id></label>".encode() for i in range(repeats)]
        _assert_splits_recover_all_labels(
            tmp_path,
            b"".join(records),
            target_chunk_bytes=200,
            expected_records=repeats,
        )

    def test_trailing_whitespace_after_closing_tag(self, tmp_path):
        """Whitespace between the closing tag's ">" and the newline must not prevent the boundary from being
        found.
        """
        repeats = 500
        records = [
            f"<label>\n  <id>{i}</id>\n</label>   \n".encode() for i in range(repeats)
        ]
        _assert_splits_recover_all_labels(
            tmp_path,
            b"".join(records),
            target_chunk_bytes=200,
            expected_records=repeats,
        )

    def test_comment_and_cdata_containing_closing_tag(self, tmp_path):
        """A comment and a CDATA section can both contain literal "</label>" text; neither may be mistaken for a
        real closing tag and thrown off the nesting depth.
        """
        repeats = 500
        records = [
            (
                f"<label>\n"
                f"  <id>{i}</id>\n"
                f"  <!-- decoy </label> text -->\n"
                f"  <profile><![CDATA[contains </label> too]]></profile>\n"
                f"  <sublabels>\n"
                f'    <label id="{i}0">A</label>\n'
                f"  </sublabels>\n"
                f"</label>\n"
            ).encode()
            for i in range(repeats)
        ]
        _assert_splits_recover_all_labels(
            tmp_path,
            b"".join(records),
            target_chunk_bytes=400,
            expected_records=repeats,
        )


class TestSplitFinderStrictValidation:
    """Regression tests for a second round of review on #17's fix.

    Synthetic XML covering structural edge cases a valid or malformed document could present — not claims about
    how real Discogs dumps are formatted. A record tag's own start tag must be parsed by attribute grammar, not
    by searching for the first ">", since XML allows ">" and even "/>" inside a quoted attribute value. And once
    the scan tracks nesting depth, it can for free detect structural damage no previous version of this code
    looked for: a closing tag with no matching opener, an element still open at end of file, or junk after the
    last record — all of which must fail loudly rather than silently return a truncated result.
    """

    def test_gt_in_quoted_attribute_of_self_closing_tag_does_not_corrupt_depth(
        self, tmp_path, find_splits
    ):
        """A self-closing record whose attribute value contains a literal ">" before the real "/>" must still be
        recognized as self-closing. Taking the first ">" after the tag name as its end would read this as a
        plain (non-closing) open tag instead, leaving the nesting depth permanently off by one and silently
        dropping every record after it.
        """
        items = b"".join(f'<item id="{i}" name="a>b"/>\n'.encode() for i in range(30))
        content = b"<?xml version='1.0'?>\n<items>\n" + items + b"</items>\n"
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        splits = find_splits(str(f), 64)

        assert len(splits) > 1
        recovered = b"".join(content[s:e] for s, e in splits)
        assert recovered == items.rstrip(b"\n")

    def test_slash_gt_in_quoted_attribute_value_is_not_self_closing(self, tmp_path):
        """A literal "/>" inside a quoted attribute value must not be mistaken for a self-closing tag."""
        repeats = 50
        records = [
            f'<label id="{i}" name="x/>y">\n  <id>{i}</id>\n</label>\n'.encode()
            for i in range(repeats)
        ]
        _assert_splits_recover_all_labels(
            tmp_path, b"".join(records), target_chunk_bytes=80, expected_records=repeats
        )

    def test_stray_closing_tag_raises(self, tmp_path, find_splits):
        """A closing tag with no matching opener must fail loudly, not silently truncate the data region."""
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(b"<items>\n" + items + b"</item>\n")

        with pytest.raises(ValueError, match="no matching"):
            find_splits(str(f), 64)

    def test_unclosed_record_at_eof_raises(self, tmp_path, find_splits):
        """An element still open at end of file (missing closing tag, or a truncated file) must fail loudly."""
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(b"<items>\n" + items + b"<item>cut off here")

        with pytest.raises(ValueError, match="unclosed"):
            find_splits(str(f), 64)

    def test_trailing_self_closing_record_is_included(self, tmp_path, find_splits):
        """A self-closing record at the top level is a complete record, not dangling open content — it must be
        included in the data region even when it is the very last record.
        """
        records = (
            b"".join(f"<item>{i}</item>\n".encode() for i in range(30)) + b"<item/>\n"
        )
        content = b"<?xml version='1.0'?>\n<items>\n" + records + b"</items>\n"
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        splits = find_splits(str(f), 1024 * 1024)

        [(start, end)] = splits
        assert content[start:end] == records.rstrip(b"\n")

    def test_file_of_only_self_closing_records_splits(self, tmp_path, find_splits):
        """A file whose records are all self-closing must split normally, not raise a misleading "no boundary"
        error.
        """
        records = b"<item/>\n" * 30
        content = b"<?xml version='1.0'?>\n<items>\n" + records + b"</items>\n"
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        splits = find_splits(str(f), 64)

        assert len(splits) > 1
        total = b"".join(content[s:e] for s, e in splits)
        assert total == records.rstrip(b"\n")

    def test_trailing_junk_after_last_record_raises(self, tmp_path, find_splits):
        """Content between the last record and the container's closing tag that isn't whitespace, a comment, or
        a processing instruction isn't classified as anything valid there, and must fail loudly instead of
        silently excluding the rest of the file.
        """
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(
            b"<?xml version='1.0'?>\n<items>\n" + items + b"garbage <<<\n</items>\n"
        )

        with pytest.raises(ValueError, match="Expected </items>"):
            find_splits(str(f), 64)

    def test_comment_and_pi_after_container_closing_tag_are_accepted(
        self, tmp_path, find_splits
    ):
        """XML allows a comment or processing instruction (``Misc``) after the root element's closing tag; a
        document ending ``</items>\\n<!-- generated -->\\n`` is well-formed and must not be rejected.
        """
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(
            b"<?xml version='1.0'?>\n<items>\n"
            + items
            + b"</items>\n<!-- generated 2026-10-01 -->\n<?done?>\n"
        )

        splits = find_splits(str(f), 64)

        assert len(splits) > 1

    def test_wrong_container_closing_tag_raises(self, tmp_path, find_splits):
        """A closing tag with the wrong name isn't the container's own closer, even though it matches the
        permissive "some closing tag" shape; it must be rejected, not accepted as good enough.
        """
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(b"<?xml version='1.0'?>\n<items>\n" + items + b"</wrong>\n")

        with pytest.raises(ValueError, match="Expected </items>"):
            find_splits(str(f), 64)

    def test_missing_container_closing_tag_raises(self, tmp_path, find_splits):
        """A file that ends right after the last complete record, with no container closer at all, is what a
        dump truncated exactly at a record boundary looks like — the one case nothing else here would catch.
        """
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(b"<?xml version='1.0'?>\n<items>\n" + items)

        with pytest.raises(ValueError, match="Expected </items>"):
            find_splits(str(f), 64)

    def test_duplicated_container_closing_tag_raises(self, tmp_path, find_splits):
        """A second, repeated container closer isn't valid XML either; only exactly one is accepted."""
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(
            b"<?xml version='1.0'?>\n<items>\n" + items + b"</items>\n</items>\n"
        )

        with pytest.raises(ValueError, match="Expected </items>"):
            find_splits(str(f), 64)

    def test_container_closing_tag_followed_by_stray_element_raises(
        self, tmp_path, find_splits
    ):
        """An element after the container's closing tag is not XML's Misc production and must be rejected."""
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(
            b"<?xml version='1.0'?>\n<items>\n" + items + b"</items>\n<extra/>\n"
        )

        with pytest.raises(ValueError, match="Expected </items>"):
            find_splits(str(f), 64)

    @pytest.mark.parametrize("closer", [b"</items >", b"</items\n>"])
    def test_whitespace_before_closing_bracket_is_accepted(
        self, tmp_path, find_splits, closer
    ):
        """XML allows whitespace between a closing tag's name and its ">"; the container's closer must still be
        recognized with a space or a newline there, not just immediately adjacent.
        """
        items = b"".join(f"<item>{i}</item>\n".encode() for i in range(30))
        f = tmp_path / "test.xml"
        f.write_bytes(b"<?xml version='1.0'?>\n<items>\n" + items + closer + b"\n")

        splits = find_splits(str(f), 64)

        assert len(splits) > 1


# Two records; a chunk ends at the last record's closing tag, so the trailing newline isn't part of it.
_RECORDS = b"<item>1</item>\n<item>2</item>"


class TestSplitFinderLeadingContent:
    """Issue #79: workers parse each chunk inside their own ``<?xml version='1.0' encoding='UTF-8'?><container>``
    envelope, so anything before the first record that the envelope doesn't reproduce must fail before parsing.

    The inputs are synthetic XML variations; they say nothing about what real dumps contain.
    """

    @pytest.mark.parametrize(
        "prolog",
        [
            b"<items>\n",
            b"\n  <items >\n\t",
            b"<?xml version='1.0'?>\n<items>\n",
            b'<?xml version="1.0" encoding="UTF-8"?>\n<items>\n',
            b"<?xml version='1.0' encoding='utf-8' standalone='yes'?><items>",
        ],
        ids=["bare", "whitespace", "decl-1.0", "decl-utf8", "decl-utf8-standalone"],
    )
    def test_supported_prolog_is_accepted(self, tmp_path, find_splits, prolog):
        f = tmp_path / "test.xml"
        f.write_bytes(prolog + _RECORDS + b"</items>\n")

        [(start, end)] = find_splits(str(f), 1024 * 1024)

        assert (start, end) == (len(prolog), len(prolog) + len(_RECORDS))

    @pytest.mark.parametrize(
        "prolog, offset, what",
        [
            (b"\xef\xbb\xbf<items>\n", 0, "a byte-order mark"),
            (
                b"<?xml version='1.0' encoding='ISO-8859-1'?>\n<items>\n",
                0,
                "an XML declaration other than",
            ),
            (b"<?xml version='1.1'?>\n<items>\n", 0, "an XML declaration other than"),
            (
                b"<?xml version='1.0'?>\n<?xml version='1.0'?>\n<items>\n",
                22,
                "an XML declaration other than",
            ),
            (b"<!DOCTYPE items [<!ENTITY e 'x'>]>\n<items>\n", 0, "a DOCTYPE"),
            (b"<!-- generated -->\n<items>\n", 0, "a comment"),
            (
                b"<?xml-stylesheet href='a.xsl'?>\n<items>\n",
                0,
                "a processing instruction",
            ),
            (b"<items xmlns:x='urn:x'>\n", 0, "attributes on <items>"),
            (b"<items version='2'>\n", 0, "attributes on <items>"),
            (b"<items>\n<!-- note -->\n", 8, "a comment"),
            (b"<items>\nstray text\n", 8, "unexpected content"),
            (b"<items>\f\n", 7, "unexpected content"),
            (b"", 0, "missing <items> start tag"),
            (b"<?xml version='1.0'?>\n", 22, "missing <items> start tag"),
            (b"<things>\n", 0, "unexpected content"),
            (b"\n<?xml version='1.0'?>\n<items>\n", 1, "an XML declaration other than"),
            (b"<items/>\n", 0, "unexpected content"),
            (b"<itemsx>\n", 0, "unexpected content"),
        ],
        ids=[
            "bom",
            "decl-latin1",
            "decl-1.1",
            "second-decl",
            "doctype",
            "comment-before-container",
            "pi",
            "namespace-attr",
            "plain-attr",
            "comment-after-container",
            "stray-text",
            "form-feed",
            "no-container",
            "decl-only-no-container",
            "wrong-container",
            "whitespace-before-decl",
            "self-closing-container",
            "longer-container-name",
        ],
    )
    def test_unsupported_prolog_is_rejected_with_offset(
        self, tmp_path, find_splits, prolog, offset, what
    ):
        f = tmp_path / "test.xml"
        f.write_bytes(prolog + _RECORDS + b"</items>\n")

        with pytest.raises(ValueError) as info:
            find_splits(str(f), 1024 * 1024)

        message = str(info.value)
        assert (
            f"Unsupported content before the first <item> record at byte {offset}: {what}"
            in message
        )

    @pytest.mark.parametrize(
        "content, what",
        [
            # The quoted <item> is the first record tag the scan sees; it opens a record that never closes.
            (
                b"<items note='<item>'>\n" + _RECORDS + b"</items>\n",
                "attributes on <items>",
            ),
            # Each later structural error would otherwise be reported instead of the prolog.
            (b"<!-- x -->\n<items>\n" + _RECORDS + b"<item>\n</items>\n", "a comment"),
            (b"<!-- x -->\n<items>\n" + _RECORDS + b"</item>\n</items>\n", "a comment"),
            (b"<!-- x -->\n<items>\n" + _RECORDS + b"<item\n</items>\n", "a comment"),
            # A stray closing tag or bad markup before any record is still preceded by the bad prolog.
            (b"<!-- x -->\n<items>\n</item>\n" + _RECORDS + b"</items>\n", "a comment"),
            (b"<!-- x -->\n<items>\n<!-- unterminated\n</items>\n", "a comment"),
        ],
        ids=[
            "record-tag-in-attribute",
            "later-unclosed-record",
            "later-stray-closer",
            "later-malformed-tag",
            "stray-closer-before-records",
            "bad-markup-before-records",
        ],
    )
    def test_unsupported_prolog_is_reported_before_later_errors(
        self, tmp_path, find_splits, content, what
    ):
        f = tmp_path / "test.xml"
        f.write_bytes(content)

        with pytest.raises(ValueError) as info:
            find_splits(str(f), 1024 * 1024)

        assert (
            f"Unsupported content before the first <item> record at byte 0: {what}"
            in str(info.value)
        )
