"""Tests for make_split_finder() — mmap-based XML splitting."""

from __future__ import annotations

import pytest

from discogskit.entities._split import make_split_finder


@pytest.fixture()
def find_splits():
    return make_split_finder(b"<item>", b"</item>\n")


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

        with pytest.raises(ValueError, match="No .* boundary found"):
            find_splits(str(f), 1024)
