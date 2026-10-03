"""Tests for find_split_points() — mmap-based XML splitting."""

from __future__ import annotations

from functools import partial

import pytest

from discogskit.entities._split import find_split_points


@pytest.fixture()
def find_splits():
    return partial(find_split_points, tag="item")


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

        assert content[start:end] == first + b"\n<item>b</item>\n"

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
