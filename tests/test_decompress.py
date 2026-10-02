"""Tests for decompress module."""

from __future__ import annotations

import gzip
import io

import pytest

from discogskit import decompress
from discogskit.decompress import DecompressError, ensure_xml

_XML = b"<?xml version='1.0'?>\n<root>hello</root>"


def _write_gz(path):
    with gzip.open(path, "wb") as f:
        f.write(_XML)
    return path


class TestPartialFile:
    """``.xml`` is the cache, so it must never hold incomplete output."""

    def test_corrupt_gz_leaves_no_xml(self, tmp_path):
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(b"not a real gz")
        xml_path = tmp_path / "test.xml"

        with pytest.raises(DecompressError):
            ensure_xml(gz_path, xml_path, workers=1)

        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_rename_failure_removes_partial_and_keeps_error(
        self, monkeypatch, tmp_path
    ):
        """A failed rename isn't reported as a corrupt gzip, and leaves no .partial."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        error = PermissionError(13, "Permission denied", str(xml_path))

        def failing_replace(src, dst):
            raise error

        monkeypatch.setattr(decompress.os, "replace", failing_replace)

        with pytest.raises(PermissionError) as excinfo:
            ensure_xml(gz_path, xml_path, workers=1)

        assert excinfo.value is error
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_stale_partial_removed_when_xml_cached(self, tmp_path):
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        xml_path.write_bytes(_XML)
        partial_path = tmp_path / "test.xml.partial"
        partial_path.write_bytes(b"<?xml version='1.0'?>\n<ro")

        ensure_xml(gz_path, xml_path, workers=1)

        assert xml_path.read_bytes() == _XML
        assert not partial_path.exists()

    def test_stale_partial_replaced(self, tmp_path):
        """A crashed run's .partial is neither reused nor appended to."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        partial_path = tmp_path / "test.xml.partial"
        partial_path.write_bytes(b"<?xml version='1.0'?>\n<ro" * 1000)

        ensure_xml(gz_path, xml_path, workers=1)

        assert xml_path.read_bytes() == _XML
        assert not partial_path.exists()

    def test_xml_appears_only_after_decompression(self, monkeypatch, tmp_path):
        xml_path = tmp_path / "test.xml"
        xml_seen_during_reads: list[bool] = []

        class RecordingReader(io.BytesIO):
            def read(self, size=-1):
                xml_seen_during_reads.append(xml_path.exists())
                return super().read(1)  # one byte per read: several reads

        monkeypatch.setattr(
            decompress.rapidgzip,
            "open",
            lambda path, parallelization: RecordingReader(_XML),
        )

        ensure_xml(tmp_path / "test.xml.gz", xml_path, workers=1)

        assert len(xml_seen_during_reads) > 1
        assert not any(xml_seen_during_reads)
        assert xml_path.read_bytes() == _XML


@pytest.mark.integration
class TestDecompress:
    def test_decompress_gz_file(self, tmp_path):
        """ensure_xml decompresses a .gz file to .xml."""
        xml_content = b"<?xml version='1.0'?>\n<root>hello</root>"
        gz_path = tmp_path / "test.xml.gz"
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content)

        xml_path = tmp_path / "test.xml"
        ensure_xml(gz_path, xml_path, workers=1)

        assert xml_path.exists()
        assert xml_path.read_bytes() == xml_content

    def test_cached_xml_skipped(self, tmp_path):
        """ensure_xml skips decompression if .xml already exists."""
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(b"not a real gz")

        xml_path = tmp_path / "test.xml"
        xml_path.write_bytes(b"already here")

        ensure_xml(gz_path, xml_path, workers=1)

        assert xml_path.read_bytes() == b"already here"
