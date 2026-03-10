"""Tests for decompress module."""

from __future__ import annotations

import gzip

import pytest

from discogskit.decompress import ensure_xml


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
