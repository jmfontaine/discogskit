"""Tests for artists entity XML parsing → IPC → tables."""

from __future__ import annotations

import os

from discogskit.entities import ChunkArgs
from discogskit.entities.artists import SCHEMAS, extract_chunk_to_ipc
from tests.conftest import ipc_to_tables


class TestArtistsParsing:
    def test_extract_chunk(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(artists_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        # 2 artists
        artists = tables["artists"]
        assert artists.num_rows == 2
        assert artists.column("id").to_pylist() == [1, 2]
        assert artists.column("name").to_pylist() == ["DJ Test", "Minimal Artist"]
        assert artists.column("realname").to_pylist() == ["Test Person", ""]
        assert artists.column("profile").to_pylist() == ["A test artist", ""]
        assert artists.column("data_quality").to_pylist() == ["Correct", "Needs Vote"]
        assert artists.column("namevariations").to_pylist() == [["DJ T", "Test"], []]
        assert artists.column("urls").to_pylist() == [["https://example.com"], []]

    def test_aliases(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(artists_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        aliases = tables["artist_aliases"]
        assert aliases.num_rows == 1
        assert aliases.column("artist_id").to_pylist() == [1]
        assert aliases.column("alias_id").to_pylist() == [10]
        assert aliases.column("name").to_pylist() == ["Alias One"]

    def test_groups(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(artists_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        groups = tables["artist_groups"]
        assert groups.num_rows == 1
        assert groups.column("artist_id").to_pylist() == [1]
        assert groups.column("group_id").to_pylist() == [20]

    def test_members(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(artists_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        members = tables["artist_members"]
        assert members.num_rows == 2
        assert members.column("artist_id").to_pylist() == [1, 1]
        assert members.column("member_id").to_pylist() == [30, 31]

    def test_schema_matches(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(artists_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        for name, table in tables.items():
            assert table.schema == SCHEMAS[name]

    def test_missing_id_skipped(self, tmp_path):
        """Artist element without <id> is skipped with a warning."""
        xml = (
            "<artist><name>No ID</name><data_quality>Correct</data_quality></artist>\n"
        )
        f = tmp_path / "no_id.xml"
        f.write_text(xml)

        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(f), 0, os.path.getsize(f)))
            tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["artists"].num_rows == 0
        assert any("missing id" in str(warning.message) for warning in w)

    def test_strict_no_warnings_on_known_xml(self, artists_xml_file):
        """Strict mode produces no warnings when all XML elements are handled."""
        import warnings

        size = os.path.getsize(artists_xml_file)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ipc_dict = extract_chunk_to_ipc(
                ChunkArgs(str(artists_xml_file), 0, size, strict=True)
            )
            tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["artists"].num_rows == 2
        unhandled = [x for x in w if "unhandled" in str(x.message)]
        assert unhandled == []

    def test_strict_warns_on_unknown_element(self, tmp_path):
        """Strict mode warns about unhandled XML elements."""
        import warnings

        xml = (
            "<artist>\n"
            "  <id>1</id>\n"
            "  <name>Test</name>\n"
            "  <data_quality>Correct</data_quality>\n"
            "  <brand_new_tag>surprise</brand_new_tag>\n"
            "</artist>\n"
        )
        f = tmp_path / "unknown.xml"
        f.write_text(xml)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            extract_chunk_to_ipc(ChunkArgs(str(f), 0, os.path.getsize(f), strict=True))

        unhandled = [x for x in w if "unhandled" in str(x.message)]
        assert len(unhandled) == 1
        assert "brand_new_tag" in str(unhandled[0].message)

    def test_no_warnings_without_strict(self, tmp_path):
        """Without strict mode, unknown elements produce no warnings."""
        import warnings

        xml = (
            "<artist>\n"
            "  <id>1</id>\n"
            "  <name>Test</name>\n"
            "  <data_quality>Correct</data_quality>\n"
            "  <brand_new_tag>surprise</brand_new_tag>\n"
            "</artist>\n"
        )
        f = tmp_path / "unknown.xml"
        f.write_text(xml)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            extract_chunk_to_ipc(ChunkArgs(str(f), 0, os.path.getsize(f)))

        unhandled = [x for x in w if "unhandled" in str(x.message)]
        assert unhandled == []
