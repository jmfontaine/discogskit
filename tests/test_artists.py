"""Tests for artists entity XML parsing → IPC → tables."""

from __future__ import annotations

import os

import pytest

from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.artists import SCHEMAS
from tests.conftest import ipc_to_tables


class TestArtistsParsing:
    def test_extract_chunk(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        # 2 artists
        artists = tables["artists"]
        assert artists.num_rows == 2
        assert artists.column("id").to_pylist() == [1, 2]
        assert artists.column("name").to_pylist() == ["DJ Test", "Minimal Artist"]
        assert artists.column("realname").to_pylist() == ["Test Person", None]
        assert artists.column("profile").to_pylist() == ["A test artist", None]
        assert artists.column("data_quality").to_pylist() == ["Correct", "Needs Vote"]
        assert artists.column("namevariations").to_pylist() == [["DJ T", "Test"], None]
        assert artists.column("urls").to_pylist() == [["https://example.com"], None]

    def test_aliases(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        aliases = tables["artist_aliases"]
        assert aliases.num_rows == 1
        assert aliases.column("artist_id").to_pylist() == [1]
        assert aliases.column("alias_id").to_pylist() == [10]
        assert aliases.column("name").to_pylist() == ["Alias One"]

    def test_groups(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        groups = tables["artist_groups"]
        assert groups.num_rows == 1
        assert groups.column("artist_id").to_pylist() == [1]
        assert groups.column("group_id").to_pylist() == [20]

    def test_members(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        members = tables["artist_members"]
        assert members.num_rows == 2
        assert members.column("artist_id").to_pylist() == [1, 1]
        assert members.column("member_id").to_pylist() == [30, 31]

    def test_schema_matches(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        for name, table in tables.items():
            assert table.schema == SCHEMAS[name]

    @pytest.mark.parametrize("id_xml", ["", "<id></id>", "<id/>"])
    def test_missing_id_fails(self, tmp_path, id_xml):
        """An artist without an id fails with its line instead of vanishing (#94)."""
        xml = (
            "<artist><id>1</id><name>A</name></artist>\n"
            f"<artist>{id_xml}<name>No ID</name></artist>\n"
        )
        f = tmp_path / "no_id.xml"
        f.write_text(xml)

        with pytest.raises(
            ValueError,
            match=r"^Invalid <artist> record in .* at chunk line 2 .*: missing or empty id$",
        ):
            extract_chunk_to_ipc(ChunkArgs("artists", str(f), 0, os.path.getsize(f)))

    @pytest.mark.filterwarnings("error")
    def test_strict_reports_nothing_on_known_xml(self, artists_xml_file):
        """Strict mode reports nothing when all XML elements are handled."""
        size = os.path.getsize(artists_xml_file)
        result = extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size, strict=True)
        )
        tables = ipc_to_tables(result.tables, SCHEMAS)

        assert tables["artists"].num_rows == 2
        assert result.unknown == []

    def test_strict_ignores_comments_and_pis(self, tmp_path):
        """Comments and processing instructions inside a record aren't elements: strict mode reports only the unknown
        element, and the text around a comment is kept whole."""

        xml = (
            "<artist>\n"
            "  <id>1</id>\n"
            "  <!-- a comment -->\n"
            "  <?some-pi data?>\n"
            "  <name>Te<!-- split -->st</name>\n"
            "  <data_quality>Correct</data_quality>\n"
            "  <brand_new_tag>surprise</brand_new_tag>\n"
            "</artist>\n"
        )
        f = tmp_path / "comments.xml"
        f.write_text(xml)

        result = extract_chunk_to_ipc(
            ChunkArgs("artists", str(f), 0, os.path.getsize(f), strict=True)
        )
        tables = ipc_to_tables(result.tables, SCHEMAS)

        assert result.unknown == ["brand_new_tag"]
        assert tables["artists"].column("name").to_pylist() == ["Test"]

    @pytest.mark.filterwarnings("error")
    def test_nothing_reported_without_strict(self, tmp_path):
        """Without strict mode, unknown elements aren't collected."""
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

        result = extract_chunk_to_ipc(
            ChunkArgs("artists", str(f), 0, os.path.getsize(f))
        )

        assert result.unknown == []
