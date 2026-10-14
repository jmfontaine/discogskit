"""Tests for masters entity XML parsing → IPC → tables."""

from __future__ import annotations

import os

import pytest

from discogskit.entities import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.masters import SCHEMAS
from tests.conftest import ipc_to_tables


class TestMastersParsing:
    def test_extract_chunk(self, masters_xml_file):
        size = os.path.getsize(masters_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("masters", str(masters_xml_file), 0, size)
        )
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        masters = tables["masters"]
        assert masters.num_rows == 2
        assert masters.column("id").to_pylist() == [1, 2]
        assert masters.column("title").to_pylist() == ["Test Master", "Minimal Master"]
        assert masters.column("year").to_pylist() == [2020, None]
        assert masters.column("main_release").to_pylist() == [100, None]
        assert masters.column("notes").to_pylist() == ["Some notes", None]
        assert masters.column("data_quality").to_pylist() == ["Correct", "Needs Vote"]

    def test_artists(self, masters_xml_file):
        size = os.path.getsize(masters_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("masters", str(masters_xml_file), 0, size)
        )
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        artists = tables["master_artists"]
        assert artists.num_rows == 1
        assert artists.column("master_id").to_pylist() == [1]
        assert artists.column("artist_id").to_pylist() == [50]
        assert artists.column("name").to_pylist() == ["Master Artist"]
        assert artists.column("anv").to_pylist() == ["M.A."]
        assert artists.column("join").to_pylist() == [","]

    def test_genres(self, masters_xml_file):
        size = os.path.getsize(masters_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("masters", str(masters_xml_file), 0, size)
        )
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        masters = tables["masters"]
        assert masters.column("genres").to_pylist() == [["Electronic", "Rock"], None]

    def test_styles(self, masters_xml_file):
        size = os.path.getsize(masters_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("masters", str(masters_xml_file), 0, size)
        )
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        masters = tables["masters"]
        assert masters.column("styles").to_pylist() == [["Techno"], None]

    def test_videos(self, masters_xml_file):
        size = os.path.getsize(masters_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("masters", str(masters_xml_file), 0, size)
        )
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        videos = tables["master_videos"]
        assert videos.num_rows == 1
        assert videos.column("master_id").to_pylist() == [1]
        assert videos.column("src").to_pylist() == ["https://example.com/video"]
        assert videos.column("duration").to_pylist() == [300]
        assert videos.column("embed").to_pylist() == [True]
        assert videos.column("title").to_pylist() == ["Video Title"]

    def test_schema_matches(self, masters_xml_file):
        size = os.path.getsize(masters_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("masters", str(masters_xml_file), 0, size)
        )
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        for name, table in tables.items():
            assert table.schema == SCHEMAS[name]

    @pytest.mark.parametrize("id_attr", ["", ' id=""'])
    def test_missing_id_fails(self, tmp_path, id_attr):
        """A master without an id fails with its line instead of vanishing (#94)."""
        xml = (
            '<master id="1"><title>A</title></master>\n'
            f"<master{id_attr}><title>No ID</title></master>\n"
        )
        f = tmp_path / "no_id.xml"
        f.write_text(xml)

        with pytest.raises(
            ValueError,
            match=r"^Invalid <master> record in .* at chunk line 2 .*: missing or empty id$",
        ):
            extract_chunk_to_ipc(ChunkArgs("masters", str(f), 0, os.path.getsize(f)))

    def test_non_matching_tags_skipped(self, tmp_path):
        """Non-<artist> siblings in <artists> and non-<video> in <videos> are skipped."""
        xml = (
            '<master id="1">\n'
            "  <title>Test</title>\n"
            "  <data_quality>Correct</data_quality>\n"
            "  <artists>\n"
            "    <bogus>ignored</bogus>\n"
            "    <artist><id>50</id><name>Real</name></artist>\n"
            "  </artists>\n"
            "  <videos>\n"
            "    <bogus>ignored</bogus>\n"
            '    <video src="http://x" duration="60" embed="true">\n'
            "      <title>V</title><description>D</description>\n"
            "    </video>\n"
            "  </videos>\n"
            "</master>\n"
        )
        f = tmp_path / "bogus_tags.xml"
        f.write_text(xml)

        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs("masters", str(f), 0, os.path.getsize(f))
        )
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["master_artists"].num_rows == 1
        assert tables["master_videos"].num_rows == 1
