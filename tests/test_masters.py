"""Tests for masters entity XML parsing → IPC → tables."""

from __future__ import annotations

import re

import pytest

from discogskit.entities.masters import SCHEMAS
from tests.conftest import (
    assert_second_copy_fails,
    ipc_to_tables,
    parse_file,
    parse_records,
)


class TestMastersParsing:
    def test_extract_chunk(self, masters_xml_file):
        ipc_dict = parse_file(masters_xml_file, "masters")
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
        ipc_dict = parse_file(masters_xml_file, "masters")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        artists = tables["master_artists"]
        assert artists.num_rows == 1
        assert artists.column("master_id").to_pylist() == [1]
        assert artists.column("artist_id").to_pylist() == [50]
        assert artists.column("name").to_pylist() == ["Master Artist"]
        assert artists.column("anv").to_pylist() == ["M.A."]
        assert artists.column("join").to_pylist() == [","]

    def test_genres(self, masters_xml_file):
        ipc_dict = parse_file(masters_xml_file, "masters")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        masters = tables["masters"]
        assert masters.column("genres").to_pylist() == [["Electronic", "Rock"], None]

    def test_styles(self, masters_xml_file):
        ipc_dict = parse_file(masters_xml_file, "masters")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        masters = tables["masters"]
        assert masters.column("styles").to_pylist() == [["Techno"], None]

    def test_videos(self, masters_xml_file):
        ipc_dict = parse_file(masters_xml_file, "masters")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        videos = tables["master_videos"]
        assert videos.num_rows == 1
        assert videos.column("master_id").to_pylist() == [1]
        assert videos.column("src").to_pylist() == ["https://example.com/video"]
        assert videos.column("duration").to_pylist() == [300]
        assert videos.column("embed").to_pylist() == [True]
        assert videos.column("title").to_pylist() == ["Video Title"]

    def test_schema_matches(self, masters_xml_file):
        ipc_dict = parse_file(masters_xml_file, "masters")
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
            parse_file(f, "masters")

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

        ipc_dict = parse_file(f, "masters")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["master_artists"].num_rows == 1
        assert tables["master_videos"].num_rows == 1


class TestMasterVideoTypedValues:
    """A video's duration and embed are stored typed, missing is NULL, and anything else fails (#95)."""

    @staticmethod
    def _video(tmp_path, attrs: str) -> dict:
        f = tmp_path / "video.xml"
        f.write_text(
            f'<master id="1"><videos><video src="http://x"{attrs}/></videos></master>\n'
        )
        ipc_dict = parse_file(f, "masters")
        return ipc_to_tables(ipc_dict, SCHEMAS)["master_videos"].to_pylist()[0]

    @pytest.mark.parametrize(
        "attr, expected",
        [(' duration="0"', 0), (' duration="245"', 245), ("", None)],
        ids=["zero", "positive", "missing"],
    )
    def test_duration(self, tmp_path, attr, expected):
        assert self._video(tmp_path, attr)["duration"] == expected

    @pytest.mark.parametrize(
        "attr, expected",
        [(' embed="true"', True), (' embed="false"', False), ("", None)],
        ids=["true", "false", "missing"],
    )
    def test_embed(self, tmp_path, attr, expected):
        assert self._video(tmp_path, attr)["embed"] is expected

    @pytest.mark.parametrize("value", ["abc", "", "1.5", " 5", "+5"])
    def test_invalid_duration_fails(self, tmp_path, value):
        with pytest.raises(
            ValueError,
            match=rf"^Invalid <master> record .*: invalid duration {re.escape(repr(value))}, expected an integer from"
            r" -2147483648 to 2147483647$",
        ):
            self._video(tmp_path, f' duration="{value}"')

    @pytest.mark.parametrize("value", ["True", "1", "yes", ""])
    def test_invalid_embed_fails(self, tmp_path, value):
        with pytest.raises(
            ValueError,
            match=rf"^Invalid <master> record .*: invalid embed {re.escape(repr(value))}, expected 'true' or 'false'$",
        ):
            self._video(tmp_path, f' embed="{value}"')


_MASTER = ("", "", "master")
_ARTIST = ("<artists><artist>", "</artist></artists>", "artist")
_VIDEO = ('<videos><video src="https://example.com">', "</video></videos>", "video")


class TestRepeatedElements:
    """An element a master holds one of fails when repeated; a container of repeated items appends (#205)."""

    @pytest.mark.parametrize(
        ("element", "table"),
        [
            ("<artists><artist><id>1</id></artist></artists>", "master_artists"),
            ('<videos><video src="https://example.com"/></videos>', "master_videos"),
        ],
        ids=["artists", "videos"],
    )
    def test_appending_container_repeated(self, tmp_path, element, table):
        tables = parse_records(
            tmp_path, "masters", f'<master id="7">{element}{element}</master>\n'
        )
        assert tables[table].num_rows == 2

    @pytest.mark.parametrize(
        ("context", "element"),
        [
            pytest.param(_MASTER, "<title>T</title>", id="title"),
            pytest.param(_MASTER, "<year>2000</year>", id="year"),
            pytest.param(_MASTER, "<year/>", id="year-empty"),
            pytest.param(_MASTER, "<main_release>5</main_release>", id="main_release"),
            pytest.param(_MASTER, "<main_release/>", id="main_release-empty"),
            pytest.param(_MASTER, "<notes>N</notes>", id="notes"),
            pytest.param(
                _MASTER, "<data_quality>Correct</data_quality>", id="data_quality"
            ),
            pytest.param(_MASTER, "<genres><genre>Rock</genre></genres>", id="genres"),
            pytest.param(_MASTER, "<styles><style>Punk</style></styles>", id="styles"),
            pytest.param(_ARTIST, "<id>1</id>", id="artist-id"),
            pytest.param(_ARTIST, "<id/>", id="artist-id-empty"),
            pytest.param(_ARTIST, "<name>N</name>", id="artist-name"),
            pytest.param(_ARTIST, "<anv>A</anv>", id="artist-anv"),
            pytest.param(_ARTIST, "<join>&amp;</join>", id="artist-join"),
            pytest.param(_VIDEO, "<title>T</title>", id="video-title"),
            pytest.param(
                _VIDEO, "<description>D</description>", id="video-description"
            ),
        ],
    )
    def test_second_copy_fails(self, tmp_path, context, element):
        assert_second_copy_fails(
            tmp_path, "masters", ('<master id="7">', "</master>"), context, element
        )
