"""Tests for releases entity XML parsing → IPC → tables."""

from __future__ import annotations

import os
import re

import pytest

from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.releases import SCHEMAS
from tests.conftest import assert_second_copy_fails, ipc_to_tables, parse_records


class TestReleasesParsing:
    def _parse(self, releases_xml_file):
        size = os.path.getsize(releases_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=size, entity="releases", file_path=str(releases_xml_file), start=0
            )
        ).tables
        return ipc_to_tables(ipc_dict, SCHEMAS)

    def test_releases_table(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        rel = tables["releases"]
        assert rel.num_rows == 2
        assert rel.column("id").to_pylist() == [1, 2]
        assert rel.column("title").to_pylist() == ["Test Release", "Minimal Release"]
        assert rel.column("status").to_pylist() == ["Accepted", "Accepted"]
        assert rel.column("country").to_pylist() == ["US", None]
        assert rel.column("released").to_pylist() == ["2020-01-01", None]
        assert rel.column("master_id").to_pylist() == [100, None]
        assert rel.column("is_main_release").to_pylist() == [True, None]
        assert rel.column("genres").to_pylist() == [["Electronic"], None]
        assert rel.column("styles").to_pylist() == [["Techno"], None]

    def test_artists(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        a = tables["release_artists"]
        assert a.num_rows == 1
        assert a.column("release_id").to_pylist() == [1]
        assert a.column("artist_id").to_pylist() == [50]
        assert a.column("name").to_pylist() == ["Release Artist"]

    def test_empty_tracks_in_track_extraartist_accepted(self, tmp_path):
        """An empty <tracks> in a track's or sub-track's extra artist holds nothing to lose, so it's accepted (#202)."""
        xml = (
            '<release id="7" status="Accepted">\n'
            "  <tracklist>\n"
            "    <track><position>1</position>\n"
            "      <extraartists><artist><id>8</id><tracks/></artist></extraartists>\n"
            "      <sub_tracks><track><position>1a</position>\n"
            "        <extraartists><artist><id>9</id><tracks></tracks></artist></extraartists>\n"
            "      </track></sub_tracks>\n"
            "    </track>\n"
            "  </tracklist>\n"
            "</release>\n"
        )
        f = tmp_path / "empty_track_tracks.xml"
        f.write_text(xml)

        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f),
                entity="releases",
                file_path=str(f),
                start=0,
                strict=True,
            )
        ).tables
        te = ipc_to_tables(ipc_dict, SCHEMAS)["release_track_extraartists"]
        assert te.column("track_idx").to_pylist() == [0, 1]
        assert te.column("artist_id").to_pylist() == [8, 9]

    def test_extraartists(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        ea = tables["release_extraartists"]
        assert ea.num_rows == 1
        assert ea.column("role").to_pylist() == ["Producer"]
        assert ea.column("tracks").to_pylist() == ["A1"]

    def test_artists_without_name_kept(self, tmp_path):
        """Artists with no <name> keep their row with a NULL name, and <name/> gives "" (#82)."""
        xml = (
            '<release id="1" status="Accepted">\n'
            "  <artists>\n"
            "    <artist><id>50</id><anv>A</anv><join>&amp;</join></artist>\n"
            "    <artist><id>51</id><name/><anv/><join>,</join></artist>\n"
            "  </artists>\n"
            "  <extraartists>\n"
            "    <artist><id>60</id><anv>E</anv><role>Mix</role><tracks>A1</tracks></artist>\n"
            "    <artist><id>61</id><name></name><role>Producer</role></artist>\n"
            "  </extraartists>\n"
            "  <tracklist>\n"
            "    <track><position>1</position>\n"
            "      <artists><artist><id>70</id><join>x</join></artist></artists>\n"
            "      <extraartists><artist><id>71</id><name/><role>Vocals</role></artist></extraartists>\n"
            "      <sub_tracks><track><position>1a</position>\n"
            "        <artists><artist><id>72</id><name/></artist></artists>\n"
            "        <extraartists><artist><id>73</id><role>Bass</role></artist></extraartists>\n"
            "      </track></sub_tracks>\n"
            "    </track>\n"
            "  </tracklist>\n"
            "</release>\n"
        )
        f = tmp_path / "nameless_artists.xml"
        f.write_text(xml)

        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
            )
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["release_artists"].to_pylist() == [
            {"release_id": 1, "artist_id": 50, "anv": "A", "join": "&", "name": None},
            {"release_id": 1, "artist_id": 51, "anv": "", "join": ",", "name": ""},
        ]
        assert tables["release_extraartists"].to_pylist() == [
            {
                "release_id": 1,
                "artist_id": 60,
                "anv": "E",
                "name": None,
                "role": "Mix",
                "tracks": "A1",
            },
            {
                "release_id": 1,
                "artist_id": 61,
                "anv": None,
                "name": "",
                "role": "Producer",
                "tracks": None,
            },
        ]
        assert tables["release_track_artists"].to_pylist() == [
            {
                "release_id": 1,
                "track_idx": 0,
                "artist_id": 70,
                "anv": None,
                "join": "x",
                "name": None,
            },
            {
                "release_id": 1,
                "track_idx": 1,
                "artist_id": 72,
                "anv": None,
                "join": None,
                "name": "",
            },
        ]
        assert tables["release_track_extraartists"].to_pylist() == [
            {
                "release_id": 1,
                "track_idx": 0,
                "artist_id": 71,
                "anv": None,
                "name": "",
                "role": "Vocals",
            },
            {
                "release_id": 1,
                "track_idx": 1,
                "artist_id": 73,
                "anv": None,
                "name": None,
                "role": "Bass",
            },
        ]

    def test_labels(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        lb = tables["release_labels"]
        assert lb.num_rows == 1
        assert lb.column("label_id").to_pylist() == [10]
        assert lb.column("catno").to_pylist() == ["TL001"]

    def test_series(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        sr = tables["release_series"]
        assert sr.num_rows == 1
        assert sr.column("series_id").to_pylist() == [20]

    def test_formats(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        fm = tables["release_formats"]
        assert fm.num_rows == 1
        assert fm.column("name").to_pylist() == ["Vinyl"]
        assert fm.column("descriptions").to_pylist() == [['12"', "33 RPM"]]

    def test_identifiers(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        ident = tables["release_identifiers"]
        assert ident.num_rows == 1
        assert ident.column("type").to_pylist() == ["Barcode"]
        assert ident.column("value").to_pylist() == ["123456"]

    def test_videos(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        v = tables["release_videos"]
        assert v.num_rows == 1
        assert v.column("duration").to_pylist() == [180]
        assert v.column("embed").to_pylist() == [True]

    def test_companies(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        co = tables["release_companies"]
        assert co.num_rows == 1
        assert co.column("company_id").to_pylist() == [60]
        assert co.column("entity_type_name").to_pylist() == ["Pressed By"]

    @pytest.mark.parametrize(
        ("track", "message"),
        [
            (
                (
                    "<track><position>1</position>\n"
                    "      <extraartists><artist><id>8</id><role>Mix</role>\n"
                    "        <tracks>A1</tracks></artist></extraartists>\n"
                    "    </track>"
                ),
                "<tracks> 'A1' in an extra artist of track '1'",
            ),
            (
                (
                    "<track><position>1</position><sub_tracks><track><position>1a</position>\n"
                    "      <extraartists><artist><id>8</id><role>Mix</role>\n"
                    "        <tracks>A1</tracks></artist></extraartists>\n"
                    "    </track></sub_tracks></track>"
                ),
                "<tracks> 'A1' in an extra artist of sub-track '1a' of track '1'",
            ),
        ],
        ids=["track", "sub-track"],
    )
    def test_tracks_in_track_extraartist_fail(self, tmp_path, track, message):
        """A non-empty <tracks> in a track's or sub-track's extra artist fails: no column stores it (#202)."""
        xml = (
            '<release id="7" status="Accepted">\n'
            "  <tracklist>\n"
            f"    {track}\n"
            "  </tracklist>\n"
            "</release>\n"
        )
        f = tmp_path / "track_tracks.xml"
        f.write_text(xml)

        with pytest.raises(
            ValueError,
            match=r"^Invalid <release> record in .* at chunk line 1 \(chunk bytes \d+-\d+, id='7'\): "
            + re.escape(message)
            + "; only release-level extra artists have a tracks column at chunk line 5$",
        ):
            extract_chunk_to_ipc(
                ChunkArgs(
                    end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
                )
            )

    def test_tracks_with_subtracks(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        t = tables["release_tracks"]
        # 1 parent track + 1 sub-track = 2 rows
        assert t.num_rows == 2
        assert t.column("track_idx").to_pylist() == [0, 1]
        assert t.column("parent_idx").to_pylist() == [None, 0]
        assert t.column("position").to_pylist() == ["A1", "A1.1"]
        assert t.column("title").to_pylist() == ["Track One", "Sub Track"]

    def test_sub_tracks_nested_in_a_sub_track_fail(self, tmp_path):
        """A <sub_tracks> inside a sub-track fails instead of vanishing with everything below it (#96)."""
        xml = (
            '<release id="7" status="Accepted">\n'
            "  <tracklist>\n"
            "    <track><position>1</position>\n"
            "      <sub_tracks>\n"
            "        <track><position>1a</position>\n"
            "          <sub_tracks>\n"
            "            <track><position>1a.i</position></track>\n"
            "          </sub_tracks>\n"
            "        </track>\n"
            "      </sub_tracks>\n"
            "    </track>\n"
            "  </tracklist>\n"
            "</release>\n"
        )
        f = tmp_path / "nested_sub_tracks.xml"
        f.write_text(xml)

        with pytest.raises(
            ValueError,
            match=r"^Invalid <release> record in .* at chunk line 1 \(chunk bytes \d+-\d+, id='7'\):"
            r" <sub_tracks> nested in sub-track '1a' of track '1' at chunk line 6$",
        ):
            extract_chunk_to_ipc(
                ChunkArgs(
                    end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
                )
            )

    def test_nested_sub_tracks_line_past_libxml2_limit(self, tmp_path):
        """The nested <sub_tracks> line is its own past line 65535, where lxml reports its parent's line 1 (#96)."""
        xml = (
            '<release id="7" status="Accepted"><tracklist><track><position>1</position>'
            "<sub_tracks><track><position>1a</position><title>"
            + "\n" * 70_000
            + "</title><sub_tracks/></track></sub_tracks></track></tracklist></release>\n"
        )
        f = tmp_path / "nested_sub_tracks.xml"
        f.write_text(xml)

        with pytest.raises(
            ValueError,
            match=r"^Invalid <release> record in .* at chunk line 1 \(chunk bytes \d+-\d+, id='7'\):"
            r" <sub_tracks> nested in sub-track '1a' of track '1' at chunk line 70001$",
        ):
            extract_chunk_to_ipc(
                ChunkArgs(
                    end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
                )
            )

    def test_track_artists(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        ta = tables["release_track_artists"]
        assert ta.num_rows == 1
        assert ta.column("track_idx").to_pylist() == [0]
        assert ta.column("artist_id").to_pylist() == [50]

    def test_track_extraartists(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        te = tables["release_track_extraartists"]
        assert te.num_rows == 1
        assert te.column("track_idx").to_pylist() == [0]
        assert te.column("role").to_pylist() == ["Remix"]

    def test_all_12_tables_present(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        assert len(tables) == 12

    def test_schema_matches(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        for name, table in tables.items():
            assert table.schema == SCHEMAS[name]

    @pytest.mark.parametrize("id_attr", ["", ' id=""'])
    def test_missing_id_fails(self, tmp_path, id_attr):
        """A release without an id fails with its line instead of vanishing (#94)."""
        xml = (
            '<release id="1" status="Accepted"><title>A</title></release>\n'
            f'<release{id_attr} status="Accepted"><title>No ID</title></release>\n'
        )
        f = tmp_path / "no_id.xml"
        f.write_text(xml)

        with pytest.raises(
            ValueError,
            match=r"^Invalid <release> record in .* at chunk line 2 .*: missing or empty id$",
        ):
            extract_chunk_to_ipc(
                ChunkArgs(
                    end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
                )
            )

    def test_missing_is_null_and_empty_is_empty_string(self, tmp_path):
        """Absent elements/attributes are NULL; present but empty ones are "" (#21)."""
        xml = (
            '<release id="1" status="">\n'
            "  <title/>\n"
            "  <genres><genre/><genre>Rock</genre></genres>\n"
            "  <formats>\n"
            '    <format name="Vinyl" qty="">'
            "<descriptions><description/></descriptions></format>\n"
            '    <format name="CD"/>\n'
            "  </formats>\n"
            '  <videos><video src="http://x"><title/></video></videos>\n'
            "</release>\n"
        )
        f = tmp_path / "missing_vs_empty.xml"
        f.write_text(xml)

        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
            )
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        rel = tables["releases"].to_pylist()[0]
        assert rel["status"] == ""
        assert rel["title"] == ""
        assert rel["genres"] == ["", "Rock"]
        assert rel["country"] is None
        assert rel["styles"] is None
        formats = tables["release_formats"].to_pylist()
        assert [(r["qty"], r["text"], r["descriptions"]) for r in formats] == [
            ("", None, [""]),
            (None, None, None),
        ]
        video = tables["release_videos"].to_pylist()[0]
        assert (video["title"], video["description"]) == ("", None)
        assert (video["duration"], video["embed"]) == (None, None)

    def test_non_matching_tags_skipped(self, tmp_path):
        """Non-matching sibling tags in artists, extraartists, companies, tracklist, sub_tracks are skipped."""
        xml = (
            '<release id="1" status="Accepted">\n'
            "  <title>Test</title>\n"
            "  <data_quality>Correct</data_quality>\n"
            "  <artists>\n"
            "    <bogus>ignored</bogus>\n"
            "    <artist><id>50</id><name>A</name><anv></anv><join></join></artist>\n"
            "  </artists>\n"
            "  <extraartists>\n"
            "    <bogus>ignored</bogus>\n"
            "    <artist><id>51</id><name>EA</name><anv></anv><role>Mix</role></artist>\n"
            "  </extraartists>\n"
            "  <companies>\n"
            "    <bogus>ignored</bogus>\n"
            "    <company><id>60</id><name>Co</name><catno></catno>"
            "<entity_type>1</entity_type><entity_type_name>P</entity_type_name></company>\n"
            "  </companies>\n"
            "  <tracklist>\n"
            "    <bogus>ignored</bogus>\n"
            "    <track>\n"
            "      <position>A1</position><title>T1</title><duration>3:00</duration>\n"
            "      <sub_tracks>\n"
            "        <bogus>ignored</bogus>\n"
            "        <track><position>A1.1</position><title>S1</title><duration>1:00</duration></track>\n"
            "      </sub_tracks>\n"
            "    </track>\n"
            "  </tracklist>\n"
            "</release>\n"
        )
        f = tmp_path / "bogus_tags.xml"
        f.write_text(xml)

        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
            )
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["release_artists"].num_rows == 1
        assert tables["release_extraartists"].num_rows == 1
        assert tables["release_companies"].num_rows == 1
        assert tables["release_tracks"].num_rows == 2  # 1 parent + 1 sub


class TestReleaseTypedValues:
    """Video duration, embed and is_main_release are stored typed, missing is NULL, and anything else fails (#95)."""

    @staticmethod
    def _release(tmp_path, master_id_attrs: str, video_attrs: str) -> dict:
        f = tmp_path / "release.xml"
        f.write_text(
            f'<release id="1" status="Accepted"><master_id{master_id_attrs}>5</master_id>'
            f'<videos><video src="http://x"{video_attrs}/></videos></release>\n'
        )
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=os.path.getsize(f), entity="releases", file_path=str(f), start=0
            )
        ).tables
        return ipc_to_tables(ipc_dict, SCHEMAS)

    @pytest.mark.parametrize(
        "attr, expected",
        [(' duration="0"', 0), (' duration="245"', 245), ("", None)],
        ids=["zero", "positive", "missing"],
    )
    def test_duration(self, tmp_path, attr, expected):
        tables = self._release(tmp_path, "", attr)
        assert tables["release_videos"].column("duration").to_pylist() == [expected]

    @pytest.mark.parametrize(
        "attr, expected",
        [(' embed="true"', True), (' embed="false"', False), ("", None)],
        ids=["true", "false", "missing"],
    )
    def test_embed(self, tmp_path, attr, expected):
        tables = self._release(tmp_path, "", attr)
        assert tables["release_videos"].column("embed").to_pylist() == [expected]

    @pytest.mark.parametrize("value", ["abc", "", "1.5", " 5", "+5"])
    def test_invalid_duration_fails(self, tmp_path, value):
        with pytest.raises(
            ValueError,
            match=rf"^Invalid <release> record .*: invalid duration {re.escape(repr(value))}, expected an integer from"
            r" -2147483648 to 2147483647$",
        ):
            self._release(tmp_path, "", f' duration="{value}"')

    @pytest.mark.parametrize("value", ["True", "1", "yes", ""])
    def test_invalid_embed_fails(self, tmp_path, value):
        with pytest.raises(
            ValueError,
            match=rf"^Invalid <release> record .*: invalid embed {re.escape(repr(value))}, expected 'true' or 'false'$",
        ):
            self._release(tmp_path, "", f' embed="{value}"')

    @pytest.mark.parametrize("value", ["True", "1", "yes", ""])
    def test_invalid_is_main_release_fails(self, tmp_path, value):
        with pytest.raises(
            ValueError,
            match=rf"^Invalid <release> record .*: invalid is_main_release {re.escape(repr(value))}, expected 'true' or 'false'$",
        ):
            self._release(tmp_path, f' is_main_release="{value}"', "")

    @pytest.mark.parametrize(
        "attr, expected",
        [
            (' is_main_release="true"', True),
            (' is_main_release="false"', False),
            ("", None),
        ],
        ids=["true", "false", "missing"],
    )
    def test_is_main_release(self, tmp_path, attr, expected):
        tables = self._release(tmp_path, attr, "")
        assert tables["releases"].column("is_main_release").to_pylist() == [expected]


_RELEASE = ("", "", "release")
_FORMAT = ('<formats><format name="CD">', "</format></formats>", "format")
_TRACK = ("<tracklist><track>", "</track></tracklist>", "track")
_SUB_TRACK = (
    "<tracklist><track><sub_tracks><track>",
    "</track></sub_tracks></track></tracklist>",
    "track",
)
_ARTIST = ("<artists><artist>", "</artist></artists>", "artist")
_EXTRA_ARTIST = ("<extraartists><artist>", "</artist></extraartists>", "artist")
_TRACK_ARTIST = (
    "<tracklist><track><artists><artist>",
    "</artist></artists></track></tracklist>",
    "artist",
)
_TRACK_EXTRA_ARTIST = (
    "<tracklist><track><extraartists><artist>",
    "</artist></extraartists></track></tracklist>",
    "artist",
)
_COMPANY = ("<companies><company>", "</company></companies>", "company")
_VIDEO = ('<videos><video src="https://example.com">', "</video></videos>", "video")


class TestRepeatedElements:
    """An element a release holds one of fails when repeated; a container of repeated items appends (#205)."""

    @pytest.mark.parametrize(
        ("element", "table"),
        [
            ('<labels><label id="1" name="L" catno="C"/></labels>', "release_labels"),
            ('<series><series id="1" name="S"/></series>', "release_series"),
            ('<formats><format name="CD"/></formats>', "release_formats"),
            (
                '<identifiers><identifier type="Barcode" value="1"/></identifiers>',
                "release_identifiers",
            ),
            ('<videos><video src="https://example.com"/></videos>', "release_videos"),
            (
                "<companies><company><id>1</id></company></companies>",
                "release_companies",
            ),
        ],
        ids=["labels", "series", "formats", "identifiers", "videos", "companies"],
    )
    def test_appending_container_repeated(self, tmp_path, element, table):
        tables = parse_records(
            tmp_path,
            "releases",
            f'<release id="7" status="Accepted">{element}{element}</release>\n',
        )
        assert tables[table].num_rows == 2

    @pytest.mark.parametrize(
        ("context", "element"),
        [
            pytest.param(_RELEASE, "<title>T</title>", id="title"),
            pytest.param(_RELEASE, "<country>UK</country>", id="country"),
            pytest.param(_RELEASE, "<released>2000</released>", id="released"),
            pytest.param(_RELEASE, "<notes>N</notes>", id="notes"),
            pytest.param(
                _RELEASE, "<data_quality>Correct</data_quality>", id="data_quality"
            ),
            pytest.param(_RELEASE, "<master_id>5</master_id>", id="master_id"),
            pytest.param(_RELEASE, "<master_id/>", id="master_id-empty"),
            pytest.param(_RELEASE, "<genres><genre>Rock</genre></genres>", id="genres"),
            pytest.param(_RELEASE, "<styles><style>Punk</style></styles>", id="styles"),
            pytest.param(
                _RELEASE, "<artists><artist><id>1</id></artist></artists>", id="artists"
            ),
            pytest.param(_RELEASE, "<artists/>", id="artists-empty"),
            pytest.param(
                _RELEASE,
                "<extraartists><artist><id>1</id></artist></extraartists>",
                id="extraartists",
            ),
            pytest.param(
                _RELEASE,
                "<tracklist><track><position>1</position></track></tracklist>",
                id="tracklist",
            ),
            pytest.param(_RELEASE, "<tracklist/>", id="tracklist-empty"),
            pytest.param(
                _FORMAT,
                "<descriptions><description>Album</description></descriptions>",
                id="format-descriptions",
            ),
            pytest.param(_FORMAT, "<descriptions/>", id="format-descriptions-empty"),
            pytest.param(_TRACK, "<position>1</position>", id="track-position"),
            pytest.param(_TRACK, "<title>T</title>", id="track-title"),
            pytest.param(_TRACK, "<duration>1:00</duration>", id="track-duration"),
            pytest.param(
                _TRACK,
                "<artists><artist><id>1</id></artist></artists>",
                id="track-artists",
            ),
            pytest.param(_TRACK, "<artists/>", id="track-artists-empty"),
            pytest.param(
                _TRACK,
                "<extraartists><artist><id>1</id></artist></extraartists>",
                id="track-extraartists",
            ),
            pytest.param(
                _TRACK,
                "<sub_tracks><track><position>1a</position></track></sub_tracks>",
                id="track-sub_tracks",
            ),
            pytest.param(_TRACK, "<sub_tracks/>", id="track-sub_tracks-empty"),
            pytest.param(
                _SUB_TRACK, "<position>1a</position>", id="sub-track-position"
            ),
            pytest.param(_SUB_TRACK, "<title>T</title>", id="sub-track-title"),
            pytest.param(
                _SUB_TRACK, "<duration>1:00</duration>", id="sub-track-duration"
            ),
            pytest.param(
                _SUB_TRACK,
                "<artists><artist><id>1</id></artist></artists>",
                id="sub-track-artists",
            ),
            pytest.param(
                _SUB_TRACK,
                "<extraartists><artist><id>1</id></artist></extraartists>",
                id="sub-track-extraartists",
            ),
            pytest.param(_ARTIST, "<id>1</id>", id="artist-id"),
            pytest.param(_ARTIST, "<id/>", id="artist-id-empty"),
            pytest.param(_ARTIST, "<name>N</name>", id="artist-name"),
            pytest.param(_ARTIST, "<anv>A</anv>", id="artist-anv"),
            pytest.param(_ARTIST, "<join>&amp;</join>", id="artist-join"),
            pytest.param(_EXTRA_ARTIST, "<id>1</id>", id="extraartist-id"),
            pytest.param(_EXTRA_ARTIST, "<name>N</name>", id="extraartist-name"),
            pytest.param(_EXTRA_ARTIST, "<anv>A</anv>", id="extraartist-anv"),
            pytest.param(_EXTRA_ARTIST, "<role>Mix</role>", id="extraartist-role"),
            pytest.param(_EXTRA_ARTIST, "<tracks>A1</tracks>", id="extraartist-tracks"),
            pytest.param(_TRACK_ARTIST, "<name>N</name>", id="track-artist-name"),
            pytest.param(
                _TRACK_EXTRA_ARTIST, "<role>Mix</role>", id="track-extraartist-role"
            ),
            pytest.param(_COMPANY, "<id>1</id>", id="company-id"),
            pytest.param(_COMPANY, "<id/>", id="company-id-empty"),
            pytest.param(_COMPANY, "<name>N</name>", id="company-name"),
            pytest.param(_COMPANY, "<catno>C</catno>", id="company-catno"),
            pytest.param(
                _COMPANY, "<entity_type>23</entity_type>", id="company-entity_type"
            ),
            pytest.param(
                _COMPANY,
                "<entity_type_name>Pressed By</entity_type_name>",
                id="company-entity_type_name",
            ),
            pytest.param(_VIDEO, "<title>T</title>", id="video-title"),
            pytest.param(
                _VIDEO, "<description>D</description>", id="video-description"
            ),
        ],
    )
    def test_second_copy_fails(self, tmp_path, context, element):
        assert_second_copy_fails(
            tmp_path,
            "releases",
            ('<release id="7" status="Accepted">', "</release>"),
            context,
            element,
        )
