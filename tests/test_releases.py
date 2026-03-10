"""Tests for releases entity XML parsing → IPC → tables."""

from __future__ import annotations

import os

from discogskit.entities import ChunkArgs
from discogskit.entities.releases import SCHEMAS, extract_chunk_to_ipc
from tests.conftest import ipc_to_tables


class TestReleasesParsing:
    def _parse(self, releases_xml_file):
        size = os.path.getsize(releases_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(releases_xml_file), 0, size))
        return ipc_to_tables(ipc_dict, SCHEMAS)

    def test_releases_table(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        rel = tables["releases"]
        assert rel.num_rows == 2
        assert rel.column("id").to_pylist() == [1, 2]
        assert rel.column("title").to_pylist() == ["Test Release", "Minimal Release"]
        assert rel.column("status").to_pylist() == ["Accepted", "Accepted"]
        assert rel.column("country").to_pylist() == ["US", ""]
        assert rel.column("released").to_pylist() == ["2020-01-01", ""]
        assert rel.column("master_id").to_pylist() == [100, None]
        assert rel.column("is_main_release").to_pylist() == [True, None]
        assert rel.column("genres").to_pylist() == [["Electronic"], []]
        assert rel.column("styles").to_pylist() == [["Techno"], []]

    def test_artists(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        a = tables["release_artists"]
        assert a.num_rows == 1
        assert a.column("release_id").to_pylist() == [1]
        assert a.column("artist_id").to_pylist() == [50]
        assert a.column("name").to_pylist() == ["Release Artist"]

    def test_extraartists(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        ea = tables["release_extraartists"]
        assert ea.num_rows == 1
        assert ea.column("role").to_pylist() == ["Producer"]
        assert ea.column("tracks").to_pylist() == ["A1"]

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

    def test_tracks_with_subtracks(self, releases_xml_file):
        tables = self._parse(releases_xml_file)
        t = tables["release_tracks"]
        # 1 parent track + 1 sub-track = 2 rows
        assert t.num_rows == 2
        assert t.column("track_idx").to_pylist() == [0, 1]
        assert t.column("parent_idx").to_pylist() == [None, 0]
        assert t.column("position").to_pylist() == ["A1", "A1.1"]
        assert t.column("title").to_pylist() == ["Track One", "Sub Track"]

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

    def test_missing_id_skipped(self, tmp_path):
        """Release without id attribute is skipped with a warning."""
        xml = '<release status="Accepted"><title>No ID</title></release>\n'
        f = tmp_path / "no_id.xml"
        f.write_text(xml)

        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(f), 0, os.path.getsize(f)))
            tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["releases"].num_rows == 0
        assert any("missing id" in str(warning.message) for warning in w)

    def test_invalid_video_duration_fallback(self, tmp_path):
        """Video with non-integer duration falls back to 0."""
        xml = (
            '<release id="1" status="Accepted">\n'
            "  <title>Test</title>\n"
            "  <data_quality>Correct</data_quality>\n"
            "  <videos>\n"
            '    <video src="http://x" duration="abc" embed="true">\n'
            "      <title>V</title><description>D</description>\n"
            "    </video>\n"
            "  </videos>\n"
            "</release>\n"
        )
        f = tmp_path / "bad_dur.xml"
        f.write_text(xml)

        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(f), 0, os.path.getsize(f)))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["release_videos"].column("duration").to_pylist() == [0]

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

        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(f), 0, os.path.getsize(f)))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["release_artists"].num_rows == 1
        assert tables["release_extraartists"].num_rows == 1
        assert tables["release_companies"].num_rows == 1
        assert tables["release_tracks"].num_rows == 2  # 1 parent + 1 sub
