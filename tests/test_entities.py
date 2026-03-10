"""Tests for entity registry: detect_entity(), get(), registration."""

from __future__ import annotations

import pytest

from discogskit.entities import ENTITIES, EntityDef, detect_entity, get


class TestDetectEntity:
    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("discogs_20240101_artists.xml.gz", "artists"),
            ("discogs_20240101_labels.xml.gz", "labels"),
            ("discogs_20240101_masters.xml.gz", "masters"),
            ("discogs_20240101_releases.xml.gz", "releases"),
        ],
    )
    def test_valid_filenames(self, filename, expected):
        assert detect_entity(filename) == expected

    def test_xml_without_gz(self):
        assert detect_entity("discogs_20240101_artists.xml") == "artists"

    def test_unknown_entity_raises(self):
        with pytest.raises(ValueError, match="Cannot detect entity"):
            detect_entity("discogs_20240101_unknown.xml.gz")


class TestGet:
    def test_known_entity(self):
        entity = get("artists")
        assert isinstance(entity, EntityDef)
        assert entity.name == "artists"

    def test_unknown_raises(self):
        with pytest.raises(KeyError):
            get("nonexistent")


class TestRegistration:
    @pytest.mark.parametrize("name", ["artists", "labels", "masters", "releases"])
    def test_entity_registered(self, name):
        assert name in ENTITIES

    @pytest.mark.parametrize("name", ["artists", "labels", "masters", "releases"])
    def test_entity_has_required_fields(self, name):
        entity = get(name)
        assert len(entity.table_order) > 0
        assert set(entity.schemas.keys()) == set(entity.table_order)
        assert set(entity.table_weights.keys()) == set(entity.table_order)
        assert callable(entity.extract_chunk_to_ipc)
        assert callable(entity.find_split_points)
        assert entity.pk_column == "id"

    @pytest.mark.parametrize(
        "name, fk_column",
        [
            ("artists", "artist_id"),
            ("labels", "label_id"),
            ("masters", "master_id"),
            ("releases", "release_id"),
        ],
    )
    def test_fk_columns(self, name, fk_column):
        assert get(name).fk_column == fk_column

    @pytest.mark.parametrize(
        "name, n_tables",
        [("artists", 4), ("labels", 2), ("masters", 3), ("releases", 12)],
    )
    def test_table_counts(self, name, n_tables):
        assert len(get(name).table_order) == n_tables
