"""Tests for the entity definitions: detect_entity(), get(), ENTITIES."""

from __future__ import annotations

import dataclasses
import itertools

import pytest

from discogskit.entities import ENTITIES, _by_name, detect_entity


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


class TestEntities:
    @pytest.mark.parametrize("name", ["artists", "labels", "masters", "releases"])
    def test_entity_listed(self, name):
        assert ENTITIES[name].name == name

    def test_duplicate_name_raises(self):
        artists = ENTITIES["artists"]
        twin = dataclasses.replace(artists, root_tag="other")
        with pytest.raises(ValueError, match="Duplicate entity name 'artists'"):
            _by_name((artists, twin))

    @pytest.mark.parametrize("name", ["artists", "labels", "masters", "releases"])
    def test_entity_has_required_fields(self, name):
        entity = ENTITIES[name]
        assert len(entity.table_order) > 0
        assert set(entity.table_weights.keys()) == set(entity.table_order)
        root_schema = entity.schemas[entity.table_order[0]]
        assert root_schema.field(0).name == "id"

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
        assert ENTITIES[name].fk_column == fk_column

    @pytest.mark.parametrize(
        "table, schema",
        [
            pytest.param(table, schema, id=table)
            for entity in ENTITIES.values()
            for table, schema in entity.schemas.items()
        ],
    )
    def test_key_columns_then_the_rest_alphabetically(self, table, schema):
        """The schema order is the Parquet field order, the JSONL key order and the CREATE TABLE order: key columns
        such as id, release_id and track_idx come first, then the rest alphabetically (#221)."""
        names = schema.names
        keys = list(
            itertools.takewhile(
                lambda name: name == "id" or name.endswith(("_id", "_idx")), names
            )
        )
        rest = names[len(keys) :]
        assert rest == sorted(rest)

    @pytest.mark.parametrize(
        "name, n_tables",
        [("artists", 4), ("labels", 2), ("masters", 3), ("releases", 12)],
    )
    def test_table_counts(self, name, n_tables):
        assert len(ENTITIES[name].table_order) == n_tables
