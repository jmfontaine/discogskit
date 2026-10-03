"""Integration tests for SQLiteWriter full lifecycle."""

from __future__ import annotations

import json
import os
import sqlite3

import pytest

from discogskit.entities import ChunkArgs, get
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.writers.sqlite import SQLiteWriter
from tests.conftest import (
    ARTISTS_XML,
    LABELS_XML,
    MASTERS_XML,
    RELEASES_XML,
    empty_ipc,
    ipc_to_record_batches,
    ipc_to_tables,
)


@pytest.mark.integration
@pytest.mark.parametrize("fk", [False, True], ids=["no-fk", "fk"])
@pytest.mark.parametrize(
    "entity_name, xml",
    [
        ("artists", ARTISTS_XML),
        ("labels", LABELS_XML),
        ("masters", MASTERS_XML),
        ("releases", RELEASES_XML),
    ],
    ids=["artists", "labels", "masters", "releases"],
)
def test_every_entity_loads(tmp_path, entity_name, xml, fk):
    """Every table round-trips, including columns named after SQL keywords like `join` (#56)."""
    entity = get(entity_name)
    f = tmp_path / f"{entity_name}.xml"
    f.write_text(xml)
    ipc_dict = extract_chunk_to_ipc(
        ChunkArgs(entity_name, str(f), 0, os.path.getsize(f))
    )
    expected = {
        name: table.num_rows
        for name, table in ipc_to_tables(ipc_dict, entity.schemas).items()
    }

    db_path = str(tmp_path / "test.db")
    writer = SQLiteWriter(db_path, fk=fk)
    try:
        writer.setup(entity)
        writer.write_chunk(ipc_to_record_batches(ipc_dict))
        writer.finalize(entity)
    finally:
        writer.close()

    conn = sqlite3.connect(db_path)
    actual = {
        name: conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        for name in entity.table_order
    }
    conn.close()
    assert actual == expected
    assert any(expected[name] for name in entity.table_order[1:])


@pytest.mark.integration
class TestSQLiteWriter:
    @pytest.fixture()
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return extract_chunk_to_ipc(
            ChunkArgs("artists", str(artists_xml_file), 0, size)
        )

    def test_full_lifecycle(self, tmp_path, entity, ipc_dict):
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        conn = sqlite3.connect(db_path)
        # Verify tables exist
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for t in entity.table_order:
            assert t in tables

        # Verify row counts
        assert conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM artist_aliases").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM artist_groups").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM artist_members").fetchone()[0] == 2

        # Verify JSON-encoded list columns
        row = conn.execute(
            "SELECT namevariations, urls FROM artists WHERE id = 1"
        ).fetchone()
        assert json.loads(row[0]) == ["DJ T", "Test"]
        assert json.loads(row[1]) == ["https://example.com"]
        # An absent list is NULL, not "[]" (#21)
        row = conn.execute(
            "SELECT namevariations, urls FROM artists WHERE id = 2"
        ).fetchone()
        assert row == (None, None)

        # Verify FK indexes created
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "artist_aliases_artist_id_idx" in indexes
        assert "artist_groups_artist_id_idx" in indexes
        assert "artist_members_artist_id_idx" in indexes

        conn.close()

    def test_multiple_chunks_accumulate(self, tmp_path, entity):
        """Two chunks with different IDs accumulate correctly."""
        # Create two distinct XML files with different artist IDs
        xml1 = "<artist><id>1</id><name>A</name><data_quality>Correct</data_quality></artist>\n"
        xml2 = "<artist><id>3</id><name>B</name><data_quality>Correct</data_quality></artist>\n"
        f1 = tmp_path / "chunk1.xml"
        f1.write_text(xml1)
        f2 = tmp_path / "chunk2.xml"
        f2.write_text(xml2)

        ipc1 = extract_chunk_to_ipc(
            ChunkArgs("artists", str(f1), 0, os.path.getsize(f1))
        )
        ipc2 = extract_chunk_to_ipc(
            ChunkArgs("artists", str(f2), 0, os.path.getsize(f2))
        )

        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc1))
            writer.write_chunk(ipc_to_record_batches(ipc2))
            writer.finalize(entity)
        finally:
            writer.close()

        conn = sqlite3.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0] == 2
        conn.close()

    def test_empty_chunk_no_error(self, tmp_path, entity):
        """Writing a chunk with zero rows should not error."""
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(empty_ipc("artists")))
            writer.finalize(entity)
        finally:
            writer.close()

        conn = sqlite3.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0] == 0
        conn.close()

    def test_write_chunk_records_table_timings(self, tmp_path, entity, ipc_dict):
        """write_chunk returns per-table flush timing, plus ``_commit``."""
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        assert "artists" in timings
        assert "_commit" in timings
        assert all(v >= 0 for v in timings.values())

    def test_empty_batch_records_table_timings(self, tmp_path, entity):
        """Empty batches still get a timing entry."""
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            timings = writer.write_chunk(ipc_to_record_batches(empty_ipc("artists")))
            writer.finalize(entity)
        finally:
            writer.close()

        assert "artists" in timings

    def test_fk_mode(self, tmp_path, entity, ipc_dict):
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path, fk=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        conn = sqlite3.connect(db_path)
        # Verify FK constraints exist on child tables
        for t in entity.table_order[1:]:
            fk_list = conn.execute(f"PRAGMA foreign_key_list({t})").fetchall()
            assert len(fk_list) > 0, f"No FK on {t}"
            assert fk_list[0][2] == entity.table_order[0]  # references root table
        conn.close()

    def test_fk_violation_reported(self, tmp_path, entity, ipc_dict):
        """FK violations are detected and reported during finalize."""
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path, fk=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))

            # Insert a child row referencing a non-existent parent
            conn = sqlite3.connect(db_path)
            conn.execute(
                "INSERT INTO artist_aliases (artist_id, alias_id, name) "
                "VALUES (99999, 1, 'Orphan')"
            )
            conn.commit()
            conn.close()

            writer.finalize(entity)
        finally:
            writer.close()

    def test_overwrite_raises_when_tables_exist(self, tmp_path, entity, ipc_dict):
        """setup() raises OutputExistsError when tables exist and overwrite=False."""
        from discogskit.writers import OutputExistsError

        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path, overwrite=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        writer2 = SQLiteWriter(db_path)
        with pytest.raises(OutputExistsError, match="--overwrite"):
            writer2.setup(entity)
        writer2.close()

    def test_overwrite_succeeds_when_enabled(self, tmp_path, entity, ipc_dict):
        """setup() succeeds when tables exist and overwrite=True."""
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path, overwrite=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_to_record_batches(ipc_dict))
            writer.finalize(entity)
        finally:
            writer.close()

        writer2 = SQLiteWriter(db_path, overwrite=True)
        try:
            writer2.setup(entity)
            writer2.write_chunk(ipc_to_record_batches(ipc_dict))
            writer2.finalize(entity)
        finally:
            writer2.close()
