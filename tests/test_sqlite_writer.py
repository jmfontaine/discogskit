"""Integration tests for SQLiteWriter full lifecycle."""

from __future__ import annotations

import json
import os
import sqlite3

import pytest

from discogskit.entities import ChunkArgs, get
from discogskit.entities.artists import extract_chunk_to_ipc as artists_extract
from discogskit.writers.sqlite import SQLiteWriter


@pytest.mark.integration
class TestSQLiteWriter:
    @pytest.fixture()
    def entity(self):
        return get("artists")

    @pytest.fixture()
    def ipc_dict(self, artists_xml_file):
        size = os.path.getsize(artists_xml_file)
        return artists_extract(ChunkArgs(str(artists_xml_file), 0, size))

    def test_full_lifecycle(self, tmp_path, entity, ipc_dict):
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            count = writer.write_chunk(ipc_dict, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2

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

        from discogskit.entities.artists import extract_chunk_to_ipc as artists_extract

        ipc1 = artists_extract(ChunkArgs(str(f1), 0, os.path.getsize(f1)))
        ipc2 = artists_extract(ChunkArgs(str(f2), 0, os.path.getsize(f2)))

        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc1, entity)
            writer.write_chunk(ipc2, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        conn = sqlite3.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0] == 2
        conn.close()

    def test_empty_chunk_no_error(self, tmp_path, entity):
        """Writing a chunk with zero rows should not error."""
        # Create IPC dict with empty batches
        from discogskit.entities.artists import _cols_to_ipc, _new_cols

        empty_ipc = _cols_to_ipc(_new_cols())

        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            count = writer.write_chunk(empty_ipc, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 0

    def test_write_chunk_with_table_timings(self, tmp_path, entity, ipc_dict):
        """write_chunk records per-table timing when table_timings is passed."""
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(ipc_dict, entity, table_timings=timings)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 2
        assert "artists" in timings
        assert "_commit" in timings
        assert all(v >= 0 for v in timings.values())

    def test_empty_batch_with_table_timings(self, tmp_path, entity):
        """Empty batches are timed correctly when table_timings is passed."""
        from discogskit.entities.artists import _cols_to_ipc, _new_cols

        empty_ipc = _cols_to_ipc(_new_cols())

        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path)
        try:
            writer.setup(entity)
            timings: dict[str, float] = {}
            count = writer.write_chunk(empty_ipc, entity, table_timings=timings)
            writer.finalize(entity)
        finally:
            writer.close()

        assert count == 0
        # Empty tables still get timing entries
        assert "artists" in timings

    def test_fk_mode(self, tmp_path, entity, ipc_dict):
        db_path = str(tmp_path / "test.db")
        writer = SQLiteWriter(db_path, fk=True)
        try:
            writer.setup(entity)
            writer.write_chunk(ipc_dict, entity)
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
            writer.write_chunk(ipc_dict, entity)

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
            writer.write_chunk(ipc_dict, entity)
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
            writer.write_chunk(ipc_dict, entity)
            writer.finalize(entity)
        finally:
            writer.close()

        writer2 = SQLiteWriter(db_path, overwrite=True)
        try:
            writer2.setup(entity)
            writer2.write_chunk(ipc_dict, entity)
            writer2.finalize(entity)
        finally:
            writer2.close()
