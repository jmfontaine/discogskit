"""Tests for SQLite DDL generation."""

from __future__ import annotations

import sqlite3

import pyarrow as pa
import pytest

from discogskit.writers.sqlite import generate_ddl


class TestGenerateDDL:
    def test_basic_table(self):
        schema = pa.schema(
            [
                pa.field("name", pa.utf8(), nullable=False),
                pa.field("value", pa.int32(), nullable=False),
            ]
        )
        ddl = generate_ddl("test_table", schema)
        assert 'CREATE TABLE "test_table"' in ddl
        assert "name" in ddl
        assert "TEXT" in ddl
        assert "INTEGER" in ddl

    def test_pk_column(self):
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("name", pa.utf8(), nullable=False),
            ]
        )
        ddl = generate_ddl("things", schema, pk_column="id")
        assert "PRIMARY KEY" in ddl

    def test_fk_column(self):
        schema = pa.schema(
            [
                pa.field("parent_id", pa.int32(), nullable=False),
                pa.field("name", pa.utf8(), nullable=False),
            ]
        )
        ddl = generate_ddl(
            "children",
            schema,
            fk_column="parent_id",
            fk_ref_table="parents",
            pk_column="id",
        )
        assert 'REFERENCES "parents"("id")' in ddl

    def test_keyword_names_are_valid_sql(self):
        """Names that are SQL keywords, like the `join` column, still execute (#56)."""
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("join", pa.utf8()),
                pa.field("order", pa.list_(pa.utf8())),
            ]
        )
        conn = sqlite3.connect(":memory:")
        conn.execute(generate_ddl("group", schema, pk_column="id"))
        conn.execute("INSERT INTO \"group\" VALUES (1, ',', NULL)")
        assert conn.execute('SELECT "join" FROM "group"').fetchone() == (",",)

    def test_nullable_no_default(self):
        schema = pa.schema(
            [
                pa.field("maybe", pa.int32(), nullable=True),
            ]
        )
        ddl = generate_ddl("t", schema)
        assert "NOT NULL" not in ddl
        assert "DEFAULT" not in ddl

    def test_not_null_without_default(self):
        """A DEFAULT would hide a missing value, e.g. DEFAULT 0 on a foreign key (#21)."""
        schema = pa.schema(
            [
                pa.field("name", pa.utf8(), nullable=False),
                pa.field("parent_id", pa.int32(), nullable=False),
                pa.field("tags", pa.list_(pa.utf8()), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema)
        assert ddl.count("NOT NULL") == 3
        assert "DEFAULT" not in ddl

    def test_list_type_becomes_text(self):
        schema = pa.schema(
            [
                pa.field("tags", pa.list_(pa.utf8()), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema)
        assert "TEXT" in ddl

    def test_bool_becomes_integer(self):
        schema = pa.schema(
            [
                pa.field("flag", pa.bool_(), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema)
        assert "INTEGER" in ddl

    def test_unsupported_type_raises(self):
        schema = pa.schema(
            [
                pa.field("ts", pa.timestamp("us"), nullable=False),
            ]
        )
        with pytest.raises(ValueError, match="Unsupported Arrow type"):
            generate_ddl("t", schema)
