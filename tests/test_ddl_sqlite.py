"""Tests for SQLite DDL generation."""

from __future__ import annotations

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
        assert "CREATE TABLE test_table" in ddl
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
        # PK column should not have DEFAULT
        lines = ddl.split("\n")
        id_line = [line for line in lines if "id" in line and "PRIMARY KEY" in line][0]
        assert "DEFAULT" not in id_line

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
        assert "REFERENCES parents(id)" in ddl

    def test_nullable_no_default(self):
        schema = pa.schema(
            [
                pa.field("maybe", pa.int32(), nullable=True),
            ]
        )
        ddl = generate_ddl("t", schema)
        assert "NOT NULL" not in ddl
        assert "DEFAULT" not in ddl

    def test_not_null_with_default(self):
        schema = pa.schema(
            [
                pa.field("name", pa.utf8(), nullable=False),
                pa.field("count", pa.int32(), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema)
        assert "NOT NULL" in ddl
        assert "DEFAULT ''" in ddl
        assert "DEFAULT 0" in ddl

    def test_list_type_becomes_text(self):
        schema = pa.schema(
            [
                pa.field("tags", pa.list_(pa.utf8()), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema)
        assert "TEXT" in ddl
        assert "DEFAULT '[]'" in ddl

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
