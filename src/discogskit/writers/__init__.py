"""Writer protocol and registry for multi-target output."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

    from discogskit.entities import EntityDef

# Table name -> seconds spent flushing it. "_commit" holds any shared commit cost.
TableTimings = dict[str, float]


class OutputExistsError(Exception):
    """Raised when output already exists and overwrite is not enabled."""


class Writer(Protocol):
    def setup(self, entity: EntityDef) -> None:
        """Prepare destination (create tables / create output dir)."""
        ...

    def write_chunk(self, tables: dict[str, pa.RecordBatch]) -> TableTimings:
        """Write one already-deserialized chunk, one RecordBatch per table.

        ``tables`` iterates in ``entity.table_order`` (root table first); writers that enforce foreign keys
        while loading (e.g. SQLite with ``fk=True``) depend on the parent table being written before its
        child tables.

        Returns per-table flush time in seconds for ``--profile``.
        """
        ...

    def finalize(self, entity: EntityDef) -> None:
        """Post-load work (build indexes for DB, close files, etc.)."""
        ...

    def close(self) -> None:
        """Release resources. Must be safe to call even after errors."""
        ...


def get_writer(dsn: str, **options: Any) -> Writer:
    """Auto-detect and construct a writer from a DSN string.

    Only database targets (PostgreSQL, SQLite) are supported via this factory.
    File-format writers (Parquet, JSONL) should be constructed directly.
    """
    if dsn.startswith("postgresql://"):
        from discogskit.writers.postgresql import PostgreSQLWriter

        return PostgreSQLWriter(dsn, **options)

    if dsn.startswith("sqlite:///") or dsn.endswith((".db", ".sqlite", ".sqlite3")):
        from discogskit.writers.sqlite import SQLiteWriter

        path = dsn.removeprefix("sqlite:///") if dsn.startswith("sqlite:///") else dsn
        return SQLiteWriter(
            path, fk=options.get("fk", False), overwrite=options.get("overwrite", False)
        )

    raise ValueError(
        f"Unsupported database DSN: {dsn!r}. "
        f"Expected postgresql://... or a SQLite path (.db/.sqlite/.sqlite3)."
    )
