"""Writer protocol and registry for multi-target output."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from discogskit.entities import EntityDef


class OutputExistsError(Exception):
    """Raised when output already exists and overwrite is not enabled."""


class Writer(Protocol):
    def setup(self, entity: EntityDef) -> None:
        """Prepare destination (create tables / create output dir)."""
        ...

    def write_chunk(
        self,
        ipc_dict: dict[str, bytes],
        entity: EntityDef,
        table_timings: dict[str, float] | None = None,
    ) -> int:
        """Write one chunk of IPC data. Returns root entity row count."""
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
