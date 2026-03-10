# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

discogskit is a CLI toolkit for converting and loading Discogs XML data dumps into Parquet, JSONL, SQLite, and PostgreSQL. Built for speed (parallel decompression, parsing, and writing) and data integrity.

## Commands

All commands use [just](https://github.com/casey/just) as the task runner and [uv](https://docs.astral.sh/uv/) for package management.

```bash
just setup              # Install deps + pre-commit hooks
just test               # All tests
just test-unit          # Unit tests only (no Docker needed)
just test-integration   # Integration tests (needs Docker for PostgreSQL)
just test-cov           # Tests with coverage
just test -- -k "test_name"  # Run a single test by name
just format             # Auto-format (ruff + pyproject-fmt)
just lint-fix           # Lint and auto-fix (ruff)
just type-check         # Type check (ty)
just verify-types       # Public API type coverage (pyright)
just qa                 # All quality checks
just dead-code          # Find unused code
just deps-unused        # Find unused/undeclared deps
```

## Architecture

### Pipeline (5 stages)

The core is a multi-process pipeline in `src/discogskit/pipeline.py`:

1. **Decompress** — `rapidgzip` parallel decompression of .xml.gz
2. **Split** — Memory-mapped scanning for XML element boundaries, producing byte-range chunks (`entities/_split.py`)
3. **Parse** — `multiprocessing.Pool` workers parse chunks with lxml, emit Arrow IPC buffers
4. **Write** — Single writer thread deserializes IPC and writes to target format via bounded queue (backpressure)
5. **Index/Cleanup** — Parallel index creation for DB targets, optional XML cleanup

Multiprocessing is used because lxml is CPU-bound and holds the GIL. Arrow IPC is the inter-process format (no pickling overhead).

### Key modules

- **`entities/`** — One module per Discogs entity (artists, labels, masters, releases). Each defines table schemas, XML→Arrow parsing, and DDL for SQL targets. Releases is the most complex (12 normalized tables).
- **`writers/`** — Output format implementations (parquet, jsonl, sqlite, postgresql). Factory in `__init__.py` selects writer by format string.
- **`cli.py`** — Typer CLI with `convert` and `load` commands.
- **`decompress.py`** — Gzip decompression wrapper using rapidgzip.

### Entity pattern

Each entity module (e.g., `artists.py`) follows the same structure:
- Dataclass with table schemas as `dict[str, pa.Schema]` for Arrow
- `parse(chunk_bytes) -> list[pa.RecordBatch]` for XML parsing
- `ddl_sqlite()` / `ddl_postgresql()` for SQL table/index creation
- Registered in `entities/__init__.py`

## Testing

- Unit tests run without external services. Integration tests need Docker (PostgreSQL via testcontainers).
- Test markers: `unit` (default), `integration`.
- Fixtures with XML snippets in `tests/conftest.py`.

## Quality gates

A Stop hook runs `just format && just lint-fix && just type-check && just test-unit` automatically. Pre-commit hooks run the same checks plus dead-code detection and deptry.

## Design principles

- **Fail early, fail hard** — Discogs data can be inconsistent. Never silently ingest bad data. Validation errors surface at parse time. When in doubt, reject with a helpful error.
- **Explicit over implicit** — No magic auto-discovery. Users specify exactly what to process.
- **Profile before optimizing** — Don't optimize on speculation. Measure first.

## Conventions

- Python 3.10+ (developed on 3.13)
- Type annotations throughout; `py.typed` marker for PEP 561
- `ty` is the primary type checker; `pyright` is used only for `--verifytypes`
- `rapidgzip` has no type stubs — it's in `allowed-unresolved-imports`
- **Alphabetical ordering** for: class attributes, CLI options, database columns, dict keys, enum members, function definitions in a class, imports, just recipes, keyword arguments. Exception: `id` always comes first in DB tables.
- **No `# type: ignore`** — Use `cast()`, `TypeGuard`, or `Protocol` instead.
- **Comment tags**: `TODO` (planned work), `FIXME` (known bug), `HACK` (temporary workaround), `KLUDGE` (intentional deviation with documented reason — not temporary, include why and a reference).
