"""CLI application."""

from __future__ import annotations

import os
from enum import Enum
from importlib.metadata import version
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table
from rich.text import Text

from discogskit import _skills, pipeline
from discogskit._console import current, error, print_text, status
from discogskit.entities import ENTITIES, detect_entity
from discogskit.writers import Writer, get_file_writer, get_writer


class OutputFormat(str, Enum):
    """Output file format for `convert`."""

    JSONL = "jsonl"
    PARQUET = "parquet"


CPUS = os.cpu_count() or 1

app: typer.Typer = typer.Typer(no_args_is_help=True)


def _version_callback(value: bool) -> None:
    if value:
        current().print(f"discogskit {version('discogskit')}")
        raise typer.Exit()


@app.callback()
def _callback(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            help="Show version and exit.",
            is_eager=True,
        ),
    ] = None,
) -> None:
    """discogskit: Discogs Data Dumps Toolkit"""


def _resolve_jobs(paths: list[Path]) -> list[tuple[Path, str]]:
    """Resolve paths to a list of (gz_path, entity_name) pairs.

    Fails on the first entity with two inputs, or file given twice, before any work: a second job for an entity
    either fails on the first job's output after it has run, or (with --overwrite) silently replaces it.
    """
    jobs: list[tuple[Path, str]] = []
    for p in paths:
        try:
            if p.is_dir():
                gz_files = sorted(p.glob("*.xml.gz"))
                if not gz_files:
                    error(f"no .xml.gz files found in {p}")
                    raise typer.Exit(1) from None
                for gz in gz_files:
                    jobs.append((gz, detect_entity(gz.name)))
            else:
                if not p.exists():
                    error(f"file not found: {p}")
                    raise typer.Exit(1) from None
                if p.name.endswith(".xml"):
                    error(
                        f"{p}: uncompressed .xml input is not supported;"
                        " pass the original .xml.gz dump"
                    )
                    raise typer.Exit(1) from None
                if not p.name.endswith(".xml.gz"):
                    error(f"{p}: not a .xml.gz dump")
                    raise typer.Exit(1) from None
                jobs.append((p, detect_entity(p.name)))
        except ValueError as exc:
            error(str(exc))
            raise typer.Exit(1) from None

    by_entity: dict[str, Path] = {}
    by_file: dict[Path, Path] = {}
    for gz, entity_name in jobs:
        if entity_name in by_entity:
            error(f"two inputs for {entity_name}: {by_entity[entity_name]} and {gz}")
            raise typer.Exit(1)
        # A file under another entity's name (e.g. a symlink) is still the same dump.
        resolved = gz.resolve()
        if resolved in by_file:
            error(f"same file given twice: {by_file[resolved]} and {gz}")
            raise typer.Exit(1)
        by_entity[entity_name] = gz
        by_file[resolved] = gz
    return jobs


def _print_result(
    result: pipeline.PipelineResult,
    entity_name: str,
    entity_def,
    *,
    verb: str,
    verbose: bool = False,
) -> None:
    """Print the summary and optional profile for a completed entity."""
    rate = (
        f"{result.total_records / result.t_parse_load:,.0f} rec/s"
        if result.t_parse_load > 0
        else ""
    )
    current().print(
        f"  [green]✓[/] {result.total_records:,} {entity_name} {verb} "
        f"[dim][{result.t_total:.2f}s][/]"
    )

    if verbose:
        current().print()
        status("Decompress", f"{result.t_decompress:.2f}s")
        status(
            "Load",
            f"{result.t_parse_load:.2f}s  {rate}"
            if result.t_parse_load > 0
            else "0.00s",
        )
        status("Finalize", f"{result.t_indexes:.2f}s")
        status("Total", f"{result.t_total:.2f}s", style="bold")

    if result.profile_data:
        pd = result.profile_data
        current().print()
        current().rule("[bold]Profile[/]", style="dim")
        status("Put blocked", f"{pd.put_blocked:.2f}s")
        status("Get wait", f"{pd.get_wait:.2f}s")
        current().print()

        table_timings = pd.table_timings
        flush_total = sum(table_timings.values())

        tbl = Table(
            show_edge=False,
            title=f"Per-table flush ({flush_total:.2f}s)",
            title_style="bold",
        )
        tbl.add_column("Table", style="cyan")
        tbl.add_column("Time", justify="right")
        tbl.add_column("%", justify="right", style="dim")
        # File writers have no commit step and record no "_commit" timing; only show it when present.
        keys = entity_def.table_order + (
            ["_commit"] if "_commit" in table_timings else []
        )
        for key in keys:
            t = table_timings.get(key, 0.0)
            pct = t / flush_total * 100 if flush_total else 0
            label = "commit" if key == "_commit" else key
            tbl.add_row(label, f"{t:.2f}s", f"{pct:.1f}%")
        current().print(tbl)


def _run_jobs(
    jobs: list[tuple[Path, str]],
    writer: Writer,
    *,
    chunk_mb: int,
    keep_xml: bool,
    parse_workers: int,
    profile: bool,
    progress: bool,
    strict: bool,
    verb: str,
    write_queue: int,
) -> None:
    """Run the pipeline for each job, printing progress and handling errors.

    Checks every job's output first, so existing output for a later job fails
    the run before an earlier one does hours of work. Builds a
    ``PipelineConfig`` per job (entity/gz_path vary, the rest is shared across
    the whole run), calls ``pipeline.run``, and prints the result. Closes
    ``writer`` once the whole run is done, whether it succeeded, failed, or was
    interrupted.
    """
    # The pipeline shows a progress bar only on a terminal; elsewhere it prints
    # per-chunk lines, as with --no-progress.
    verbose = not (progress and current().is_terminal) or profile
    try:
        for _, entity_name in jobs:
            writer.check(ENTITIES[entity_name])
        for gz_path, entity_name in jobs:
            current().print()
            print_text(
                Text.assemble(
                    (entity_name.capitalize(), "bold green"),
                    "  ",
                    (gz_path.name, "dim"),
                )
            )
            # Without a progress bar (--no-progress, --profile, or output that
            # isn't a terminal), echo the run's settings.
            if verbose:
                status("Workers", str(parse_workers))
                status("Chunk size", f"{chunk_mb} MiB")
                status("Write queue", str(write_queue))
                status("Source", str(gz_path))

            config = pipeline.PipelineConfig(
                chunk_mb=chunk_mb,
                entity=entity_name,
                gz_path=gz_path,
                keep_xml=keep_xml,
                parse_workers=parse_workers,
                profile=profile,
                progress=progress,
                strict=strict,
                write_queue=write_queue,
            )
            result = pipeline.run(config, writer)

            entity_def = ENTITIES[entity_name]
            _print_result(
                result,
                entity_name,
                entity_def,
                verb=verb,
                verbose=verbose,
            )
    except KeyboardInterrupt:
        current().print("\n  [yellow]Interrupted — cleaning up …[/]")
        raise typer.Exit(130) from None
    # Top-level error boundary: any failure becomes a one-line error and exit 1,
    # named by its type when it has no message (e.g. MemoryError).
    except Exception as exc:  # noqa: BLE001
        error(str(exc) or type(exc).__name__)
        raise typer.Exit(1) from None
    finally:
        writer.close()


@app.command()
def convert(
    paths: Annotated[
        list[Path],
        typer.Argument(help="One or more .xml.gz files or directories containing them"),
    ],
    # Output
    # A string, not a Choice: an unknown or wrongly cased codec then fails with the writers' own "unsupported
    # compression" error (exit 1), like a codec the format can't use, rather than a usage error (exit 2).
    compression: Annotated[
        str | None,
        typer.Option(
            help="Compression codec. Parquet: brotli, gzip, snappy, zstd (default), none. JSONL: bzip2, gzip, none (default).",
            metavar="<codec>",
        ),
    ] = None,
    compression_level: Annotated[
        int | None,
        typer.Option(
            help=(
                "Compression level for codecs that support one: JSONL gzip/bzip2, Parquet brotli/gzip/zstd. "
                "Rejected for snappy and none. Default: JSONL gzip uses level 6 (faster, slightly "
                "larger output than 9); every other codec uses its own library default."
            )
        ),
    ] = None,
    output_format: Annotated[
        OutputFormat,
        typer.Option("-f", "--format", case_sensitive=False, help="Output format"),
    ] = OutputFormat.PARQUET,
    output: Annotated[
        Path,
        typer.Option(help="Output directory"),
    ] = Path("."),
    # Performance tuning
    chunk_mb: Annotated[
        int,
        typer.Option(help="Split XML into chunks of roughly this size (MiB)", min=1),
    ] = 256,
    parse_workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel parse workers",
            min=1,
            show_default="half the CPU count, at least 1",
        ),
    ] = max(1, CPUS // 2),
    write_queue: Annotated[
        int,
        typer.Option(
            help="Chunks parsing may run ahead of writing: at most --parse-workers plus this many chunks are parsing or waiting to be written",
            min=1,
        ),
    ] = 2,
    # Behavior
    keep_xml: Annotated[
        bool,
        typer.Option(help="Keep decompressed XML file after converting"),
    ] = False,
    overwrite: Annotated[
        bool,
        typer.Option(help="Overwrite existing output files"),
    ] = False,
    profile: Annotated[
        bool,
        typer.Option(help="Print detailed per-table timing breakdown after convert"),
    ] = False,
    progress: Annotated[
        bool,
        typer.Option(help="Show a progress bar instead of per-chunk output"),
    ] = True,
    strict: Annotated[
        bool,
        typer.Option(
            help="Fail on unhandled XML elements, attributes and text, after listing them all"
        ),
    ] = False,
) -> None:
    """Convert Discogs XML dumps into flat files (Parquet or JSONL)."""
    jobs = _resolve_jobs(paths)

    try:
        writer = get_file_writer(
            output,
            output_format.value,
            compression=compression,
            compression_level=compression_level,
            overwrite=overwrite,
        )
    # An unsupported codec or level for the format, rejected before any file exists.
    except ValueError as exc:
        error(str(exc))
        raise typer.Exit(1) from None

    _run_jobs(
        jobs,
        writer,
        chunk_mb=chunk_mb,
        keep_xml=keep_xml,
        parse_workers=parse_workers,
        profile=profile,
        progress=progress,
        strict=strict,
        verb="converted",
        write_queue=write_queue,
    )


@app.command()
def load(
    paths: Annotated[
        list[Path],
        typer.Argument(help="One or more .xml.gz files or directories containing them"),
    ],
    # Connection
    dsn: Annotated[
        str,
        typer.Option(
            envvar="DATABASE_URL",
            help="Database DSN (e.g., postgresql://localhost/postgres) or path to SQLite file",
        ),
    ] = "postgresql://localhost/discogskit",
    # Performance tuning
    chunk_mb: Annotated[
        int,
        typer.Option(help="Split XML into chunks of roughly this size (MiB)", min=1),
    ] = 256,
    parse_workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel parse workers",
            min=1,
            show_default="half the CPU count, at least 1",
        ),
    ] = max(1, CPUS // 2),
    write_queue: Annotated[
        int,
        typer.Option(
            help="Chunks parsing may run ahead of writing: at most --parse-workers plus this many chunks are parsing or waiting to be written",
            min=1,
        ),
    ] = 2,
    # Behavior
    fk: Annotated[
        bool,
        typer.Option(help="Enforce foreign key constraints (SQLite and PostgreSQL)"),
    ] = False,
    keep_xml: Annotated[
        bool,
        typer.Option(help="Keep decompressed XML file after loading"),
    ] = False,
    overwrite: Annotated[
        bool,
        typer.Option(help="Overwrite existing tables in the database"),
    ] = False,
    profile: Annotated[
        bool,
        typer.Option(help="Print detailed per-table timing breakdown after load"),
    ] = False,
    progress: Annotated[
        bool,
        typer.Option(help="Show a progress bar instead of per-chunk output"),
    ] = True,
    strict: Annotated[
        bool,
        typer.Option(
            help="Fail on unhandled XML elements, attributes and text, after listing them all"
        ),
    ] = False,
    # PostgreSQL options
    pg_create_schema: Annotated[
        bool,
        typer.Option(
            help="Create --pg-schema if it doesn't exist",
            rich_help_panel="PostgreSQL",
        ),
    ] = False,
    pg_index_workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel index creation workers",
            min=1,
            rich_help_panel="PostgreSQL",
        ),
    ] = 2,
    pg_schema: Annotated[
        str | None,
        typer.Option(
            help="Schema to create tables in (must exist unless --pg-create-schema)",
            rich_help_panel="PostgreSQL",
        ),
    ] = None,
    pg_unlogged: Annotated[
        bool,
        typer.Option(
            help="Skip WAL for faster writes (tables stay unlogged; data lost on crash)",
            rich_help_panel="PostgreSQL",
        ),
    ] = False,
    pg_write_workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel database write workers. Above 1, each table group commits separately, so a failed load can leave part of a chunk in some tables: re-run the whole load with --overwrite",
            min=1,
            rich_help_panel="PostgreSQL",
        ),
    ] = 1,
) -> None:
    """Load Discogs XML dumps into a database."""
    jobs = _resolve_jobs(paths)

    if pg_create_schema and not pg_schema:
        error("--pg-create-schema requires --pg-schema.")
        raise typer.Exit(1)

    try:
        writer = get_writer(
            dsn,
            create_schema=pg_create_schema,
            fk=fk,
            index_workers=pg_index_workers,
            overwrite=overwrite,
            schema=pg_schema,
            unlogged=pg_unlogged,
            write_workers=pg_write_workers,
        )
    # Writer construction can fail many ways (bad DSN, driver, connection).
    except Exception as exc:  # noqa: BLE001
        error(str(exc) or type(exc).__name__)
        raise typer.Exit(1) from None
    _run_jobs(
        jobs,
        writer,
        chunk_mb=chunk_mb,
        keep_xml=keep_xml,
        parse_workers=parse_workers,
        profile=profile,
        progress=progress,
        strict=strict,
        verb="loaded",
        write_queue=write_queue,
    )


app.add_typer(_skills.app, name="skills")
