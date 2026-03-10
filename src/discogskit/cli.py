"""CLI application."""

from __future__ import annotations

import os
from importlib.metadata import version
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from discogskit import pipeline
from discogskit._console import console, status
from discogskit.decompress import DecompressError
from discogskit.entities import detect_entity, get as get_entity
from discogskit.writers import OutputExistsError, get_writer

CPUS = os.cpu_count() or 1

app: typer.Typer = typer.Typer(no_args_is_help=True)


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"discogskit {version('discogskit')}")
        raise typer.Exit()


@app.callback()
def _callback(
    version: Annotated[  # noqa: ARG001
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show version and exit.",
        ),
    ] = None,
) -> None:
    """discogskit: Discogs Data Dumps Toolkit"""


def _resolve_jobs(paths: list[Path]) -> list[tuple[Path, str]]:
    """Resolve paths to a list of (gz_path, entity_name) pairs."""
    jobs: list[tuple[Path, str]] = []
    for p in paths:
        try:
            if p.is_dir():
                gz_files = sorted(p.glob("*.xml.gz"))
                if not gz_files:
                    console.print(f"[red]Error:[/] no .xml.gz files found in {p}")
                    raise typer.Exit(1) from None
                for gz in gz_files:
                    jobs.append((gz, detect_entity(gz.name)))
            else:
                if not p.exists():
                    console.print(f"[red]Error:[/] file not found: {p}")
                    raise typer.Exit(1) from None
                if p.name.endswith((".xml.gz", ".xml")):
                    jobs.append((p, detect_entity(p.name)))
        except ValueError as exc:
            console.print(f"[red]Error:[/] {exc}")
            raise typer.Exit(1) from None
    return jobs


def _print_result(
    result: pipeline.PipelineResult,
    entity_name: str,
    n_tables: int,
    entity_def,
    *,
    verb: str = "loaded",
    target: str = "tables",
    verbose: bool = False,
) -> None:
    """Print the summary and optional profile for a completed entity."""
    rate = (
        f"{result.total_records / result.t_parse_load:,.0f} rec/s"
        if result.t_parse_load > 0
        else ""
    )
    console.print(
        f"  [green]✓[/] {result.total_records:,} {entity_name} {verb} "
        f"[dim][{result.t_total:.2f}s][/]"
    )

    if verbose:
        console.print()
        status("Decompress", f"{result.t_decompress:.2f}s")
        status(
            "Load",
            f"{result.t_parse_load:.2f}s  {rate}"
            if result.t_parse_load > 0
            else "0.00s",
        )
        status("Finalize", f"{result.t_indexes:.2f}s")
        status("Total", f"[bold]{result.t_total:.2f}s[/]")

    if result.profile_data:
        pd = result.profile_data
        console.print()
        console.rule("[bold]Profile[/]", style="dim")
        status("Put blocked", f"{pd['put_blocked']:.2f}s")
        status("Get wait", f"{pd['get_wait']:.2f}s")
        console.print()

        table_timings: dict[str, float] = pd.get("table_timings", {})  # type: ignore[assignment]
        flush_total = sum(table_timings.values())

        tbl = Table(
            title=f"Per-table flush ({flush_total:.2f}s)",
            show_edge=False,
            title_style="bold",
        )
        tbl.add_column("Table", style="cyan")
        tbl.add_column("Time", justify="right")
        tbl.add_column("%", justify="right", style="dim")
        for key in entity_def.table_order + ["_commit"]:
            t = table_timings.get(key, 0.0)
            pct = t / flush_total * 100 if flush_total else 0
            label = "commit" if key == "_commit" else key
            tbl.add_row(label, f"{t:.2f}s", f"{pct:.1f}%")
        console.print(tbl)


@app.command()
def convert(
    paths: Annotated[
        list[Path],
        typer.Argument(help="One or more .xml.gz files or directories containing them"),
    ],
    # Output
    format: Annotated[
        str,
        typer.Option("-f", "--format", help="Output format: parquet or jsonl"),
    ] = "parquet",
    output: Annotated[
        Path,
        typer.Option(help="Output directory"),
    ] = Path("."),
    compression: Annotated[
        str,
        typer.Option(
            help="Compression codec. Parquet: gzip, snappy, zstd (default), none. JSONL: bzip2, gzip, none (default)."
        ),
    ] = "",
    # Performance tuning
    parse_workers: Annotated[
        int,
        typer.Option(help="Number of parallel parse workers"),
    ] = max(1, CPUS // 2),
    chunk_mb: Annotated[
        int,
        typer.Option(help="Split XML into chunks of roughly this size (MB)"),
    ] = 256,
    write_queue: Annotated[
        int,
        typer.Option(help="Max chunks buffered in memory before writes must catch up"),
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
        typer.Option(help="Warn about unhandled XML elements during parsing"),
    ] = False,
) -> None:
    """Convert Discogs XML dumps into flat files (Parquet or JSONL)."""
    from discogskit.writers.jsonl import JSONLWriter
    from discogskit.writers.parquet import ParquetWriter

    jobs = _resolve_jobs(paths)
    if not jobs:
        console.print("[red]Error:[/] No files to convert.")
        raise typer.Exit(1)

    fmt = format.lower()
    _VALID_CODECS = {
        "parquet": {"zstd", "snappy", "gzip", "none"},
        "jsonl": {"gzip", "bzip2", "none"},
    }
    if fmt not in _VALID_CODECS:
        console.print(
            f"[red]Error:[/] unsupported format '{format}'. Use 'parquet' or 'jsonl'."
        )
        raise typer.Exit(1)

    # Apply per-format defaults when not specified
    if not compression:
        compression = "zstd" if fmt == "parquet" else "none"

    if compression not in _VALID_CODECS[fmt]:
        valid = ", ".join(sorted(_VALID_CODECS[fmt] - {"none"}) + ["none"])
        console.print(
            f"[red]Error:[/] unsupported compression '{compression}' for {fmt}. Valid: {valid}."
        )
        raise typer.Exit(1)

    if fmt == "parquet":
        writer = ParquetWriter(
            str(output), compression=compression, overwrite=overwrite
        )
    else:
        writer = JSONLWriter(str(output), compression=compression, overwrite=overwrite)

    verbose = not progress or profile
    try:
        for gz_path, entity_name in jobs:
            console.print()
            console.print(
                f"[bold green]{entity_name.capitalize()}[/]  [dim]{gz_path.name}[/]"
            )

            config = pipeline.PipelineConfig(
                gz_path=gz_path,
                entity=entity_name,
                parse_workers=parse_workers,
                chunk_mb=chunk_mb,
                write_queue=write_queue,
                profile=profile,
                progress=progress,
                strict=strict,
                keep_xml=keep_xml,
            )
            result = pipeline.run(config, writer)

            entity_def = get_entity(entity_name)
            n_tables = len(entity_def.table_order)
            _print_result(
                result,
                entity_name,
                n_tables,
                entity_def,
                verb="converted",
                target=f"{fmt} files",
                verbose=verbose,
            )
    except (DecompressError, OutputExistsError) as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    except KeyboardInterrupt:
        console.print(
            "\n  [yellow]Interrupted — cleaning up (Ctrl-C again to force quit) …[/]"
        )
        raise typer.Exit(130) from None
    except Exception as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    finally:
        writer.close()


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
    parse_workers: Annotated[
        int,
        typer.Option(help="Number of parallel parse workers"),
    ] = max(1, CPUS // 2),
    write_workers: Annotated[
        int,
        typer.Option(help="Number of parallel database write workers"),
    ] = 1,
    index_workers: Annotated[
        int,
        typer.Option(help="Number of parallel index creation workers"),
    ] = 2,
    chunk_mb: Annotated[
        int,
        typer.Option(help="Split XML into chunks of roughly this size (MB)"),
    ] = 256,
    write_queue: Annotated[
        int,
        typer.Option(help="Max chunks buffered in memory before writes must catch up"),
    ] = 2,
    # Behavior
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
        typer.Option(help="Warn about unhandled XML elements during parsing"),
    ] = False,
    # PostgreSQL options
    pg_unlogged: Annotated[
        bool,
        typer.Option(
            help="Skip WAL for faster writes (tables stay unlogged; data lost on crash)",
            rich_help_panel="PostgreSQL",
        ),
    ] = False,
    pg_tune: Annotated[
        bool,
        typer.Option(
            help="Temporarily apply settings optimized for bulk loading",
            rich_help_panel="PostgreSQL",
        ),
    ] = False,
    pg_fk: Annotated[
        bool,
        typer.Option(
            help="Add foreign key constraints after load", rich_help_panel="PostgreSQL"
        ),
    ] = False,
) -> None:
    """Load Discogs XML dumps into a database."""
    jobs = _resolve_jobs(paths)
    if not jobs:
        console.print("[red]Error:[/] No files to load.")
        raise typer.Exit(1)

    try:
        writer = get_writer(
            dsn,
            overwrite=overwrite,
            unlogged=pg_unlogged,
            tune=pg_tune,
            write_workers=write_workers,
            index_workers=index_workers,
            fk=pg_fk,
        )
    except Exception as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    verbose = not progress or profile
    try:
        for gz_path, entity_name in jobs:
            console.print()
            console.print(
                f"[bold green]{entity_name.capitalize()}[/]  [dim]{gz_path.name}[/]"
            )

            config = pipeline.PipelineConfig(
                gz_path=gz_path,
                entity=entity_name,
                parse_workers=parse_workers,
                chunk_mb=chunk_mb,
                write_queue=write_queue,
                profile=profile,
                progress=progress,
                strict=strict,
                keep_xml=keep_xml,
            )
            result = pipeline.run(config, writer)

            entity_def = get_entity(entity_name)
            n_tables = len(entity_def.table_order)
            _print_result(result, entity_name, n_tables, entity_def, verbose=verbose)
    except (DecompressError, OutputExistsError) as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    except KeyboardInterrupt:
        console.print(
            "\n  [yellow]Interrupted — cleaning up (Ctrl-C again to force quit) …[/]"
        )
        raise typer.Exit(130) from None
    except Exception as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    finally:
        writer.close()
