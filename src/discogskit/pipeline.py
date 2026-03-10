"""Pipeline orchestration: decompress, split, parse, load, index.

Architecture
============

The pipeline has 5 stages.  Stages 1-3 overlap via a producer/consumer queue::

    .xml.gz file
        |  [Stage 1: Decompress]  rapidgzip (parallel)
        v
    .xml file on disk
        |  [Stage 2: Split]  mmap scan for element boundaries
        v
    N byte-range chunks
        |
        +---> [Stage 3a: Parse workers]  multiprocessing.Pool
        |         Each worker reads its byte range, wraps in an XML
        |         envelope, parses with lxml iterparse, and produces
        |         Arrow IPC buffers (one per normalized table).
        |              |
        |              v  queue.Queue (bounded, backpressure)
        |              |
        +---> [Stage 3b: Writer thread]
                  Deserializes IPC, calls writer.write_chunk() which
                  uses ADBC's COPY protocol under the hood.
                       |
                       v
                  Database tables (bare, no indexes)
                       |
                       v
                  [Stage 4: Indexes]  PK + FK-column indexes (parallel)
                       |
                       v
                  [Stage 5: Cleanup]  delete XML unless --keep-xml

Key design decisions
--------------------
- **Multiprocessing for parse**: lxml is CPU-bound and holds the GIL during
  parsing.  Threads would serialize. Workers communicate results via Arrow
  IPC byte buffers which cross process boundaries efficiently.
- **Arrow IPC as inter-process format**: compact (columnar, no copies on
  deserialization), avoids pickling overhead. Buffers are ~150 MB per chunk;
  the bounded queue (default depth 2) caps memory at ~300 MB.
- **Writer thread**: decouples parsing from database writes.  Without it,
  the main process would sequentially consume an IPC dict and flush it,
  leaving parse workers idle during flushes. The bounded queue provides
  backpressure: if the writer falls behind, ``put()`` blocks and parse
  workers naturally pause.
"""

from __future__ import annotations

import queue
import signal
import threading
import time
from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
from typing import TYPE_CHECKING

from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TextColumn,
)
from rich.text import Text

from discogskit import decompress
from discogskit._console import console, status
from discogskit.entities import ChunkArgs, EntityDef, get as get_entity
from discogskit.writers import Writer


class _ElapsedEstTotalColumn(ProgressColumn):
    """Shows ``elapsed / ~estimated_total``."""

    def render(self, task: Task) -> Text:
        elapsed = task.elapsed or 0.0
        elapsed_str = _fmt_time(elapsed)
        if (
            task.total and task.completed and task.completed < task.total
        ):  # pragma: no cover
            est_total = elapsed * task.total / task.completed
            return Text(f"{elapsed_str}/~{_fmt_time(est_total)}", style="cyan")
        return Text(elapsed_str, style="cyan")


def _fmt_time(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    if m:
        return f"{m}:{s:02d}"
    return f"{s}s"


if TYPE_CHECKING:
    from rich.progress import TaskID


class _ProgressBar:
    """Thin wrapper around ``rich.progress.Progress`` for the writer thread.

    The bar advances per chunk (the unit of pipeline work) but displays
    throughput, which is more meaningful to users than chunk counts.
    The rich Progress object is thread-safe.
    """

    def __init__(self, progress: Progress, task_id: TaskID, total: int) -> None:
        self._progress = progress
        self._task_id = task_id
        self._total = total
        self._total_records = 0
        self._started = False

    def update(self, chunk_records: int, avg_rate: float) -> None:
        if not self._started:
            # Switch from indeterminate pulse to determinate bar
            self._progress.update(self._task_id, total=self._total)
            self._started = True
        self._total_records += chunk_records
        self._progress.update(
            self._task_id,
            advance=1,
            description=f"[cyan]{avg_rate:,.0f} rec/s",
        )

    @property
    def total_records(self) -> int:  # pragma: no cover
        return self._total_records


@dataclass
class PipelineConfig:
    chunk_mb: int
    entity: str
    gz_path: Path
    keep_xml: bool
    parse_workers: int
    profile: bool
    progress: bool
    strict: bool
    write_queue: int


@dataclass
class PipelineResult:
    profile_data: dict[str, object] | None
    t_decompress: float
    t_indexes: float
    t_parse_load: float
    t_total: float
    total_records: int


def _writer_thread_fn(
    writer: Writer,
    write_q: queue.Queue,
    n_chunks: int,
    result: dict,
    entity: EntityDef,
    *,
    profile: bool = False,
    progress_bar: _ProgressBar | None = None,
) -> None:
    """Writer thread: pull IPC dicts from queue, write via the writer."""
    try:
        total = 0
        t_start = time.perf_counter()
        get_wait = 0.0
        table_timings = {} if profile else None

        while True:
            t_get = time.perf_counter()
            ipc_dict = write_q.get()
            if ipc_dict is None:
                break
            if profile:
                get_wait += time.perf_counter() - t_get
            t_chunk = time.perf_counter()
            chunk_count = writer.write_chunk(
                ipc_dict, entity, table_timings if profile else None
            )
            chunk_elapsed = time.perf_counter() - t_chunk
            total += chunk_count
            elapsed = time.perf_counter() - t_start
            result["chunks_done"] = result.get("chunks_done", 0) + 1

            if progress_bar is not None:
                avg_rate = total / elapsed if elapsed > 0 else 0
                progress_bar.update(chunk_count, avg_rate)
            else:
                print(
                    f"  chunk {result['chunks_done']}/{n_chunks}: "
                    f"{chunk_count:,} {entity.name} ({total:,} total) "
                    f"[{chunk_count / chunk_elapsed:,.0f} rec/s chunk, "
                    f"{total / elapsed:,.0f} rec/s avg]"
                )

        result["total"] = total
        if profile:
            assert table_timings is not None
            # For multi-writer, timings accumulate inside the writer;
            # merge them into table_timings so both paths produce the same output.
            get_timings = getattr(writer, "get_table_timings", None)
            if get_timings is not None:  # pragma: no cover
                for k, v in get_timings().items():
                    table_timings[k] = table_timings.get(k, 0.0) + v
            result["table_timings"] = table_timings
            result["get_wait"] = get_wait
    except Exception as exc:
        result["error"] = exc


def run(config: PipelineConfig, writer: Writer) -> PipelineResult:
    """Execute the full ingest pipeline."""
    entity = get_entity(config.entity)

    gz_path = config.gz_path
    xml_path = gz_path.with_suffix("")
    parse_workers = config.parse_workers
    chunk_bytes = config.chunk_mb * 1024 * 1024

    # Profiling requires verbose per-chunk output, so disable progress bar
    use_progress = config.progress and not config.profile

    if not use_progress:
        status("Workers", str(parse_workers))
        status("Chunk size", f"{config.chunk_mb} MB")
        status("Write queue", str(config.write_queue))
        status("Source", str(gz_path))

    # Stage 1: Decompress
    t0 = time.perf_counter()
    decompress.ensure_xml(gz_path, xml_path, parse_workers)
    t_decompress = time.perf_counter() - t0

    # Stage 2: Create bare tables (no PK, no FK, no indexes).
    # Indexes are built AFTER bulk load (Stage 4) — inserting into indexed
    # tables triggers per-row index maintenance which is dramatically slower.
    writer.setup(entity)

    # Stage 3: Parallel parse + write
    #
    # Main thread: drains pool.imap_unordered → puts IPC dicts into queue.
    # Writer thread: pulls IPC dicts from queue → flushes to the database.
    #
    # The bounded queue couples them with backpressure: if the writer falls
    # behind, put() blocks, which stops the main thread from consuming pool
    # results, which stops workers from starting new chunks. This naturally
    # limits memory to ~write_queue × ~150 MB of IPC data.
    t1 = time.perf_counter()
    splits = entity.find_split_points(xml_path, chunk_bytes)
    worker_args = [ChunkArgs(str(xml_path), s, e, config.strict) for s, e in splits]
    n_chunks = len(splits)
    if not use_progress:
        status("Chunks", f"{n_chunks} chunks, {parse_workers} workers")

    # Set up progress bar (if enabled).
    # redirect_stdout/stderr ensures that any print() calls from writer
    # setup/finalize or subprocess warnings render above the bar cleanly.
    progress_bar: _ProgressBar | None = None
    progress_ctx: Progress | None = None
    if use_progress:
        progress_ctx = Progress(
            TextColumn("  [bold]{task.fields[label]:<14s}[/]"),
            BarColumn(),
            TextColumn("[progress.description]{task.description}"),
            TextColumn("[dim]·[/]"),
            _ElapsedEstTotalColumn(),
            console=console,
            transient=True,
            redirect_stdout=True,
            redirect_stderr=True,
        )
        task_id = progress_ctx.add_task(
            "",
            total=None,
            label="Load",
        )
        progress_bar = _ProgressBar(progress_ctx, task_id, total=n_chunks)
        progress_ctx.start()

    write_q = queue.Queue(maxsize=config.write_queue)
    writer_result = {}
    writer_thread = threading.Thread(
        target=_writer_thread_fn,
        args=(writer, write_q, n_chunks, writer_result, entity),
        kwargs={"profile": config.profile, "progress_bar": progress_bar},
        daemon=True,
    )
    writer_thread.start()

    # put_blocked measures how long the main thread waits for queue space.
    # High put_blocked = write-bound pipeline.  See --profile output.
    put_blocked = 0.0
    # Workers ignore SIGINT so they don't dump tracebacks on Ctrl+C;
    # the parent handles the interrupt and terminates workers cleanly.
    pool = Pool(
        parse_workers,
        initializer=signal.signal,
        initargs=(signal.SIGINT, signal.SIG_IGN),
    )
    try:
        for ipc_dict in pool.imap_unordered(entity.extract_chunk_to_ipc, worker_args):
            if config.profile:
                t_put = time.perf_counter()
            write_q.put(ipc_dict)  # blocks if queue full (backpressure)
            if config.profile:
                put_blocked += time.perf_counter() - t_put
    except KeyboardInterrupt:  # pragma: no cover
        # Stop the progress bar first to restore terminal state
        if progress_ctx is not None:
            progress_ctx.stop()
        pool.terminate()
        pool.join()
        # Clean up decompressed XML unless user wants to keep it
        if not config.keep_xml:
            xml_path.unlink(missing_ok=True)
        raise
    else:
        pool.close()
        pool.join()

    write_q.put(None)  # sentinel tells writer thread to exit
    writer_thread.join()

    if progress_ctx is not None:
        progress_ctx.stop()

    if "error" in writer_result:
        raise writer_result["error"]
    total = writer_result.get("total", 0)
    t_load = time.perf_counter() - t1

    if use_progress and t_load > 0:
        rate = total / t_load
        status("Load", f"{total:,} records, {rate:,.0f} rec/s", f"[{t_load:.2f}s]")

    # Stage 4: Indexes
    t2 = time.perf_counter()
    writer.finalize(entity)
    t_indexes = time.perf_counter() - t2

    # Stage 5: Cleanup
    if not config.keep_xml:
        xml_path.unlink(missing_ok=True)

    t_total = t_decompress + t_load + t_indexes

    profile_data = None
    if config.profile:
        profile_data = {
            "put_blocked": put_blocked,
            "get_wait": writer_result.get("get_wait", 0),
            "table_timings": writer_result.get("table_timings", {}),
        }

    return PipelineResult(
        profile_data=profile_data,
        t_decompress=t_decompress,
        t_indexes=t_indexes,
        t_parse_load=t_load,
        t_total=t_total,
        total_records=total,
    )
