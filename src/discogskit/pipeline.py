"""Pipeline orchestration: decompress, split, parse, load, index.

Architecture
============

The pipeline has 5 stages.  Stages 1-3 overlap via a bounded backlog of pending writes::

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
        |              v  bounded backlog of pending writes (backpressure)
        |              |
        +---> [Stage 3b: Writer thread]  one-worker ThreadPoolExecutor
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
  the bounded backlog (default depth 2) caps memory at ~300 MB.
- **Writer thread**: decouples parsing from database writes.  Without it,
  the main process would sequentially consume an IPC dict and flush it,
  leaving parse workers idle during flushes. The bounded backlog provides
  backpressure: if the writer falls behind, the main thread waits on the
  oldest pending write and parse workers naturally pause.
- **Write futures**: each chunk write is a ``Future``, so a writer error
  reaches the main thread as soon as the oldest write fails. Writes queued
  behind a failed one are skipped. Any failure (writer, parser, Ctrl+C)
  terminates the pool, cancels queued writes and waits for the in-flight
  one, so the writer is idle before the caller closes it.
"""

from __future__ import annotations

import signal
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
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
from discogskit.entities import ChunkArgs, EntityDef
from discogskit.entities import get as get_entity
from discogskit.entities._worker import extract_chunk_to_ipc
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


class _ChunkWriter:
    """Writes one chunk per call and accumulates load statistics.

    Runs only on the single writer-executor thread; ``run()`` reads the
    statistics after the executor has shut down.
    """

    def __init__(
        self,
        writer: Writer,
        entity: EntityDef,
        n_chunks: int,
        *,
        profile: bool,
        progress_bar: _ProgressBar | None,
    ) -> None:
        self.chunks_done = 0
        self.entity = entity
        # Writer idle time between chunks. High get_wait = parse-bound pipeline.
        self.get_wait = 0.0
        self.n_chunks = n_chunks
        self.progress_bar = progress_bar
        self.table_timings: dict[str, float] | None = {} if profile else None
        self.total = 0
        self.writer = writer
        self._failed = False
        self._t_idle = self._t_start = time.perf_counter()

    def write(self, ipc_dict: dict[str, bytes]) -> None:
        if self._failed:
            # run() re-raises the earlier failure; writing later chunks would
            # land data after the reported error.
            return
        t_chunk = time.perf_counter()
        self.get_wait += t_chunk - self._t_idle
        try:
            chunk_count = self.writer.write_chunk(
                ipc_dict, self.entity, self.table_timings
            )
        except BaseException:
            self._failed = True
            raise
        self._t_idle = time.perf_counter()
        chunk_elapsed = self._t_idle - t_chunk
        elapsed = self._t_idle - self._t_start
        self.total += chunk_count
        self.chunks_done += 1

        if self.progress_bar is not None:
            avg_rate = self.total / elapsed if elapsed > 0 else 0
            self.progress_bar.update(chunk_count, avg_rate)
        else:
            print(
                f"  chunk {self.chunks_done}/{self.n_chunks}: "
                f"{chunk_count:,} {self.entity.name} ({self.total:,} total) "
                f"[{chunk_count / chunk_elapsed:,.0f} rec/s chunk, "
                f"{self.total / elapsed:,.0f} rec/s avg]"
            )


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

    # Stage 1: Decompress. The lease keeps other runs from deleting or
    # rewriting the XML while this run reads it.
    t0 = time.perf_counter()
    xml_lease = decompress.ensure_xml(gz_path, xml_path, parse_workers)
    t_decompress = time.perf_counter() - t0

    try:
        # Stage 2: Split. Runs before writer.setup() so an input without records
        # fails before --overwrite drops the existing output.
        t_split_start = time.perf_counter()
        splits = entity.find_split_points(str(xml_path), chunk_bytes)
        t_split = time.perf_counter() - t_split_start
        worker_args = [
            ChunkArgs(entity.name, str(xml_path), s, e, config.strict)
            for s, e in splits
        ]
        n_chunks = len(splits)
        if not use_progress:
            status("Chunks", f"{n_chunks} chunks, {parse_workers} workers")

        # Create bare tables (no PK, no FK, no indexes).
        # Indexes are built AFTER bulk load (Stage 4) — inserting into indexed
        # tables triggers per-row index maintenance which is dramatically slower.
        writer.setup(entity)

        # Stage 3: Parallel parse + write
        #
        # Main thread: drains pool.imap_unordered → submits each IPC dict to a
        # one-worker writer executor and keeps the Future in `pending`.
        # Writer thread: runs the writes in submission order → flushes to the target.
        #
        # Backpressure: once more than write_queue chunks wait behind the in-flight
        # write, the main thread blocks on the oldest Future, which stops it from
        # consuming pool results, which stops workers from starting new chunks.
        # This naturally limits memory to ~write_queue × ~150 MB of IPC data.
        # Waiting on a Future also re-raises a writer error in the main thread.
        t1 = time.perf_counter()

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
                redirect_stderr=True,
                redirect_stdout=True,
                transient=True,
            )
            task_id = progress_ctx.add_task(
                "",
                label="Load",
                total=None,
            )
            progress_bar = _ProgressBar(progress_ctx, task_id, total=n_chunks)
            progress_ctx.start()

        chunk_writer = _ChunkWriter(
            writer, entity, n_chunks, profile=config.profile, progress_bar=progress_bar
        )
        pending: deque[Future[None]] = deque()
        # put_blocked measures how long the main thread waits for the writer to
        # catch up. High put_blocked = write-bound pipeline.  See --profile output.
        put_blocked = 0.0
        write_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="discogskit-writer"
        )
        # Workers ignore SIGINT so they don't dump tracebacks on Ctrl+C;
        # the parent handles the interrupt and terminates workers cleanly.
        pool = Pool(
            parse_workers,
            initargs=(signal.SIGINT, signal.SIG_IGN),
            initializer=signal.signal,
        )
        try:
            for ipc_dict in pool.imap_unordered(extract_chunk_to_ipc, worker_args):
                pending.append(write_executor.submit(chunk_writer.write, ipc_dict))
                # Writes finish in order; reap finished ones so errors surface early.
                while pending and pending[0].done():
                    pending.popleft().result()
                if config.profile:
                    t_put = time.perf_counter()
                while len(pending) > config.write_queue + 1:
                    pending.popleft().result()  # blocks until the writer catches up
                if config.profile:
                    put_blocked += time.perf_counter() - t_put
            while pending:
                pending.popleft().result()
        except BaseException as exc:
            # Stop the progress bar first to restore terminal state
            if progress_ctx is not None:
                progress_ctx.stop()
            pool.terminate()
            pool.join()
            if isinstance(exc, KeyboardInterrupt):
                # The in-flight write can't be interrupted; say why we pause.
                console.print(
                    "\n  [yellow]Interrupted — waiting for the in-flight write to finish …[/]"
                )
            # Drop queued writes and wait for the in-flight one, so the writer is
            # idle before the caller closes it.
            write_executor.shutdown(cancel_futures=True)
            raise
        else:
            pool.close()
            pool.join()
            write_executor.shutdown()
        finally:
            if progress_ctx is not None:
                progress_ctx.stop()

        total = chunk_writer.total
        # Load time covers splitting and parse + write, but not writer setup.
        t_load = t_split + time.perf_counter() - t1

        if use_progress and t_load > 0:
            rate = total / t_load
            status("Load", f"{total:,} records, {rate:,.0f} rec/s", f"[{t_load:.2f}s]")

        # Stage 4: Indexes
        t2 = time.perf_counter()
        writer.finalize(entity)
        t_indexes = time.perf_counter() - t2

        # Stage 5: Cleanup. Only a successful run deletes the XML.
        xml_lease.release(remove_xml=not config.keep_xml)
    finally:
        # A failed or interrupted run keeps the complete XML for the next run to
        # reuse; release() is a no-op if the cleanup above already ran.
        xml_lease.release(remove_xml=False)

    t_total = t_decompress + t_load + t_indexes

    profile_data = None
    if config.profile:
        table_timings = chunk_writer.table_timings
        assert table_timings is not None
        # For multi-writer, timings accumulate inside the writer;
        # merge them into table_timings so both paths produce the same output.
        get_timings = getattr(writer, "get_table_timings", None)
        if get_timings is not None:  # pragma: no cover
            for k, v in get_timings().items():
                table_timings[k] = table_timings.get(k, 0.0) + v
        profile_data = {
            "put_blocked": put_blocked,
            "get_wait": chunk_writer.get_wait,
            "table_timings": table_timings,
        }

    return PipelineResult(
        profile_data=profile_data,
        t_decompress=t_decompress,
        t_indexes=t_indexes,
        t_parse_load=t_load,
        t_total=t_total,
        total_records=total,
    )
