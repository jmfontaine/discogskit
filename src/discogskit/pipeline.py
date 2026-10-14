"""Pipeline orchestration: decompress, split, parse, load, index.

Architecture
============

The pipeline has 5 stages.  Parsing and writing (stage 3) overlap, with a
bounded number of chunks in flight::

    .xml.gz file
        |  [Stage 1: Decompress]  rapidgzip (parallel)
        v
    .xml file on disk
        |  [Stage 2: Split]  mmap scan for element boundaries
        v
    N byte-range chunks
        |
        +---> [Stage 3a: Parse workers]  ProcessPoolExecutor (spawn)
        |         Each worker reads its byte range, wraps in an XML
        |         envelope, parses with lxml iterparse, and produces
        |         Arrow IPC buffers (one per normalized table).
        |              |
        |              v  at most parse_workers + write_queue chunks in flight
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
  IPC byte buffers which cross process boundaries efficiently. Workers are
  spawned, never forked: by the time they start, this process runs Rich's
  refresh thread and the writer thread and holds database connections, none
  of which a forked child can safely inherit.
- **Arrow IPC as inter-process format**: compact (columnar, no copies on
  deserialization), avoids pickling overhead. Buffers are ~150 MB per chunk
  at the default ``--chunk-mb``.
- **Bounded chunks in flight**: a chunk counts against a budget of
  ``parse_workers + write_queue`` from its submission to a parse worker until
  its write completes, and the main thread submits a chunk only while under
  budget.  If the writer falls behind, parsed chunks wait for it and the
  workers go idle, so the main process never holds more than that many
  parsed chunks, however slow the writer is.
- **Writer thread**: decouples parsing from database writes.  Without it,
  the main process would sequentially consume an IPC dict and flush it,
  leaving parse workers idle during flushes.
- **Futures for both stages**: a parse error, a dead worker
  (``BrokenProcessPool``) or a writer error reaches the main thread as soon
  as its future completes. Writes queued behind a failed one are skipped.
  Any failure (writer, parser, dead worker, Ctrl+C) cancels queued chunks
  and writes, then waits for the chunks already being parsed and the
  in-flight write, so the writer is idle before the caller closes it. On
  Ctrl+C, the writer's ``cancel()`` first cancels the in-flight write, which
  may otherwise wait forever on a stalled database server.
"""

from __future__ import annotations

import multiprocessing
import signal
import threading
import time
from collections import deque
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from contextlib import nullcontext, suppress
from dataclasses import dataclass
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
from discogskit._ipc import deserialize_batch
from discogskit.entities import EntityDef
from discogskit.entities import get as get_entity
from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import ChunkResult, extract_chunk_to_ipc
from discogskit.writers import Writer


class _ElapsedEstTotalColumn(ProgressColumn):
    """Shows ``elapsed / ~estimated_total``."""

    def render(self, task: Task) -> Text:
        elapsed = task.elapsed or 0.0
        elapsed_str = _fmt_time(elapsed)
        if task.total and task.completed and task.completed < task.total:
            est_total = elapsed * task.total / task.completed
            return Text(f"{elapsed_str}/~{_fmt_time(est_total)}", style="cyan")
        return Text(elapsed_str, style="cyan")


def _fmt_time(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    if m:
        return f"{m}:{s:02d}"
    return f"{s}s"


if TYPE_CHECKING:
    import pyarrow as pa


class _ProgressBar:
    """The Rich progress display for one load, shown from ``__enter__`` until ``stop()`` or ``__exit__``.

    The bar advances per chunk (the unit of pipeline work) but displays
    throughput, which is more meaningful to users than chunk counts.
    ``update()`` runs on the writer thread; the rich Progress object is
    thread-safe.
    """

    def __enter__(self) -> None:
        self._progress.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def __init__(self, total: int) -> None:
        # redirect_stdout/stderr keeps print() calls and warnings from this
        # process (writer setup/finalize, for example) above the bar.
        self._progress = Progress(
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
        self._started = False
        self._task_id = self._progress.add_task("", label="Load", total=None)
        self._total = total

    def stop(self) -> None:
        """Remove the display and restore stdout, stderr and the cursor; later calls do nothing."""
        self._progress.stop()

    def update(self, avg_rate: float) -> None:
        if not self._started:
            # Switch from indeterminate pulse to determinate bar
            self._progress.update(self._task_id, total=self._total)
            self._started = True
        self._progress.update(
            self._task_id,
            advance=1,
            description=f"[cyan]{avg_rate:,.0f} rec/s",
        )


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
class ProfileData:
    """Per-run ``--profile`` timings, surfaced in ``PipelineResult``."""

    get_wait: float
    put_blocked: float
    table_timings: dict[str, float]


@dataclass
class PipelineResult:
    profile_data: ProfileData | None
    t_decompress: float
    t_indexes: float
    t_parse_load: float
    t_total: float
    total_records: int


class _DuplicateIdTracker:
    """Tracks root-table ids seen so far in one run and raises on the first duplicate.

    Ids are tracked in a sparse paged bitmap (page index -> one bit-per-id bytearray) rather than a flat bitmap
    sized to the highest id seen or a Python ``set``. A flat bitmap breaks for negative ids (Python indexes a
    negative offset from the end of the array); a ``set`` retains one Python int object per id. Paging costs
    ``_PAGE_SIZE // 8`` bytes per distinct page an id falls on, however far apart ids are, instead of one
    allocation spanning the full range between the lowest and highest id seen.

    Correctness doesn't depend on how ids are distributed — any int32 value works. Memory does: it's
    proportional to the number of distinct pages touched, so ids that are dense (consecutive or close
    together) share pages and cost little, while ids spread apart each force a new page. Worst case, one id
    per page across the entire int32 range (65,536 pages), is about 520 MiB (512 MiB of bitmap payload plus
    ~8 MiB of per-page bytearray object and dict overhead, measured via ``tracemalloc``) — worse than a
    ``set`` in that case, better for any input dense enough to share pages.

    Measured on 20M dense ids: 2.5 MB retained for this bitmap versus roughly 1.5 GB peak for an equivalent
    ``set`` build, at a per-id cost of ~0.65 us versus a plain ``set.add``'s ~0.29 us — the bitmap is slower
    per id and faster to reach for only because of the memory it saves.
    """

    _PAGE_BITS = 16  # 65,536 ids per page, 8 KiB per page bytearray
    _PAGE_SIZE = 1 << _PAGE_BITS

    def __init__(self) -> None:
        self._pages: dict[int, bytearray] = {}

    def check(self, entity_name: str, ids: pa.Array) -> None:
        for id_value in ids.to_pylist():
            if id_value is None:
                raise ValueError(f"Missing {entity_name} id")
            page_index, offset = divmod(id_value, self._PAGE_SIZE)
            byte_index, bit = divmod(offset, 8)
            page = self._pages.get(page_index)
            if page is None:
                page = bytearray(self._PAGE_SIZE // 8)
                self._pages[page_index] = page
            mask = 1 << bit
            if page[byte_index] & mask:
                raise ValueError(f"Duplicate {entity_name} id: {id_value}")
            page[byte_index] |= mask


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
        self._dup_tracker = _DuplicateIdTracker()
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
            tables = {name: deserialize_batch(data) for name, data in ipc_dict.items()}
            root_batch = tables[self.entity.table_order[0]]
            chunk_count = root_batch.num_rows
            self._dup_tracker.check(self.entity.name, root_batch.column("id"))
            timings = self.writer.write_chunk(tables)
        except BaseException:
            self._failed = True
            raise
        if self.table_timings is not None:
            for k, v in timings.items():
                self.table_timings[k] = self.table_timings.get(k, 0.0) + v
        self._t_idle = time.perf_counter()
        chunk_elapsed = self._t_idle - t_chunk
        elapsed = self._t_idle - self._t_start
        self.total += chunk_count
        self.chunks_done += 1

        if self.progress_bar is not None:
            avg_rate = self.total / elapsed if elapsed > 0 else 0
            self.progress_bar.update(avg_rate)
        else:
            # Through the console, which flushes each line, so piped output
            # shows every chunk as it's written. Literal text, never wrapped.
            console.print(
                f"  chunk {self.chunks_done}/{self.n_chunks}: "
                f"{chunk_count:,} {self.entity.name} ({self.total:,} total) "
                f"[{chunk_count / chunk_elapsed:,.0f} rec/s chunk, "
                f"{self.total / elapsed:,.0f} rec/s avg]",
                markup=False,
                soft_wrap=True,
            )


# Seconds between cancel requests while an interrupted write stops. The server ignores a cancel that reaches a
# connection before its statement starts (e.g. while the chunk is still being deserialized, or between two tables), so
# the request repeats until the write has stopped.
_CANCEL_INTERVAL = 0.5


def _cancel_until_done(writer: Writer, writer_done: threading.Event) -> None:
    """Call ``writer.cancel()`` every ``_CANCEL_INTERVAL`` seconds until ``writer_done`` is set.

    ``writer_done`` is set once the writer thread has exited, which its executor, already shut down, lets it do once
    its current write ends. Waiting on the thread rather than on the write futures also covers a Ctrl+C that lands
    after a write was handed to the thread but before its future was recorded.
    """
    while True:
        # A second Ctrl+C mustn't cut this short: the caller would close the
        # writer while a write still runs on it.
        try:
            # Best effort: the interrupt is what gets reported, not a failed cancel.
            with suppress(Exception):
                writer.cancel()
            if writer_done.wait(_CANCEL_INTERVAL):
                return
        except KeyboardInterrupt:
            continue


def _watch(thread: threading.Thread) -> threading.Event:
    """An Event set once ``thread`` has exited.

    The join runs on a thread of its own: before Python 3.13, a Ctrl+C that interrupts ``join()`` on the main thread
    marks the thread as stopped while it still runs.
    """
    done = threading.Event()

    def join_then_set() -> None:
        thread.join()
        done.set()

    threading.Thread(
        daemon=True, name="discogskit-writer-watch", target=join_then_set
    ).start()
    return done


def _parse_and_write(
    chunks: list[ChunkArgs],
    chunk_writer: _ChunkWriter,
    *,
    parse_workers: int,
    profile: bool,
    progress_bar: _ProgressBar | None,
    write_queue: int,
) -> tuple[float, list[str]]:
    """Parse ``chunks`` in worker processes and write each one on the writer thread.

    A chunk counts against a budget of ``parse_workers + write_queue`` from its
    submission to a parse worker until its write completes; a chunk is
    submitted only while under budget, and only to an otherwise idle worker.
    A slow writer therefore idles the workers instead of letting parsed chunks
    pile up in memory.

    Returns ``put_blocked``: how long parse workers sat idle because the
    writer held the whole budget (measured only with ``profile``).  High
    put_blocked = write-bound pipeline.  See --profile output. Also returns the
    sorted paths of the elements no parser handled, across all chunks (found
    only with ``--strict``).
    """
    budget = parse_workers + write_queue
    queued = deque(chunks)
    parsing: set[Future[ChunkResult]] = set()
    put_blocked = 0.0
    unknown: set[str] = set()
    # Unfinished writes in submission order, which is the order they finish in.
    writes: deque[Future[None]] = deque()

    write_executor = ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="discogskit-writer"
    )
    parse_executor: ProcessPoolExecutor | None = None
    writer_done: threading.Event | None = None
    try:
        # Start the writer thread before the first real write. The executor
        # tracks its thread only once submit() has started it, and that write
        # can begin first: a Ctrl+C landing in between would leave it running
        # where shutdown() can't wait for it. One worker, so this is the thread
        # every write runs on; a Ctrl+C before it's watched comes before any
        # write, so there's nothing to cancel.
        writer_done = _watch(write_executor.submit(threading.current_thread).result())
        # Workers ignore SIGINT so they don't dump tracebacks on Ctrl+C; the
        # parent handles the interrupt and stops them.
        parse_executor = ProcessPoolExecutor(
            initargs=(signal.SIGINT, signal.SIG_IGN),
            initializer=signal.signal,
            max_workers=parse_workers,
            mp_context=multiprocessing.get_context("spawn"),
        )
        while True:
            # At most one chunk per worker: the executor marks a submitted chunk
            # as running once it's queued for a worker, and a running chunk
            # can't be cancelled, so a deeper queue would all get parsed after
            # a failure or Ctrl+C.
            while (
                queued
                and len(parsing) < parse_workers
                and len(parsing) + len(writes) < budget
            ):
                parsing.add(
                    parse_executor.submit(extract_chunk_to_ipc, queued.popleft())
                )
            if not parsing and not writes:
                break
            # Chunks still queued with a worker idle means the budget is full
            # and the writer is what holds it.
            writer_behind = bool(queued) and len(parsing) < parse_workers
            t_wait = time.perf_counter()
            done, _ = wait([*parsing, *writes], return_when=FIRST_COMPLETED)
            if profile and writer_behind:
                put_blocked += time.perf_counter() - t_wait
            # result() re-raises a writer error, a parse error, or
            # BrokenProcessPool when a worker died and took its chunk with it.
            while writes and writes[0].done():
                writes.popleft().result()
            parsed = parsing & done
            parsing -= parsed
            # pop() leaves no name bound to a handed-off future, and tables is
            # deleted once handed off: either would hold the chunk's IPC buffers
            # until released; the write keeps its own reference only until the
            # write is done.
            while parsed:
                tables, chunk_unknown = parsed.pop().result()
                unknown.update(chunk_unknown)
                writes.append(write_executor.submit(chunk_writer.write, tables))
                del tables
        parse_executor.shutdown()
        write_executor.shutdown()
    except BaseException as exc:
        # Restore the terminal before anything below waits.
        if progress_bar is not None:
            progress_bar.stop()
        if isinstance(exc, KeyboardInterrupt):
            # Chunks being parsed can't be interrupted; say why we pause.
            console.print(
                "\n  [yellow]Interrupted — waiting for in-flight chunks to finish …[/]"
            )
        # Drop queued writes now, so none starts while the workers stop.
        write_executor.shutdown(cancel_futures=True, wait=False)
        if isinstance(exc, KeyboardInterrupt) and writer_done is not None:
            # Cancel the in-flight write, which may otherwise wait forever on a
            # stalled server: ADBC's own Ctrl+C handling only covers statements
            # run on the main thread. After dropping the queued writes, so the
            # thread exits once that write ends.
            _cancel_until_done(chunk_writer.writer, writer_done)
        if parse_executor is not None:
            # Drop queued chunks and wait for those being parsed (about one per
            # worker). Not terminate_workers() (3.14+): a worker killed while
            # sending its result leaves a partial message in the result pipe,
            # and the executor's manager thread then blocks on it forever,
            # hanging interpreter exit.
            parse_executor.shutdown(cancel_futures=True)
        # Wait for the in-flight write, so the writer is idle before the
        # caller closes it.
        write_executor.shutdown()
        raise
    return put_blocked, sorted(unknown)


def run(config: PipelineConfig, writer: Writer) -> PipelineResult:
    """Execute the full ingest pipeline."""
    entity = get_entity(config.entity)

    gz_path = config.gz_path
    xml_path = gz_path.with_suffix("")
    parse_workers = config.parse_workers
    chunk_bytes = config.chunk_mb * 1024 * 1024

    # Profiling requires verbose per-chunk output, so disable progress bar. So
    # does output that isn't a terminal (a pipe, a file, CI): a live display
    # only shows there once it ends.
    use_progress = config.progress and not config.profile and console.is_terminal

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
        t1 = time.perf_counter()
        progress_bar = _ProgressBar(n_chunks) if use_progress else None
        chunk_writer = _ChunkWriter(
            writer, entity, n_chunks, profile=config.profile, progress_bar=progress_bar
        )
        # One boundary for the display and the executors: whatever fails, even
        # starting the parse workers, removes the bar and restores the terminal.
        with progress_bar or nullcontext():
            put_blocked, unknown = _parse_and_write(
                worker_args,
                chunk_writer,
                parse_workers=parse_workers,
                profile=config.profile,
                progress_bar=progress_bar,
                write_queue=config.write_queue,
            )
        # Reported here, once per run: each worker only sees its own chunks.
        for path in unknown:
            status("Strict", f"unhandled XML element <{path}> in <{entity.root_tag}>")

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
        profile_data = ProfileData(
            get_wait=chunk_writer.get_wait,
            put_blocked=put_blocked,
            table_timings=table_timings,
        )

    return PipelineResult(
        profile_data=profile_data,
        t_decompress=t_decompress,
        t_indexes=t_indexes,
        t_parse_load=t_load,
        t_total=t_total,
        total_records=total,
    )
