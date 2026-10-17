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
        +---> [Stage 3a: Parse workers]  multiprocessing.Process (spawn)
        |         Each worker feeds its byte range, in an XML envelope,
        |         to lxml's pull parser 64 KiB at a time, and produces
        |         Arrow IPC buffers (one per normalized table).
        |              |
        |              v  at most parse_workers + write_queue chunks in flight
        |              |
        +---> [Stage 3b: Writer thread]  one-worker ThreadPoolExecutor
                  Deserializes IPC, calls writer.write_chunk() (ADBC
                  COPY, SQLite executemany, or staged Parquet/JSONL
                  files).
                       |
                       v
                  Database tables (no FK-column indexes yet) or staged
                  files
                       |
                       v
                  [Stage 4: Finalize]  indexes for databases, commit of
                  staged files
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
- **Arrow IPC as inter-process format**: each worker serializes every table
  of a chunk to an Arrow IPC buffer and writes each buffer to its pipe as it
  is, after a small pickled header naming the tables. Pickling the rows as
  Python objects would walk every value, and pickling the buffers would copy
  them all into one message first (issue #31); the main process then reads
  each buffer without copying it again (columnar). Buffers are ~150 MB per
  chunk at the default ``--chunk-mb``.
- **Bounded chunks in flight**: a chunk counts against a budget of
  ``parse_workers + write_queue`` from its submission to a parse worker until
  its write completes, and the main thread submits a chunk only while under
  budget.  If the writer falls behind, parsed chunks wait for it and the
  workers go idle, so the main process never holds more than that many
  parsed chunks, however slow the writer is.
- **Writer thread**: decouples parsing from writes (ADBC COPY, SQLite
  executemany, or staged Parquet/JSONL files).  Without it, the main
  process would sequentially consume an IPC dict and write it, leaving
  parse workers idle during writes.
- **Parse worker processes owned by the pipeline**: each worker has pipes
  of its own, and the main thread waits on the workers' result pipes, their
  process sentinels and the writes together. A worker holds the only write
  end of its result pipe, so a worker that dies, even halfway through
  sending its result, ends the pipe, and the run fails with
  ``BrokenProcessPool`` instead of waiting forever for the rest of the
  message. A parse error or a writer error reaches the main thread as soon
  as it happens. Writes queued behind a failed one are skipped.
- **Stopping**: any failure (writer, parser, dead worker, Ctrl+C) kills the
  parse workers, drops queued writes and waits for the in-flight write, so
  the writer is idle before the caller closes it. On Ctrl+C, even one that
  lands during that wait after another failure, the writer's ``cancel()``
  first cancels the in-flight write, which may otherwise wait forever on a
  stalled database server.
"""

from __future__ import annotations

import multiprocessing
import re
import signal
import socket
import threading
import time
import traceback
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from multiprocessing.connection import wait
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import pyarrow as pa
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TextColumn,
)
from rich.text import Text

from discogskit._console import current, set_current, status, using
from discogskit._ipc import deserialize_batch
from discogskit.decompress import ensure_xml
from discogskit.entities import ENTITIES, EntityDef
from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import ChunkResult, extract_chunk_to_ipc
from discogskit.writers import Writer

__all__ = [
    "PipelineConfig",
    "PipelineResult",
    "ProfileData",
    "UnhandledElementsError",
    "run",
]


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
    from collections.abc import Callable
    from multiprocessing.connection import Connection
    from multiprocessing.process import BaseProcess


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
            console=current(),
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
    """Settings for one ``run()``: one dump of one entity.

    ``entity`` is a key of ``ENTITIES`` and must match the dump at ``gz_path`` (``detect_entity()`` reads it from a
    Discogs dump file name). ``chunk_mb`` sets the rough size, in MiB, of the chunks the XML is split into, at least 1,
    and ``parse_workers`` the number of parse processes, at least 1. At most ``parse_workers + write_queue`` chunks are
    being parsed or waiting to be written at once; ``write_queue`` is at least 0. ``run()`` raises ``ValueError`` for a
    value out of these ranges before it decompresses the dump or touches the output. ``keep_xml`` keeps the
    decompressed XML after a successful run. ``profile`` fills ``PipelineResult.profile_data`` and prints per-chunk
    lines instead of a progress bar; ``progress`` shows a progress bar when the console is a terminal. ``strict``
    reports the XML elements, attributes and text no parser handled and then fails the run with
    ``UnhandledElementsError``, before the writer's ``finalize()``.
    """

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
    """What one ``run()`` did: records loaded and seconds spent per stage.

    ``t_parse_load`` covers splitting, parsing and writing; ``t_indexes`` the writer's ``finalize()``; ``t_total``
    is their sum with ``t_decompress``. ``profile_data`` is set only with ``PipelineConfig.profile``.
    """

    profile_data: ProfileData | None
    t_decompress: float
    t_indexes: float
    t_parse_load: float
    t_total: float
    total_records: int


class UnhandledElementsError(Exception):
    """Raised by a ``strict`` run whose input has XML elements, attributes or text no parser handled, which it would
    otherwise drop.

    ``run()`` raises it once the entity's records are parsed and written and a ``Strict`` line is printed for each
    path, but before the writer's ``finalize()``: file writers publish nothing, and database writers build no indexes.
    ``entity`` is the entity's name (``artists``); ``paths`` are the sorted paths, relative to the record, of the
    elements (``videos/video/foo``), attributes (``videos/video/@foo``) and text (``urls/text()``, text in an element
    whose text no parser reads).
    """

    entity: str
    paths: list[str]

    def __init__(self, entity: EntityDef, paths: list[str]) -> None:
        noun = "path" if len(paths) == 1 else "paths"
        super().__init__(
            f"--strict: {len(paths)} unhandled XML {noun} in <{entity.root_tag}>; "
            "see the Strict lines above"
        )
        self.entity = entity.name
        self.paths = paths


# A namespaced name in a path is "{uri}name", and the URI can hold "/" or "@".
_NAMESPACES = re.compile(r"\{[^}]*\}")


def _kind(path: str) -> str:
    """What ``path``, one of ``UnhandledElementsError.paths``, names."""
    last = _NAMESPACES.sub("", path).rpartition("/")[2]
    if last[:1] == "@":
        return "attribute"
    return "text" if last == "text()" else "element"


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

    def write(self, ipc_dict: dict[str, pa.Buffer]) -> None:
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
            current().print(
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


class _ChunkHeader(NamedTuple):
    """A worker's reply for a chunk it parsed, followed on the pipe by one message per table: its IPC buffer."""

    # The chunk's table names, in the order their buffers follow.
    tables: list[str]
    # ChunkResult.unknown.
    unknown: list[str]


class _ParseFailure(NamedTuple):
    """A worker's reply for a chunk it failed to parse, in place of its ``ChunkResult``."""

    error: BaseException
    # The error's traceback in the worker, formatted.
    traceback: str


class _RemoteTraceback(Exception):
    """A parse error's traceback in the worker, chained to the error as its ``__cause__``."""

    def __str__(self) -> str:
        return self.args[0]


@dataclass(eq=False)
class _ParseWorker:
    """One parse worker process and the main process's ends of its pipes."""

    # The chunk the worker is parsing, or None while it's idle.
    chunk: ChunkArgs | None
    process: BaseProcess
    # Replies from the worker.
    results: Connection
    # Chunks to the worker.
    tasks: Connection


def _format_traceback(error: BaseException) -> str:
    return "".join(traceback.format_exception(type(error), error, error.__traceback__))


def _parse_worker(
    tasks: Connection, results: Connection, parse: Callable[[ChunkArgs], ChunkResult]
) -> None:
    """Main function of a parse worker process: parse each chunk from ``tasks`` and send its ``ChunkResult``, or a
    ``_ParseFailure``, to ``results``, until the main process closes ``tasks``."""
    # Ctrl+C reaches the whole process group. The main process handles it and
    # stops the workers, which would otherwise dump tracebacks.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    while True:
        try:
            chunk = tasks.recv()
        except EOFError:
            return
        try:
            reply: ChunkResult | _ParseFailure = parse(chunk)
        except Exception as exc:  # noqa: BLE001
            # The main process re-raises it.
            reply = _ParseFailure(exc, _format_traceback(exc))
        try:
            if isinstance(reply, _ParseFailure):
                results.send(reply)
            else:
                _send_chunk(results, reply)
        except OSError:
            # The main process is gone: nobody wants the result.
            return
        except Exception as exc:  # noqa: BLE001
            if results.closed:
                # _send_chunk() failed after its header: the main process sees the pipe end, as for a dead worker.
                return
            # A reply pickle can't serialize fails before any of it is sent, so
            # the pipe can still carry the error that stopped it.
            results.send(_ParseFailure(exc, _format_traceback(exc)))
        # Otherwise the sent result stays in memory while the next chunk parses.
        del reply


def _send_chunk(results: Connection, result: ChunkResult) -> None:
    """Send a parsed chunk to the main process: a ``_ChunkHeader``, then each table's IPC buffer, uncopied.

    Pickling the whole result copied every buffer into one message first, which held the chunk's tables twice and
    raised a long-lived worker's peak by 0.4-0.9 GiB at 256 MiB chunks (issue #31).
    """
    results.send(_ChunkHeader(tables=list(result.tables), unknown=result.unknown))
    try:
        for buffer in result.tables.values():
            results.send_bytes(buffer)
    except BaseException:
        # The main process would read anything sent now as a table, then wait for the rest. Ending the pipe tells
        # it the worker is gone.
        results.close()
        raise


def _start_parse_worker() -> _ParseWorker:
    """Start one parse worker process, idle until it's sent a chunk."""
    context = multiprocessing.get_context("spawn")
    # Pipes rather than one duplex connection, a socket pair, which moved a
    # 150 MB result about 2.5 times slower than a pipe on macOS.
    results, child_results = context.Pipe(duplex=False)
    child_tasks, tasks = context.Pipe(duplex=False)
    # extract_chunk_to_ipc is looked up when the worker starts, so tests can
    # replace it.
    process = context.Process(
        args=(child_tasks, child_results, extract_chunk_to_ipc),
        daemon=True,
        name="discogskit-parse",
        target=_parse_worker,
    )
    try:
        process.start()
    except BaseException:
        results.close()
        tasks.close()
        raise
    finally:
        # From here on the worker holds the only write end of its results pipe,
        # so the pipe ends when the worker dies, even mid-message.
        child_results.close()
        child_tasks.close()
    return _ParseWorker(chunk=None, process=process, results=results, tasks=tasks)


def _stop_parse_workers(workers: list[_ParseWorker]) -> None:
    """Kill every worker, busy or not, wait for it to exit and release its resources.

    Each worker leaves ``workers`` once it has exited and before it's closed, so calling this again, even after an
    interrupted call, never terminates a closed process.
    """
    for worker in workers:
        worker.process.terminate()
    while workers:
        workers[-1].process.join()
        worker = workers.pop()
        try:
            worker.process.close()
        finally:
            worker.results.close()
            worker.tasks.close()


def _worker_died(worker: _ParseWorker) -> BrokenProcessPool:
    """The error for a worker that died; ``worker.chunk`` is the chunk it took with it, if any."""
    # Its pipe or sentinel already shows the worker gone, so this
    # returns at once; the timeout only bounds the wait if the OS lags.
    worker.process.join(timeout=1)
    exit_code = worker.process.exitcode
    how = "" if exit_code is None else f" with exit code {exit_code}"
    chunk = worker.chunk
    where = (
        ""
        if chunk is None
        else f" while parsing {chunk.file_path} (chunk bytes {chunk.start}-{chunk.end})"
    )
    # Status 1 is Python's for an uncaught exception, such as the one run() raises in a worker that runs, again, a
    # script calling it outside a __main__ guard. Bootstrapping a worker leaves nothing that tells that apart from
    # an exception raised later.
    guard = (
        " Exit code 1 is also how a worker ends when the script that runs discogskit doesn't do so under `if"
        ' __name__ == "__main__":`: each worker imports that script again.'
        if exit_code == 1
        else ""
    )
    return BrokenProcessPool(
        f"A parse worker died{how}{where}. A worker killed for using too much"
        f" memory dies this way; fewer parse workers or smaller chunks need less.{guard}"
    )


def _receive(worker: _ParseWorker) -> ChunkResult:
    """Receive the result of the chunk ``worker`` is parsing; re-raise its parse error, or raise if it died."""
    try:
        reply: _ChunkHeader | _ParseFailure = worker.results.recv()
        # The worker sends the buffers right after the header. py_buffer wraps the received bytes without a copy.
        tables = (
            {}
            if isinstance(reply, _ParseFailure)
            else {
                name: pa.py_buffer(worker.results.recv_bytes()) for name in reply.tables
            }
        )
    except (EOFError, OSError):
        # The pipe ended: before the reply (EOFError) or halfway through it (OSError).
        raise _worker_died(worker) from None
    worker.chunk = None
    if isinstance(reply, _ParseFailure):
        reply.error.__cause__ = _RemoteTraceback(f'\n"""\n{reply.traceback}"""')
        raise reply.error
    return ChunkResult(tables=tables, unknown=reply.unknown)


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
    submitted only while under budget, and only to an idle worker.
    A slow writer therefore idles the workers instead of letting parsed chunks
    pile up in memory.

    Returns ``put_blocked``: how long parse workers sat idle because the
    writer held the whole budget (measured only with ``profile``).  High
    put_blocked = write-bound pipeline.  See --profile output. Also returns the
    sorted paths of the elements, attributes and text no parser handled, across
    all chunks (found only with ``--strict``).
    """
    budget = parse_workers + write_queue
    queued = deque(chunks)
    put_blocked = 0.0
    unknown: set[str] = set()
    # The writer thread wakes the main thread through this socket pair when a
    # write completes, so one wait() covers the workers and the writes.
    wake_r, wake_w = socket.socketpair()
    # A full socket already holds a wake-up: one is enough, since the main
    # thread checks every write once woken.
    wake_w.setblocking(False)
    workers: list[_ParseWorker] = []
    # Unfinished writes in submission order, which is the order they finish in.
    writes: deque[Future[None]] = deque()

    def wake(_: Future[None]) -> None:
        # Best effort: a full socket (BlockingIOError) already holds a wake-up,
        # and a write that ends after the run has nobody to wake.
        with suppress(OSError):
            wake_w.send(b"\0")

    # The writer thread prints to the run's console: a new thread may not inherit the caller's context.
    write_executor = ThreadPoolExecutor(
        initargs=(current(),),
        initializer=set_current,
        max_workers=1,
        thread_name_prefix="discogskit-writer",
    )
    writer_done: threading.Event | None = None
    try:
        # Start the writer thread before the first real write. The executor
        # tracks its thread only once submit() has started it, and that write
        # can begin first: a Ctrl+C landing in between would leave it running
        # where shutdown() can't wait for it. One worker, so this is the thread
        # every write runs on; a Ctrl+C before it's watched comes before any
        # write, so there's nothing to cancel.
        writer_done = _watch(write_executor.submit(threading.current_thread).result())
        # No more workers than chunks: the others would start only to be stopped.
        for _ in range(min(parse_workers, len(chunks))):
            workers.append(_start_parse_worker())
        while True:
            idle = [worker for worker in workers if worker.chunk is None]
            parsing = len(workers) - len(idle)
            while queued and idle and parsing + len(writes) < budget:
                worker = idle.pop()
                chunk = queued.popleft()
                try:
                    worker.tasks.send(chunk)
                except OSError:
                    # The worker died before its sentinel was seen.
                    raise _worker_died(worker) from None
                worker.chunk = chunk
                parsing += 1
            if not parsing and not writes:
                break
            # Chunks still queued with a worker idle means the budget is full
            # and the writer is what holds it.
            writer_behind = bool(queued) and bool(idle)
            t_wait = time.perf_counter()
            # A sentinel shows a worker that died, even an idle one.
            ready = wait(
                [
                    wake_r,
                    *(worker.results for worker in workers if worker.chunk is not None),
                    *(worker.process.sentinel for worker in workers),
                ]
            )
            if profile and writer_behind:
                put_blocked += time.perf_counter() - t_wait
            if wake_r in ready:
                wake_r.recv(4096)
            # result() re-raises a writer error.
            while writes and writes[0].done():
                writes.popleft().result()
            for worker in workers:
                if worker.chunk is not None and worker.results in ready:
                    # tables is deleted once handed off: it would hold the
                    # chunk's IPC buffers until rebound; the write keeps its
                    # own reference only until the write is done.
                    tables, chunk_unknown = _receive(worker)
                    unknown.update(chunk_unknown)
                    write = write_executor.submit(chunk_writer.write, tables)
                    del tables
                    write.add_done_callback(wake)
                    writes.append(write)
                if worker.process.sentinel in ready:
                    raise _worker_died(worker)
        _stop_parse_workers(workers)
        write_executor.shutdown()
    except BaseException as exc:
        # Restore the terminal before anything below waits.
        if progress_bar is not None:
            progress_bar.stop()
        if isinstance(exc, KeyboardInterrupt):
            # The in-flight write may not stop at once; say why we pause.
            current().print(
                "\n  [yellow]Interrupted — waiting for the in-flight write to stop …[/]"
            )
        # Nothing will use what the workers are parsing: stop them now. Waiting
        # for them to exit comes after the in-flight write has stopped, so a
        # second Ctrl+C can't skip that wait; nor can one landing here, as
        # _stop_parse_workers() below terminates any worker this misses.
        with suppress(KeyboardInterrupt):
            for worker in workers:
                worker.process.terminate()
        # Drop queued writes now, so none starts while the in-flight one stops.
        write_executor.shutdown(cancel_futures=True, wait=False)
        try:
            if isinstance(exc, KeyboardInterrupt) and writer_done is not None:
                # Cancel the in-flight write, which may otherwise wait forever on a
                # stalled server: ADBC's own Ctrl+C handling only covers statements
                # run on the main thread. After dropping the queued writes, so the
                # thread exits once that write ends.
                _cancel_until_done(chunk_writer.writer, writer_done)
            elif writer_done is not None:
                # Wait for the in-flight write, so the writer is idle before the
                # caller closes it. On the watcher, not with join(): a Ctrl+C
                # that interrupts join() before Python 3.13 marks the thread as
                # stopped while it still runs (see _watch()).
                try:
                    writer_done.wait()
                except KeyboardInterrupt:
                    # That write may be the one blocked on a stalled server:
                    # cancel it as above.
                    current().print(
                        "\n  [yellow]Interrupted — waiting for the in-flight write to stop …[/]"
                    )
                    _cancel_until_done(chunk_writer.writer, writer_done)
                    raise
        finally:
            try:
                # The writer thread has exited by now, or never ran a write.
                write_executor.shutdown()
            finally:
                _stop_parse_workers(workers)
        raise
    finally:
        wake_r.close()
        wake_w.close()
    return put_blocked, sorted(unknown)


def run(
    config: PipelineConfig, writer: Writer, *, console: Console | None = None
) -> PipelineResult:
    """Run one dump through the whole pipeline: decompress, split, parse, write, finalize.

    Everything the run prints, from the pipeline and from ``writer``'s ``setup()`` and ``finalize()``, goes to
    ``console``; by default, a console on stdout. Pass ``Console(quiet=True)`` to print nothing, or a ``Console``
    of your own to capture the output. The caller keeps ownership of ``writer`` and closes it.

    With ``config.strict``, raises ``UnhandledElementsError`` before ``finalize()`` if the input has XML elements no
    parser handled.
    """
    # A process discogskit started (a parse worker, the decompression process) imports the calling script again, so
    # one without a __main__ guard calls run() there too. That run would split the XML and set up the writer before
    # failing to start a parse worker, as multiprocessing refuses in a process that is still starting. Spawn names the
    # process before it imports the script.
    if multiprocessing.current_process().name.startswith("discogskit-"):
        raise RuntimeError(
            "discogskit.run() was called again inside one of discogskit's own worker processes: a script that"
            ' runs discogskit must do so under `if __name__ == "__main__":`'
        )
    if console is None:
        return _run(config, writer)
    with using(console):
        return _run(config, writer)


def _run(config: PipelineConfig, writer: Writer) -> PipelineResult:
    entity = ENTITIES[config.entity]
    # Fail before touching the XML or the output. Without a parse worker no chunk would be parsed and the run would
    # publish empty output, as it would with a negative write_queue that leaves no chunk in flight (parse_workers +
    # write_queue < 1); a chunk_mb below 1 would only fail at the split, after decompressing.
    if config.parse_workers < 1:
        raise ValueError(
            f"parse_workers must be at least 1, got {config.parse_workers}"
        )
    if config.write_queue < 0:
        raise ValueError(f"write_queue must be at least 0, got {config.write_queue}")
    if config.chunk_mb < 1:
        raise ValueError(f"chunk_mb must be at least 1, got {config.chunk_mb}")

    gz_path = config.gz_path
    xml_path = gz_path.with_suffix("")
    parse_workers = config.parse_workers
    chunk_bytes = config.chunk_mb * 1024 * 1024

    # Profiling requires verbose per-chunk output, so disable progress bar. So
    # does output that isn't a terminal (a pipe, a file, CI): a live display
    # only shows there once it ends.
    use_progress = config.progress and not config.profile and current().is_terminal

    # Stage 1: Decompress. The lease keeps other runs from deleting or
    # rewriting the XML while this run reads it.
    t0 = time.perf_counter()
    xml_lease = ensure_xml(gz_path, xml_path, parse_workers)
    t_decompress = time.perf_counter() - t0

    try:
        # Stage 2: Split. Runs before writer.setup() so an input without records
        # fails before --overwrite drops the existing output.
        t_split_start = time.perf_counter()
        splits = entity.find_split_points(str(xml_path), chunk_bytes)
        t_split = time.perf_counter() - t_split_start
        worker_args = [
            ChunkArgs(
                end=e,
                entity=entity.name,
                file_path=str(xml_path),
                start=s,
                strict=config.strict,
            )
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
        # Reported here, once per run: each worker only sees its own chunks. All of them, so the user sees every
        # finding at once, then the run fails before finalize() publishes the output or builds indexes.
        for path in unknown:
            status(
                "Strict", f"unhandled XML {_kind(path)} <{path}> in <{entity.root_tag}>"
            )
        if unknown:
            raise UnhandledElementsError(entity, unknown)

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
