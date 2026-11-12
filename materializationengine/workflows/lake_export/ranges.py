"""Reading one id range of a table and writing it into staging tables inside each lake.

A range is streamed by the main thread while a writer thread flushes full buffers through
the export's unchanged _flush_buffer, with at most one buffer waiting between them, so
Postgres waits on the client only when the writer falls behind. Each lake gets the range
in a staging table of its own, <lake>/staging/r<range>-<attempt>/, written with as many
commits as it needs and invisible to the lake until publish adds its files in one commit.
Every attempt stages afresh, so a retried range never writes into a failed attempt's files.
"""

import dataclasses
import os
import queue
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from materializationengine.workflows import deltalake_export as dx

STAGING = "staging"
BATCH_SIZE_HINT = "adbc.postgresql.batch_size_hint_bytes"


def range_query(source: "dx.TableSource", columns: Sequence[str], lo: int, hi: int,
                row_filter: Optional[str] = None, join: str = "left") -> str:
    """SELECT columns of the export's FROM clause for lo <= id < hi.

    The bound is applied to the segmentation side too, inside the join: Postgres does not
    carry a range qual across a LEFT JOIN's equivalence, so without it every range would
    scan the whole segmentation table. With it, a range is an index merge join.

    row_filter: SQL ANDed into the WHERE (the export source's, e.g. a live table's rows
    as of a timestamp). join: "left" keeps annotations with no segmentation row, "inner"
    drops them (as a merged version does).
    """
    lo, hi = int(lo), int(hi)
    select = ", ".join(f'"{dx._validate_identifier(c)}"' for c in columns)
    anno = f'"{dx._validate_identifier(source.annotation_table)}"'
    from_clause = anno
    if source.segmentation_table is not None:
        seg = f'"{dx._validate_identifier(source.segmentation_table)}"'
        kind = {"left": "LEFT JOIN", "inner": "JOIN"}[join]
        from_clause = (f"{anno} {kind} (SELECT * FROM {seg} WHERE id >= {lo} AND id < {hi}) {seg} "
                       f"USING (id)")
    where = f"{anno}.id >= {lo} AND {anno}.id < {hi}"
    if row_filter:
        where += f" AND ({row_filter})"
    return f"SELECT {select} FROM {from_clause} WHERE {where}"


def cut_ranges(lo: int, hi: int, n: int, id_bounds: Optional[Sequence] = None) -> List[Tuple[int, int]]:
    """Up to n contiguous [a, b) id ranges covering [lo, hi).

    With the id spec's partition bounds, cuts fall on its partition edges, so the ranges
    hold about equal rows and each writes whole id partitions. Its bins are right-closed
    (pl.cut), so a cut goes one past a bound. Without bounds (or with fewer than n), the
    span is cut evenly.
    """
    lo, hi = int(lo), int(hi)
    if n <= 1 or hi - lo <= 1:
        return [(lo, hi)]
    if id_bounds and len(id_bounds) >= n:
        b = sorted(id_bounds)
        cuts = [int(b[len(b) * j // n]) + 1 for j in range(1, n)]
    else:
        cuts = [lo + (hi - lo) * j // n for j in range(1, n)]
    edges = [lo] + sorted({c for c in cuts if lo < c < hi}) + [hi]
    return list(zip(edges[:-1], edges[1:]))


def staging_name(range_index: int, attempt: Optional[str] = None) -> str:
    return f"{STAGING}/r{int(range_index):05d}-{attempt or uuid.uuid4().hex[:8]}"


@dataclass
class RangeResult:
    range_index: int
    stage: str
    rows: int = 0
    bytes: int = 0
    flushes: int = 0
    read_s: float = 0.0
    write_s: float = 0.0
    stall_s: float = 0.0  # the reader waiting on a busy writer
    seconds: float = 0.0
    host: str = ""


def export_range(
    connection_string: str,
    source: "dx.TableSource",
    columns: Sequence[str],
    specs: Sequence["dx.DeltaLakeOutputSpec"],
    geometry_columns: Sequence[str],
    output_uri_base: str,
    range_index: int,
    lo: int,
    hi: int,
    flush_bytes: int,
    stage: Optional[str] = None,
    progress_callback=None,
    batch_bytes: Optional[int] = None,
    row_filter: Optional[str] = None,
    join: str = "left",
) -> RangeResult:
    """Stream ids [lo, hi) into a fresh staging table in each lake; returns its stage.

    progress_callback(rows) is called after each batch with the range's rows so far.
    batch_bytes: ADBC's batch size hint (its default is ~16 MB). row_filter, join: the
    export source's (see range_query).
    """
    stage = stage or staging_name(range_index)
    staged = [dataclasses.replace(s, name=f"{s.name}/{stage}") for s in specs]
    result = RangeResult(range_index=range_index, stage=stage, host=f"{socket.gethostname()}:{os.getpid()}")

    buffers: queue.Queue = queue.Queue(maxsize=1)
    failure: List[BaseException] = []

    def writer():
        while (buffer := buffers.get()) is not None:
            if failure:
                continue  # drain, so the reader never blocks on a dead writer
            started = time.monotonic()
            try:
                dx._flush_buffer(buffer, staged, output_uri_base, list(geometry_columns))
            except BaseException as e:  # noqa: BLE001 - re-raised by the reader below
                failure.append(e)
            result.write_s += time.monotonic() - started
            result.flushes += 1

    started = time.monotonic()
    writer_thread = threading.Thread(target=writer, name=f"lake-export-writer-r{range_index}", daemon=True)
    writer_thread.start()
    try:
        buffer, buffered = [], 0
        with dx.pg_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cur:
                if batch_bytes:
                    cur.adbc_statement.set_options(**{BATCH_SIZE_HINT: str(int(batch_bytes))})
                cur.execute(range_query(source, columns, lo, hi, row_filter=row_filter, join=join))
                batches = iter(cur.fetch_record_batch())
                while not failure:
                    t = time.monotonic()
                    batch = next(batches, None)
                    result.read_s += time.monotonic() - t
                    if batch is None:
                        break
                    buffer.append(batch)
                    buffered += batch.nbytes
                    result.rows += batch.num_rows
                    result.bytes += batch.nbytes
                    if progress_callback is not None:
                        progress_callback(result.rows)
                    if buffered >= flush_bytes:
                        t = time.monotonic()
                        buffers.put(buffer)  # blocks while the writer is behind
                        result.stall_s += time.monotonic() - t
                        buffer, buffered = [], 0
        if buffer and not failure:
            buffers.put(buffer)
    finally:
        buffers.put(None)
        writer_thread.join()
    if failure:
        raise failure[0]
    result.seconds = time.monotonic() - started
    return result
