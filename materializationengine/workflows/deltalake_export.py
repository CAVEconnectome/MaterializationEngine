from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import adbc_driver_postgresql.dbapi as pg_dbapi
import numpy as np
import polars as pl
import pyarrow as pa
import shapely
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import BYTEA

# Register a minimal geometry type so SQLAlchemy's inspector doesn't warn
# "Did not recognize type 'geometry'" during reflection.  We decode WKB
# ourselves — all SQLAlchemy needs to know is that it's binary.
from sqlalchemy.dialects.postgresql import dialect as _pg_dialect
from sqlalchemy.engine import Engine

from materializationengine.celery_init import celery

_pg_dialect.ischema_names["geometry"] = BYTEA

celery_logger = logging.getLogger(__name__)

# Columns to drop from every Delta Lake export by default.
_DEFAULT_DROP_COLUMNS = ["created", "deleted", "superceded_id"]


@dataclass
class TableSource:
    """Encapsulates the (possibly joined) table identity for a frozen DB export.

    Provides a unified ``from_clause`` for SQL queries that need the full
    joined result set (streaming, etc.) and a ``table_names`` list for
    inspecting physical tables (indexes, ``pg_class`` stats, etc.).

    When ``segmentation_table`` is ``None`` the table is treated as a
    single flat table (either annotation-only or already merged).
    """

    annotation_table: str
    segmentation_table: str | None = None

    @property
    def from_clause(self) -> str:
        """SQL FROM fragment: single table or JOIN."""
        if self.segmentation_table is None:
            return f'"{self.annotation_table}"'
        return f'"{self.annotation_table}" JOIN "{self.segmentation_table}" USING (id)'

    @property
    def table_names(self) -> list[str]:
        """Physical table names for inspection (indexes, pg_class, etc.)."""
        names = [self.annotation_table]
        if self.segmentation_table is not None:
            names.append(self.segmentation_table)
        return names


@dataclass
class DeltaLakeOutputSpec:
    partition_by: str | None = None
    partition_strategy: Literal["percentile_range", "uniform_range", "hash"] | None = (
        None
    )
    n_partitions: int | Literal["auto"] = "auto"
    zorder_columns: list[str] = field(default_factory=list)
    bloom_filter_columns: list[str] = field(default_factory=list)
    source_geometry_column: str | None = None
    source_table: str | None = None
    bounds: list | None = None


def _adbc_fetchone(connection_string: str, query: str):
    """Execute *query* via ADBC and return a single row (or ``None``)."""
    with pg_dbapi.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()


def _adbc_fetchall(connection_string: str, query: str) -> list:
    """Execute *query* via ADBC and return all rows."""
    with pg_dbapi.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


def _resolve_select_columns(
    connection_string: str,
    source: TableSource,
    drop_columns: list[str],
) -> list[str]:
    """Query column names from the source table(s) and exclude *drop_columns*.

    Returns the ordered list of column names to SELECT.  Columns present
    in *drop_columns* but absent from the table are silently ignored.
    When the source is a JOIN, columns from both tables are included
    (de-duplicated, preserving order).
    """
    drop_set = set(drop_columns)
    seen: set[str] = set()
    columns: list[str] = []

    for tbl in source.table_names:
        query = (
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{tbl}' ORDER BY ordinal_position"
        )
        rows = _adbc_fetchall(connection_string, query)
        for (col_name,) in rows:
            if col_name not in drop_set and col_name not in seen:
                columns.append(col_name)
                seen.add(col_name)

    return columns


def discover_default_output_specs(
    source: TableSource,
    engine: Engine,
) -> list[DeltaLakeOutputSpec]:
    """Derive one output spec per indexed column on the exported table(s).

    Indexes from all physical tables in *source* are inspected so that
    segmentation columns (e.g. root-id columns) are also considered for
    partitioning.  Each emitted spec records which physical table its
    partition column came from (``source_table``), so that downstream
    pre-queries (e.g. percentile boundaries) can target the correct table
    without an unnecessary JOIN.

    For non-spatial (B-tree) indexes, each indexed column becomes the
    partition column and z-order column.  For spatial (GiST) indexes,
    at most **one** column is selected using the prefix priority
    ``ctr > post > pre > pt > bb``.  The chosen column is partitioned
    on a Morton code derived from its decoded coordinates, with z-ordering
    on the individual coordinate columns.  Timestamp columns are skipped.
    """
    inspector = inspect(engine)

    # Gather indexes from all physical tables, tracking provenance.
    _timestamp_types = {
        "TIMESTAMP",
        "TIMESTAMP WITHOUT TIME ZONE",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMPTZ",
    }
    timestamp_columns: set[str] = set()
    # List of (index_dict, owning_table_name) tuples.
    indexed_entries: list[tuple[dict, str]] = []

    for tbl in source.table_names:
        indexed_entries.extend((idx, tbl) for idx in inspector.get_indexes(tbl))

        for c in inspector.get_columns(tbl):
            try:
                type_str = str(c["type"]).upper()
            except Exception:
                continue
            if type_str in _timestamp_types:
                timestamp_columns.add(c["name"])

    specs: list[DeltaLakeOutputSpec] = []

    # Always include a spec for the primary key ``id`` column.
    # get_indexes() excludes the implicit PK index, but partitioning by
    # ``id`` is useful as a baseline output (e.g. for row-level lookups).
    pk = inspector.get_pk_constraint(source.annotation_table)
    pk_columns = pk.get("constrained_columns", []) if pk else []
    if pk_columns:
        pk_col = pk_columns[0]
        specs.append(
            DeltaLakeOutputSpec(
                partition_by=pk_col,
                partition_strategy="uniform_range",
                n_partitions="auto",
                zorder_columns=[pk_col],
                bloom_filter_columns=[],
                source_table=source.annotation_table,
            )
        )

    # Spatial column prefix priority (lower index = higher priority).
    _spatial_prefix_priority = ["ctr", "post", "pre", "pt", "bb"]

    def _spatial_col_rank(col_name: str) -> int:
        for i, prefix in enumerate(_spatial_prefix_priority):
            if col_name.startswith(prefix):
                return i
        return len(_spatial_prefix_priority)

    # Collect spatial (GiST) candidates; pick at most one after the loop.
    spatial_candidates: list[tuple[str, str]] = []  # (column_name, owning_table)

    for idx, owning_table in indexed_entries:
        column_names = idx.get("column_names", [])
        if not column_names:
            continue
        col = column_names[0]

        # Skip timestamp columns — they are typically metadata
        # (created, deleted) and don't benefit from partitioning.
        if col in timestamp_columns:
            continue

        dialect_options = idx.get("dialect_options", {})
        if "gist" in (dialect_options or {}).values():
            spatial_candidates.append((col, owning_table))
        else:
            specs.append(
                DeltaLakeOutputSpec(
                    partition_by=col,
                    partition_strategy="uniform_range",
                    n_partitions="auto",
                    zorder_columns=[col],
                    bloom_filter_columns=[],
                    source_table=owning_table,
                )
            )

    # Select at most one spatial column, preferring ctr > post > pre > pt > bb.
    if spatial_candidates:
        spatial_candidates.sort(key=lambda c: _spatial_col_rank(c[0]))
        col, owning_table = spatial_candidates[0]
        # Spatial index — partition on Morton code, z-order on coordinates
        # NOTE: using the uniform range approach here as the percentile approach
        # won't work without some extra tooling, as the morton column doesn't
        # exist in the db
        specs.append(
            DeltaLakeOutputSpec(
                partition_by=f"{col}_morton",
                partition_strategy="uniform_range",
                n_partitions="auto",
                zorder_columns=[f"{col}_x", f"{col}_y", f"{col}_z"],
                bloom_filter_columns=[],
                source_geometry_column=col,
                source_table=owning_table,
            )
        )

    return specs


# Fallback estimate when pg_class stats are unavailable.
_DEFAULT_BYTES_PER_ROW = 200


def estimate_bytes_per_row(
    connection_string: str,
    source: TableSource,
) -> int:
    """Estimate per-row byte size from Postgres catalog statistics.

    Sums ``pg_class.relpages`` and ``pg_class.reltuples`` across all
    physical tables in *source* (to account for join-widened rows) and
    derives an on-disk average row width.  Falls back to
    ``_DEFAULT_BYTES_PER_ROW`` if no table has stats.
    """
    total_pages = 0
    total_tuples = 0.0

    for tbl in source.table_names:
        query = f"SELECT relpages, reltuples FROM pg_class WHERE relname = '{tbl}'"
        row = _adbc_fetchone(connection_string, query)
        if row is not None and row[0] > 0 and row[1] > 0:
            total_pages += row[0]
            total_tuples = max(total_tuples, row[1])  # rows are shared via JOIN

    if total_tuples <= 0 or total_pages <= 0:
        return _DEFAULT_BYTES_PER_ROW

    # Each page is 8 KiB in Postgres.
    return int(total_pages * 8192 / total_tuples)


def resolve_n_partitions(
    n_partitions: int | Literal["auto"],
    row_count: int,
    target_file_size_mb: int = 256,
    bytes_per_row: int = _DEFAULT_BYTES_PER_ROW,
) -> int:
    """Return a concrete partition count.

    When *n_partitions* is ``"auto"``, compute from *row_count* and the
    target file size heuristic.  Otherwise pass through the explicit value.

    For a table-specific estimate of *bytes_per_row*, call
    :func:`estimate_bytes_per_row` and pass the result here.
    """
    if n_partitions != "auto":
        return int(n_partitions)

    target_bytes = target_file_size_mb * 1024 * 1024
    estimated_total_bytes = row_count * bytes_per_row
    n = max(1, math.ceil(estimated_total_bytes / target_bytes))
    return n


def compute_partition_boundaries(
    connection_string: str,
    table_name: str,
    column_name: str,
    n_partitions: int,
) -> list:
    """Query Postgres for approximate percentile boundaries.

    Returns a sorted list of ``n_partitions - 1`` boundary values that
    split *column_name* into roughly equal-sized partitions.
    """
    if n_partitions <= 1:
        return []

    fractions = [i / n_partitions for i in range(1, n_partitions)]
    fractions_sql = ", ".join(str(f) for f in fractions)

    query = (
        f"SELECT percentile_disc(ARRAY[{fractions_sql}]) "
        f'WITHIN GROUP (ORDER BY "{column_name}") '
        f'FROM "{table_name}"'
    )

    row = _adbc_fetchone(connection_string, query)

    # percentile_disc with an array argument returns a single-element row
    # containing a Postgres array → Python list.
    boundaries = list(row[0])
    return boundaries


def _parse_box3d(box3d_str: str) -> tuple[float, float, float, float, float, float]:
    """Parse a PostGIS ``BOX3D(xmin ymin zmin, xmax ymax zmax)`` string.

    Returns ``(x_min, y_min, z_min, x_max, y_max, z_max)``.
    """
    # Strip the "BOX3D(" prefix and ")" suffix.
    inner = box3d_str.strip().removeprefix("BOX3D(").removesuffix(")")
    lo, hi = inner.split(",")
    x_min, y_min, z_min = (float(v) for v in lo.split())
    x_max, y_max, z_max = (float(v) for v in hi.split())
    return x_min, y_min, z_min, x_max, y_max, z_max


def compute_uniform_range_bounds(
    connection_string: str,
    table_name: str,
    column_name: str,
    source_geometry_column: str | None = None,
    sample_pages: int = 1000,
) -> tuple[float, float]:
    """Query Postgres for the min/max range of a partition column.

    For geometry-derived Morton columns (*source_geometry_column* is set),
    uses ``ST_3DExtent`` with ``TABLESAMPLE SYSTEM`` to approximate the
    3-D bounding box from a page-level sample, then Morton-encodes the
    corners.  *sample_pages* controls how many 8 KiB pages are sampled
    (default 1000 ≈ 8 MB of heap data).

    For regular columns, queries ``MIN``/``MAX`` directly (no sampling).

    Returns a ``(min_val, max_val)`` tuple suitable for
    :func:`assign_partition`.
    """
    if source_geometry_column is not None:
        col = source_geometry_column
        query = (
            f"SELECT ST_XMin(bbox), ST_YMin(bbox), ST_ZMin(bbox), "
            f"ST_XMax(bbox), ST_YMax(bbox), ST_ZMax(bbox) FROM ("
            f'SELECT ST_3DExtent("{col}") AS bbox '
            f'FROM "{table_name}" TABLESAMPLE SYSTEM ('
            f"{sample_pages}.0 / ("
            f"SELECT relpages FROM pg_class WHERE relname = '{table_name}'"
            f") * 100)) sub"
        )
        row = _adbc_fetchone(connection_string, query)
        x_min, y_min, z_min, x_max, y_max, z_max = (float(v) for v in row)

        # Morton-encode the bounding-box corners.  The actual min/max
        # Morton values in the data may not correspond to opposite
        # corners, but assign_partition clips out-of-range
        # values so slightly wider bounds are safe.
        corners = morton_encode_3d(
            np.array([x_min, x_max], dtype=np.uint64),
            np.array([y_min, y_max], dtype=np.uint64),
            np.array([z_min, z_max], dtype=np.uint64),
        )
        return float(corners.min()), float(corners.max())
    else:
        query = (
            f'SELECT MIN("{column_name}")::float, MAX("{column_name}")::float '
            f'FROM "{table_name}"'
        )
        row = _adbc_fetchone(connection_string, query)
        return float(row[0]), float(row[1])


def resolve_bounds(
    spec: DeltaLakeOutputSpec,
    connection_string: str,
    table_name: str,
) -> None:
    """Populate ``spec.bounds`` in place if not already set.

    Dispatches on ``spec.partition_strategy``:

    * ``percentile_range`` → :func:`compute_partition_boundaries` (list of
      N-1 breakpoints stored directly).
    * ``uniform_range`` → :func:`compute_uniform_range_bounds` followed
      by ``np.linspace`` to produce the same N-1 interior breakpoints.
    * ``hash`` / ``None`` → no-op (hash partitioning doesn't use bounds).

    If ``spec.bounds`` is already non-``None`` (user-supplied), this
    function is a no-op regardless of strategy.
    """
    if spec.bounds is not None:
        return
    if spec.partition_by is None or spec.partition_strategy in (None, "hash"):
        return

    boundary_table = spec.source_table or table_name
    n = spec.n_partitions if isinstance(spec.n_partitions, int) else 1

    if spec.partition_strategy == "percentile_range":
        spec.bounds = compute_partition_boundaries(
            connection_string,
            boundary_table,
            spec.partition_by,
            n,
        )

    elif spec.partition_strategy == "uniform_range":
        col_min, col_max = compute_uniform_range_bounds(
            connection_string,
            boundary_table,
            spec.partition_by,
            source_geometry_column=spec.source_geometry_column,
        )
        if n <= 1 or col_min == col_max:
            spec.bounds = []
        else:
            spec.bounds = np.linspace(col_min, col_max, n + 1)[1:-1].tolist()


def assign_partition(
    table: pl.DataFrame,
    column_name: str,
    breakpoints: list,
) -> pl.DataFrame:
    """Add a ``{column_name}_partition`` column using pre-computed breakpoints.

    Works for any strategy that produces breakpoints (percentile-range,
    uniform-range, or user-supplied).  Rows are assigned to bins defined by
    ``pl.cut`` over the sorted *breakpoints*.

    If *breakpoints* is empty, all rows land in partition 0.
    """
    partition_col = f"{column_name}_partition"

    if not breakpoints:
        return table.with_columns(pl.lit(0).cast(pl.Int32).alias(partition_col))

    col = table[column_name]
    sorted_breaks = sorted(breakpoints)

    partition_series = col.cast(pl.Float64).cut(
        breaks=[float(b) for b in sorted_breaks],
        labels=[str(i) for i in range(len(sorted_breaks) + 1)],
    )
    return table.with_columns(
        partition_series.cast(pl.Utf8).cast(pl.Int32).alias(partition_col)
    )


def assign_hash_partition(
    table: pl.DataFrame,
    column_name: str,
    n_partitions: int,
) -> pl.DataFrame:
    """Add a ``{column_name}_partition`` column using hash partitioning.

    ``hash(value) % n_partitions`` distributes rows across partitions.
    """
    partition_col = f"{column_name}_partition"

    if n_partitions <= 1:
        return table.with_columns(pl.lit(0).cast(pl.Int32).alias(partition_col))

    partition_expr = (pl.col(column_name).hash() % n_partitions).cast(pl.Int32)
    return table.with_columns(partition_expr.alias(partition_col))


def stream_table_to_arrow(
    connection_string: str,
    source: TableSource,
    chunk_size: int = 1_000_000,
    row_limit: int | None = None,
    drop_columns: list[str] | None = None,
):
    """Stream a table from a frozen Postgres DB as Arrow RecordBatches.

    Yields :class:`pyarrow.RecordBatch` objects.  The driver determines
    batch sizing; *chunk_size* is reserved for future use (e.g. setting
    an ADBC batch-size hint).

    If *drop_columns* is set, those columns are excluded from the SQL
    ``SELECT`` so they never leave Postgres.  This reduces data over the
    wire and lowers memory pressure in the streaming buffer.

    If *row_limit* is set, a SQL ``LIMIT`` clause is appended so that at
    most that many rows are streamed.  Useful for local testing; does not
    affect partition calculations upstream.
    """
    if drop_columns:
        columns = _resolve_select_columns(connection_string, source, drop_columns)
        select_clause = ", ".join(f'"{c}"' for c in columns)
    else:
        select_clause = "*"

    query = f"SELECT {select_clause} FROM {source.from_clause}"
    if row_limit is not None:
        query += f" LIMIT {int(row_limit)}"

    with pg_dbapi.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            reader = cur.fetch_record_batch()
            for batch in reader:
                yield batch


# TODO see if this is already implemented somewhere easy to import to reduce footprint
def _spread_bits_21(v: np.ndarray) -> np.ndarray:
    """Spread a 21-bit value so each bit occupies every 3rd position.

    Standard bit-interleaving step for 3D Morton codes.  Input values
    must be unsigned 64-bit integers with at most 21 significant bits.
    """
    v = np.asarray(v, dtype=np.uint64)
    v &= np.uint64(0x1FFFFF)
    v = (v | (v << np.uint64(32))) & np.uint64(0x001F00000000FFFF)
    v = (v | (v << np.uint64(16))) & np.uint64(0x001F0000FF0000FF)
    v = (v | (v << np.uint64(8))) & np.uint64(0x100F00F00F00F00F)
    v = (v | (v << np.uint64(4))) & np.uint64(0x10C30C30C30C30C3)
    v = (v | (v << np.uint64(2))) & np.uint64(0x1249249249249249)
    return v


# TODO see if this is already implemented somewhere easy to import to reduce footprint
def morton_encode_3d(x: np.ndarray, y: np.ndarray, z: np.ndarray) -> np.ndarray:
    """Bit-interleave three coordinate arrays into 63-bit Morton codes.

    Each coordinate should fit in 21 bits (values 0 – 2 097 151).
    Returns an ``int64`` array.
    """
    return (
        _spread_bits_21(x)
        | (_spread_bits_21(y) << np.uint64(1))
        | (_spread_bits_21(z) << np.uint64(2))
    ).astype(np.int64)


def decode_geometry_columns(
    table: pl.DataFrame,
    geometry_columns: list[str],
    morton_columns: list[str] | None = None,
) -> pl.DataFrame:
    """Replace WKB geometry columns with decoded coordinate columns.

    For each column in *geometry_columns*:

    * Decode WKB binary → x, y, z (Int32) columns named
      ``{col}_x``, ``{col}_y``, ``{col}_z``.
    * Drop the original binary column.

    If the column also appears in *morton_columns*, compute a Morton code
    column ``{col}_morton`` (Int64) via bit-interleaving of coordinates.

    .. warning::

       Morton encoding currently assumes coordinates are non-negative and
       fit in 21 bits (< 2 097 152).  Negative values (possible if the
       column stores signed Int32 in the DB) will wrap to large unsigned
       values and be silently truncated.  Values exceeding 21 bits will
       also be truncated.  Both cases degrade spatial locality of the
       resulting Morton codes but do not cause errors.
    """
    # TODO: normalise coordinates before Morton encoding so that negative
    #  and/or large-range values are handled correctly.  This requires
    #  global coordinate bounds (min/max per axis across the full table)
    #  so the shift + coarsen step is consistent across streaming flushes.
    #  Options:
    #    A. Pre-query ST_X/ST_Y/ST_Z min/max from Postgres (needs PostGIS).
    #    B. Pre-scan a small sample batch to estimate bounds.
    #    C. Accept caller-supplied bounds via export_table_to_deltalake.
    #  Until then, Morton codes are only fully correct for non-negative
    #  coordinates that fit in 21 bits.
    morton_set = set(morton_columns or [])

    for col in geometry_columns:
        wkb_data = table[col].to_list()
        n_rows = len(wkb_data)

        # Build a boolean mask of non-null geometries.
        valid_mask = np.array([v is not None for v in wkb_data], dtype=bool)
        n_valid = int(valid_mask.sum())

        if n_valid == 0:
            # All nulls — emit null columns and skip decoding.
            new_cols = [
                pl.Series(f"{col}_x", [None] * n_rows, dtype=pl.Int32),
                pl.Series(f"{col}_y", [None] * n_rows, dtype=pl.Int32),
                pl.Series(f"{col}_z", [None] * n_rows, dtype=pl.Int32),
            ]
            if col in morton_set:
                new_cols.append(
                    pl.Series(f"{col}_morton", [None] * n_rows, dtype=pl.Int64)
                )
        else:
            # Decode only non-null geometries, then scatter back.
            valid_wkb = [wkb_data[i] for i in range(n_rows) if valid_mask[i]]
            points = shapely.from_wkb(valid_wkb)
            coords = shapely.get_coordinates(points, include_z=True)

            if n_valid == n_rows:
                # Fast path — no nulls to scatter.
                x = coords[:, 0].astype(np.int32)
                y = coords[:, 1].astype(np.int32)
                z = coords[:, 2].astype(np.int32)
            else:
                # Scatter decoded coords into full-length nullable arrays.
                x_full = np.full(n_rows, None, dtype=object)
                y_full = np.full(n_rows, None, dtype=object)
                z_full = np.full(n_rows, None, dtype=object)
                x_full[valid_mask] = coords[:, 0].astype(np.int32)
                y_full[valid_mask] = coords[:, 1].astype(np.int32)
                z_full[valid_mask] = coords[:, 2].astype(np.int32)
                x, y, z = x_full, y_full, z_full

            new_cols = [
                pl.Series(f"{col}_x", x, dtype=pl.Int32),
                pl.Series(f"{col}_y", y, dtype=pl.Int32),
                pl.Series(f"{col}_z", z, dtype=pl.Int32),
            ]

            if col in morton_set:
                if n_valid == n_rows:
                    morton = morton_encode_3d(
                        x.astype(np.uint64),
                        y.astype(np.uint64),
                        z.astype(np.uint64),
                    )
                else:
                    morton = np.full(n_rows, None, dtype=object)
                    valid_x = coords[:, 0].astype(np.uint64)
                    valid_y = coords[:, 1].astype(np.uint64)
                    valid_z = coords[:, 2].astype(np.uint64)
                    morton[valid_mask] = morton_encode_3d(valid_x, valid_y, valid_z)
                new_cols.append(pl.Series(f"{col}_morton", morton, dtype=pl.Int64))

        table = table.with_columns(new_cols).drop(col)

    return table


def _strip_arrow_extension_types(table: pa.Table) -> pa.Table:
    """Replace all Arrow extension-typed columns with their plain storage arrays.

    ADBC streams PostGIS geometry columns as binary with the
    ``arrow.opaque`` extension type.  This metadata is meaningless once
    the data leaves Postgres — we decode WKB ourselves — and causes
    warnings when Polars encounters an unregistered extension type.

    Stripping extension wrappers here, at the Arrow ↔ Polars boundary,
    is the correct fix: we're removing semantically void metadata before a
    conversion where it would only cause confusion.
    """
    for idx, field in enumerate(table.schema):
        if not isinstance(field.type, pa.BaseExtensionType):
            continue
        col = table.column(idx)
        storage_type = field.type.storage_type
        storage_chunks = [chunk.storage for chunk in col.chunks]
        plain = pa.chunked_array(storage_chunks, type=storage_type)
        table = table.set_column(idx, pa.field(field.name, storage_type), plain)
    return table


def _flush_buffer(
    buffer: list[pa.RecordBatch],
    output_specs: list[DeltaLakeOutputSpec],
    output_uri_base: str,
    geometry_columns: list[str],
    morton_columns: list[str],
) -> None:
    """Convert accumulated batches to Polars, decode geometry, assign
    partitions, and append to each target Delta Lake."""
    from deltalake import write_deltalake

    arrow_table = pa.Table.from_batches(buffer)

    # Strip extension types (e.g. arrow.opaque on PostGIS columns) before
    # Polars conversion.
    # NOTE: I've debated whether to make this decoding happen on the Postgres side
    # but it somehow felt more robust to have a piece of code that could handle whatever
    arrow_table = _strip_arrow_extension_types(arrow_table)

    df = pl.from_arrow(arrow_table)

    # Decode geometry columns (and Morton codes) once for all specs.
    if geometry_columns:
        df = decode_geometry_columns(df, geometry_columns, morton_columns)

    for spec in output_specs:
        write_df = df
        partition_by: list[str] | None = None

        if spec.partition_by is not None and spec.partition_strategy is not None:
            part_col = spec.partition_by

            if spec.bounds is not None:
                write_df = assign_partition(write_df, part_col, spec.bounds)
                partition_by = [f"{part_col}_partition"]

            elif spec.partition_strategy == "hash":
                n = spec.n_partitions if isinstance(spec.n_partitions, int) else 1
                write_df = assign_hash_partition(write_df, part_col, n)
                partition_by = [f"{part_col}_partition"]

        # Build the URI for this particular Delta Lake.
        lake_name = spec.partition_by or "flat"
        uri = f"{output_uri_base}/{lake_name}"

        write_deltalake(
            uri,
            write_df.to_arrow(),
            mode="append",
            partition_by=partition_by,
        )


def export_table_to_deltalake(
    connection_string: str,
    source: TableSource,
    output_specs: list[DeltaLakeOutputSpec],
    output_uri_base: str,
    chunk_size: int = 1_000_000,
    flush_threshold_bytes: int = 2 * 1024 * 1024 * 1024,
    total_rows: int | None = None,
    progress_callback: Callable[[int, int | None], None] | None = None,
    row_limit: int | None = None,
) -> None:
    """Stream a table from Postgres and write to one or more Delta Lakes.

    This is the main entry point for the export pipeline.  It:

    1. Streams Arrow batches from the frozen Postgres DB.
    2. Accumulates batches until *flush_threshold_bytes* is exceeded.
    3. On each flush: decodes geometry, assigns partition buckets, and
       appends to each target Delta Lake.

    Partition bounds must be pre-resolved on each spec's ``bounds`` field
    (via :func:`resolve_bounds`) before calling this function, so that bin
    edges are consistent across flushes.

    Parameters
    ----------
    progress_callback
        If provided, called after each Arrow batch with
        ``(rows_processed_so_far, total_rows)``.  *total_rows* may be
        ``None`` if the caller doesn't know the table size.
    total_rows
        Total expected row count (e.g. from ``MaterializedMetadata``).
        Passed through to *progress_callback*.
    row_limit
        If set, only stream this many rows from Postgres (SQL ``LIMIT``).
        Useful for local testing.  Does **not** affect partition
        calculations or bounds — those still use the full table.
    """
    if row_limit is not None and total_rows is not None:
        total_rows = min(total_rows, row_limit)
    # Collect geometry columns that need decoding and Morton codes.
    geometry_columns = sorted(
        {
            spec.source_geometry_column
            for spec in output_specs
            if spec.source_geometry_column is not None
        }
    )
    morton_columns = [
        spec.source_geometry_column
        for spec in output_specs
        if spec.source_geometry_column is not None
    ]

    buffer: list[pa.RecordBatch] = []
    buffer_bytes = 0
    rows_processed = 0

    for batch in stream_table_to_arrow(
        connection_string,
        source,
        chunk_size,
        row_limit=row_limit,
        drop_columns=_DEFAULT_DROP_COLUMNS,
    ):
        buffer.append(batch)
        buffer_bytes += batch.nbytes
        rows_processed += batch.num_rows

        if progress_callback is not None:
            progress_callback(rows_processed, total_rows)

        if buffer_bytes >= flush_threshold_bytes:
            _flush_buffer(
                buffer,
                output_specs,
                output_uri_base,
                geometry_columns,
                morton_columns,
            )
            buffer = []
            buffer_bytes = 0

    # Flush any remaining data.
    if buffer:
        _flush_buffer(
            buffer,
            output_specs,
            output_uri_base,
            geometry_columns,
            morton_columns,
        )

    # Optimize each Delta Lake: z-order, bloom filters, and vacuum.
    for spec in output_specs:
        lake_name = spec.partition_by or "flat"
        uri = f"{output_uri_base}/{lake_name}"
        optimize_deltalake(
            uri,
            zorder_columns=spec.zorder_columns or None,
            bloom_filter_columns=spec.bloom_filter_columns or None,
        )


def optimize_deltalake(
    uri: str,
    zorder_columns: list[str] | None = None,
    bloom_filter_columns: list[str] | None = None,
    fpp: float = 0.001,
) -> None:
    """Z-order, add bloom filters, and vacuum a completed Delta Lake.

    Parameters
    ----------
    uri
        Path or URI of the Delta Lake table.
    zorder_columns
        Columns to z-order on.  If empty or ``None``, runs ``compact``
        instead (file compaction without reordering).
    bloom_filter_columns
        Columns to create bloom filters for.  Bloom filter properties
        are passed to the optimizer via ``WriterProperties``.
    fpp
        False-positive probability for bloom filters.
    """
    from deltalake import DeltaTable
    from deltalake.writer import (
        BloomFilterProperties,
        ColumnProperties,
        WriterProperties,
    )

    dt = DeltaTable(uri)

    writer_properties = None
    if bloom_filter_columns:
        bloom = BloomFilterProperties(set_bloom_filter_enabled=True, fpp=fpp)
        col_props = ColumnProperties(bloom_filter_properties=bloom)
        writer_properties = WriterProperties(
            column_properties={col: col_props for col in bloom_filter_columns}
        )

    if zorder_columns:
        dt.optimize.z_order(
            columns=zorder_columns,
            writer_properties=writer_properties,
        )
    else:
        dt.optimize.compact(writer_properties=writer_properties)

    try:
        dt.vacuum(
            dry_run=False,
            retention_hours=0,
            enforce_retention_duration=False,
            full=True,
        )
    except Exception as exc:
        # delta-rs can fail on tables with all-null columns due to
        # "unmasked nulls for non-nullable StructArray" in the
        # transaction log stats.  Log and continue — the data is
        # already written and optimized; vacuum just cleans up old files.
        celery_logger.warning("vacuum failed for %s: %s", uri, exc)


def make_tqdm_progress_callback(
    total_rows: int,
    **tqdm_kwargs,
) -> tuple[Callable[[int, int | None], None], Callable[[], None]]:
    """Create a tqdm-based progress callback for :func:`export_table_to_deltalake`.

    Returns ``(callback, close)`` — pass *callback* as the
    ``progress_callback`` argument; call *close* when the export is done
    to finalize the progress bar.

    Example::

        cb, close = make_tqdm_progress_callback(row_count)
        try:
            export_table_to_deltalake(..., total_rows=row_count, progress_callback=cb)
        finally:
            close()
    """
    from tqdm import tqdm

    bar = tqdm(total=total_rows, unit="rows", **tqdm_kwargs)
    _prev = {"rows": 0}

    def _callback(rows_so_far: int, _total: int | None) -> None:
        delta = rows_so_far - _prev["rows"]
        if delta > 0:
            bar.update(delta)
        _prev["rows"] = rows_so_far

    def _close() -> None:
        bar.close()

    return _callback, _close


_DELTALAKE_PROGRESS_TTL = 86400  # 24 hours


def deltalake_export_redis_key(datastack: str, version: int, table_name: str) -> str:
    """Return the Redis key used to track Delta Lake export progress."""
    return f"deltalake_export:{datastack}:v{version}:{table_name}"


def _get_redis_client():
    """Lazy-create a Redis client for deltalake export progress."""
    import redis

    from materializationengine.utils import get_config_param

    return redis.StrictRedis(
        host=get_config_param("REDIS_HOST"),
        port=get_config_param("REDIS_PORT"),
        password=get_config_param("REDIS_PASSWORD"),
        db=0,
    )


def make_redis_progress_callback(
    datastack: str,
    version: int,
    table_name: str,
) -> Callable[[int, int | None], None]:
    """Create a Redis-backed progress callback for :func:`export_table_to_deltalake`.

    Writes a JSON blob to Redis on each invocation so that API consumers
    can poll export progress.  The key expires after 24 hours.
    """
    client = _get_redis_client()
    key = deltalake_export_redis_key(datastack, version, table_name)

    def _callback(rows_so_far: int, total: int | None) -> None:
        pct = (rows_so_far / total * 100) if total else None
        payload = {
            "status": "exporting",
            "rows_processed": rows_so_far,
            "total_rows": total,
            "percent_complete": round(pct, 2) if pct is not None else None,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        client.set(key, json.dumps(payload), ex=_DELTALAKE_PROGRESS_TTL)

    return _callback


def set_deltalake_export_status(
    datastack: str,
    version: int,
    table_name: str,
    status: str,
    total_rows: int | None = None,
    rows_processed: int | None = None,
) -> None:
    """Write a terminal status (``complete``, ``failed``, etc.) to Redis."""
    client = _get_redis_client()
    key = deltalake_export_redis_key(datastack, version, table_name)
    pct = None
    if rows_processed is not None and total_rows:
        pct = round(rows_processed / total_rows * 100, 2)
    payload = {
        "status": status,
        "rows_processed": rows_processed,
        "total_rows": total_rows,
        "percent_complete": pct,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    client.set(key, json.dumps(payload), ex=_DELTALAKE_PROGRESS_TTL)


def get_deltalake_export_progress(
    datastack: str,
    version: int,
    table_name: str,
) -> dict | None:
    """Read the current export progress from Redis.

    Returns the progress dict, or ``None`` if no export is tracked.
    """
    client = _get_redis_client()
    key = deltalake_export_redis_key(datastack, version, table_name)
    raw = client.get(key)
    if raw is None:
        return None
    return json.loads(raw)


def _build_frozen_db_connection_string(
    sql_uri_config: str,
    datastack: str,
    version: int,
) -> str:
    """Build a ``postgresql://`` connection string for a frozen DB.

    The frozen DB naming convention is ``{datastack}__mat{version}``.
    *sql_uri_config* is the base SQLAlchemy URI (e.g. from
    ``SQLALCHEMY_DATABASE_URI``); the database name is replaced.
    """
    base = sql_uri_config.rpartition("/")[0]
    return f"{base}/{datastack}__mat{version}"


@celery.task(
    name="deltalake:write_deltalake_table",
    bind=True,
    acks_late=True,
)
def write_deltalake_table(
    self,
    datastack_info: dict,
    version: int,
    table_name: str,
    output_specs: list[dict] | None = None,
) -> None:
    """Orchestrate a full Delta Lake export for one table.

    1. Resolve output specs (from *output_specs* arg or index-derived
       defaults).
    2. Compute partition boundaries.
    3. Stream the table from the frozen DB → Delta Lake.
    4. Optimize each Delta Lake (z-order, bloom filters, vacuum).

    Parameters
    ----------
    datastack_info
        Datastack info dict (from ``get_datastack_info``).
    version
        Materialization version number.
    table_name
        Annotation table name in the frozen DB.
    output_specs
        Optional list of output spec dicts.  If ``None``, defaults are
        derived from table indexes.
    """
    from dynamicannotationdb.key_utils import build_segmentation_table_name
    from sqlalchemy import create_engine

    from materializationengine.database import db_manager
    from materializationengine.models import MaterializedMetadata
    from materializationengine.utils import get_config_param

    datastack = datastack_info["datastack"]
    pcg_table_name = datastack_info["segmentation_source"].split("/")[-1]
    analysis_database = f"{datastack}__mat{version}"

    sql_uri_config = get_config_param("SQLALCHEMY_DATABASE_URI")
    connection_string = _build_frozen_db_connection_string(
        sql_uri_config, datastack, version
    )

    output_bucket = get_config_param("DELTALAKE_OUTPUT_BUCKET")
    if not output_bucket:
        raise ValueError("DELTALAKE_OUTPUT_BUCKET not set in app config")
    output_uri_base = f"{output_bucket}/{datastack}/v{version}/{table_name}"

    flush_threshold = get_config_param(
        "DELTALAKE_FLUSH_THRESHOLD_BYTES", 2 * 1024 * 1024 * 1024
    )
    chunk_size = get_config_param("DELTALAKE_CHUNK_SIZE", 1_000_000)
    target_partition_size_mb = get_config_param(
        "DELTALAKE_TARGET_PARTITION_SIZE_MB", 256
    )

    # --- Resolve table structure from frozen DB metadata ---
    engine = db_manager.get_engine(analysis_database)

    # Determine if the table was merged and look up row count.
    with db_manager.session_scope(analysis_database) as session:
        metadata_row = (
            session.query(MaterializedMetadata)
            .filter(MaterializedMetadata.table_name == table_name)
            .first()
        )
        if metadata_row is None:
            raise ValueError(
                f"No MaterializedMetadata entry for table {table_name!r} "
                f"in {analysis_database}"
            )
        row_count = metadata_row.row_count

    # Detect segmentation table presence.
    seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
    has_seg_table = engine.dialect.has_table(engine, seg_table_name)
    # If merged (no separate seg table), segmentation_table stays None.
    segmentation_table_name = seg_table_name if has_seg_table else None

    source = TableSource(
        annotation_table=table_name,
        segmentation_table=segmentation_table_name,
    )

    # --- Resolve output specs ---
    if output_specs is not None:
        resolved_specs = [DeltaLakeOutputSpec(**s) for s in output_specs]
    else:
        resolved_specs = discover_default_output_specs(source, engine)

    if not resolved_specs:
        celery_logger.warning(
            "No output specs for table %s — skipping Delta Lake export", table_name
        )
        return

    # --- Partial-export detection (task 8.4) ---
    for spec in resolved_specs:
        lake_name = spec.partition_by or "flat"
        uri = f"{output_uri_base}/{lake_name}"
        try:
            from deltalake import DeltaTable

            dt = DeltaTable(uri)
            existing_rows = dt.to_pyarrow_dataset().count_rows()
        except Exception:
            existing_rows = None

        if existing_rows is not None and existing_rows != row_count:
            raise RuntimeError(
                f"Delta Lake for table {table_name!r} already exists at "
                f"{uri} but has {existing_rows} rows (expected {row_count}). "
                f"This may be the result of a partial export. "
                f"Delete the existing Delta Lake before re-exporting."
            )

    # --- Estimate bytes per row and resolve partition counts / bounds ---
    bytes_per_row = estimate_bytes_per_row(connection_string, source)

    for spec in resolved_specs:
        if spec.n_partitions == "auto":
            spec.n_partitions = resolve_n_partitions(
                "auto",
                row_count,
                target_file_size_mb=target_partition_size_mb,
                bytes_per_row=bytes_per_row,
            )

        resolve_bounds(spec, connection_string, table_name)

    # --- Stream and write ---
    celery_logger.info(
        "Exporting table %s (v%d) to Delta Lake: %d specs, %d rows",
        table_name,
        version,
        len(resolved_specs),
        row_count,
    )

    def _log_progress(rows_so_far: int, total: int | None) -> None:
        if total:
            pct = rows_so_far / total * 100
            celery_logger.info(
                "Delta Lake export progress for %s (v%d): %d / %d rows (%.1f%%)",
                table_name,
                version,
                rows_so_far,
                total,
                pct,
            )
        else:
            celery_logger.info(
                "Delta Lake export progress for %s (v%d): %d rows",
                table_name,
                version,
                rows_so_far,
            )

    redis_callback = make_redis_progress_callback(datastack, version, table_name)

    def _progress(rows_so_far: int, total: int | None) -> None:
        _log_progress(rows_so_far, total)
        redis_callback(rows_so_far, total)

    set_deltalake_export_status(
        datastack, version, table_name, "exporting", total_rows=row_count
    )

    try:
        export_table_to_deltalake(
            connection_string=connection_string,
            source=source,
            output_specs=resolved_specs,
            output_uri_base=output_uri_base,
            chunk_size=chunk_size,
            flush_threshold_bytes=flush_threshold,
            total_rows=row_count,
            progress_callback=_progress,
        )
    except Exception:
        set_deltalake_export_status(
            datastack,
            version,
            table_name,
            "failed",
            total_rows=row_count,
        )
        raise

    set_deltalake_export_status(
        datastack,
        version,
        table_name,
        "complete",
        total_rows=row_count,
        rows_processed=row_count,
    )

    celery_logger.info(
        "Delta Lake export complete for table %s (v%d)", table_name, version
    )
