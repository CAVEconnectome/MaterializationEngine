from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Literal

import adbc_driver_postgresql.dbapi as pg_dbapi
import numpy as np
import polars as pl
import pyarrow as pa
import shapely
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from materializationengine.celery_init import celery

celery_logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# 3.3  discover_default_output_specs
# ---------------------------------------------------------------------------


def discover_default_output_specs(
    table_name: str,
    segmentation_table_name: str | None,
    engine: Engine,
) -> list[DeltaLakeOutputSpec]:
    """Derive one output spec per indexed column on the exported table(s).

    When the table is unmerged (separate annotation and segmentation
    tables), indexes from both tables are inspected so that segmentation
    columns (e.g. root-id columns) are also considered for partitioning.

    For non-spatial (B-tree) indexes, each indexed column becomes the
    partition column and z-order column.  For spatial (GiST) indexes,
    emit a spec that partitions on a Morton code derived from the decoded
    coordinates and z-orders on the individual coordinate columns.
    Bounding-box columns (names starting with ``bb_``) are skipped for
    spatial indexes.  Timestamp columns are also skipped.
    """
    inspector = inspect(engine)

    # Gather indexes from all tables that will be part of the export.
    table_names = [table_name]
    if segmentation_table_name is not None:
        table_names.append(segmentation_table_name)

    indexes: list[dict] = []
    for tbl in table_names:
        indexes.extend(inspector.get_indexes(tbl))

    # Build a set of timestamp column names so we can skip them.
    # Inspect columns from all participating tables.
    _timestamp_types = {
        "TIMESTAMP",
        "TIMESTAMP WITHOUT TIME ZONE",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMPTZ",
    }
    timestamp_columns: set[str] = set()
    for tbl in table_names:
        for c in inspector.get_columns(tbl):
            try:
                type_str = str(c["type"]).upper()
            except Exception:
                continue
            if type_str in _timestamp_types:
                timestamp_columns.add(c["name"])

    specs: list[DeltaLakeOutputSpec] = []
    for idx in indexes:
        column_names = idx.get("column_names", [])
        if not column_names:
            continue
        col = column_names[0]

        # Skip timestamp columns — they are typically metadata
        # (created, deleted) and don't benefit from partitioning.
        if col in timestamp_columns:
            continue

        # NOTE: we may want to expand on different approaches in the future, e.g.
        # partitioning on one column and z-ordering on another, but for now we keep it
        # simple and just use the first column of each index for everything. This
        # helps us get going with zero-config defaults and we can iterate from there.
        dialect_options = idx.get("dialect_options", {})
        if "gist" in (dialect_options or {}).values():
            # Skip bounding-box spatial columns — they are frequently
            # all-null and produce degenerate partitions.
            if col.startswith("bb_"):
                continue
            # Spatial index — partition on Morton code, z-order on coordinates
            specs.append(
                DeltaLakeOutputSpec(
                    partition_by=f"{col}_morton",
                    partition_strategy="percentile_range",
                    n_partitions="auto",
                    zorder_columns=[f"{col}_x", f"{col}_y", f"{col}_z"],
                    bloom_filter_columns=[],
                    source_geometry_column=col,
                )
            )
        else:
            specs.append(
                DeltaLakeOutputSpec(
                    partition_by=col,
                    partition_strategy="percentile_range",
                    n_partitions="auto",
                    zorder_columns=[col],
                    bloom_filter_columns=[],
                )
            )

    return specs


# ---------------------------------------------------------------------------
# 3.4  resolve_n_partitions
# ---------------------------------------------------------------------------

# Fallback estimate when pg_class stats are unavailable.
_DEFAULT_BYTES_PER_ROW = 200


def estimate_bytes_per_row(
    connection_string: str,
    table_name: str,
) -> int:
    """Estimate per-row byte size from Postgres catalog statistics.

    Uses ``pg_class.relpages`` and ``pg_class.reltuples`` to derive an
    on-disk average row width.  Falls back to ``_DEFAULT_BYTES_PER_ROW``
    if the table has no stats (e.g. never analyzed) or zero rows.
    """
    query = f"SELECT relpages, reltuples FROM pg_class WHERE relname = '{table_name}'"

    with pg_dbapi.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()

    if row is None:
        return _DEFAULT_BYTES_PER_ROW

    relpages, reltuples = row[0], row[1]
    if reltuples <= 0 or relpages <= 0:
        return _DEFAULT_BYTES_PER_ROW

    # Each page is 8 KiB in Postgres.
    return int(relpages * 8192 / reltuples)


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


# ---------------------------------------------------------------------------
# 3.5  compute_bucket_boundaries
# ---------------------------------------------------------------------------


def compute_bucket_boundaries(
    connection_string: str,
    table_name: str,
    column_name: str,
    n_partitions: int,
) -> list:
    """Query Postgres for approximate percentile boundaries.

    Returns a sorted list of ``n_partitions - 1`` boundary values that
    split *column_name* into roughly equal-sized buckets.
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

    with pg_dbapi.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()

    # percentile_disc with an array argument returns a single-element row
    # containing a Postgres array → Python list.
    boundaries = list(row[0])
    return boundaries


# ---------------------------------------------------------------------------
# 3.6  assign_percentile_range_bucket
# ---------------------------------------------------------------------------


def assign_percentile_range_bucket(
    table: pl.DataFrame,
    column_name: str,
    boundaries: list,
) -> pl.DataFrame:
    """Add a ``{column_name}_partition`` column using percentile boundaries.

    Rows are assigned to the bucket whose upper boundary they fall into
    (or below), preserving natural ordering within each bucket.
    """
    partition_col = f"{column_name}_partition"

    col = table[column_name]
    # Build bucket labels 0 .. len(boundaries)
    # Each row gets the index of the first boundary >= its value,
    # or len(boundaries) if it exceeds all boundaries.
    breakpoints = sorted(boundaries)

    # Use Polars cut for numeric columns.
    # cut() expects the breakpoints as floats and produces a categorical.
    bucket_series = col.cast(pl.Float64).cut(
        breaks=[float(b) for b in breakpoints],
        labels=[str(i) for i in range(len(breakpoints) + 1)],
    )
    # Convert category labels to integers for partition directory names.
    return table.with_columns(
        bucket_series.cast(pl.Utf8).cast(pl.Int32).alias(partition_col)
    )


# ---------------------------------------------------------------------------
# 3.7  assign_uniform_range_bucket
# ---------------------------------------------------------------------------


def assign_uniform_range_bucket(
    table: pl.DataFrame,
    column_name: str,
    min_val: float,
    max_val: float,
    n_partitions: int,
) -> pl.DataFrame:
    """Add a ``{column_name}_partition`` column using equal-width bins.

    The ``[min_val, max_val]`` range is divided into *n_partitions*
    equal-width bins and each row is assigned to the bin it falls into.
    """
    partition_col = f"{column_name}_partition"

    if n_partitions <= 1 or min_val == max_val:
        return table.with_columns(pl.lit(0).cast(pl.Int32).alias(partition_col))

    bin_width = (max_val - min_val) / n_partitions
    bucket_expr = (
        ((pl.col(column_name).cast(pl.Float64) - min_val) / bin_width)
        .floor()
        .cast(pl.Int32)
        .clip(0, n_partitions - 1)
    )
    return table.with_columns(bucket_expr.alias(partition_col))


# ---------------------------------------------------------------------------
# 3.8  assign_hash_bucket
# ---------------------------------------------------------------------------


def assign_hash_bucket(
    table: pl.DataFrame,
    column_name: str,
    n_partitions: int,
) -> pl.DataFrame:
    """Add a ``{column_name}_partition`` column using hash bucketing.

    ``hash(value) % n_partitions`` distributes rows across buckets.
    """
    partition_col = f"{column_name}_partition"

    if n_partitions <= 1:
        return table.with_columns(pl.lit(0).cast(pl.Int32).alias(partition_col))

    bucket_expr = (pl.col(column_name).hash() % n_partitions).cast(pl.Int32)
    return table.with_columns(bucket_expr.alias(partition_col))


# ===========================================================================
# Section 5 — Core Streaming Writer
# ===========================================================================


# ---------------------------------------------------------------------------
# 5.1  stream_table_to_arrow
# ---------------------------------------------------------------------------


def _build_stream_query(
    annotation_table_name: str,
    segmentation_table_name: str | None,
    is_merged: bool,
) -> str:
    """Construct the SQL query for streaming a table from a frozen DB.

    Three cases:
    1. Merged table or no segmentation table → ``SELECT * FROM annotation``
    2. Unmerged with segmentation → ``SELECT * FROM anno JOIN seg USING (id)``
    """
    if is_merged or segmentation_table_name is None:
        return f'SELECT * FROM "{annotation_table_name}"'
    return (
        f'SELECT * FROM "{annotation_table_name}" '
        f'JOIN "{segmentation_table_name}" USING (id)'
    )


def stream_table_to_arrow(
    connection_string: str,
    annotation_table_name: str,
    segmentation_table_name: str | None = None,
    is_merged: bool = True,
    chunk_size: int = 1_000_000,
):
    """Stream a table from a frozen Postgres DB as Arrow RecordBatches.

    Yields :class:`pyarrow.RecordBatch` objects.  The driver determines
    batch sizing; *chunk_size* is reserved for future use (e.g. setting
    an ADBC batch-size hint).
    """
    query = _build_stream_query(
        annotation_table_name, segmentation_table_name, is_merged
    )

    with pg_dbapi.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            reader = cur.fetch_record_batch()
            for batch in reader:
                yield batch


# ---------------------------------------------------------------------------
# 5.3  decode_geometry_columns  (+ Morton code helpers)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 5.4  Buffered write loop
# ---------------------------------------------------------------------------


def _flush_buffer(
    buffer: list[pa.RecordBatch],
    output_specs: list[DeltaLakeOutputSpec],
    output_uri_base: str,
    geometry_columns: list[str],
    morton_columns: list[str],
    boundaries: dict[str, list],
    uniform_ranges: dict[str, tuple[float, float]],
) -> None:
    """Convert accumulated batches to Polars, decode geometry, assign
    partitions, and append to each target Delta Lake."""
    from deltalake import write_deltalake

    arrow_table = pa.Table.from_batches(buffer)
    df = pl.from_arrow(arrow_table)

    # Decode geometry columns (and Morton codes) once for all specs.
    if geometry_columns:
        df = decode_geometry_columns(df, geometry_columns, morton_columns)

    for spec in output_specs:
        write_df = df
        partition_by: list[str] | None = None

        if spec.partition_by is not None and spec.partition_strategy is not None:
            part_col = spec.partition_by

            if spec.partition_strategy == "percentile_range":
                bnd = boundaries[part_col]
                write_df = assign_percentile_range_bucket(write_df, part_col, bnd)
                partition_by = [f"{part_col}_partition"]

            elif spec.partition_strategy == "hash":
                n = spec.n_partitions if isinstance(spec.n_partitions, int) else 1
                write_df = assign_hash_bucket(write_df, part_col, n)
                partition_by = [f"{part_col}_partition"]

            elif spec.partition_strategy == "uniform_range":
                col_min, col_max = uniform_ranges[part_col]
                n = spec.n_partitions if isinstance(spec.n_partitions, int) else 1
                write_df = assign_uniform_range_bucket(
                    write_df, part_col, col_min, col_max, n
                )
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
    annotation_table_name: str,
    output_specs: list[DeltaLakeOutputSpec],
    output_uri_base: str,
    segmentation_table_name: str | None = None,
    is_merged: bool = True,
    chunk_size: int = 1_000_000,
    flush_threshold_bytes: int = 2 * 1024 * 1024 * 1024,
    boundaries: dict[str, list] | None = None,
    uniform_ranges: dict[str, tuple[float, float]] | None = None,
) -> None:
    """Stream a table from Postgres and write to one or more Delta Lakes.

    This is the main entry point for the export pipeline.  It:

    1. Streams Arrow batches from the frozen Postgres DB.
    2. Accumulates batches until *flush_threshold_bytes* is exceeded.
    3. On each flush: decodes geometry, assigns partition buckets, and
       appends to each target Delta Lake.

    *boundaries* is a dict mapping partition column names to their
    pre-computed percentile boundary lists (from
    :func:`compute_bucket_boundaries`).

    *uniform_ranges* is a dict mapping partition column names to
    ``(min, max)`` tuples for uniform-range bucketing.  Must be
    pre-computed globally so bin edges are consistent across flushes.
    """
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

    for batch in stream_table_to_arrow(
        connection_string,
        annotation_table_name,
        segmentation_table_name,
        is_merged,
        chunk_size,
    ):
        buffer.append(batch)
        buffer_bytes += batch.nbytes

        if buffer_bytes >= flush_threshold_bytes:
            _flush_buffer(
                buffer,
                output_specs,
                output_uri_base,
                geometry_columns,
                morton_columns,
                boundaries or {},
                uniform_ranges or {},
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
            boundaries or {},
            uniform_ranges or {},
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


# ===========================================================================
# Section 7 — Delta Lake Optimization
# ===========================================================================


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


# ===========================================================================
# Section 8 — Celery Task
# ===========================================================================


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
    # Check if already merged (single flat table).
    is_merged = not has_seg_table and engine.dialect.has_table(engine, table_name)
    segmentation_table_name = seg_table_name if has_seg_table else None

    # --- Resolve output specs ---
    if output_specs is not None:
        resolved_specs = [DeltaLakeOutputSpec(**s) for s in output_specs]
    else:
        resolved_specs = discover_default_output_specs(
            table_name, segmentation_table_name, engine
        )

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

    # --- Estimate bytes per row and resolve partition counts ---
    bytes_per_row = estimate_bytes_per_row(connection_string, table_name)

    boundaries: dict[str, list] = {}
    uniform_ranges: dict[str, tuple[float, float]] = {}

    for spec in resolved_specs:
        if spec.n_partitions == "auto":
            spec.n_partitions = resolve_n_partitions(
                "auto",
                row_count,
                target_file_size_mb=target_partition_size_mb,
                bytes_per_row=bytes_per_row,
            )

        if spec.partition_by and spec.partition_strategy == "percentile_range":
            boundaries[spec.partition_by] = compute_bucket_boundaries(
                connection_string,
                table_name,
                spec.partition_by,
                spec.n_partitions,
            )

    # --- Stream and write ---
    celery_logger.info(
        "Exporting table %s (v%d) to Delta Lake: %d specs, %d rows",
        table_name,
        version,
        len(resolved_specs),
        row_count,
    )

    export_table_to_deltalake(
        connection_string=connection_string,
        annotation_table_name=table_name,
        output_specs=resolved_specs,
        output_uri_base=output_uri_base,
        segmentation_table_name=segmentation_table_name,
        is_merged=is_merged,
        chunk_size=chunk_size,
        flush_threshold_bytes=flush_threshold,
        boundaries=boundaries,
        uniform_ranges=uniform_ranges,
    )

    celery_logger.info(
        "Delta Lake export complete for table %s (v%d)", table_name, version
    )
