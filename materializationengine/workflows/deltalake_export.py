from __future__ import annotations

import dataclasses
import json
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import adbc_driver_postgresql.dbapi as pg_dbapi
import numpy as np
import polars as pl
import pyarrow as pa
import shapely

# Importing geoalchemy2 registers the PostGIS ``geometry`` type in the shared
# ``PGDialect.ischema_names`` map, so SQLAlchemy's inspector recognizes geometry
# columns instead of warning "Did not recognize type 'geometry'".
#
# Do NOT re-alias ``geometry`` to BYTEA here.  ``PGDialect.ischema_names`` is a
# single process-global dict shared with geoalchemy2, so mutating it would
# un-register ``Geometry`` for the whole process -- including the Flask workers
# that import this module via the Celery ``include`` list.  Client code keys off
# the reflected type (``isinstance(column.type, Geometry)``) to expand points
# into x/y/z and to build view jsonschemas, and would silently emit raw WKB and
# 500 on /views/schemas.  See ViewSchemas in blueprints/client/api2.py.
#
# Nothing in this module needs geometry typed as binary: geometry columns are
# discovered from information_schema by _get_geometry_columns, and the data path
# is ADBC, which streams geometry as WKB bytes regardless of SQLAlchemy types.
import geoalchemy2  # noqa: F401
from celery.utils.log import get_task_logger
from sqlalchemy import inspect
from sqlalchemy.engine import Engine


celery_logger = get_task_logger(__name__)

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
        """SQL FROM fragment: single table or LEFT JOIN."""
        if self.segmentation_table is None:
            return f'"{self.annotation_table}"'
        # LEFT JOIN, as the query api's QueryManager does: an annotation whose
        # supervoxel lookup never produced a segmentation row is still a row of the
        # table, with null supervoxel and root ids, and MaterializedMetadata counts it.
        return f'"{self.annotation_table}" LEFT JOIN "{self.segmentation_table}" USING (id)'

    @property
    def table_names(self) -> list[str]:
        """Physical table names for inspection (indexes, pg_class, etc.)."""
        names = [self.annotation_table]
        if self.segmentation_table is not None:
            names.append(self.segmentation_table)
        return names


@dataclass
class DeltaLakeOutputSpec:
    name: str
    partition_by: str | None = None
    partition_strategy: Literal["percentile_range", "uniform_range", "hash"] | None = (
        None
    )
    n_partitions: int | Literal["auto"] = "auto"
    zorder_columns: list[str] = field(default_factory=list)
    bloom_filter_columns: list[str] = field(default_factory=list)
    bloom_filter_fpp: float | None = None
    source_geometry_column: str | None = None
    source_table: str | None = None
    bounds: list | None = None
    target_file_size_mb: int | None = None
    # Rows per Parquet row group; None uses DELTALAKE_MAX_ROW_GROUP_SIZE, then delta-rs's
    # default (see apply_default_row_group_size).
    max_row_group_size: int | None = None

    def __post_init__(self):
        if not self.name:
            raise ValueError("DeltaLakeOutputSpec.name must be a non-empty string")
        if self.max_row_group_size is not None:
            if isinstance(self.max_row_group_size, bool) or not isinstance(self.max_row_group_size, int) \
                    or self.max_row_group_size < 1:
                raise ValueError("DeltaLakeOutputSpec.max_row_group_size must be a positive integer")


def apply_spec_overrides(specs: list, overrides: dict) -> None:
    """Set the given DeltaLakeOutputSpec fields on every spec, in place.

    ``name_suffix`` is appended to each spec's name (e.g. ``"__test1"``) so a second
    export of the same version writes beside the first instead of colliding with it.
    """
    allowed = {f.name for f in dataclasses.fields(DeltaLakeOutputSpec)} - {"name"}
    unknown = set(overrides) - allowed - {"name_suffix"}
    if unknown:
        raise ValueError(f"unknown output spec override(s): {sorted(unknown)}")
    for spec in specs:
        for key, value in overrides.items():
            if key == "name_suffix":
                spec.name = f"{spec.name}{value}"
            else:
                setattr(spec, key, value)
        spec.__post_init__()  # re-validate (e.g. a non-positive max_row_group_size)


def apply_default_row_group_size(specs: list[DeltaLakeOutputSpec], default: int | None) -> None:
    """Give every spec without its own max_row_group_size the deployment default."""
    if not default:
        return
    for spec in specs:
        if spec.max_row_group_size is None:
            spec.max_row_group_size = int(default)


# Spatial column prefix priority (lower index = higher priority) used when a
# table/view exposes more than one geometry column and we must pick a single
# one to partition on via its Morton code.
_SPATIAL_PREFIX_PRIORITY = ["ctr", "post", "pre", "pt", "bb"]


def _spatial_col_rank(col_name: str) -> int:
    """Rank a geometry column by ``_SPATIAL_PREFIX_PRIORITY`` (lower = preferred)."""
    for i, prefix in enumerate(_SPATIAL_PREFIX_PRIORITY):
        if col_name.startswith(prefix):
            return i
    return len(_SPATIAL_PREFIX_PRIORITY)


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$]*$")


def _validate_identifier(name: str) -> str:
    """Raise ``ValueError`` if *name* is not a safe SQL identifier.

    Prevents SQL injection at f-string interpolation sites where identifier
    quoting alone is insufficient (e.g. subquery table names, pg_class lookups).
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


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
        _validate_identifier(tbl)
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


def _classify_relation(connection_string: str, name: str) -> str | None:
    """Classify a frozen-DB relation as ``"table"``, ``"view"``, or ``None``.

    Inspects ``pg_class.relkind``: ordinary/partitioned tables (``r``/``p``)
    are ``"table"``; ordinary and materialized views (``v``/``m``) are
    ``"view"``.  Returns ``None`` when no relation with *name* exists.

    Analysis views are cloned into the frozen DB as materialized views (via
    ``CREATE DATABASE ... WITH TEMPLATE``) and so are not registered in
    ``MaterializedMetadata`` — this lets callers route them to the
    view-specific row-count and spec-discovery paths.
    """
    _validate_identifier(name)
    row = _adbc_fetchone(
        connection_string,
        f"SELECT relkind FROM pg_class WHERE relname = '{name}'",
    )
    if row is None:
        return None
    relkind = row[0]
    if relkind in ("r", "p"):
        return "table"
    if relkind in ("v", "m"):
        return "view"
    return None


def _get_geometry_columns(connection_string: str, name: str) -> list[str]:
    """Return PostGIS geometry column names on *name*, in ordinal order.

    Detects columns via ``information_schema.columns.udt_name = 'geometry'``,
    which works identically for tables, views, and materialized views -- unlike
    SQLAlchemy reflection, which cannot see the columns of a plain view.
    """
    _validate_identifier(name)
    rows = _adbc_fetchall(
        connection_string,
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{name}' AND udt_name = 'geometry' "
        f"ORDER BY ordinal_position",
    )
    return [col_name for (col_name,) in rows]


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
                name=pk_col,
                partition_by=pk_col,
                partition_strategy="percentile_range",
                n_partitions="auto",
                zorder_columns=[pk_col],
                bloom_filter_columns=[],
                source_table=source.annotation_table,
            )
        )

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
                    name=col,
                    partition_by=col,
                    partition_strategy="percentile_range",
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
        # Spatial index — partition on Morton code, z-order on Morton column
        # NOTE: using the uniform range approach here as the percentile approach
        # won't work without some extra tooling, as the morton column doesn't
        # exist in the db
        specs.append(
            DeltaLakeOutputSpec(
                name=f"{col}_morton",
                partition_by=f"{col}_morton",
                partition_strategy="uniform_range",
                n_partitions="auto",
                zorder_columns=[f"{col}_morton"],
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
        _validate_identifier(tbl)
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
    sample_percent: float | None = None,
) -> list:
    """Query Postgres for approximate percentile boundaries.

    Returns a sorted list of ``n_partitions - 1`` boundary values that
    split *column_name* into roughly equal-sized partitions.

    If *sample_percent* is set (0–100), a ``TABLESAMPLE SYSTEM`` clause
    is appended so that only a random page-level sample of the table is
    scanned.  This makes the query fast even on very large tables, at the
    cost of slightly approximate boundaries.
    """
    if n_partitions <= 1:
        return []

    _validate_identifier(table_name)
    _validate_identifier(column_name)

    fractions = [i / n_partitions for i in range(1, n_partitions)]
    fractions_sql = ", ".join(str(f) for f in fractions)

    from_clause = f'"{table_name}"'
    if sample_percent is not None:
        from_clause += f" TABLESAMPLE SYSTEM({sample_percent})"

    query = (
        f"SELECT percentile_disc(ARRAY[{fractions_sql}]) "
        f'WITHIN GROUP (ORDER BY "{column_name}") '
        f"FROM {from_clause}"
    )

    row = _adbc_fetchone(connection_string, query)

    # percentile_disc with an array argument returns a single-element row
    # containing a Postgres array → Python list.
    # Guard against NULL result (empty table or all-NULL column).
    if row is None or row[0] is None:
        return []
    boundaries = list(row[0])
    return boundaries


_DEFAULT_SAMPLE_ROWS = 500_000


def _estimate_sample_percent(
    connection_string: str,
    table_name: str,
    target_rows: int = _DEFAULT_SAMPLE_ROWS,
) -> float | None:
    """Compute a TABLESAMPLE SYSTEM percentage for *target_rows*.

    Returns ``None`` if the table is small enough that a full scan is
    faster than sampling overhead (i.e. ``reltuples <= target_rows``).
    """
    _validate_identifier(table_name)
    row = _adbc_fetchone(
        connection_string,
        f"SELECT reltuples FROM pg_class WHERE relname = '{table_name}'",
    )
    if row is None or row[0] <= 0:
        return None
    reltuples = float(row[0])
    if reltuples <= target_rows:
        return None  # Table is small — scan the whole thing.
    return min(target_rows / reltuples * 100, 100.0)


def _compute_sampled_percentile_bounds(
    connection_string: str,
    table_name: str,
    column_partitions: dict[str, int],
    target_sample_rows: int = _DEFAULT_SAMPLE_ROWS,
) -> dict[str, list]:
    """Compute percentile boundaries for multiple columns in one query.

    Groups all columns into a single ``SELECT`` with one
    ``percentile_disc`` aggregate per column, optionally sampling via
    ``TABLESAMPLE SYSTEM``.  This avoids repeated scans of the same
    table.

    Parameters
    ----------
    column_partitions
        Mapping of ``{column_name: n_partitions}``.  Columns with
        ``n_partitions <= 1`` are skipped (returned as empty lists).
    target_sample_rows
        Desired sample size.  If the table has fewer rows, the full
        table is scanned instead.

    Returns
    -------
    dict[str, list]
        Mapping of ``{column_name: [breakpoints]}``.
    """
    # Separate trivial columns (no partitioning needed).
    results: dict[str, list] = {}
    nontrivial: dict[str, int] = {}
    for col, n in column_partitions.items():
        if n <= 1:
            results[col] = []
        else:
            nontrivial[col] = n

    if not nontrivial:
        return results

    _validate_identifier(table_name)
    for col in nontrivial:
        _validate_identifier(col)

    sample_percent = _estimate_sample_percent(
        connection_string, table_name, target_sample_rows
    )

    from_clause = f'"{table_name}"'
    if sample_percent is not None:
        from_clause += f" TABLESAMPLE SYSTEM({sample_percent})"

    # Build one percentile_disc aggregate per column.
    agg_parts = []
    col_order = list(nontrivial.keys())
    for col in col_order:
        n = nontrivial[col]
        fractions = [i / n for i in range(1, n)]
        fractions_sql = ", ".join(str(f) for f in fractions)
        agg_parts.append(
            f'percentile_disc(ARRAY[{fractions_sql}]) WITHIN GROUP (ORDER BY "{col}")'
        )

    query = f"SELECT {', '.join(agg_parts)} FROM {from_clause}"
    row = _adbc_fetchone(connection_string, query)

    # Guard against NULL result (empty table or all-NULL columns).
    if row is None:
        for col in col_order:
            results[col] = []
        return results

    for i, col in enumerate(col_order):
        results[col] = [] if row[i] is None else list(row[i])

    return results


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
    _validate_identifier(table_name)

    if source_geometry_column is not None:
        col = source_geometry_column
        _validate_identifier(col)

        # Pre-fetch relpages to compute a safe TABLESAMPLE percentage.
        # This avoids the inline subquery `sample_pages / relpages * 100`
        # overflowing 100% (or dividing by zero) for small/empty tables.
        relpages_row = _adbc_fetchone(
            connection_string,
            f"SELECT relpages FROM pg_class WHERE relname = '{table_name}'",
        )
        relpages = relpages_row[0] if relpages_row is not None else 0

        if relpages <= 0:
            # Table is empty or has no stats — full scan (no TABLESAMPLE).
            tablesample_clause = ""
        else:
            pct = min(sample_pages / relpages * 100.0, 100.0)
            tablesample_clause = f" TABLESAMPLE SYSTEM({pct})"

        query = (
            f"SELECT ST_XMin(bbox), ST_YMin(bbox), ST_ZMin(bbox), "
            f"ST_XMax(bbox), ST_YMax(bbox), ST_ZMax(bbox) FROM ("
            f'SELECT ST_3DExtent("{col}") AS bbox '
            f'FROM "{table_name}"{tablesample_clause}) sub'
        )
        row = _adbc_fetchone(connection_string, query)
        # Guard: ST_3DExtent returns NULL when the sample has no geometries.
        if row is None or row[0] is None:
            return 0.0, 0.0
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
        _validate_identifier(column_name)
        query = (
            f'SELECT MIN("{column_name}")::float, MAX("{column_name}")::float '
            f'FROM "{table_name}"'
        )
        row = _adbc_fetchone(connection_string, query)
        # Guard: MIN/MAX return NULL for empty tables or all-NULL columns.
        if row is None or row[0] is None:
            return 0.0, 0.0
        return float(row[0]), float(row[1])


def resolve_bounds(
    spec: DeltaLakeOutputSpec,
    connection_string: str,
    table_name: str,
) -> None:
    """Populate ``spec.bounds`` in place if not already set.

    Dispatches on ``spec.partition_strategy``:

    * ``percentile_range`` → :func:`compute_partition_boundaries` with
      optional sampling (list of N-1 breakpoints stored directly).
    * ``uniform_range`` → :func:`compute_uniform_range_bounds` followed
      by ``np.linspace`` to produce the same N-1 interior breakpoints.
    * ``hash`` / ``None`` → no-op (hash partitioning doesn't use bounds).

    If ``spec.bounds`` is already non-``None`` (user-supplied), this
    function is a no-op regardless of strategy.

    .. note::

       Prefer :func:`resolve_all_bounds` when resolving multiple specs —
       it batches percentile queries per physical table into a single SQL
       statement, avoiding repeated scans.
    """
    if spec.bounds is not None:
        return
    if spec.partition_by is None or spec.partition_strategy in (None, "hash"):
        return

    boundary_table = spec.source_table or table_name
    n = spec.n_partitions if isinstance(spec.n_partitions, int) else 1

    if spec.partition_strategy == "percentile_range":
        sample_percent = _estimate_sample_percent(connection_string, boundary_table)
        spec.bounds = compute_partition_boundaries(
            connection_string,
            boundary_table,
            spec.partition_by,
            n,
            sample_percent=sample_percent,
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


def resolve_all_bounds(
    specs: list[DeltaLakeOutputSpec],
    connection_string: str,
    default_table: str,
) -> None:
    """Resolve bounds for all *specs* in place, batching per physical table.

    ``percentile_range`` specs that share the same ``source_table`` are
    grouped into a single SQL query with one ``percentile_disc`` aggregate
    per column, optionally using ``TABLESAMPLE SYSTEM`` for large tables.

    ``uniform_range`` and other strategies are resolved individually via
    :func:`resolve_bounds`.
    """
    from collections import defaultdict

    # Separate percentile specs (batchable) from others.
    percentile_groups: dict[str, list[DeltaLakeOutputSpec]] = defaultdict(list)
    other_specs: list[DeltaLakeOutputSpec] = []

    for spec in specs:
        if spec.bounds is not None:
            continue
        if spec.partition_by is None or spec.partition_strategy in (None, "hash"):
            continue
        if spec.partition_strategy == "percentile_range":
            table = spec.source_table or default_table
            percentile_groups[table].append(spec)
        else:
            other_specs.append(spec)

    # Batch percentile queries per physical table.
    for table, group in percentile_groups.items():
        column_partitions = {}
        for spec in group:
            n = spec.n_partitions if isinstance(spec.n_partitions, int) else 1
            column_partitions[spec.partition_by] = n

        bounds_map = _compute_sampled_percentile_bounds(
            connection_string, table, column_partitions
        )

        for spec in group:
            spec.bounds = bounds_map.get(spec.partition_by, [])

    # Resolve remaining specs individually.
    for spec in other_specs:
        resolve_bounds(spec, connection_string, default_table)


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


def add_morton_column(
    table: pl.DataFrame,
    geometry_col: str,
) -> pl.DataFrame:
    """Add a ``{geometry_col}_morton`` column from decoded coordinate columns.

    Reads ``{geometry_col}_x``, ``{geometry_col}_y``, ``{geometry_col}_z``
    (Int32) and bit-interleaves them into a 63-bit Morton code (Int64).
    Null rows in the coordinate columns produce null Morton values.

    .. warning::

       Morton encoding currently assumes coordinates are non-negative and
       fit in 21 bits (< 2 097 152).  Negative values will wrap to large
       unsigned values and be silently truncated.  Values exceeding 21 bits
       will also be truncated.  Both cases degrade spatial locality of the
       resulting Morton codes but do not cause errors.
    """
    # TODO: normalise coordinates before Morton encoding so that negative
    #  and/or large-range values are handled correctly.  This requires
    #  global coordinate bounds (min/max per axis across the full table)
    #  so the shift + coarsen step is consistent across streaming flushes.
    col = geometry_col
    x_series = table[f"{col}_x"]
    y_series = table[f"{col}_y"]
    z_series = table[f"{col}_z"]
    n_rows = len(table)

    valid_mask = (
        x_series.is_not_null() & y_series.is_not_null() & z_series.is_not_null()
    ).to_numpy()
    n_valid = int(valid_mask.sum())

    if n_valid == 0:
        morton_series = pl.Series(f"{col}_morton", [None] * n_rows, dtype=pl.Int64)
    elif n_valid == n_rows:
        morton = morton_encode_3d(
            x_series.to_numpy().astype(np.uint64),
            y_series.to_numpy().astype(np.uint64),
            z_series.to_numpy().astype(np.uint64),
        )
        morton_series = pl.Series(f"{col}_morton", morton, dtype=pl.Int64)
    else:
        valid_x = x_series.drop_nulls().to_numpy().astype(np.uint64)
        valid_y = y_series.drop_nulls().to_numpy().astype(np.uint64)
        valid_z = z_series.drop_nulls().to_numpy().astype(np.uint64)
        morton_full = np.full(n_rows, None, dtype=object)
        morton_full[valid_mask] = morton_encode_3d(valid_x, valid_y, valid_z)
        morton_series = pl.Series(f"{col}_morton", morton_full.tolist(), dtype=pl.Int64)

    return table.with_columns(morton_series)


def decode_geometry_columns(
    table: pl.DataFrame,
    geometry_columns: list[str],
) -> pl.DataFrame:
    """Replace WKB geometry columns with decoded coordinate columns.

    For each column in *geometry_columns*:

    * Decode WKB binary → x, y, z (Int32) columns named
      ``{col}_x``, ``{col}_y``, ``{col}_z``.
    * Drop the original binary column.

    To also compute Morton codes from the decoded coordinates, call
    :func:`add_morton_column` separately.
    """
    for col in geometry_columns:
        wkb_series = table[col]
        n_rows = len(wkb_series)

        # Build the null mask without materializing the full column as Python
        # objects — avoids duplicating buffered WKB data for large flushes.
        valid_mask = wkb_series.is_not_null().to_numpy()
        n_valid = int(valid_mask.sum())

        if n_valid == 0:
            # All nulls — emit null columns and skip decoding.
            new_cols = [
                pl.Series(f"{col}_x", [None] * n_rows, dtype=pl.Int32),
                pl.Series(f"{col}_y", [None] * n_rows, dtype=pl.Int32),
                pl.Series(f"{col}_z", [None] * n_rows, dtype=pl.Int32),
            ]
        else:
            # Decode only non-null geometries, then scatter back.
            # drop_nulls() preserves order, matching the True positions in valid_mask.
            valid_wkb = wkb_series.drop_nulls().to_list()
            points = shapely.from_wkb(valid_wkb)
            coords = shapely.get_coordinates(points, include_z=True)

            if n_valid == n_rows:
                # Fast path — no nulls to scatter.
                x = coords[:, 0].astype(np.int32)
                y = coords[:, 1].astype(np.int32)
                z = coords[:, 2].astype(np.int32)
            else:
                # Scatter decoded coords into full-length nullable arrays.
                # Use object dtype for intermediate storage (supports None),
                # then convert to list so Polars can construct nullable Int32
                # series (object arrays can't be cast to Int32 directly).
                x_full = np.full(n_rows, None, dtype=object)
                y_full = np.full(n_rows, None, dtype=object)
                z_full = np.full(n_rows, None, dtype=object)
                x_full[valid_mask] = coords[:, 0].astype(np.int32)
                y_full[valid_mask] = coords[:, 1].astype(np.int32)
                z_full[valid_mask] = coords[:, 2].astype(np.int32)
                x = x_full.tolist()
                y = y_full.tolist()
                z = z_full.tolist()

            new_cols = [
                pl.Series(f"{col}_x", x, dtype=pl.Int32),
                pl.Series(f"{col}_y", y, dtype=pl.Int32),
                pl.Series(f"{col}_z", z, dtype=pl.Int32),
            ]

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


# ---------------------------------------------------------------------------
# Flush hot path.
#
# The flush used to convert the whole accumulated buffer to Polars at once,
# decode WKB through shapely, and then hand each output spec its own full-size
# Arrow copy.  Measured against a synthetic synapses_pni_2 buffer that peaked at
# 8.2x the buffer size, which is what OOM-killed the v1822 export: a 1 GiB
# threshold against an 8 GiB container limit leaves no room for an 8x spike.
#
# Two costs dominated.  Shapely decoding cost 4.2x on its own -- `.to_list()`
# materialises every WKB blob as a Python bytes object and `shapely.from_wkb`
# then builds a GEOS geometry per row.  The rest came from materialising the
# full table once per spec.  Both are fixed by working one RecordBatch at a
# time and streaming batches into the writer, so peak memory is set by the
# decoded buffer rather than by a multiple of it.
# ---------------------------------------------------------------------------

_WKB_LITTLE_ENDIAN = 1
_WKB_TYPE_POINT = 1
_WKB_Z_FLAG = 0x80000000
_WKB_M_FLAG = 0x40000000
_WKB_SRID_FLAG = 0x20000000
_WKB_HEADER_COMPARE_BYTES = 5  # endianness byte + 4-byte type word


def _wkb_pointz_coord_offset(header: bytes, width: int) -> int | None:
    """Byte offset of the X ordinate within a fixed-width EWKB Point Z blob.

    Returns ``None`` for anything the vectorised reader should not attempt --
    big-endian blobs, non-point geometries, an M dimension, 2D points, or a
    width that disagrees with the header.  The caller then falls back to
    shapely, which handles the general case.

    2D points are deliberately excluded rather than zero-filled: the shapely
    path asks for `include_z=True` and gets NaN for them, so matching its
    behaviour here would mean reproducing a cast of NaN to int32.
    """
    if len(header) < _WKB_HEADER_COMPARE_BYTES:
        return None
    if header[0] != _WKB_LITTLE_ENDIAN:
        return None
    type_word = int.from_bytes(header[1:5], "little")
    if type_word & _WKB_M_FLAG:
        return None
    if not type_word & _WKB_Z_FLAG:
        return None
    if (type_word & 0x0000FFFF) != _WKB_TYPE_POINT:
        return None
    offset = 5 + (4 if type_word & _WKB_SRID_FLAG else 0)
    if width != offset + 24:  # three float64 ordinates
        return None
    return offset


def _decode_wkb_pointz_vectorised(arr: pa.Array):
    """Decode a binary array of EWKB Point Z blobs with numpy alone.

    Returns ``(x, y, z, valid_mask)`` as Int32 arrays covering only the valid
    rows, with *valid_mask* ``None`` when there are no nulls.  Returns ``None``
    if the array is not uniform fixed-width little-endian Point Z, leaving the
    caller to fall back to shapely.

    Arrow stores no bytes at all for a null entry, so the valid blobs remain
    contiguous with a constant stride even when nulls are present.  That lets
    this reshape the values buffer directly instead of building a gather index,
    which for a large flush would itself cost several times the coordinate data.
    """
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if isinstance(arr, pa.ExtensionArray):
        arr = arr.storage
    if not isinstance(arr, (pa.BinaryArray, pa.LargeBinaryArray)):
        return None

    n_rows = len(arr)
    if n_rows == 0:
        return None

    buffers = arr.buffers()
    if len(buffers) != 3 or buffers[1] is None or buffers[2] is None:
        return None

    offset_dtype = np.int64 if isinstance(arr, pa.LargeBinaryArray) else np.int32
    offsets = np.frombuffer(buffers[1], dtype=offset_dtype)
    offsets = offsets[arr.offset : arr.offset + n_rows + 1]
    if offsets.size != n_rows + 1:
        return None
    values = np.frombuffer(buffers[2], dtype=np.uint8)

    valid = None
    if arr.null_count:
        valid = arr.is_valid().to_numpy(zero_copy_only=False)

    lengths = np.diff(offsets)
    valid_lengths = lengths if valid is None else lengths[valid]
    if valid_lengths.size == 0:
        return None
    width = int(valid_lengths[0])
    if width <= 0 or not np.all(valid_lengths == width):
        return None

    starts = offsets[:-1] if valid is None else offsets[:-1][valid]
    base = int(starts[0])
    n_valid = int(starts.size)

    # Contiguity is what makes the reshape below correct; verify rather than
    # assume it, since a sliced or concatenated array could break it.
    if int(starts[-1]) != base + (n_valid - 1) * width:
        return None
    if base + n_valid * width > values.size:
        return None

    coord_offset = _wkb_pointz_coord_offset(
        bytes(values[base : base + _WKB_HEADER_COMPARE_BYTES]), width
    )
    if coord_offset is None:
        return None

    block = values[base : base + n_valid * width].reshape(n_valid, width)

    # Every blob must carry the same endianness and geometry type; a column
    # mixing point types would otherwise be silently misread. SRID is allowed
    # to vary because it does not move the ordinates.
    headers = block[:, :_WKB_HEADER_COMPARE_BYTES]
    if not np.array_equal(headers, np.broadcast_to(headers[0], headers.shape)):
        return None

    coords = np.ascontiguousarray(block[:, coord_offset : coord_offset + 24])
    coords = coords.view(np.float64).reshape(n_valid, 3)
    return (
        coords[:, 0].astype(np.int32),
        coords[:, 1].astype(np.int32),
        coords[:, 2].astype(np.int32),
        valid,
    )


def _int32_column(values: np.ndarray, valid: np.ndarray | None, n_rows: int) -> pa.Array:
    """Build a nullable Int32 Arrow array from valid-row values plus a mask."""
    if valid is None:
        return pa.array(values, type=pa.int32())
    full = np.zeros(n_rows, dtype=np.int32)
    full[valid] = values
    return pa.array(full, mask=~valid, type=pa.int32())


def _decode_geometry_batch(
    batch: pa.RecordBatch,
    geometry_columns: list[str],
) -> pa.RecordBatch:
    """Replace WKB geometry columns with ``{col}_x/_y/_z`` Int32 columns.

    The Arrow-level equivalent of :func:`decode_geometry_columns`, operating on
    a single RecordBatch so peak memory tracks the batch rather than the whole
    flush buffer.  Also strips ``arrow.opaque`` extension wrappers, which is why
    the streaming path does not need a separate
    :func:`_strip_arrow_extension_types` pass.
    """
    names = list(batch.schema.names)
    columns = [batch.column(i) for i in range(batch.num_columns)]

    # Drop extension wrappers on every column, not just geometry ones, so the
    # Polars conversion downstream never meets an unregistered extension type.
    for idx, field in enumerate(batch.schema):
        if isinstance(field.type, pa.BaseExtensionType):
            columns[idx] = columns[idx].storage

    n_rows = batch.num_rows
    for col in geometry_columns:
        if col not in names:
            continue
        idx = names.index(col)
        source = columns[idx]

        decoded = _decode_wkb_pointz_vectorised(source)
        if decoded is not None:
            x, y, z, valid = decoded
            new_cols = [
                _int32_column(x, valid, n_rows),
                _int32_column(y, valid, n_rows),
                _int32_column(z, valid, n_rows),
            ]
        else:
            # General case: hand this one batch to shapely. Still bounded,
            # because a batch is orders of magnitude smaller than a flush.
            frame = pl.from_arrow(pa.record_batch([source], names=[col]))
            frame = decode_geometry_columns(frame, [col])
            decoded_tbl = frame.to_arrow().combine_chunks()
            new_cols = [
                decoded_tbl.column(f"{col}_{axis}").combine_chunks()
                for axis in ("x", "y", "z")
            ]
            new_cols = [
                c.chunk(0) if isinstance(c, pa.ChunkedArray) else c for c in new_cols
            ]

        del names[idx], columns[idx]
        for axis, arr in zip(("x", "y", "z"), new_cols):
            names.append(f"{col}_{axis}")
            columns.append(arr)

    return pa.RecordBatch.from_arrays(columns, names=names)


def _partition_indices(values: pl.Series, breakpoints: list) -> np.ndarray | None:
    """Bin indices matching ``pl.cut``, computed with ``np.searchsorted``.

    ``assign_partition`` goes through ``pl.cut``, whose categorical result has
    to be cast Utf8 then Int32 -- materialising a string per row. ``searchsorted``
    with ``side="left"`` reproduces the same ``(b[i-1], b[i]]`` bins directly as
    integers.  Returns ``None`` when the column has nulls, which ``pl.cut``
    propagates and this shortcut does not model.
    """
    if values.null_count():
        return None
    array = values.to_numpy()
    if array.dtype.kind not in "iuf":
        return None
    breaks = np.asarray(sorted(float(b) for b in breakpoints), dtype=np.float64)
    return np.searchsorted(breaks, array.astype(np.float64), side="left").astype(
        np.int32
    )


def _as_array(column) -> pa.Array:
    """Flatten a possibly-chunked column to a single Array.

    ``RecordBatch.from_arrays`` will not take a ChunkedArray, and a zero-row
    batch produces a column with no chunks at all, so neither ``chunk(0)`` nor
    ``combine_chunks`` is safe on its own.
    """
    if isinstance(column, pa.ChunkedArray):
        if column.num_chunks == 1:
            return column.chunk(0)
        return pa.concat_arrays([chunk for chunk in column.chunks] or
                                [pa.array([], type=column.type)])
    return column


def _spec_derived_columns(
    batch: pa.RecordBatch,
    spec: DeltaLakeOutputSpec,
) -> list[tuple[str, pa.Array]]:
    """The Morton and/or partition columns this spec adds to a decoded batch.

    Returns only the *new* columns rather than a rebuilt batch, so the caller can
    attach them to the decoded batches without copying the batches themselves.
    """
    needs_morton = spec.source_geometry_column is not None
    needs_partition = (
        spec.partition_by is not None and spec.partition_strategy is not None
    )
    if not needs_morton and not needs_partition:
        return []

    frame = pl.from_arrow(batch)
    if isinstance(frame, pl.Series):
        frame = frame.to_frame()

    added: list[str] = []
    if needs_morton:
        morton_col = f"{spec.source_geometry_column}_morton"
        frame = add_morton_column(frame, spec.source_geometry_column)
        added.append(morton_col)

    if needs_partition:
        part_col = spec.partition_by
        if spec.bounds is not None:
            indices = _partition_indices(frame[part_col], spec.bounds)
            if indices is not None:
                frame = frame.with_columns(
                    pl.Series(f"{part_col}_partition", indices, dtype=pl.Int32)
                )
            else:
                frame = assign_partition(frame, part_col, spec.bounds)
            added.append(f"{part_col}_partition")
        elif spec.partition_strategy == "hash":
            n = spec.n_partitions if isinstance(spec.n_partitions, int) else 1
            frame = assign_hash_partition(frame, part_col, n)
            added.append(f"{part_col}_partition")

    table = frame.select(added).to_arrow().combine_chunks()
    return [(name, _as_array(table.column(name))) for name in added]


def _apply_spec_to_batch(
    batch: pa.RecordBatch,
    spec: DeltaLakeOutputSpec,
) -> pa.RecordBatch:
    """A decoded batch plus this spec's derived columns."""
    columns = [batch.column(i) for i in range(batch.num_columns)]
    names = list(batch.schema.names)
    for name, array in _spec_derived_columns(batch, spec):
        names.append(name)
        columns.append(array)
    return pa.RecordBatch.from_arrays(columns, names=names)


# How many partitions one ``write_deltalake`` call may span. The writer holds an
# open Parquet writer per partition it has seen and flushes them only when the
# call returns, so a single call covering every partition pins them all at once
# -- measured at 3.9x the flush buffer for 63 partitions. Writing a bounded group
# at a time caps the open writers without fragmenting the output: each group
# still emits exactly the files for its own partitions, so the file count is
# unchanged. Lowering ``target_file_size`` also bounds memory, but only by
# splitting the output into many more, much smaller files.
#
# The tension is commit count: each group is its own commit, and every
# ``write_deltalake`` replays the table's transaction log, so smaller groups
# make later flushes progressively slower. Over eight 256 MiB flushes, 8 gave
# 2521 MiB at 2.4 s/flush and 16 gave 2604 MiB at 1.6 s/flush, while one call
# per spec gave 3308 MiB at 1.3 s. 16 buys nearly all of the memory saving for a
# third less time.
_PARTITION_WRITE_GROUP = int(
    os.environ.get("DELTALAKE_PARTITION_WRITE_GROUP", 16) or 16
)


def _write_partitioned_in_groups(
    table: pa.Table,
    uri: str,
    partition_column: str,
    partition_values: np.ndarray | None = None,
    group_size: int = _PARTITION_WRITE_GROUP,
) -> None:
    """Append *table* to a partitioned lake a bounded group of partitions at a time.

    Selects each group with a filter rather than sorting the whole table: the
    filter allocates only the rows of the group it is about to write, whereas a
    sort would hold a second full-size copy alongside the original.
    """
    from deltalake import write_deltalake

    if table.num_rows == 0:
        return

    column = table.column(partition_column)
    if column.null_count:
        # Nulls have no numeric group; rare enough to take the simple path.
        write_deltalake(uri, table, mode="append", partition_by=[partition_column])
        return

    if partition_values is None:
        partition_values = column.to_numpy()

    lo, hi = int(partition_values.min()), int(partition_values.max())
    if hi - lo < group_size:
        write_deltalake(uri, table, mode="append", partition_by=[partition_column])
        return

    for start in range(lo, hi + 1, group_size):
        mask = (partition_values >= start) & (partition_values < start + group_size)
        if not mask.any():
            continue
        chunk = table.filter(pa.array(mask))
        try:
            write_deltalake(
                uri, chunk, mode="append", partition_by=[partition_column]
            )
        finally:
            del chunk


def _spec_partition_by(spec: "DeltaLakeOutputSpec") -> list[str] | None:
    """The ``partition_by`` argument for this spec's ``write_deltalake`` call."""
    if spec.partition_by is None or spec.partition_strategy is None:
        return None
    if spec.bounds is not None or spec.partition_strategy == "hash":
        return [f"{spec.partition_by}_partition"]
    return None


_malloc_trim = None
_malloc_trim_checked = False


def _release_freed_heap() -> None:
    """Hand memory freed during a flush back to the OS. glibc only, best effort.

    The Parquet writer allocates and frees large native buffers on every flush.
    glibc keeps that memory in its arenas instead of returning it, so RSS
    ratchets upward across a long export even though almost nothing stays live --
    measured climbing from 1.3 GB to 2.1 GB over six 256 MiB flushes, and the
    container limit is enforced on RSS, not on live bytes. ``malloc_trim``
    releases the free pages and flattens that curve.

    A no-op anywhere without glibc (musl, macOS), so it is safe to call
    unconditionally.
    """
    global _malloc_trim, _malloc_trim_checked
    if not _malloc_trim_checked:
        _malloc_trim_checked = True
        try:
            import ctypes

            libc = ctypes.CDLL("libc.so.6")
            libc.malloc_trim.argtypes = [ctypes.c_size_t]
            libc.malloc_trim.restype = ctypes.c_int
            _malloc_trim = libc.malloc_trim
        except (OSError, AttributeError):
            _malloc_trim = None
    if _malloc_trim is not None:
        try:
            _malloc_trim(0)
        except Exception:  # pragma: no cover - never fail a flush over this
            pass


def _flush_buffer(
    buffer: list[pa.RecordBatch],
    output_specs: list[DeltaLakeOutputSpec],
    output_uri_base: str,
    geometry_columns: list[str],
) -> None:
    """Decode geometry once, then append to each target Delta Lake.

    Peak memory tracks the decoded buffer rather than a multiple of it. Three
    things make that hold:

    * Geometry is decoded a batch at a time and only once, up front. Decoding
      *shrinks* the data -- three 33-byte WKB blobs become nine Int32s -- so the
      decoded buffer is smaller than the raw batches it replaces, and the raw
      batches are released as they are consumed.
    * Each spec's derived columns are attached to those same decoded batches
      instead of rebuilding them, so the only new allocation per spec is the
      derived columns. Previously every spec copied the whole table twice while
      the shared frame stayed alive alongside it.
    * Partitioned lakes are written a bounded group of partitions at a time; see
      :func:`_write_partitioned_in_groups`.

    Together these took a synthetic 512 MiB synapse buffer from 6.8x the buffer
    size to 3.6x, and the sustained peak over eight flushes from 5.3 GB to 3.4 GB.
    """
    from deltalake import write_deltalake

    # Consume from the end so the source batches can be freed as we go; the
    # caller's list is emptied either way.
    decoded: list[pa.RecordBatch] = []
    while buffer:
        decoded.append(_decode_geometry_batch(buffer.pop(), geometry_columns))
    decoded.reverse()

    if not decoded:
        return

    try:
        for spec in output_specs:
            uri = f"{output_uri_base}/{spec.name}"
            partition_by = _spec_partition_by(spec)

            if partition_by is None:
                # Nothing to group by, so stream and never hold the full table.
                # The schema comes from a one-row probe rather than a zero-row
                # slice, so Morton encoding and binning see real data.
                schema = _apply_spec_to_batch(decoded[0].slice(0, 1), spec).schema
                reader = pa.RecordBatchReader.from_batches(
                    schema,
                    (_apply_spec_to_batch(batch, spec) for batch in decoded),
                )
                write_deltalake(uri, reader, mode="append")
                continue

            # Attach the derived columns to the decoded batches rather than
            # rebuilding them: from_batches shares the existing buffers, so the
            # only new allocation is the derived columns themselves.
            part_col = partition_by[0]
            derived = [_spec_derived_columns(batch, spec) for batch in decoded]
            names = [name for name, _ in derived[0]]
            table = pa.Table.from_batches(decoded)
            for position, name in enumerate(names):
                table = table.append_column(
                    name, pa.chunked_array([cols[position][1] for cols in derived])
                )
            # Keep the partition values as numpy so the group filter below does
            # not have to re-read them off the table for every group.
            part_position = names.index(part_col)
            values = np.concatenate(
                [
                    cols[part_position][1].to_numpy(zero_copy_only=False)
                    for cols in derived
                ]
            )
            del derived
            try:
                _write_partitioned_in_groups(table, uri, part_col, values)
            finally:
                del table, values
    finally:
        decoded.clear()
        _release_freed_heap()


def _table_geometry_columns(
    connection_string: str,
    source: TableSource,
    output_specs: list[DeltaLakeOutputSpec],
) -> list[str]:
    """Every geometry column the export decodes from WKB into ``{col}_x/_y/_z``.

    That is all PostGIS geometry columns of the exported table(s), not only the
    one a Morton spec partitions on: :func:`discover_default_output_specs` keeps
    at most one spatial column (``ctr`` > ``post`` > ``pre`` ...), and decoding
    only that one left a synapse table's ``pre_pt_position`` and
    ``post_pt_position`` as raw WKB.
    """
    columns = {
        spec.source_geometry_column
        for spec in output_specs
        if spec.source_geometry_column is not None
    }
    for name in source.table_names:
        columns.update(_get_geometry_columns(connection_string, name))
    return sorted(columns)


def optimize_deltalake(
    uri: str,
    zorder_columns: list[str] | None = None,
    bloom_filter_columns: list[str] | None = None,
    fpp: float = 0.001,
    max_concurrent_tasks: int = 1,
    target_size: int | None = None,
    max_spill_size: int | None = None,
    max_row_group_size: int | None = None,
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
    max_concurrent_tasks
        Maximum number of concurrent file-merge tasks.  Lower values
        reduce peak memory (default 1 = process one bin at a time).
    target_size
        Target output file size in bytes.  If ``None``, uses the
        delta-rs default (``delta.targetFileSize`` or 256 MB).
    max_spill_size
        Maximum bytes allowed in memory before spilling to disk.
        If ``None``, uses DataFusion's default.
    max_row_group_size
        Maximum rows per Parquet row group in the rewritten files.  If
        ``None``, uses the delta-rs default.
    """
    from deltalake import DeltaTable
    from deltalake.writer import (
        BloomFilterProperties,
        ColumnProperties,
        WriterProperties,
    )

    dt = DeltaTable(uri)

    properties: dict = {}
    if bloom_filter_columns:
        bloom = BloomFilterProperties(set_bloom_filter_enabled=True, fpp=fpp)
        col_props = ColumnProperties(bloom_filter_properties=bloom)
        properties["column_properties"] = {col: col_props for col in bloom_filter_columns}
    if max_row_group_size:
        properties["max_row_group_size"] = int(max_row_group_size)
    writer_properties = WriterProperties(**properties) if properties else None

    celery_logger.info(
        "Optimizing Delta Lake at %s (z-order: %s, bloom filters: %s, row group rows: %s)",
        uri,
        zorder_columns or "none",
        bloom_filter_columns or "none",
        max_row_group_size or "default",
    )
    optimize_kwargs: dict = {
        "max_concurrent_tasks": max_concurrent_tasks,
        "writer_properties": writer_properties,
    }
    if target_size is not None:
        optimize_kwargs["target_size"] = target_size
    if max_spill_size is not None:
        optimize_kwargs["max_spill_size"] = max_spill_size

    if zorder_columns:
        dt.optimize.z_order(
            columns=zorder_columns,
            **optimize_kwargs,
        )
    else:
        dt.optimize.compact(**optimize_kwargs)

    celery_logger.info("Vacuuming Delta Lake at %s", uri)
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
        celery_logger.warning("Vacuum failed for %s: %s", uri, exc)


_DELTALAKE_PROGRESS_TTL = 86400  # 24 hours


def deltalake_export_redis_key(
    datastack: str, version: int, table_name: str, job_id: str | None = None
) -> str:
    """Return the Redis key used to track Delta Lake export progress."""
    base = f"deltalake_export:{datastack}:v{version}:{table_name}"
    if job_id:
        return f"{base}:{job_id}"
    return base


def _deltalake_log_redis_key(
    datastack: str, version: int, table_name: str, job_id: str | None = None
) -> str:
    """Return the Redis key for the capped log list."""
    base = f"deltalake_log:{datastack}:v{version}:{table_name}"
    if job_id:
        return f"{base}:{job_id}"
    return base


_DELTALAKE_LOG_MAX_ENTRIES = 100


def append_deltalake_log(
    datastack: str,
    version: int,
    table_name: str,
    message: str,
    job_id: str | None = None,
) -> None:
    """Append a timestamped log entry to the capped Redis log list."""
    client = _get_redis_client()
    key = _deltalake_log_redis_key(datastack, version, table_name, job_id=job_id)
    timestamped = f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {message}"
    client.rpush(key, timestamped)
    client.ltrim(key, -_DELTALAKE_LOG_MAX_ENTRIES, -1)
    client.expire(key, _DELTALAKE_PROGRESS_TTL)


def _get_redis_client():
    """The shared Redis client, for deltalake export progress."""
    from materializationengine.redis_client import get_redis_client

    return get_redis_client(0)


def set_deltalake_export_status(
    datastack: str,
    version: int,
    table_name: str,
    status: str,
    total_rows: int | None = None,
    rows_processed: int | None = None,
    phase: str | None = None,
    error: str | None = None,
    job_id: str | None = None,
    elapsed_seconds: float | None = None,
    rows_per_second: float | None = None,
) -> None:
    """Write a terminal status (``complete``, ``failed``, etc.) to Redis.

    *elapsed_seconds* and *rows_per_second* provide a live "heartbeat" for
    exports whose total row count is unknown (e.g. views): the UI can show that
    work is progressing — and how fast — even before/without a percentage.
    """
    client = _get_redis_client()
    key = deltalake_export_redis_key(datastack, version, table_name, job_id=job_id)
    pct = None
    if rows_processed is not None and total_rows:
        pct = round(rows_processed / total_rows * 100, 2)
    payload = {
        "status": status,
        "phase": phase,
        "rows_processed": rows_processed,
        "total_rows": total_rows,
        "percent_complete": pct,
        "elapsed_seconds": (
            round(elapsed_seconds, 1) if elapsed_seconds is not None else None
        ),
        "rows_per_second": (
            round(rows_per_second) if rows_per_second is not None else None
        ),
        "error": error,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    client.set(key, json.dumps(payload), ex=_DELTALAKE_PROGRESS_TTL)


def get_deltalake_export_progress(
    datastack: str,
    version: int,
    table_name: str,
    job_id: str | None = None,
) -> dict | None:
    """Read the current export progress from Redis.

    Returns the progress dict (including ``log_entries``), or ``None``
    if no export is tracked.
    """
    client = _get_redis_client()
    key = deltalake_export_redis_key(datastack, version, table_name, job_id=job_id)
    raw = client.get(key)
    if raw is None:
        return None
    progress = json.loads(raw)
    # Attach log entries from the capped Redis list.
    log_key = _deltalake_log_redis_key(datastack, version, table_name, job_id=job_id)
    log_entries = client.lrange(log_key, 0, -1)
    progress["log_entries"] = [
        entry.decode() if isinstance(entry, bytes) else entry
        for entry in (log_entries or [])
    ]
    return progress


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

