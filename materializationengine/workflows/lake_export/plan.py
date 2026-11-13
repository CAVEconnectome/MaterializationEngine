"""Planning one table's export: its specs, partition bounds, size and id ranges.

The same preparation the export has always done (spec discovery, overrides, the
deployment's row-group size, the small-table trim, partition counts and bounds), plus the
size policy and the id ranges. The plan is saved next to the table's lakes
(<table>/_lake_export/<job_id>.json) before anything is written, and a restarted job
loads it instead of planning again: partition bounds come from a random sample, so a
second planning would bin the remaining ranges differently from the ones already
published. Once the table is complete, <job_id>.done.json replaces the plan, and a
restarted job skips the table.
"""

import dataclasses
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from materializationengine.workflows import deltalake_export as dx
from materializationengine.workflows.lake_export import publish as pub
from materializationengine.workflows.lake_export.ranges import cut_ranges
from materializationengine.workflows.lake_export.settings import ExportSettings
from materializationengine.workflows.lake_export.source import ExportSource, FrozenSource, source_from_dict
from materializationengine.workflows.lake_export.sizing import TableSizing, size_table

logger = logging.getLogger(__name__)

PLAN_DIR = "_lake_export"


def _json_default(value):
    # numpy scalars (partition bounds) stay numbers; anything else becomes its string
    return value.item() if hasattr(value, "item") else str(value)


@dataclass
class TablePlan:
    datastack: str
    version: int
    table: str
    job_id: str
    connection_string: str
    output_uri_base: str  # <bucket>/<datastack>/v<version>/<table>; each spec's lake is <base>/<spec.name>
    annotation_table: str
    segmentation_table: Optional[str]
    columns: List[str]
    specs: List[Dict]  # DeltaLakeOutputSpec fields, with n_partitions and bounds resolved
    geometry_columns: List[str]
    row_count: int
    bytes_per_row: int
    sizing: Dict  # TableSizing fields
    ranges: List[Tuple[int, int]]
    # where the table is read from (ExportSource fields); a plan saved before sources is frozen
    source: Dict = field(default_factory=lambda: FrozenSource().to_dict())

    @property
    def table_source(self) -> "dx.TableSource":
        return dx.TableSource(annotation_table=self.annotation_table, segmentation_table=self.segmentation_table)

    @property
    def export_source(self) -> ExportSource:
        return source_from_dict(self.source)

    @property
    def row_filter(self) -> Optional[str]:
        return self.export_source.row_filter(self.annotation_table)

    @property
    def output_specs(self) -> List["dx.DeltaLakeOutputSpec"]:
        return [dx.DeltaLakeOutputSpec(**s) for s in self.specs]

    @property
    def flush_bytes(self) -> int:
        return int(self.sizing["flush_bytes"])

    def lake_uri(self, spec_name: str) -> str:
        return f"{self.output_uri_base}/{spec_name}"

    def catalog_lakes(self) -> List[Dict]:
        """The lakes as deltalake_catalog records them."""
        return [{"spec_name": s["name"], "uri": self.lake_uri(s["name"]), "partition_by": s["partition_by"],
                 "spec": json.loads(json.dumps(s, default=_json_default))} for s in self.specs]

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), default=_json_default)

    @classmethod
    def from_json(cls, raw: str) -> "TablePlan":
        data = json.loads(raw)
        data["ranges"] = [tuple(r) for r in data["ranges"]]
        return cls(**data)


def plan_path(job_id: str) -> str:
    return f"{PLAN_DIR}/{job_id}.json"


def load_plan(output_uri_base: str, job_id: str) -> Optional[TablePlan]:
    import obstore

    from materializationengine.blueprints.client.lake_index import store

    try:
        raw = bytes(obstore.get(store(output_uri_base), plan_path(job_id)).bytes())
    except FileNotFoundError:
        return None
    return TablePlan.from_json(raw.decode())


def save_plan(plan: TablePlan) -> None:
    import obstore

    from materializationengine.blueprints.client.lake_index import store

    obstore.put(store(plan.output_uri_base), plan_path(plan.job_id), plan.to_json().encode())


def delete_plan(plan: TablePlan) -> None:
    from materializationengine.workflows.lake_export import storage

    storage.delete_paths(plan.output_uri_base, [plan_path(plan.job_id)])


def done_path(job_id: str) -> str:
    return f"{PLAN_DIR}/{job_id}.done.json"


def mark_done(plan: TablePlan, summary: Dict) -> None:
    """Record that this job completed the table (it replaces the plan), so a restarted job
    skips the table instead of planning it again -- a new plan's sampled bounds and range
    edges would not line up with the ranges already published."""
    import obstore

    from materializationengine.blueprints.client.lake_index import store

    obstore.put(store(plan.output_uri_base), done_path(plan.job_id), json.dumps(summary, default=str).encode())
    delete_plan(plan)


def load_done(output_uri_base: str, job_id: str) -> Optional[Dict]:
    """What mark_done recorded for this job, or None if it has not completed the table."""
    import obstore

    from materializationengine.blueprints.client.lake_index import store

    try:
        raw = bytes(obstore.get(store(output_uri_base), done_path(job_id)).bytes())
    except FileNotFoundError:
        return None
    return json.loads(raw.decode())


def resolve_specs(
    source: "dx.TableSource",
    engine,
    connection_string: str,
    row_count: int,
    bytes_per_row: int,
    settings: ExportSettings,
    output_specs: Optional[Sequence[Dict]] = None,
    spec_overrides: Optional[Dict] = None,
) -> List["dx.DeltaLakeOutputSpec"]:
    """The table's output specs: given or discovered, with the overrides, the deployment's
    row-group size, the small-table trim, and resolved partition counts and bounds."""
    if output_specs is not None:
        specs = [dx.DeltaLakeOutputSpec(**s) for s in output_specs]
    else:
        specs = dx.discover_default_output_specs(source, engine)
    if spec_overrides:
        dx.apply_spec_overrides(specs, spec_overrides)
    dx.apply_default_row_group_size(specs, settings.max_row_group_size)
    names = [s.name for s in specs]
    if len(set(names)) != len(names):
        raise ValueError(f"duplicate output spec names: {names}")
    # A small table is one lake: partitioning it only makes undersized files.
    if row_count * bytes_per_row / 2**20 < settings.small_table_threshold_mb and len(specs) > 1:
        specs = specs[:1]
    for spec in specs:
        if spec.n_partitions in ("auto", None):
            spec.n_partitions = dx.resolve_n_partitions(
                "auto", row_count, target_file_size_mb=spec.target_file_size_mb or settings.target_partition_size_mb,
                bytes_per_row=bytes_per_row)
    dx.resolve_all_bounds(specs, connection_string, source.annotation_table)
    return specs


def plan_table(
    *,
    datastack: str,
    version: int,
    table: str,
    job_id: str,
    connection_string: str,
    engine,
    segmentation_table: Optional[str],
    row_count: int,
    settings: ExportSettings,
    output_specs: Optional[Sequence[Dict]] = None,
    spec_overrides: Optional[Dict] = None,
    source: Optional[ExportSource] = None,
) -> TablePlan:
    """A new plan for exporting table, read from source (default: its frozen version's
    database, at connection_string)."""
    import psycopg2

    source = source or FrozenSource()

    dx._validate_identifier(table)
    if dx._classify_relation(connection_string, table) != "table":
        raise ValueError(f"{table!r} is not a table of {datastack} v{version}; views are not exported from SQL")
    if not settings.output_bucket:
        raise ValueError("DELTALAKE_OUTPUT_BUCKET is not set")
    table_source = dx.TableSource(annotation_table=table, segmentation_table=segmentation_table)
    bytes_per_row = dx.estimate_bytes_per_row(connection_string, table_source)
    specs = resolve_specs(table_source, engine, connection_string, row_count, bytes_per_row, settings,
                          output_specs=output_specs, spec_overrides=spec_overrides)
    sizing: TableSizing = size_table(row_count, bytes_per_row, settings.sizing)

    conn = psycopg2.connect(connection_string)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT min(id), max(id) FROM "{table}"')
            lo, hi = cur.fetchone()
    finally:
        conn.close()
    lo, hi = (0, 0) if lo is None else (int(lo), int(hi) + 1)
    id_bounds = next((s.bounds for s in specs if s.partition_by == "id" and s.bounds), None)
    ranges = cut_ranges(lo, hi, sizing.ranges, id_bounds)

    return TablePlan(
        datastack=datastack, version=int(version), table=table, job_id=job_id,
        connection_string=connection_string,
        output_uri_base=f"{settings.output_bucket}/{datastack}/v{version}/{table}",
        annotation_table=table, segmentation_table=segmentation_table,
        columns=dx._resolve_select_columns(connection_string, table_source, dx._DEFAULT_DROP_COLUMNS),
        specs=[json.loads(json.dumps(dataclasses.asdict(s), default=_json_default)) for s in specs],
        geometry_columns=dx._table_geometry_columns(connection_string, table_source, specs),
        row_count=int(row_count), bytes_per_row=int(bytes_per_row),
        sizing=dataclasses.asdict(sizing), ranges=ranges, source=source.to_dict(),
    )


class LakeExistsError(RuntimeError):
    pass


def published(plan: TablePlan) -> Dict[str, set]:
    """Ranges this job has already published, per lake (spec name).

    A lake that exists but was written by anything other than this job is refused, as
    the export always has: delete it, or export with a name_suffix.
    """
    out = {}
    for spec in plan.specs:
        uri = plan.lake_uri(spec["name"])
        jobs = pub.export_jobs(uri)
        if jobs and jobs != [plan.job_id]:
            raise LakeExistsError(f"a Delta Lake already exists at {uri} (written by {jobs}); "
                                  f"delete it before exporting again, or export with a name_suffix")
        if not jobs:
            from deltalake import DeltaTable

            if DeltaTable.is_deltatable(uri):
                raise LakeExistsError(f"a Delta Lake already exists at {uri}; delete it before exporting again, "
                                      f"or export with a name_suffix")
        out[spec["name"]] = pub.published_ranges(uri, plan.job_id)
    return out
