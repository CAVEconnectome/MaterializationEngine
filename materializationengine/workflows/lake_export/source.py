"""Where a table export reads from: a frozen version's database, or (later) the live
database as of a timestamp.

A source decides the database a table is read from, the version's tables and their
expected rows, how a table's rows are joined to its segmentation, and the filter each id
range adds to its SELECT. Its fields are saved in the table's plan, so a restarted job
reads from the same source with the same filter.

- frozen: the version's own database (<datastack>__mat<version>), as exports always read:
  no filter, a LEFT JOIN to the segmentation table when the version's tables are split
  (merged versions have none), MaterializedMetadata's tables and row counts.
- live (step 7 of DELTA_LAKE_EXPORT_PLAN.md; not implemented yet): the aligned volume's
  database as of T, filtering created <= T AND (deleted IS NULL OR deleted > T) in each
  range, INNER JOIN to segmentation; only while the version workflow holds the
  segmentation write lock.
"""

from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

JOINS = ("left", "inner")


@dataclass(frozen=True)
class ExportSource:
    kind: str = "frozen"
    as_of: Optional[str] = None  # live only: the version's timestamp T, ISO 8601 UTC
    join: str = "left"

    def __post_init__(self):
        if self.join not in JOINS:
            raise ValueError(f"join must be one of {JOINS}, not {self.join!r}")

    def to_dict(self) -> Dict:
        return asdict(self)

    def row_filter(self, annotation_table: str) -> Optional[str]:
        """SQL each range adds to its WHERE (None: no filter)."""
        return None

    def database(self, datastack: str, version: int) -> str:
        raise NotImplementedError

    def inputs(self, datastack_info: Dict, version: int, table: str) -> Dict:
        """connection_string, engine, segmentation_table (or None) and row_count of a table."""
        raise NotImplementedError

    def list_tables(self, datastack: str, version: int) -> List[Dict]:
        """The version's tables, biggest first: {"table", "rows", "kind"}."""
        raise NotImplementedError


@dataclass(frozen=True)
class FrozenSource(ExportSource):
    kind: str = "frozen"

    def database(self, datastack: str, version: int) -> str:
        return f"{datastack}__mat{version}"

    def inputs(self, datastack_info: Dict, version: int, table: str) -> Dict:
        from dynamicannotationdb.key_utils import build_segmentation_table_name

        from materializationengine.database import db_manager
        from materializationengine.models import MaterializedMetadata
        from materializationengine.utils import get_config_param
        from materializationengine.workflows import deltalake_export as dx

        datastack = datastack_info["datastack"]
        database = self.database(datastack, version)
        engine = db_manager.get_engine(database)
        with db_manager.session_scope(database) as session:
            row = session.query(MaterializedMetadata).filter(MaterializedMetadata.table_name == table).first()
            if row is None:
                raise ValueError(f"no MaterializedMetadata entry for {table!r} in {database}")
            row_count = int(row.row_count or 0)
        seg_table = build_segmentation_table_name(table, datastack_info["segmentation_source"].split("/")[-1])
        return {
            "connection_string": dx._build_frozen_db_connection_string(
                get_config_param("SQLALCHEMY_DATABASE_URI"), datastack, version),
            "engine": engine,
            "segmentation_table": seg_table if engine.dialect.has_table(engine, seg_table) else None,
            "row_count": row_count,
        }

    def list_tables(self, datastack: str, version: int) -> List[Dict]:
        from materializationengine.workflows import deltalake_version_export as dve

        return dve.list_version_tables(datastack, version)


def source_from_dict(data: Optional[Dict]) -> ExportSource:
    """The source a saved plan recorded (a plan saved before sources existed is frozen)."""
    data = dict(data or {"kind": "frozen"})
    kind = data.get("kind", "frozen")
    if kind == "frozen":
        return FrozenSource(**data)
    raise NotImplementedError(f"export source {kind!r} is not implemented")
