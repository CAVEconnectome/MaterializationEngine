"""A Ray upload's tables, built in the production database under temporary names and
published by renaming them.

The staging path built every table twice: the annotation table's indexes in staging
(the spatial lookup needs them) and again in production after copying it there, with
both copies on the instance's disk until the end (for the 337M-row minnie upload,
2026-10-28: ~166 GB twice, and ~6.5 h of the ~22 h run spent re-importing, re-indexing
and re-exporting). Here the tables are created in the aligned volume database as
upl<hash>_<table> and upl<hash>_<table>__<pcg>, built there once, and published in one
transaction: tables, indexes, constraints and the id sequence are renamed to the names
a normal table has, and only then are the annotation_table_metadata and
segmentation_table_metadata rows inserted. Until that commit nothing in production
lists the tables (materialization and the APIs go by the metadata rows), and a frozen
version copied in the meantime drops them, as it drops every table it does not
materialize.

Every temporary table carries a comment naming its job, so cleanup only ever drops
tables that this job built and that never got a metadata row.
"""

import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional

from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import AnnoMetadata, SegmentationMetadata
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Postgres truncates longer identifiers, which could make two index names collide.
MAX_IDENTIFIER_LENGTH = 63
MARKER_PREFIX = "cave-upload:"


class PublishConflict(RuntimeError):
    """A table or metadata row with a final name appeared while the upload was built.
    The built tables are kept: they are hours of work, and the conflict is for a person
    to resolve."""


def build_prefix(job_id: str) -> str:
    return f"upl{hashlib.sha1(job_id.encode()).hexdigest()[:6]}_"


def build_names(job_id: str, table_name: str, pcg_table_name: str) -> Dict[str, str]:
    """The temporary and final names of the upload's annotation and segmentation tables."""
    temp = f"{build_prefix(job_id)}{table_name}"
    return {
        "annotation": temp,
        "segmentation": build_segmentation_table_name(temp, pcg_table_name),
        "final_annotation": table_name,
        "final_segmentation": build_segmentation_table_name(table_name, pcg_table_name),
    }


def marker(job_id: str) -> str:
    return f"{MARKER_PREFIX}{job_id}"


def table_metadata_from_file(file_metadata: dict) -> dict:
    """What the annotation_table_metadata row will hold, from the upload request (as
    upload_to_database reads it)."""
    meta = file_metadata["metadata"]
    reference_table = meta.get("reference_table") if meta.get("is_reference_schema") else None
    return {
        "schema_type": meta["schema_type"],
        "description": meta.get("description", ""),
        "user_id": meta.get("user_id", ""),
        "voxel_resolution_x": meta.get("voxel_resolution_nm_x", 1.0),
        "voxel_resolution_y": meta.get("voxel_resolution_nm_y", 1.0),
        "voxel_resolution_z": meta.get("voxel_resolution_nm_z", 1.0),
        "reference_table": reference_table,
        "flat_segmentation_source": meta.get("flat_segmentation_source"),
        "read_permission": meta.get("read_permission", "PUBLIC"),
        "write_permission": meta.get("write_permission", "PRIVATE"),
        "notice_text": meta.get("notice_text"),
    }


def annotation_model(table_name: str, table_meta: dict):
    from dynamicannotationdb.schema import DynamicSchemaClient

    reference = table_meta.get("reference_table")
    return DynamicSchemaClient().create_annotation_model(
        table_name,
        table_meta["schema_type"],
        table_metadata={"reference_table": reference} if reference else None,
        with_crud_columns=True,
    )


def segmentation_model(table_name: str, table_meta: dict, pcg_table_name: str):
    from materializationengine.utils import create_segmentation_model

    return create_segmentation_model(
        {
            "annotation_table_name": table_name,
            "schema": table_meta["schema_type"],
            "reference_table": table_meta.get("reference_table"),
            "pcg_table_name": pcg_table_name,
        }
    )


def _derived_names(table_name: str, model) -> List[str]:
    """The index and sequence names index_manager and Postgres give table_name, with
    the columns of model (a model of the same schema under any name)."""
    from geoalchemy2.types import Geometry

    names = [f"{table_name}_pkey", f"{table_name}_id_seq"]
    for column in model.__table__.columns:
        if column.index:
            names.append(f"ix_{table_name}_{column.name}")
        if isinstance(column.type, Geometry):
            names.append(f"idx_{table_name}_{column.name}")
    return names


# A model is built only under this short name for the length check: SQLAlchemy refuses
# to build one whose own index names are too long.
_LENGTH_CHECK_NAME = "upl_length_check"


def names_too_long(names: Dict[str, str], table_meta: dict, pcg_table_name: str) -> List[str]:
    """Temporary or final identifiers that Postgres would truncate."""
    models = {
        "annotation": annotation_model(_LENGTH_CHECK_NAME, table_meta),
        "segmentation": segmentation_model(_LENGTH_CHECK_NAME, table_meta, pcg_table_name),
    }
    too_long = []
    for kind, model in models.items():
        for table in (names[kind], names[f"final_{kind}"]):
            for name in [table, *_derived_names(table, model)]:
                if len(name.encode()) > MAX_IDENTIFIER_LENGTH:
                    too_long.append(name)
    return too_long


def _quote(engine, name: str) -> str:
    return engine.dialect.identifier_preparer.quote(name)


def _table_exists(conn, name: str) -> bool:
    return bool(conn.execute(text("SELECT to_regclass(:t)"), {"t": f'"{name}"'}).scalar())


def _comment(conn, name: str) -> Optional[str]:
    return conn.execute(
        text("SELECT obj_description(to_regclass(:t), 'pg_class')"), {"t": f'"{name}"'}
    ).scalar()


def _metadata_rows(conn, names: List[str]) -> List[str]:
    rows = conn.execute(
        text(
            "SELECT table_name FROM annotation_table_metadata WHERE table_name = ANY(:n)"
            " UNION SELECT table_name FROM segmentation_table_metadata WHERE table_name = ANY(:n)"
        ),
        {"n": names},
    )
    return [r[0] for r in rows]


def names_in_use(engine, names: Dict[str, str], job_id: Optional[str] = None) -> List[str]:
    """The temporary and final names already taken in the production database, by a
    table or a metadata row. A temporary table this job created does not count."""
    taken = []
    with engine.connect() as conn:
        for key, name in names.items():
            if _table_exists(conn, name):
                if key in ("annotation", "segmentation") and job_id and _comment(conn, name) == marker(job_id):
                    continue
                taken.append(name)
        taken.extend(_metadata_rows(conn, list(names.values())))
    return sorted(set(taken))


def create_table(engine, model, job_id: str, keep_primary_key: bool = True) -> None:
    """Create a temporary table from its model, marked with its job, without the
    model's other indexes and foreign keys (they are built after the bulk load)."""
    from materializationengine.index_manager import index_cache

    name = model.__table__.name
    model.__table__.create(bind=engine, checkfirst=True)
    with engine.begin() as conn:
        conn.execute(
            text(f"COMMENT ON TABLE {_quote(engine, name)} IS :c"), {"c": marker(job_id)}
        )
    index_cache.drop_table_indices(name, engine, drop_primary_key=not keep_primary_key)


def index_commands(engine, table_name: str, model) -> List[str]:
    """The model's indexes the table lacks, as index_manager builds them."""
    from materializationengine.index_manager import index_cache

    return index_cache.add_indices_sql_commands(table_name, model, engine)


def _relation_renames(conn, quote, temp: str, final: str) -> List[str]:
    """ALTER statements renaming temp's indexes, constraints and id sequence to the names
    index_manager expects for final (by what each index is, not by string replacement),
    then the table itself."""
    statements = []
    rows = conn.execute(
        text(
            """
            SELECT ic.relname AS index_name, i.indisprimary, am.amname,
                   (SELECT a.attname FROM pg_attribute a
                     WHERE a.attrelid = i.indrelid AND a.attnum = i.indkey[0]) AS column_name,
                   (SELECT c.conname FROM pg_constraint c WHERE c.conindid = i.indexrelid
                     AND c.conrelid = i.indrelid LIMIT 1) AS constraint_name
              FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              JOIN pg_am am ON am.oid = ic.relam
             WHERE i.indrelid = to_regclass(:t)
            """
        ),
        {"t": f'"{temp}"'},
    ).fetchall()
    for row in (dict(r) for r in rows):
        if row["indisprimary"]:
            new = f"{final}_pkey"
            statements.append(
                f"ALTER TABLE {quote(temp)} RENAME CONSTRAINT {quote(row['constraint_name'])} TO {quote(new)}"
            )
            continue
        if row["amname"] == "gist":
            new = f"idx_{final}_{row['column_name']}".lower()
        else:
            new = f"ix_{final}_{row['column_name']}"
        if row["constraint_name"]:
            statements.append(
                f"ALTER TABLE {quote(temp)} RENAME CONSTRAINT {quote(row['constraint_name'])} TO {quote(new)}"
            )
        else:
            statements.append(f"ALTER INDEX {quote(row['index_name'])} RENAME TO {quote(new)}")
    sequence = conn.execute(
        text("SELECT pg_get_serial_sequence(:t, 'id')"), {"t": f'"{temp}"'}
    ).scalar()
    if sequence:
        statements.append(f"ALTER SEQUENCE {sequence} RENAME TO {quote(f'{final}_id_seq')}")
    statements.append(f"ALTER TABLE {quote(temp)} RENAME TO {quote(final)}")
    return statements


def publish(
    engine,
    job_id: str,
    names: Dict[str, str],
    table_meta: dict,
    pcg_table_name: str,
    created: datetime,
    last_updated: datetime,
    lock_timeout: str = "5min",
) -> str:
    """Rename the built tables into place and register them, in one transaction.

    Returns "published", or "already published" when an earlier attempt committed (the
    final table carries this job's marker). Raises PublishConflict, changing nothing,
    if a final name is taken.
    """
    quote = engine.dialect.identifier_preparer.quote
    finals = [names["final_annotation"], names["final_segmentation"]]
    with engine.begin() as conn:
        conn.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout}'"))
        if _table_exists(conn, names["final_annotation"]) and _comment(
            conn, names["final_annotation"]
        ) == marker(job_id):
            return "already published"
        taken = [n for n in finals if _table_exists(conn, n)] + _metadata_rows(conn, finals)
        if taken:
            raise PublishConflict(
                f"cannot publish {names['annotation']} as {names['final_annotation']}: "
                f"{sorted(set(taken))} already exist in the production database"
            )
        temps = [names["annotation"], names["segmentation"]]
        missing = [t for t in temps if not _table_exists(conn, t)]
        if missing:
            raise RuntimeError(f"built tables {missing} are missing")
        conn.execute(text(f"LOCK TABLE {', '.join(quote(t) for t in temps)} IN ACCESS EXCLUSIVE MODE"))
        for temp, final in zip(temps, finals):
            for statement in _relation_renames(conn, quote, temp, final):
                conn.execute(text(statement))
        # rows were imported with their ids, so the sequence never moved; without this
        # the first annotation added through the API would collide with id 1
        conn.execute(
            text(
                f"SELECT setval(pg_get_serial_sequence(:t, 'id'),"
                f" coalesce((SELECT max(id) FROM {quote(finals[0])}), 1),"
                f" (SELECT max(id) IS NOT NULL FROM {quote(finals[0])}))"
            ),
            {"t": f'"{finals[0]}"'},
        )
        now = datetime.utcnow()
        conn.execute(
            AnnoMetadata.__table__.insert().values(
                table_name=finals[0],
                valid=True,
                created=created,
                last_modified=now,
                **{k: table_meta[k] for k in (
                    "schema_type", "description", "user_id", "reference_table",
                    "flat_segmentation_source", "voxel_resolution_x", "voxel_resolution_y",
                    "voxel_resolution_z", "read_permission", "write_permission", "notice_text",
                )},
            )
        )
        conn.execute(
            SegmentationMetadata.__table__.insert().values(
                table_name=finals[1],
                annotation_table=finals[0],
                schema_type=table_meta["schema_type"],
                valid=True,
                created=now,
                pcg_table_name=pcg_table_name,
                last_updated=last_updated,
            )
        )
    logger.info(f"published {names['annotation']} as {finals[0]} (and {finals[1]})")
    return "published"


def drop_build_tables(engine, job_id: str, names: Dict[str, str]) -> List[str]:
    """Drop this job's temporary tables: only ones carrying its marker, never a table
    with a metadata row (a published one). Returns the tables dropped."""
    dropped = []
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as conn:
        registered = set(_metadata_rows(conn, [names["annotation"], names["segmentation"]]))
        # the segmentation table first: it may reference the annotation table
        for key in ("segmentation", "annotation"):
            name = names[key]
            if not name.startswith(build_prefix(job_id)) or name in registered:
                continue
            if _table_exists(conn, name) and _comment(conn, name) == marker(job_id):
                conn.execute(text(f"DROP TABLE {quote(name)}"))
                dropped.append(name)
    return dropped
