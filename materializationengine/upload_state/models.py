"""Tables of the upload state database (UPLOAD_STATE_DATABASE, "upload_state" by default).

Everything a Ray-run bulk upload needs to resume after its driver restarts, and that
the upload pages show, lives here instead of in redis: the job record, each stage's
outcome, the Cloud SQL import, the CSV/parquet parts and the spatial lookup chunks.
Every table hangs off upload_jobs with ON DELETE CASCADE, so deleting a job (the 30
day retention sweep) removes all of it.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

SCHEMA_VERSION = 1

# The job statuses the upload pages use; a job in one of these is still running.
ACTIVE_STATUSES = ("pending", "processing")
TERMINAL_STATUSES = ("done", "error", "cancelled")

metadata = MetaData()


def _now_column(name):
    return Column(name, DateTime(timezone=True), nullable=False, server_default=func.now())


schema_version = Table(
    "schema_version",
    metadata,
    Column("version", Integer, primary_key=True),
    _now_column("applied_at"),
)

upload_jobs = Table(
    "upload_jobs",
    metadata,
    Column("job_id", Text, primary_key=True),
    Column("user_id", Text),
    Column("datastack_name", Text, nullable=False),
    Column("table_name", Text, nullable=False),
    Column("staging_database", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("current_stage", Text),
    # The job record exactly as the upload pages read it from redis (status, phase,
    # progress, processed_rows, total_rows, error, active_workflow_part, ...).
    Column("record", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    # What the driver needs to run, fixed once per job: the upload request, and values
    # that must not change between attempts (created_ts, dest_blob,
    # materialization_time_stamp).
    Column("params", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    # False once a failure is known to repeat: a resubmitted driver then exits at once.
    Column("retryable", Boolean, nullable=False, server_default=text("true")),
    Column("rayjob_name", Text),
    Column("rayjob_namespace", Text),
    Column("rayjob_submissions", Integer, nullable=False, server_default=text("0")),
    Column("rayjob_last_status", JSONB),
    Column("rayjob_checked_at", DateTime(timezone=True)),
    # Lease: the driver that holds the highest epoch owns the job; writes fenced on it.
    Column("driver_epoch", Integer, nullable=False, server_default=text("0")),
    Column("driver_host", Text),
    Column("driver_heartbeat_at", DateTime(timezone=True)),
    Column("cancel_requested_at", DateTime(timezone=True)),
    Column("cancel_requested_by", Text),
    _now_column("created_at"),
    _now_column("updated_at"),
    Column("finished_at", DateTime(timezone=True)),
)
Index("ix_upload_jobs_user_updated", upload_jobs.c.user_id, upload_jobs.c.updated_at)
Index("ix_upload_jobs_datastack_updated", upload_jobs.c.datastack_name, upload_jobs.c.updated_at)
Index("ix_upload_jobs_updated", upload_jobs.c.updated_at)
# Two running uploads into one staging table would overwrite each other.
Index(
    "ux_upload_jobs_one_active_per_table",
    upload_jobs.c.staging_database,
    upload_jobs.c.table_name,
    unique=True,
    postgresql_where=upload_jobs.c.status.in_(ACTIVE_STATUSES),
)


def _job_fk():
    return Column(
        "job_id", Text, ForeignKey("upload_jobs.job_id", ondelete="CASCADE"), nullable=False
    )


upload_stages = Table(
    "upload_stages",
    metadata,
    _job_fk(),
    Column("stage", Text, nullable=False),
    Column("status", Text, nullable=False),  # running | done | failed
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("epoch", Integer),
    Column("result", JSONB),
    Column("error", Text),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    PrimaryKeyConstraint("job_id", "stage"),
)

# The Cloud SQL import. The row is written before the import is requested, so a driver
# that dies between the request and recording its operation name can find the
# operation again instead of starting a second import.
upload_cloudsql_ops = Table(
    "upload_cloudsql_ops",
    metadata,
    _job_fk(),
    Column("kind", Text, nullable=False),  # import
    Column("uri", Text, nullable=False),
    Column("database", Text, nullable=False),
    Column("target_table", Text, nullable=False),
    Column("operation_name", Text),
    Column("status", Text, nullable=False),  # requested | running | done | failed
    Column("error", Text),
    _now_column("requested_at"),
    Column("finished_at", DateTime(timezone=True)),
    PrimaryKeyConstraint("job_id", "kind"),
)

upload_parts = Table(
    "upload_parts",
    metadata,
    _job_fk(),
    Column("part_index", Integer, nullable=False),
    Column("kind", Text, nullable=False),  # csv | parquet
    Column("spec", JSONB, nullable=False),
    Column("line_count", BigInteger),
    Column("id_counter_start", BigInteger),
    Column("output_blob", Text, nullable=False),
    Column("status", Text, nullable=False),  # pending | done | failed
    Column("rows_read", BigInteger),
    Column("rows_written", BigInteger),
    Column("dropped_rows", BigInteger),
    Column("split_error", Text),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("error", Text),
    Column("updated_at", DateTime(timezone=True)),
    PrimaryKeyConstraint("job_id", "part_index"),
)

# The spatial lookup of a job; the fields of checkpoint_manager.WorkflowData.
upload_spatial = Table(
    "upload_spatial",
    metadata,
    Column(
        "job_id", Text, ForeignKey("upload_jobs.job_id", ondelete="CASCADE"), primary_key=True
    ),
    Column("database_name", Text, nullable=False),
    Column("workflow_name", Text, nullable=False),
    Column("datastack_name", Text),
    Column("status", Text, nullable=False),
    Column("total_chunks", Integer),
    Column("completed_chunks", Integer, nullable=False, server_default=text("0")),
    Column("failed_chunks", Integer, nullable=False, server_default=text("0")),
    Column("rows_processed", BigInteger, nullable=False, server_default=text("0")),
    Column("total_row_estimate", BigInteger),
    Column("total_point_estimate", BigInteger),
    Column("chunking_strategy", Text),
    Column("used_chunk_size", Integer),
    Column("chunking_parameters", JSONB),
    Column("min_enclosing_bbox", JSONB),
    # mat_metadata for the lookup, including its materialization_time_stamp: fixed once
    # so a resumed lookup asks for root ids as of the same time.
    Column("mat_metadata", JSONB, nullable=False),
    Column("rate_samples", JSONB),
    Column("processing_rate", Text),
    Column("estimated_completion", Text),
    Column("last_error", Text),
    Column("start_time", DateTime(timezone=True)),
    _now_column("created_at"),
    Column("updated_at", DateTime(timezone=True)),
)
Index(
    "ix_upload_spatial_workflow",
    upload_spatial.c.database_name,
    upload_spatial.c.workflow_name,
    upload_spatial.c.created_at,
)

# One row per spatial chunk that has been dispatched; a chunk with no row is pending.
upload_spatial_chunks = Table(
    "upload_spatial_chunks",
    metadata,
    Column(
        "job_id", Text, ForeignKey("upload_spatial.job_id", ondelete="CASCADE"), nullable=False
    ),
    Column("chunk_index", Integer, nullable=False),
    # PROCESSING | COMPLETED | FAILED_RETRYABLE | FAILED_PERMANENT (checkpoint_manager's)
    Column("status", Text, nullable=False),
    Column("attempt_count", Integer, nullable=False, server_default=text("0")),
    Column("rows_processed", BigInteger),
    Column("points", BigInteger),
    Column("error_message", Text),
    Column("error_type", Text),
    _now_column("updated_at"),
    PrimaryKeyConstraint("job_id", "chunk_index"),
)
Index(
    "ix_upload_spatial_chunks_open",
    upload_spatial_chunks.c.job_id,
    upload_spatial_chunks.c.status,
    postgresql_where=upload_spatial_chunks.c.status != "COMPLETED",
)
