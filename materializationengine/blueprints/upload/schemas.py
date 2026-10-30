from marshmallow import (
    Schema,
    fields,
    validates,
    ValidationError,
    EXCLUDE,
    validates_schema,
)
from typing import Dict, Any
import datetime

from marshmallow import validate
from materializationengine.blueprints.upload.parquet_source import is_parquet_source
from materializationengine.blueprints.upload.source import parse_source_uri


def _parse_time(value: str, name: str) -> datetime.datetime:
    try:
        ts = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise ValidationError(f"Not an ISO date and time: {value!r}", name)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)
    return ts


class RootIdsLookupSchema(Schema):
    """When the source's root id columns were looked up: at one time, sometime between
    two, or not known. Every supplied root is checked either way (supplied_ids.py);
    this is kept with the job and used by the pre-upload sample check."""

    mode = fields.Str(required=True, validate=validate.OneOf(["at", "between", "unknown"]))
    at = fields.Str(allow_none=True, missing=None)
    start = fields.Str(allow_none=True, missing=None)
    end = fields.Str(allow_none=True, missing=None)

    @validates_schema
    def validate_times(self, data, **kwargs):
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
        if data["mode"] == "at":
            if not data.get("at"):
                raise ValidationError("Give the time the root ids were looked up at", "at")
            if _parse_time(data["at"], "at") > now:
                raise ValidationError("The time is in the future", "at")
        elif data["mode"] == "between":
            if not (data.get("start") and data.get("end")):
                raise ValidationError("Give both ends of the time range", "start")
            start, end = _parse_time(data["start"], "start"), _parse_time(data["end"], "end")
            if start >= end:
                raise ValidationError("The range must start before it ends", "start")
            if end > now:
                raise ValidationError("The range ends in the future", "end")


class MetadataSchema(Schema):
    schema_type = fields.Str(required=True)
    datastack_name = fields.Str(required=True)
    table_name = fields.Str(required=True)
    description = fields.Str(required=True)
    notice_text = fields.Str(allow_none=True)
    reference_table = fields.Str(allow_none=True)
    flat_segmentation_source = fields.Str(allow_none=True)
    write_permission = fields.Str(
        required=True, validate=lambda x: x in ["PRIVATE", "GROUP", "PUBLIC"]
    )
    read_permission = fields.Str(
        required=True, validate=lambda x: x in ["PRIVATE", "GROUP", "PUBLIC"]
    )
    is_reference_schema = fields.Bool(data_key="isReferenceSchema")
    metadata_saved = fields.Bool(data_key="metadataSaved")
    voxel_resolution_nm_x = fields.Float(required=True)
    voxel_resolution_nm_y = fields.Float(required=True)
    voxel_resolution_nm_z = fields.Float(required=True)
    root_ids_lookup = fields.Nested(RootIdsLookupSchema, allow_none=True, missing=None)


class UploadRequestSchema(Schema):
    filename = fields.Str(required=True)
    # A gs:// or s3:// location to read the CSV from in place, instead of the file
    # uploaded to the upload bucket as filename.
    source_uri = fields.Str(data_key="sourceUri", missing=None, allow_none=True)
    column_mapping = fields.Dict(required=True, data_key="columnMapping")
    ignored_columns = fields.List(fields.Str(), data_key="ignoredColumns", missing=[])
    metadata = fields.Nested(MetadataSchema, required=True)

    @validates("source_uri")
    def validate_source_uri(self, value):
        if value:
            try:
                parse_source_uri(value)
                is_parquet_source(value)  # refuses a file that is neither CSV nor parquet
            except ValueError as e:
                raise ValidationError(str(e))

    @validates_schema
    def validate_filename(self, data, **kwargs):
        # a file uploaded through the browser is a CSV; a cloud location may also be
        # parquet (a file, a folder of files or a Delta table)
        if not data.get("source_uri") and not data.get("filename", "").endswith(".csv"):
            raise ValidationError("File must be a CSV", "filename")

    @validates_schema
    def validate_ignored_columns(self, data, **kwargs):
        """Ensure ignored columns aren't mapped"""
        if not data.get("ignored_columns"):
            return

        mapped_columns = set(data["column_mapping"].keys())
        ignored_columns = set(data["ignored_columns"])

        if mapped_columns & ignored_columns:
            raise ValidationError(
                "Columns cannot be both mapped and ignored: "
                f"{mapped_columns & ignored_columns}"
            )
