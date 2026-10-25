from marshmallow import (
    Schema,
    fields,
    validates,
    ValidationError,
    EXCLUDE,
    validates_schema,
)
from typing import Dict, Any
from materializationengine.blueprints.upload.source import parse_source_uri


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
            except ValueError as e:
                raise ValidationError(str(e))

    @validates("filename")
    def validate_filename(self, value):
        if not value.endswith(".csv"):
            raise ValidationError("File must be a CSV")

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
