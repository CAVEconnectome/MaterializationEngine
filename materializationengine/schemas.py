from dynamicannotationdb.models import (
    AnalysisTable,
    AnalysisVersion,
    AnalysisView,
    VersionErrorTable,
)
from flask_marshmallow import Marshmallow
from marshmallow import (
    Schema,
    ValidationError,
    fields,
    post_load,
    validate,
    validates_schema,
)
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

ma = Marshmallow()


class AnalysisVersionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisVersion
        load_instance = True


class AnalysisTableSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisTable
        load_instance = True


class AnalysisViewSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisView
        load_instance = True
        fields = ("id", "table_name", "description")
        ordered = True


class VersionErrorTableSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = VersionErrorTable
        load_instance = True


class CronField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, (str, int, list)):
            return value
        else:
            raise ValidationError("Field should be str, int or list")


class TaskParamsSchema(Schema):
    days_to_expire = fields.Int(
        required=False,
        validate=validate.Range(min=1),
        metadata={
            "description": "Number of days until the materialized database expires"
        },
    )
    merge_tables = fields.Boolean(
        required=False,
        default=False,
        metadata={"description": "Whether to merge tables during materialization"},
    )
    datastack = fields.Str(
        required=False, metadata={"description": "The datastack to use for this task"}
    )
    delete_threshold = fields.Int(
        required=False,
        validate=validate.Range(min=1),
        metadata={"description": "Threshold for deleting expired databases"},
    )


class CeleryBeatSchema(Schema):
    name = fields.Str(required=True, metadata={"description": "Name of the task"})
    minute = CronField(
        default="*", metadata={"description": "Minute field for cron schedule"}
    )
    hour = CronField(
        default="*", metadata={"description": "Hour field for cron schedule"}
    )
    day_of_week = CronField(
        default="*",
        metadata={"description": "Day of week for cron schedule"},
    )
    day_of_month = CronField(
        default="*",
        metadata={"description": "Day of month for cron schedule"},
    )
    month_of_year = CronField(
        default="*",
        metadata={"description": "Month of year for cron schedule"},
    )
    task = fields.Str(required=True, metadata={"description": "Type of task to run"})
    datastack_params = fields.Nested(
        TaskParamsSchema,
        required=False,
        metadata={"description": "Parameters specific to the task"},
    )

class MaterializationInfoSchema(Schema):
    """Schema for serializing and deserializing materialization metadata."""
    
    datastack = fields.Str(required=True)
    aligned_volume = fields.Str(required=True)
    database = fields.Str(required=True)
    annotation_table_name = fields.Str(required=True)
    segmentation_table_name = fields.Str(allow_none=True)
    temp_mat_table_name = fields.Str(allow_none=True)
    reference_table = fields.Str(allow_none=True)
    
    schema = fields.Str(required=True)
    pcg_table_name = fields.Str(allow_none=True)
    segmentation_source = fields.Str(allow_none=True)
    
    max_id = fields.Int(allow_none=True)
    min_id = fields.Int(allow_none=True)
    row_count = fields.Int(allow_none=True)
    table_count = fields.Int(allow_none=True)
    
    coord_resolution = fields.List(fields.Float(), allow_none=True)
    
    materialization_time_stamp = fields.Str(required=True)
    last_updated_time_stamp = fields.Str(allow_none=True)
    
    create_segmentation_table = fields.Bool(default=False)
    add_indices = fields.Bool(default=True)
    merge_table = fields.Bool(default=True)
    lookup_all_root_ids = fields.Bool(default=False)
    
    chunk_size = fields.Int(allow_none=True)
    queue_length_limit = fields.Int(allow_none=True)
    throttle_queues = fields.Bool(default=True)
    
    analysis_version = fields.Int(allow_none=True)
    analysis_database = fields.Str(allow_none=True)
    
    @post_load
    def make_materialization_info(self, data, **kwargs):
        """Return the processed data as a dictionary."""
        return data