import datetime

from dynamicannotationdb.models import AnalysisView
from flask_marshmallow import Marshmallow
from marshmallow import Schema, fields
from marshmallow.validate import Length

ma = Marshmallow()

class Metadata(Schema):
    user_id = fields.Str(required=False)
    description = fields.Str(required=True)
    reference_table = fields.Str(required=False)


class SegmentationInfoSchema(Schema):
    pcg_table_name = fields.Str(required=True)


class SegmentationTableSchema(SegmentationInfoSchema):
    table_name = fields.Str(order=0, required=True)


class CreateTableSchema(SegmentationTableSchema):
    metadata = fields.Nested(
        Metadata, required=True, example={"description": "my description"}
    )


class GetDeleteAnnotationSchema(SegmentationInfoSchema):
    annotation_ids = fields.List(fields.Int, required=True)


class PostPutAnnotationSchema(SegmentationInfoSchema):
    annotations = fields.List(fields.Dict, required=True)


class SegmentationDataSchema(Schema):
    pcg_table_name = fields.Str(required=True)
    segmentations = fields.List(fields.Dict, required=True)


class V2QuerySchema(Schema):
    table = fields.Str(required=True)
    timestamp = fields.AwareDateTime(
        default_timezone=datetime.timezone.utc, required=True
    )
    join_tables = fields.List(
        fields.List(fields.Str),
        required=False,
    )
    filter_in_dict = fields.Dict()
    filter_notin_dict = fields.Dict()
    filter_equal_dict = fields.Dict()
    filter_greater_dict = fields.Dict()
    filter_less_dict = fields.Dict()
    filter_greater_equal_dict = fields.Dict()
    filter_less_equal_dict = fields.Dict()
    filter_spatial_dict = fields.Dict()
    filter_regex_dict = fields.Dict(required=False)
    select_columns = fields.Dict()
    offset = fields.Integer()
    limit = fields.Integer()
    suffixes = fields.Dict()
    desired_resolution = fields.List(
        fields.Float, validate=Length(equal=3), required=False
    )


class SimpleQuerySchema(Schema):
    filter_in_dict = fields.Dict()
    filter_notin_dict = fields.Dict()
    filter_equal_dict = fields.Dict()
    filter_greater_dict = fields.Dict()
    filter_less_dict = fields.Dict()
    filter_greater_equal_dict = fields.Dict()
    filter_less_equal_dict = fields.Dict()
    filter_spatial_dict = fields.Dict()
    filter_regex_dict = fields.Dict(required=False)
    select_columns = fields.List(fields.Str)
    offset = fields.Integer()
    limit = fields.Integer()
    desired_resolution = fields.List(
        fields.Float, validate=Length(equal=3), required=False
    )


class ComplexQuerySchema(Schema):
    tables = fields.List(
        fields.List(fields.Str, validate=Length(equal=2)), required=True
    )
    filter_in_dict = fields.Dict()
    filter_notin_dict = fields.Dict()
    filter_equal_dict = fields.Dict()
    filter_greater_dict = fields.Dict()
    filter_less_dict = fields.Dict()
    filter_greater_equal_dict = fields.Dict()
    filter_less_equal_dict = fields.Dict()
    filter_spatial_dict = fields.Dict()
    filter_regex_dict = fields.Dict(required=False)
    select_columns = fields.List(fields.Str)
    select_column_map = fields.Dict()
    offset = fields.Integer()
    limit = fields.Integer()
    suffixes = fields.List(fields.Str)
    suffix_map = fields.Dict()
    desired_resolution = fields.List(
        fields.Float, validate=Length(equal=3), required=False
    )

class AnalysisViewSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = AnalysisView