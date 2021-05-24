from materializationengine.models import AnalysisTable, AnalysisVersion
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError, Schema
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

class CronField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, (str, int, list)):
            return value
        else:
            raise ValidationError('Field should be str, int or list')

class CeleryBeatSchema(Schema):
    name = fields.Str(required=True)
    minute = CronField(default='*')
    hour = CronField(default='*')
    day_of_week = CronField(default='*')
    day_of_month = CronField(default='*')
    month_of_year = CronField(default='*')
    task = fields.Str(required=True)
