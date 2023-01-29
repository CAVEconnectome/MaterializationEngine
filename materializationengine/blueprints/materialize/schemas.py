from marshmallow import fields, Schema

class AnnotationIDListSchema(Schema):
    annotation_ids = fields.List(fields.Int, required=True)
    
class VirtualVersionSchema(Schema):
    datastack_name = fields.Str()
    target_version = fields.Integer()
    tables_to_include = fields.List(fields.Str(), example=None)
    virtual_version_name = fields.Str()
