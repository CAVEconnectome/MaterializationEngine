from marshmallow import fields, Schema


class VirtualVersionSchema(Schema):
    datastack_name = fields.Str()
    target_version = fields.Integer()
    tables_to_include = fields.List(fields.Str(), example=None)
    virtual_version_name = fields.Str()
