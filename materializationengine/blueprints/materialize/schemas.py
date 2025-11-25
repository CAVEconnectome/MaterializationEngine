from marshmallow import fields, Schema


class AnnotationIDListSchema(Schema):
    annotation_ids = fields.List(fields.Int, required=False)


class VirtualVersionSchema(Schema):
    target_version = fields.Integer()
    tables_to_include = fields.List(fields.Str(), example=None)
    virtual_version_name = fields.Str()


class BadRootsSchema(Schema):
    bad_roots = fields.List(fields.Int(), example=None)
