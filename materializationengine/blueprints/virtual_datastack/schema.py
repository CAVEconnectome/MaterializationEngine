from marshmallow import Schema, fields, pre_load
from materializationengine.database import dynamic_annotation_cache
from typing import List


class Version(Schema):
    version = fields.Int()
    tables = fields.List(fields.Str())


class VirtualDatastacksSchema(Schema):
    virtual_datastack_name = fields.Str()
    datastack = fields.Str()
    versions = fields.List(fields.Nested(lambda: Version()))


def load_virtual_datastack_info(data: List[dict]):
    """Helper method to load datastack info

    Args:
        data (List[dict]): List of datastacks

    Returns:
        _type_: _description_
    """
    schema = VirtualDatastacksSchema(many=True)
    return schema.load(data)


if __name__ == "__main__":
    example_virtual_datasets_dict = [
        {
            "virtual_datastack_name": "example_public_v1",
            "datastack": "real_datastack",
            "versions": [{"version": 1, "tables": ["some_table_1", "some_table_2"]}],
        },
        {
            "virtual_datastack_name": "example_public_v2",
            "datastack": "real_datastack",
            "versions": [
                {
                    "version": 2,
                    "tables": ["some_table_1", "some_table_2", "some_table_3"],
                },
                {"version": 3, "tables": ["some_table_4"]},
            ],
        },
    ]
    print(load_virtual_datastack_info(example_virtual_datasets_dict))