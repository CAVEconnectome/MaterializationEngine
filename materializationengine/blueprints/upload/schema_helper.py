import marshmallow as mm
from emannotationschemas import get_schema, get_types
from emannotationschemas.flatten import create_flattened_schema


def _get_nested_fields_info(field):
    """Helper function to extract nested field information."""
    nested_fields = {}
    if hasattr(field, "nested"):
        if isinstance(field.nested, mm.Schema):
            for nested_name, nested_field in field.nested._declared_fields.items():
                nested_fields[nested_name] = {
                    "type": type(nested_field).__name__,
                    "required": nested_field.required,
                    "description": nested_field.metadata.get("description", ""),
                }
    return nested_fields


def get_schema_types(schema_name: str, name_only: bool = True) -> list:
    """Get schema types and their fields.

    Args:
        schema_name (str): name of the schema to get information for
        name_only (bool, optional): whether to return only schema names. Defaults to True.

    Returns:
        list: schema types and their fields
    """
    schema_types = get_types()

    if schema_name:
        schema_types = [schema_name]
    schemas_info = []
    for schema_type in schema_types:
        schema = get_schema(schema_type)
        flattened_schema = create_flattened_schema(schema)

        fields_info = {}
        for field_name, field in flattened_schema._declared_fields.items():
            field_type = type(field).__name__
            field_info = {
                "type": field_type,
                "required": field.required,
                "description": field.metadata.get("description", ""),
            }

            if hasattr(field, "nested"):
                field_info["nested_fields"] = _get_nested_fields_info(field)

            fields_info[field_name] = field_info
        if name_only:
            schemas_info.append(schema_type)
        else:
            schema_info = {
                "name": schema_type,
                "description": schema.__doc__ or "",
                "fields": fields_info,
            }
            schemas_info.append(schema_info)
    return schemas_info
