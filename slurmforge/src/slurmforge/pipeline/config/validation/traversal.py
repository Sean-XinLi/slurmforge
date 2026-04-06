from __future__ import annotations

from typing import Any

from ....errors import ConfigContractError
from .definitions import DynamicMapping, ObjectSchema


def validate_allowed_keys(data: dict[str, Any], *, name: str, schema: ObjectSchema) -> None:
    unknown = sorted(str(key) for key in data.keys() if key not in schema.fields)
    if unknown:
        raise ConfigContractError(f"{name} contains unsupported keys: {unknown}")


def validate_mapping_schema(data: dict[str, Any], *, name: str, schema: ObjectSchema) -> None:
    validate_allowed_keys(data, name=name, schema=schema)


def schema_without_fields(schema: ObjectSchema, *field_names: str) -> ObjectSchema:
    return ObjectSchema({key: value for key, value in schema.fields.items() if key not in set(field_names)})


def validate_override_path(path: str, *, context_name: str, schema: ObjectSchema) -> None:
    node: object = schema
    consumed: list[str] = []
    for part in path.split("."):
        consumed.append(part)
        if isinstance(node, DynamicMapping):
            return
        if not isinstance(node, ObjectSchema):
            raise ConfigContractError(f"{context_name}.{path} targets unsupported field `{'.'.join(consumed)}`")
        if part not in node.fields:
            raise ConfigContractError(f"{context_name}.{path} targets unsupported field `{'.'.join(consumed)}`")
        node = node.fields[part]
