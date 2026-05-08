from __future__ import annotations

from typing import Any

from .errors import RecordContractError


def required_record(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RecordContractError(f"{label} must be an object")
    return dict(value)


def required_field(payload: dict[str, Any], field_name: str, *, label: str) -> Any:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    return payload[field_name]


def required_string(
    payload: dict[str, Any],
    field_name: str,
    *,
    label: str,
    non_empty: bool = False,
) -> str:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, str):
        raise RecordContractError(f"{label}.{field_name} must be a string")
    if non_empty and not value:
        raise RecordContractError(f"{label}.{field_name} must be a non-empty string")
    return value


def required_nullable_string(
    payload: dict[str, Any],
    field_name: str,
    *,
    label: str,
) -> str | None:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if value is None:
        return None
    if not isinstance(value, str):
        raise RecordContractError(f"{label}.{field_name} must be a string or null")
    return value


def required_bool(payload: dict[str, Any], field_name: str, *, label: str) -> bool:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, bool):
        raise RecordContractError(f"{label}.{field_name} must be a bool")
    return value


def required_nullable_bool(
    payload: dict[str, Any], field_name: str, *, label: str
) -> bool | None:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if value is None:
        return None
    if not isinstance(value, bool):
        raise RecordContractError(f"{label}.{field_name} must be a bool or null")
    return value


def required_int(payload: dict[str, Any], field_name: str, *, label: str) -> int:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecordContractError(f"{label}.{field_name} must be an integer")
    return value


def required_nullable_int(
    payload: dict[str, Any], field_name: str, *, label: str
) -> int | None:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise RecordContractError(f"{label}.{field_name} must be an integer or null")
    return value


def required_nullable_float(
    payload: dict[str, Any], field_name: str, *, label: str
) -> float | None:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise RecordContractError(f"{label}.{field_name} must be a number or null")
    return float(value)


def required_object(
    payload: dict[str, Any], field_name: str, *, label: str
) -> dict[str, Any]:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, dict):
        raise RecordContractError(f"{label}.{field_name} must be an object")
    return dict(value)


def required_nullable_object(
    payload: dict[str, Any], field_name: str, *, label: str
) -> dict[str, Any] | None:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RecordContractError(f"{label}.{field_name} must be an object or null")
    return dict(value)


def required_json_value(payload: dict[str, Any], field_name: str, *, label: str) -> Any:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    return payload[field_name]


def required_array(
    payload: dict[str, Any], field_name: str, *, label: str
) -> tuple[Any, ...]:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, list):
        raise RecordContractError(f"{label}.{field_name} must be an array")
    return tuple(value)


def required_string_tuple(
    payload: dict[str, Any],
    field_name: str,
    *,
    label: str,
    non_empty: bool = False,
    non_empty_items: bool = False,
) -> tuple[str, ...]:
    values = required_array(payload, field_name, label=label)
    return _string_tuple_items(
        values,
        label=f"{label}.{field_name}",
        non_empty=non_empty,
        non_empty_items=non_empty_items,
    )


def required_string_array(
    payload: dict[str, Any],
    field_name: str,
    *,
    label: str,
    non_empty: bool = False,
    non_empty_items: bool = False,
) -> tuple[str, ...]:
    return required_string_tuple(
        payload,
        field_name,
        label=label,
        non_empty=non_empty,
        non_empty_items=non_empty_items,
    )


def required_object_array(
    payload: dict[str, Any],
    field_name: str,
    *,
    label: str,
    non_empty: bool = False,
) -> tuple[dict[str, Any], ...]:
    values = required_array(payload, field_name, label=label)
    if non_empty and not values:
        raise RecordContractError(f"{label}.{field_name} must be non-empty")
    result: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            raise RecordContractError(f"{label}.{field_name} items must be objects")
        result.append(dict(item))
    return tuple(result)


def required_string_map(
    payload: dict[str, Any],
    field_name: str,
    *,
    label: str,
    non_empty_values: bool = False,
) -> dict[str, str]:
    values = required_object(payload, field_name, label=label)
    result: dict[str, str] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not key:
            raise RecordContractError(
                f"{label}.{field_name} keys must be non-empty strings"
            )
        if not isinstance(value, str):
            raise RecordContractError(f"{label}.{field_name}.{key} must be a string")
        if non_empty_values and not value:
            raise RecordContractError(
                f"{label}.{field_name}.{key} must be a non-empty string"
            )
        result[key] = value
    return result


def string_tuple_record_field(
    value: Any,
    *,
    label: str,
    non_empty: bool = False,
    non_empty_items: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise RecordContractError(f"{label} must be a tuple")
    return _string_tuple_items(
        value,
        label=label,
        non_empty=non_empty,
        non_empty_items=non_empty_items,
    )


def _string_tuple_items(
    values: tuple[Any, ...],
    *,
    label: str,
    non_empty: bool,
    non_empty_items: bool,
) -> tuple[str, ...]:
    result = tuple(_string_array_item(item, label=label) for item in values)
    if non_empty and not result:
        raise RecordContractError(f"{label} must be non-empty")
    if non_empty_items and any(not item for item in result):
        raise RecordContractError(f"{label} must contain non-empty strings")
    return result


def enum_value(value: str, allowed: tuple[str, ...], *, label: str) -> str:
    if value not in allowed:
        raise RecordContractError(f"Unsupported {label}: {value}")
    return value


def _string_array_item(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise RecordContractError(f"{label} items must be strings")
    return value
