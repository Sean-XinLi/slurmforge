from __future__ import annotations

from .fields import CONFIG_FIELDS
from .models import ConfigField

FIELD_BY_PATH: dict[str, ConfigField] = {field.path: field for field in CONFIG_FIELDS}


def all_fields() -> tuple[ConfigField, ...]:
    return CONFIG_FIELDS


def field_by_path(path: str) -> ConfigField:
    return FIELD_BY_PATH[path]


def fields_for_template(template: str) -> tuple[ConfigField, ...]:
    return tuple(field for field in CONFIG_FIELDS if _field_applies(field, template))


def first_edit_fields_for_template(template: str) -> tuple[ConfigField, ...]:
    return tuple(field for field in fields_for_template(template) if field.first_edit)


def options_for(path: str) -> tuple[str, ...]:
    return tuple(option.value for option in field_by_path(path).options)


def options_csv(path: str) -> str:
    return ", ".join(options_for(path))


def options_sentence(path: str) -> str:
    return _sentence_join(options_for(path))


def comment_for(path: str) -> str:
    field = field_by_path(path)
    return field.yaml_comment or field.short_help


def option_comment(path: str, *, indent: int) -> str:
    return f"{' ' * indent}# Options: {options_csv(path)}. {comment_for(path)}"


def _field_applies(field: ConfigField, template: str) -> bool:
    return not field.templates or template in field.templates


def _sentence_join(values: tuple[str, ...]) -> str:
    if len(values) <= 1:
        return "".join(values)
    return f"{', '.join(values[:-1])}, or {values[-1]}"
