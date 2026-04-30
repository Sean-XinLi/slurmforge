from __future__ import annotations

from typing import Any, Final

from .fields.dispatch import FIELDS as DISPATCH_FIELDS
from .fields.hardware import FIELDS as HARDWARE_FIELDS
from .fields.identity import FIELDS as IDENTITY_FIELDS
from .fields.launcher import FIELDS as LAUNCHER_FIELDS
from .fields.notifications import FIELDS as NOTIFICATIONS_FIELDS
from .fields.resources import FIELDS as RESOURCES_FIELDS
from .fields.runtime import FIELDS as RUNTIME_FIELDS
from .fields.runs import FIELDS as RUNS_FIELDS
from .fields.sizing import FIELDS as SIZING_FIELDS
from .fields.stage_gpu import FIELDS as STAGE_GPU_FIELDS
from .fields.stage_io import FIELDS as STAGE_IO_FIELDS
from .fields.stages import FIELDS as STAGES_FIELDS
from .fields.storage import FIELDS as STORAGE_FIELDS
from .models import ConfigField, ConfigOption

CONFIG_FIELDS: Final[tuple[ConfigField, ...]] = (
    *IDENTITY_FIELDS,
    *STORAGE_FIELDS,
    *HARDWARE_FIELDS,
    *RUNTIME_FIELDS,
    *SIZING_FIELDS,
    *RUNS_FIELDS,
    *DISPATCH_FIELDS,
    *RESOURCES_FIELDS,
    *STAGES_FIELDS,
    *LAUNCHER_FIELDS,
    *STAGE_GPU_FIELDS,
    *STAGE_IO_FIELDS,
    *NOTIFICATIONS_FIELDS,
)

CONFIG_FIELDS_BY_PATH: Final[dict[str, ConfigField]] = {
    field.path: field for field in CONFIG_FIELDS
}

OPTIONS_BY_PATH: Final[dict[str, tuple[ConfigOption, ...]]] = {
    field.path: field.options for field in CONFIG_FIELDS if field.options
}


def all_fields() -> tuple[ConfigField, ...]:
    return CONFIG_FIELDS


def field_by_path(path: str) -> ConfigField:
    return CONFIG_FIELDS_BY_PATH[path]


def fields_for_template(template: str) -> tuple[ConfigField, ...]:
    return tuple(field for field in CONFIG_FIELDS if _field_applies(field, template))


def first_edit_fields_for_template(template: str) -> tuple[ConfigField, ...]:
    return tuple(field for field in fields_for_template(template) if field.first_edit)


def default_for(path: str) -> Any:
    return field_by_path(path).default_value


def options_for(path: str) -> tuple[str, ...]:
    return option_values(OPTIONS_BY_PATH[path])


def options_csv(path: str) -> str:
    return ", ".join(options_for(path))


def option_values(options: tuple[ConfigOption, ...]) -> tuple[str, ...]:
    return tuple(option.value for option in options)


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
