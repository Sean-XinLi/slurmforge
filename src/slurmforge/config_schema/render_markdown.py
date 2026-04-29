from __future__ import annotations

from collections import defaultdict

from .models import ConfigField
from .query import all_fields, fields_for_template, first_edit_fields_for_template

SECTION_ORDER = (
    "Identity",
    "Storage",
    "Hardware",
    "Runtime",
    "Sizing",
    "Runs",
    "Dispatch",
    "Resources",
    "Stages",
    "Launcher",
    "Stage GPU",
    "Stage IO",
    "Notifications",
)
LEVEL_ORDER = {"common": 0, "intermediate": 1, "advanced": 2}


def render_first_edit_list(template: str) -> str:
    rows = []
    for field in first_edit_fields_for_template(template):
        rows.append(f"- `{field.path}`: {field.short_help}")
    return "\n".join(rows)


def render_template_config_guide(template: str) -> str:
    fields = fields_for_template(template)
    grouped = _group_by_section(fields)
    lines = [
        "# Starter Config Guide",
        "",
        f"Template: `{template}`",
        "",
        "This guide covers only the fields used by this starter. The full config reference lives in `docs/config.md`.",
        "",
        "## Fields To Edit First",
        "",
        render_first_edit_list(template),
    ]
    for section in SECTION_ORDER:
        section_fields = grouped.get(section)
        if not section_fields:
            continue
        lines.extend(["", f"## {section}", ""])
        for field in section_fields:
            lines.extend(_render_field_block(field))
    return "\n".join(lines).rstrip() + "\n"


def render_global_field_reference() -> str:
    rows = [
        "| Field | Type | Required | Level | Default | Options | Meaning | When To Change |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for field in sorted(
        all_fields(), key=lambda item: (_section_index(item.section), item.path)
    ):
        rows.append(
            "| "
            f"`{field.path}` | "
            f"{_field_value_type(field)} | "
            f"{_required_text(field)} | "
            f"{field.level} | "
            f"{_cell(field.default)} | "
            f"{_cell(_options_text(field))} | "
            f"{_cell(field.short_help)} | "
            f"{_cell(field.when_to_change)} |"
        )
    return "\n".join(rows)


def _render_field_block(field: ConfigField) -> list[str]:
    lines = [
        f"### `{field.path}`",
        "",
        field.short_help,
        "",
        f"- Type: `{_field_value_type(field)}`",
        f"- Required: {_required_text(field)}",
        f"- Level: `{field.level}`",
        f"- Default: `{field.default}`"
        if field.default is not None
        else "- Default: template-specific",
        f"- When to change: {field.when_to_change}",
    ]
    if field.options:
        lines.extend(["- Options:"])
        for option in field.options:
            lines.append(f"  - `{option.value}`: {option.description}")
    lines.append("")
    return lines


def _group_by_section(fields: tuple[ConfigField, ...]) -> dict[str, list[ConfigField]]:
    grouped: dict[str, list[ConfigField]] = defaultdict(list)
    for field in fields:
        grouped[field.section].append(field)
    for section_fields in grouped.values():
        section_fields.sort(key=lambda item: (LEVEL_ORDER[item.level], item.path))
    return grouped


def _section_index(section: str) -> int:
    try:
        return SECTION_ORDER.index(section)
    except ValueError:
        return len(SECTION_ORDER)


def _options_text(field: ConfigField) -> str:
    if not field.options:
        return ""
    return ", ".join(f"`{option.value}`" for option in field.options)


def _field_value_type(field: ConfigField) -> str:
    if field.options:
        return "enum"
    return field.value_type


def _required_text(field: ConfigField) -> str:
    if field.required is True:
        return "yes"
    if field.required is False:
        return "no"
    return "contextual"


def _cell(value: str | None) -> str:
    if value is None or value == "":
        return ""
    return value.replace("|", "\\|").replace("\n", "<br>")
