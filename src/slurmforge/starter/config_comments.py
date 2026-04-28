from __future__ import annotations

from ..field_options import FIELD_OPTIONS, options_comment, options_csv


def option_comment(field: str, *, indent: int) -> str:
    return options_comment(field, indent=indent)


def option_table() -> str:
    rows = ["| Field | Options |", "| --- | --- |"]
    for field in sorted(FIELD_OPTIONS):
        rows.append(f"| `{field}` | `{options_csv(field).replace(', ', '`, `')}` |")
    return "\n".join(rows)
