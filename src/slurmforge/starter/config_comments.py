from __future__ import annotations

from ..field_options import option_table as field_option_table
from ..field_options import options_comment


def option_comment(field: str, *, indent: int) -> str:
    return options_comment(field, indent=indent)


def option_table() -> str:
    return field_option_table()
