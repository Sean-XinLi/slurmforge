from __future__ import annotations

from ..config_schema import comment_for as schema_comment_for
from ..config_schema import option_comment as schema_option_comment


def comment_for(field: str, *, indent: int) -> str:
    return f"{' ' * indent}# {schema_comment_for(field)}"


def inline_comment_for(field: str) -> str:
    return schema_comment_for(field)


def option_comment(field: str, *, indent: int) -> str:
    return schema_option_comment(field, indent=indent)
