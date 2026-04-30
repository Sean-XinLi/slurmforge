from __future__ import annotations

from ..config_contract.registry import comment_for as contract_comment_for
from ..config_contract.registry import option_comment as contract_option_comment


def comment_for(field: str, *, indent: int) -> str:
    return f"{' ' * indent}# {contract_comment_for(field)}"


def inline_comment_for(field: str) -> str:
    return contract_comment_for(field)


def option_comment(field: str, *, indent: int) -> str:
    return contract_option_comment(field, indent=indent)
