from __future__ import annotations


def missing_required_output(output_name: str) -> str:
    return f"required output `{output_name}` was not produced"
