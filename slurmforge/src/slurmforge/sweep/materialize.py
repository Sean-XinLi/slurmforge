from __future__ import annotations

import copy
from typing import Any


def clone_with_overrides(base: dict[str, Any], assignments: list[tuple[str, Any]]) -> dict[str, Any]:
    # Copy-on-write overlay: only copy containers on override paths instead of deepcopy(base).
    out: dict[str, Any] = dict(base)
    for path, value in assignments:
        parts = path.split(".")
        cur_out: dict[str, Any] = out
        cur_base: Any = base
        for part in parts[:-1]:
            base_child = cur_base.get(part) if isinstance(cur_base, dict) else None
            current_child = cur_out.get(part)
            if current_child is base_child:
                if isinstance(base_child, dict):
                    next_child: dict[str, Any] = dict(base_child)
                else:
                    next_child = {}
                cur_out[part] = next_child
                current_child = next_child
            elif not isinstance(current_child, dict):
                current_child = {}
                cur_out[part] = current_child
            cur_out = current_child
            cur_base = base_child if isinstance(base_child, dict) else {}
        cur_out[parts[-1]] = copy.deepcopy(value)
    return out


def materialize_override_assignments(
    base: dict[str, Any],
    assignments: tuple[tuple[str, Any], ...] | list[tuple[str, Any]],
) -> dict[str, Any]:
    return clone_with_overrides(base, list(assignments))
