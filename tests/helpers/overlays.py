from __future__ import annotations

from typing import Any


def apply_overlay(
    base: dict[str, Any],
    override: dict[str, Any],
    *,
    replace_sections: tuple[str, ...] = (),
) -> dict[str, Any]:
    return _deep_merge(base, override, replace_sections=frozenset(replace_sections))


def _deep_merge(
    base: dict[str, Any],
    override: dict[str, Any],
    *,
    replace_sections: frozenset[str] = frozenset(),
    path: tuple[str, ...] = (),
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        section_path = (*path, str(key))
        dotted_path = ".".join(section_path)
        existing = merged.get(key)
        if (
            dotted_path not in replace_sections
            and isinstance(existing, dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(
                existing,
                value,
                replace_sections=replace_sections,
                path=section_path,
            )
        else:
            merged[key] = value
    return merged
