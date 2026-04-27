from __future__ import annotations

from typing import Any

from ...io import SchemaVersion, require_schema


def require_plan_schema(payload: dict[str, Any], *, name: str) -> None:
    require_schema(payload, name=name, version=SchemaVersion.PLAN)
