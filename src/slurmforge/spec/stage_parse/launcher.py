from __future__ import annotations

import copy
from typing import Any

from ..models import LauncherSpec
from ..parse_common import optional_mapping


def parse_launcher(raw: Any, *, name: str) -> LauncherSpec:
    data = optional_mapping(raw, f"stages.{name}.launcher")
    launcher_type = str(data.get("type") or "single")
    options = copy.deepcopy(data)
    options.pop("type", None)
    return LauncherSpec(type=launcher_type, options=options)
