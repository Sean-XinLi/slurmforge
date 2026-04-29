from __future__ import annotations

import copy
from typing import Any

from ...config_schema import reject_unknown_config_keys
from ..models import LauncherSpec
from ..parse_common import optional_mapping


def parse_launcher(raw: Any, *, name: str) -> LauncherSpec:
    data = optional_mapping(raw, f"stages.{name}.launcher")
    reject_unknown_config_keys(data, parent=f"stages.{name}.launcher")
    rendezvous = optional_mapping(
        data.get("rendezvous"), f"stages.{name}.launcher.rendezvous"
    )
    reject_unknown_config_keys(rendezvous, parent=f"stages.{name}.launcher.rendezvous")
    launcher_type = str(data.get("type") or "single")
    options = copy.deepcopy(data)
    options.pop("type", None)
    return LauncherSpec(type=launcher_type, options=options)
