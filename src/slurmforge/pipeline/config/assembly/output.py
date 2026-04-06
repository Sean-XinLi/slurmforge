from __future__ import annotations

from pathlib import Path
from typing import Any

from ....errors import ConfigContractError
from ..normalize.slurm_deps import normalize_dependency_mapping
from ..models import OutputConfigSpec
from ..utils import ensure_dict, ensure_path_segment


def normalize_output_config(value: Any, *, config_path: Path | str) -> OutputConfigSpec:
    data = ensure_dict(value, "output")
    batch_name = data.get("batch_name")
    if isinstance(batch_name, str):
        batch_name = batch_name.strip() or None
    elif batch_name is not None:
        raise ConfigContractError(f"{config_path}: output.batch_name must be a string when provided")
    if batch_name is not None:
        batch_name = ensure_path_segment(batch_name, name=f"{config_path}: output.batch_name")
    return OutputConfigSpec(
        base_output_dir=str(data.get("base_output_dir", "./runs")).strip() or "./runs",
        batch_name=batch_name,
        dependencies=normalize_dependency_mapping(
            data.get("dependencies"),
            field_name=f"{config_path}: output.dependencies",
        ),
    )
