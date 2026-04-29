from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigContractError
from .models import ExperimentSpec
from .parser import parse_experiment_spec
from .validation import validate_experiment_spec


def spec_snapshot_path(root: Path) -> Path:
    return root / "spec_snapshot.yaml"


def load_spec_snapshot(root: Path) -> dict[str, Any]:
    path = spec_snapshot_path(root)
    if not path.exists():
        raise ConfigContractError(f"spec_snapshot.yaml not found under {root}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ConfigContractError(f"spec_snapshot.yaml must contain a mapping: {path}")
    return payload


def load_experiment_spec_from_snapshot(
    root: Path, *, project_root: Path
) -> ExperimentSpec:
    path = spec_snapshot_path(root)
    spec = parse_experiment_spec(
        load_spec_snapshot(root),
        config_path=path.resolve(),
        project_root=project_root.resolve(),
    )
    validate_experiment_spec(spec)
    return spec
