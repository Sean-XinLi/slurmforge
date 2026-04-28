from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigContractError
from ..io import to_jsonable
from ..orchestration import build_dry_run_audit
from ..spec import ExperimentSpec
from .args import dry_run_mode_from_args


def emit_machine_dry_run_if_requested(
    args: argparse.Namespace,
    spec: ExperimentSpec,
    plan,
    *,
    command: str,
) -> bool:
    mode = dry_run_mode_from_args(args)
    if mode not in {"json", "full"}:
        return False
    audit = build_dry_run_audit(spec, plan, command=command, full=mode == "full")
    payload = json.dumps(to_jsonable(audit), indent=2, sort_keys=True) + "\n"
    output = getattr(args, "output", None)
    if output:
        Path(output).write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    return True


def load_snapshot_yaml(root: Path) -> dict[str, Any]:
    path = root / "spec_snapshot.yaml"
    if not path.exists():
        raise FileNotFoundError(f"spec_snapshot.yaml not found under {root}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ConfigContractError(f"spec_snapshot.yaml must contain a mapping: {path}")
    return payload
