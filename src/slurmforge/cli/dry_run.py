from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..io import to_jsonable
from ..orchestration.audit import build_dry_run_audit, dry_run_audit_to_dict
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
    emit_machine_payload(args, dry_run_audit_to_dict(audit))
    return True


def emit_machine_payload(args: argparse.Namespace, payload: object) -> None:
    text = json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n"
    output = getattr(args, "output", None)
    if output:
        Path(output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")
