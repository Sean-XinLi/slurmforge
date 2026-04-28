from __future__ import annotations

from ..planner import build_dry_run_audit as _build_dry_run_audit
from ..spec import ExperimentSpec


def build_dry_run_audit(spec: ExperimentSpec, plan, *, command: str, full: bool = False):
    return _build_dry_run_audit(spec, plan, command=command, full=full)
