from __future__ import annotations

from ..planner.audit_models import dry_run_audit_to_dict as _dry_run_audit_to_dict
from ..planner.audit import (
    build_dry_run_audit as _build_dry_run_audit,
    build_empty_source_selection_audit as _build_empty_source_selection_audit,
)
from ..spec import ExperimentSpec


def build_dry_run_audit(
    spec: ExperimentSpec, plan, *, command: str, full: bool = False
):
    return _build_dry_run_audit(spec, plan, command=command, full=full)


def build_empty_source_selection_audit(
    *, command: str, stage: str, query: str, source_root: str
):
    return _build_empty_source_selection_audit(
        command=command,
        stage=stage,
        query=query,
        source_root=source_root,
    )


def dry_run_audit_to_dict(audit):
    return _dry_run_audit_to_dict(audit)
