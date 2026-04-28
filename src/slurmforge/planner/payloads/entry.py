from __future__ import annotations

import copy
from pathlib import Path

from ...plans.launcher import BeforeStepPlan
from ...plans.stage import EntryPlan
from ...spec import ExperimentSpec, StageSpec


def entry_payload(spec: ExperimentSpec, stage: StageSpec) -> EntryPlan:
    workdir = Path(stage.entry.workdir)
    resolved_workdir = workdir if workdir.is_absolute() else spec.project_root / workdir
    return EntryPlan(
        type=stage.entry.type,
        script=stage.entry.script,
        command=copy.deepcopy(stage.entry.command),
        workdir=str(resolved_workdir.resolve()),
        args=copy.deepcopy(stage.entry.args),
    )


def before_payload(stage: StageSpec) -> tuple[BeforeStepPlan, ...]:
    return tuple(BeforeStepPlan(name=step.name, run=step.run) for step in stage.before)
