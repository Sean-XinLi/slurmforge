from __future__ import annotations

from typing import Any

from ...record_fields import (
    required_field,
    required_int,
    required_nullable_int,
    required_record,
    required_string,
    required_string_array,
)
from ..launcher import BeforeStepPlan, LauncherPlan, RendezvousPlan


def before_step_plan_from_dict(payload: dict[str, Any]) -> BeforeStepPlan:
    return BeforeStepPlan(
        run=required_string(payload, "run", label="before_step_plan", non_empty=True),
        name=required_string(payload, "name", label="before_step_plan"),
    )


def launcher_plan_from_dict(payload: dict[str, Any]) -> LauncherPlan:
    rendezvous_raw = required_field(payload, "rendezvous", label="launcher_plan")
    return LauncherPlan(
        type=required_string(payload, "type", label="launcher_plan", non_empty=True),
        mode=required_string(payload, "mode", label="launcher_plan"),
        nnodes=required_nullable_int(payload, "nnodes", label="launcher_plan"),
        nproc_per_node=required_nullable_int(
            payload, "nproc_per_node", label="launcher_plan"
        ),
        rendezvous=None
        if rendezvous_raw is None
        else _rendezvous_plan_from_dict(
            required_record(rendezvous_raw, "launcher_plan.rendezvous")
        ),
        args=required_string_array(payload, "args", label="launcher_plan"),
        srun_args=required_string_array(payload, "srun_args", label="launcher_plan"),
    )


def _rendezvous_plan_from_dict(payload: dict[str, Any]) -> RendezvousPlan:
    return RendezvousPlan(
        backend=required_string(
            payload, "backend", label="launcher_plan.rendezvous", non_empty=True
        ),
        endpoint=required_string(payload, "endpoint", label="launcher_plan.rendezvous"),
        port=required_int(payload, "port", label="launcher_plan.rendezvous"),
    )
