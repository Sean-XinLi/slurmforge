from __future__ import annotations

from pathlib import Path

from ...config_contract.registry import default_for
from ...plans.notifications import FinalizerPlan
from ...plans.resources import ControlResourcesPlan
from ...plans.runtime import EnvironmentPlan
from ...plans.stage import StageBatchPlan
from ..sbatch_helpers import _environment_lines, _job_name, _q
from .headers import render_control_job_headers


def _submit_root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root) / "submit"


def _finalizer_plan(batch: StageBatchPlan) -> FinalizerPlan:
    return batch.notification_plan.finalizer


def _finalizer_resources(batch: StageBatchPlan) -> ControlResourcesPlan:
    return _finalizer_plan(batch).resources


def _finalizer_environment(batch: StageBatchPlan) -> EnvironmentPlan:
    return _finalizer_plan(batch).environment_plan


def _finalizer_python_bin(batch: StageBatchPlan) -> str:
    runtime_plan = _finalizer_plan(batch).runtime_plan
    if runtime_plan is None:
        return default_for("runtime.executor.python.bin")
    return runtime_plan.executor.python.bin


def render_stage_notification_sbatch(
    batch: StageBatchPlan, event: str, *, generation_id: str
) -> str:
    notification_dir = _submit_root(batch) / "notifications" / generation_id
    logs_dir = _submit_root(batch) / "logs" / generation_id
    python_bin = _finalizer_python_bin(batch)
    resources = _finalizer_resources(batch)
    lines = render_control_job_headers(
        job_name=_job_name("sforge", batch.project, batch.stage_name, "notify", event),
        stdout_path=logs_dir / f"notify-{event}-%j.out",
        stderr_path=logs_dir / f"notify-{event}-%j.err",
        resources=resources,
    )
    lines.extend(
        [
            "set -euo pipefail",
            *_environment_lines(_finalizer_environment(batch)),
            f"BATCH_ROOT={_q(batch.submission_root)}",
            f'{_q(python_bin)} -m slurmforge.notifications.finalizer_runtime --root "${{BATCH_ROOT}}" '
            f"--event {_q(event)}",
            "",
        ]
    )
    notification_dir.mkdir(parents=True, exist_ok=True)
    return "\n".join(lines)


def render_stage_notification_barrier_sbatch(
    batch: StageBatchPlan,
    event: str,
    *,
    generation_id: str,
    barrier_index: int,
) -> str:
    logs_dir = _submit_root(batch) / "logs" / generation_id
    resources = _finalizer_resources(batch)
    lines = render_control_job_headers(
        job_name=_job_name(
            "sforge",
            batch.project,
            batch.stage_name,
            "notify-barrier",
            str(barrier_index),
        ),
        stdout_path=logs_dir / f"notify-barrier-{event}-{barrier_index:03d}-%j.out",
        stderr_path=logs_dir / f"notify-barrier-{event}-{barrier_index:03d}-%j.err",
        resources=resources,
    )
    lines.extend(
        [
            "set -euo pipefail",
            f'printf "%s\\n" "notification barrier event={event}"',
            "",
        ]
    )
    return "\n".join(lines)
