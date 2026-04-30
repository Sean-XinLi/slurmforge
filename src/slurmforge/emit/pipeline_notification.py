from __future__ import annotations

from pathlib import Path

from ..plans.train_eval import TrainEvalPipelinePlan
from .sbatch_helpers import _job_name, _q
from .stage_render.headers import render_control_job_headers


def _notification_root(plan: TrainEvalPipelinePlan) -> Path:
    return Path(plan.root_dir) / "control" / "notifications"


def _notification_logs_root(plan: TrainEvalPipelinePlan) -> Path:
    return Path(plan.root_dir) / "control" / "logs"


def _notification_file_stem(event: str) -> str:
    return f"notify_{event}"


def render_pipeline_notification_sbatch(
    plan: TrainEvalPipelinePlan, *, event: str
) -> str:
    stem = _notification_file_stem(event)
    lines = render_control_job_headers(
        job_name=_job_name("sforge", plan.pipeline_id, "notify", event),
        stdout_path=_notification_logs_root(plan) / f"{stem}-%j.out",
        stderr_path=_notification_logs_root(plan) / f"{stem}-%j.err",
        resources=plan.notification_plan.resources,
    )
    lines.extend(
        [
            "set -euo pipefail",
            f"PIPELINE_ROOT={_q(plan.root_dir)}",
            f"NOTIFICATION_EVENT={_q(event)}",
            'printf "%s\\n" "[NOTIFY] event=${NOTIFICATION_EVENT} root=${PIPELINE_ROOT}"',
            "true",
            "",
        ]
    )
    return "\n".join(lines)


def write_pipeline_notification_submit_file(
    plan: TrainEvalPipelinePlan, *, event: str
) -> Path:
    path = _notification_root(plan) / f"{_notification_file_stem(event)}.sbatch"
    path.parent.mkdir(parents=True, exist_ok=True)
    _notification_logs_root(plan).mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_pipeline_notification_sbatch(plan, event=event),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def render_pipeline_notification_barrier_sbatch(
    plan: TrainEvalPipelinePlan,
    *,
    event: str,
    barrier_index: int,
) -> str:
    stem = _notification_file_stem(event)
    lines = render_control_job_headers(
        job_name=_job_name(
            "sforge",
            plan.pipeline_id,
            "notify",
            event,
            "barrier",
            str(barrier_index),
        ),
        stdout_path=_notification_logs_root(plan)
        / f"{stem}-barrier-{barrier_index:03d}-%j.out",
        stderr_path=_notification_logs_root(plan)
        / f"{stem}-barrier-{barrier_index:03d}-%j.err",
        resources=plan.notification_plan.resources,
    )
    lines.extend(
        [
            "set -euo pipefail",
            f'printf "%s\\n" "pipeline notification barrier event={event} index={barrier_index}"',
            "",
        ]
    )
    return "\n".join(lines)


def write_pipeline_notification_barrier_file(
    plan: TrainEvalPipelinePlan,
    *,
    event: str,
    barrier_index: int,
) -> Path:
    path = (
        _notification_root(plan)
        / f"{_notification_file_stem(event)}_barrier_{barrier_index:03d}.sbatch"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _notification_logs_root(plan).mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_pipeline_notification_barrier_sbatch(
            plan,
            event=event,
            barrier_index=barrier_index,
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path
