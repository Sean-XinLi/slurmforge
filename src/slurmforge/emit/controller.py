from __future__ import annotations

from pathlib import Path

from ..plans.train_eval import TrainEvalPipelinePlan
from .sbatch_helpers import _environment_lines, _job_name, _q


def render_controller_sbatch(plan: TrainEvalPipelinePlan) -> str:
    root = Path(plan.root_dir)
    resources = plan.controller_plan.resources
    environment_plan = plan.controller_plan.environment_plan
    executor_plan = plan.controller_plan.runtime_plan.executor
    python_bin = executor_plan.python.bin
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={_job_name('sforge', plan.pipeline_id, 'controller')}",
        f"#SBATCH --output={_q(str(root / 'controller' / 'controller-%j.out'))}",
        f"#SBATCH --error={_q(str(root / 'controller' / 'controller-%j.err'))}",
    ]
    if resources.partition:
        lines.append(f"#SBATCH --partition={resources.partition}")
    if resources.time_limit:
        lines.append(f"#SBATCH --time={resources.time_limit}")
    if resources.cpus:
        lines.append(f"#SBATCH --cpus-per-task={int(resources.cpus)}")
    if resources.mem:
        lines.append(f"#SBATCH --mem={resources.mem}")
    lines.extend(
        [
            "set -euo pipefail",
            *_environment_lines(environment_plan),
            f"PIPELINE_ROOT={_q(plan.root_dir)}",
            f"{_q(python_bin)} -m slurmforge.controller.train_eval_pipeline --train-eval-pipeline-root "
            + '"${PIPELINE_ROOT}"',
            "",
        ]
    )
    return "\n".join(lines)


def write_controller_submit_file(plan: TrainEvalPipelinePlan) -> Path:
    root = Path(plan.root_dir)
    path = root / "controller" / "controller.sbatch"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_controller_sbatch(plan), encoding="utf-8")
    path.chmod(0o755)
    return path
