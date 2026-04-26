from __future__ import annotations

from pathlib import Path

from ..plans import PipelinePlan
from .sbatch import _job_name, _q, _runtime_bootstrap_lines


def render_controller_sbatch(plan: PipelinePlan) -> str:
    root = Path(plan.root_dir)
    resources = dict(plan.controller_plan.resources or {})
    runtime_plan = dict(plan.controller_plan.runtime_plan or {})
    executor_plan = dict(runtime_plan.get("executor") or runtime_plan)
    python_plan = dict(executor_plan.get("python") or {})
    python_bin = str(python_plan.get("bin") or "python3")
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={_job_name('sforge', plan.pipeline_id, 'controller')}",
        f"#SBATCH --output={_q(str(root / 'controller' / 'controller-%j.out'))}",
        f"#SBATCH --error={_q(str(root / 'controller' / 'controller-%j.err'))}",
    ]
    if resources.get("partition"):
        lines.append(f"#SBATCH --partition={resources['partition']}")
    if resources.get("time_limit"):
        lines.append(f"#SBATCH --time={resources['time_limit']}")
    if resources.get("cpus"):
        lines.append(f"#SBATCH --cpus-per-task={int(resources['cpus'])}")
    if resources.get("mem"):
        lines.append(f"#SBATCH --mem={resources['mem']}")
    lines.extend(
        [
            "set -euo pipefail",
            *_runtime_bootstrap_lines(runtime_plan),
            f"PIPELINE_ROOT={_q(plan.root_dir)}",
            f"{_q(python_bin)} -m slurmforge.controller.pipeline --pipeline-root " + '"${PIPELINE_ROOT}"',
            "",
        ]
    )
    return "\n".join(lines)


def write_controller_submit_file(plan: PipelinePlan) -> Path:
    root = Path(plan.root_dir)
    path = root / "controller" / "controller.sbatch"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_controller_sbatch(plan), encoding="utf-8")
    path.chmod(0o755)
    return path
