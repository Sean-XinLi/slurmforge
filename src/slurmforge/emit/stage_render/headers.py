from __future__ import annotations

from pathlib import Path

from ...plans.resources import ControlResourcesPlan
from ..sbatch_helpers import _job_name, _q


def render_control_job_headers(
    *,
    job_name: str,
    stdout_path: Path,
    stderr_path: Path,
    resources: ControlResourcesPlan,
) -> list[str]:
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={_job_name(job_name)}",
        f"#SBATCH --output={_q(str(stdout_path))}",
        f"#SBATCH --error={_q(str(stderr_path))}",
    ]
    if resources.partition:
        lines.append(f"#SBATCH --partition={resources.partition}")
    if resources.time_limit:
        lines.append(f"#SBATCH --time={resources.time_limit}")
    if resources.cpus:
        lines.append(f"#SBATCH --cpus-per-task={int(resources.cpus)}")
    if resources.mem:
        lines.append(f"#SBATCH --mem={resources.mem}")
    return lines
