from __future__ import annotations

import os
import shlex
from pathlib import Path

from ..config.runtime import NotifyConfig
from ..records.io_utils import atomic_write_text
from ...text_safety import slurm_safe_job_name
from .context import MaterializationLayout
from .layout import map_to_staging


def build_submit_lines(*, total_runs: int, notify_cfg: NotifyConfig | None) -> list[str]:
    submit_lines = ["#!/usr/bin/env bash", "set -euo pipefail", f'echo "[BATCH] total_jobs={total_runs}"']
    if notify_cfg is not None and notify_cfg.enabled:
        submit_lines.append("JOB_IDS=()")
    return submit_lines


def append_notify_submit_lines(
    lines: list[str],
    *,
    notify_cfg: NotifyConfig | None,
    notify_sbatch: Path,
    array_log_dir: Path,
    project: str,
    experiment_name: str,
) -> None:
    if notify_cfg is None or not notify_cfg.enabled:
        return
    notify_job_name = shlex.quote(slurm_safe_job_name(f"{project}_{experiment_name}_notify"))
    notify_out = shlex.quote(str(array_log_dir / "notify-%j.out"))
    notify_err = shlex.quote(str(array_log_dir / "notify-%j.err"))
    notify_email = shlex.quote(notify_cfg.email)
    notify_sbatch_path = shlex.quote(str(notify_sbatch))
    lines.extend(
        [
            'if [[ ${#JOB_IDS[@]} -gt 0 ]]; then',
            '  DEPENDENCY_IDS=$(IFS=:; echo "${JOB_IDS[*]}")',
            '  echo "[SUBMIT-NOTIFY] when=' + notify_cfg.when + ' deps=${DEPENDENCY_IDS}"',
            "  NOTIFY_JOB_ID=$(sbatch --parsable "
            + f"--dependency={notify_cfg.when}:${{DEPENDENCY_IDS}} "
            + f"--job-name={notify_job_name} "
            + f"--output={notify_out} "
            + f"--error={notify_err} "
            + f"--mail-user={notify_email} "
            + "--mail-type=END "
            + f"{notify_sbatch_path})",
            '  echo "[SUBMITTED] notify_job_id=${NOTIFY_JOB_ID}"',
            "fi",
        ]
    )


def write_submit_script(
    submit_lines: list[str],
    *,
    layout: MaterializationLayout,
    group_count: int,
) -> None:
    submit_staging = map_to_staging(
        layout.submit_script,
        final_root=layout.final_batch_root,
        staging_root=layout.staging_root,
    )
    submit_lines.insert(
        3,
        f'echo "[BATCH] array_groups={group_count} manifest={layout.manifest_path}"',
    )
    atomic_write_text(submit_staging, "\n".join(submit_lines) + "\n")
    os.chmod(submit_staging, 0o755)
