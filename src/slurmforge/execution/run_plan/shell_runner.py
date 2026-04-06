from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

from .helper_bins import resolve_runtime_helper_path


def execute_script(script: str) -> int:
    tmp_script_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".sh",
            prefix="slurmforge_exec_",
            delete=False,
        ) as fp:
            fp.write(script)
            fp.flush()
            tmp_script_path = Path(fp.name)

        tmp_script_path.chmod(0o700)
        env = os.environ.copy()
        artifact_sync_bin = resolve_runtime_helper_path("sforge-artifact-sync", env)
        if artifact_sync_bin:
            env.setdefault("AI_INFRA_ARTIFACT_SYNC_BIN", artifact_sync_bin)
        train_outputs_bin = resolve_runtime_helper_path("sforge-write-train-outputs", env)
        if train_outputs_bin:
            env.setdefault("AI_INFRA_WRITE_TRAIN_OUTPUTS_BIN", train_outputs_bin)
        attempt_result_bin = resolve_runtime_helper_path("sforge-write-attempt-result", env)
        if attempt_result_bin:
            env.setdefault("AI_INFRA_WRITE_ATTEMPT_RESULT_BIN", attempt_result_bin)
        completed = subprocess.run(
            ["bash", str(tmp_script_path)],
            check=False,
            env=env,
        )
        return int(completed.returncode)
    finally:
        if tmp_script_path is not None and tmp_script_path.exists():
            tmp_script_path.unlink()
