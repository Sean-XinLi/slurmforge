from __future__ import annotations

import os
from pathlib import Path

from ..plans import StageInstancePlan
from ..contracts import InputBinding
from .bindings import binding_injected_value


def _input_bindings_path(run_dir: Path) -> Path:
    return run_dir / "input_bindings.json"


def build_execution_env(
    instance: StageInstancePlan,
    bindings: tuple[InputBinding, ...],
    *,
    run_dir: Path,
    attempt_dir: Path,
) -> dict[str, str]:
    env = os.environ.copy()
    for key, value in instance.environment_plan.env.items():
        env[str(key)] = str(value)
    if instance.runtime_plan.user is not None:
        for key, value in instance.runtime_plan.user.env.items():
            env[str(key)] = str(value)
    env["SFORGE_RUN_ID"] = instance.run_id
    env["SFORGE_STAGE_NAME"] = instance.stage_name
    env["SFORGE_STAGE_INSTANCE_ID"] = instance.stage_instance_id
    env["SFORGE_INPUT_BINDINGS"] = str(_input_bindings_path(run_dir))
    env["SFORGE_ATTEMPT_DIR"] = str(attempt_dir)
    for binding in bindings:
        env_name = binding.inject.get("env")
        injected = binding_injected_value(binding)
        if env_name and injected is not None:
            env[str(env_name)] = injected
    return env
