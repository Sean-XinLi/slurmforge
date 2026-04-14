from __future__ import annotations

from typing import TYPE_CHECKING

from ...config.runtime import EnvConfig
from ...records import RunPlan
from .common import q, sanitize_activate_command

if TYPE_CHECKING:
    from ...config.api import StorageConfigSpec


def runtime_environment_setup_lines(env_cfg: EnvConfig) -> list[str]:
    lines: list[str] = []
    for module_name in env_cfg.modules:
        lines.append(f"module load {q(module_name)}")

    conda_activate = sanitize_activate_command("env.conda_activate", env_cfg.conda_activate)
    if conda_activate:
        lines.append(conda_activate)

    venv_activate = sanitize_activate_command("env.venv_activate", env_cfg.venv_activate)
    if venv_activate:
        lines.append(venv_activate)

    for key, value in env_cfg.extra_env.items():
        lines.append(f"export {key}={q(value)}")
    return lines


def append_env_setup(
    lines: list[str],
    plan: RunPlan,
    *,
    storage_config: StorageConfigSpec | None = None,
) -> None:
    train_runtime = plan.train_stage.runtime

    # When planning_recovery=False (pure DB mode), planning files don't exist.
    # Export empty strings so user scripts see the env var but skip gracefully.
    planning_recovery = True
    if storage_config is not None:
        planning_recovery = storage_config.exports.planning_recovery

    if planning_recovery:
        execution_plan_json_path = q(str(plan.dispatch.record_path or ""))
        run_snapshot_json_path = q(f"{plan.run_dir}/meta/run_snapshot.json")
    else:
        execution_plan_json_path = q("")
        run_snapshot_json_path = q("")

    resolved_config_yaml_path = q(f"{plan.run_dir}/resolved_config.yaml")
    lines.extend(runtime_environment_setup_lines(plan.env))
    lines.append("")
    lines.append('export AI_INFRA_RESULT_DIR="${JOB_RESULT_DIR}"')
    lines.append('export AI_INFRA_CHECKPOINT_DIR="${JOB_RESULT_DIR}/checkpoints"')
    lines.append('export AI_INFRA_EVAL_CSV_DIR="${JOB_RESULT_DIR}/eval_csv"')
    lines.append('export AI_INFRA_EVAL_IMAGE_DIR="${JOB_RESULT_DIR}/eval_images"')
    lines.append('export AI_INFRA_META_DIR="${META_DIR}"')
    lines.append(f"export AI_INFRA_EXECUTION_PLAN_JSON_PATH={execution_plan_json_path}")
    lines.append(f"export AI_INFRA_RUN_SNAPSHOT_JSON_PATH={run_snapshot_json_path}")
    lines.append(f"export AI_INFRA_RESOLVED_CONFIG_YAML_PATH={resolved_config_yaml_path}")
    lines.append(f"export AI_INFRA_TRAIN_RUNTIME_NNODES={q(train_runtime.nnodes)}")
    lines.append(f"export AI_INFRA_TRAIN_RUNTIME_NPROC_PER_NODE={q(train_runtime.nproc_per_node)}")
    lines.append(
        "export AI_INFRA_TRAIN_MASTER_PORT="
        + q("" if train_runtime.master_port is None else train_runtime.master_port)
    )
    eval_runtime = None if plan.eval_stage is None else plan.eval_stage.runtime
    lines.append(f"export AI_INFRA_EVAL_RUNTIME_NNODES={q(1 if eval_runtime is None else eval_runtime.nnodes)}")
    lines.append(
        f"export AI_INFRA_EVAL_RUNTIME_NPROC_PER_NODE={q(1 if eval_runtime is None else eval_runtime.nproc_per_node)}"
    )
    lines.append(
        "export AI_INFRA_EVAL_MASTER_PORT="
        + q("" if eval_runtime is None or eval_runtime.master_port is None else eval_runtime.master_port)
    )
    lines.append("")
    lines.append("TRAIN_STATUS=0")
    lines.append("EVAL_STATUS=0")
    lines.append("")
