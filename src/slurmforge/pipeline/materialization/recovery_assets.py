from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ..records.snapshot_io import run_snapshot_path_for_run

if TYPE_CHECKING:
    from ..config.api import StorageConfigSpec
    from ..records import RunPlan


@dataclass(frozen=True)
class RecoveryArtifactSpec:
    env_var: str
    target_name: str
    source_path_for_plan: Callable[[RunPlan], str]


def resolved_config_yaml_path_for_run(run_dir: Path) -> Path:
    return run_dir / "resolved_config.yaml"


def run_snapshot_json_path_for_run(run_dir: Path) -> Path:
    return run_snapshot_path_for_run(run_dir)


def _execution_plan_json_path_for_plan(plan: RunPlan) -> str:
    return "" if plan.dispatch.record_path is None else str(plan.dispatch.record_path)


def _resolved_config_yaml_path_for_plan(plan: RunPlan) -> str:
    return str(resolved_config_yaml_path_for_run(Path(plan.run_dir)))


def _run_snapshot_json_path_for_plan(plan: RunPlan) -> str:
    return str(run_snapshot_json_path_for_run(Path(plan.run_dir)))


EXECUTION_PLAN_RECOVERY_ARTIFACT = RecoveryArtifactSpec(
    env_var="AI_INFRA_EXECUTION_PLAN_JSON_PATH",
    target_name="execution_plan.json",
    source_path_for_plan=_execution_plan_json_path_for_plan,
)

RESOLVED_CONFIG_RECOVERY_ARTIFACT = RecoveryArtifactSpec(
    env_var="AI_INFRA_RESOLVED_CONFIG_YAML_PATH",
    target_name="resolved_config.yaml",
    source_path_for_plan=_resolved_config_yaml_path_for_plan,
)

RUN_SNAPSHOT_RECOVERY_ARTIFACT = RecoveryArtifactSpec(
    env_var="AI_INFRA_RUN_SNAPSHOT_JSON_PATH",
    target_name="run_snapshot.json",
    source_path_for_plan=_run_snapshot_json_path_for_plan,
)

RUN_RECOVERY_ARTIFACTS = (
    RESOLVED_CONFIG_RECOVERY_ARTIFACT,
    RUN_SNAPSHOT_RECOVERY_ARTIFACT,
)

FINALIZE_RECOVERY_ARTIFACTS = (
    EXECUTION_PLAN_RECOVERY_ARTIFACT,
    *RUN_RECOVERY_ARTIFACTS,
)


def planning_recovery_enabled(storage_config: StorageConfigSpec | None) -> bool:
    if storage_config is None:
        return True
    return storage_config.exports.planning_recovery


def recovery_env_exports(
    plan: RunPlan,
    *,
    planning_recovery: bool = True,
) -> tuple[tuple[str, str], ...]:
    if not planning_recovery:
        return tuple((spec.env_var, "") for spec in FINALIZE_RECOVERY_ARTIFACTS)
    return tuple((spec.env_var, spec.source_path_for_plan(plan)) for spec in FINALIZE_RECOVERY_ARTIFACTS)


def finalize_recovery_artifacts(*, planning_recovery: bool = True) -> tuple[RecoveryArtifactSpec, ...]:
    if not planning_recovery:
        return ()
    return FINALIZE_RECOVERY_ARTIFACTS
