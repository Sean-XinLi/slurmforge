from __future__ import annotations

import shutil
from dataclasses import replace
from pathlib import Path

from slurmforge.pipeline.config.normalize import normalize_artifacts, normalize_cluster, normalize_env, normalize_launcher
from slurmforge.pipeline.planning.contracts import (
    AllocationRequest,
    ExecutionTopology,
    ResourceEstimate,
    StageCapabilities,
    StageExecutionPlan,
)
from slurmforge.templating import build_template_env


GENERATED_ARTIFACT_DIR_NAMES = {"__pycache__", ".pytest_cache", "build", "dist"}


def slurmforge_root() -> Path:
    return Path(__file__).resolve().parents[1]


def list_generated_artifact_dirs(root: Path | None = None) -> list[Path]:
    search_root = (root or slurmforge_root()).resolve()
    out: list[Path] = []
    for path in search_root.rglob("*"):
        if not path.is_dir():
            continue
        if path.name in GENERATED_ARTIFACT_DIR_NAMES or path.name.endswith(".egg-info"):
            out.append(path)
    return sorted(out)


def remove_generated_artifact_dirs(root: Path | None = None) -> list[Path]:
    paths = list_generated_artifact_dirs(root)
    for path in reversed(paths):
        shutil.rmtree(path, ignore_errors=True)
    return paths


def make_template_env():
    return build_template_env()


def sample_cluster():
    return normalize_cluster(
        {
            "partition": "p",
            "account": "a",
            "qos": "q",
            "time_limit": "01:00:00",
            "nodes": 1,
            "gpus_per_node": 1,
            "cpus_per_task": 2,
            "mem": "0",
            "constraint": "",
            "extra_sbatch_args": [],
        }
    )


def sample_env():
    return normalize_env({"modules": [], "conda_activate": "", "venv_activate": "", "extra_env": {}})


def sample_artifacts():
    return normalize_artifacts(
        {"checkpoint_globs": [], "eval_csv_globs": [], "eval_image_globs": [], "extra_globs": []}
    )


def sample_stage_plan(**overrides):
    base = StageExecutionPlan(
        name="train",
        stage_kind="train",
        invocation_kind="model_cli",
        launcher_kind="single",
        command_text="python3 train.py",
        workdir=Path(".").resolve(),
        topology=ExecutionTopology(nodes=1, processes_per_node=1, master_port=None),
        allocation=AllocationRequest(nodes=1, gpus_per_node=1, cpus_per_task=2, mem="0"),
        estimate=ResourceEstimate(
            min_total_gpus=1,
            recommended_total_gpus=1,
            max_useful_total_gpus=1,
            estimated_vram_gb=8.0,
            reason="ok",
        ),
        capabilities=StageCapabilities(
            ddp_supported=True,
            ddp_required=False,
            uses_gpu=True,
            external_launcher=False,
            runtime_probe="cuda",
        ),
        python_bin="python3",
        launcher_cfg=normalize_launcher({"mode": "single", "python_bin": "python3", "workdir": "."}),
        cluster_cfg=sample_cluster(),
        script_path=Path("train.py").resolve(),
        cli_args={},
        command_mode=None,
        requested_launcher_mode="auto",
        max_gpus_per_job=8,
    )
    return replace(base, **overrides) if overrides else base


def sample_run_plan(**overrides):
    from slurmforge.pipeline.records import DispatchInfo, RunPlan
    from slurmforge.pipeline.config.api import EvalTrainOutputsConfig

    run_id = str(overrides.get("run_id", "r1"))
    run_dir = str(overrides.get("run_dir", "/tmp/run_1"))
    run_dir_rel = overrides.get("run_dir_rel", str(Path("runs") / Path(run_dir).name))

    base = RunPlan(
        run_index=int(overrides.get("run_index", 1)),
        total_runs=int(overrides.get("total_runs", 1)),
        run_id=run_id,
        project=str(overrides.get("project", "demo")),
        experiment_name=str(overrides.get("experiment_name", "exp")),
        model_name=str(overrides.get("model_name", "convbert")),
        train_mode=str(overrides.get("train_mode", "model_cli")),
        train_stage=overrides.get("train_stage", sample_stage_plan()),
        eval_stage=overrides.get("eval_stage"),
        eval_train_outputs=overrides.get("eval_train_outputs", EvalTrainOutputsConfig()),
        cluster=overrides.get("cluster", sample_cluster()),
        env=overrides.get("env", sample_env()),
        run_dir=run_dir,
        run_dir_rel=run_dir_rel,
        dispatch=overrides.get("dispatch", DispatchInfo()),
        artifacts=overrides.get("artifacts", sample_artifacts()),
        planning_diagnostics=overrides.get("planning_diagnostics", tuple()),
        sweep_case_name=overrides.get("sweep_case_name"),
        sweep_assignments=overrides.get("sweep_assignments", {}),
    )
    extra_overrides = {
        key: value
        for key, value in overrides.items()
        if key
        not in {
            "run_index",
            "total_runs",
            "run_id",
            "project",
            "experiment_name",
            "model_name",
            "train_mode",
            "train_stage",
            "eval_stage",
            "eval_train_outputs",
            "cluster",
            "env",
            "run_dir",
            "run_dir_rel",
            "dispatch",
            "artifacts",
            "planning_diagnostics",
            "sweep_case_name",
            "sweep_assignments",
        }
    }
    return replace(base, **extra_overrides) if extra_overrides else base


def sample_run_snapshot(**overrides):
    from slurmforge.pipeline.records import RunSnapshot, build_replay_spec

    base = RunSnapshot(
        run_index=1,
        total_runs=1,
        run_id="r1",
        project="demo",
        experiment_name="exp",
        model_name="convbert",
        train_mode="model_cli",
        replay_spec=build_replay_spec(
            {
                "project": "demo",
                "experiment_name": "exp",
                "run": {"mode": "model_cli", "args": {}},
                "launcher": {},
                "cluster": {},
                "env": {},
                "resources": {},
                "artifacts": {},
                "eval": {},
                "output": {},
                "notify": {},
                "validation": {},
                "resolved_model_catalog": {"models": []},
                "model": {"name": "convbert", "script": "train.py"},
            },
            planning_root="/tmp/project_root",
        ),
    )
    if overrides:
        base = replace(base, **overrides)
    return base


def write_test_descriptor(batch_root: Path) -> None:
    """Write a default storage descriptor to a manually set up test batch."""
    from slurmforge.pipeline.config.api import StorageConfigSpec
    from slurmforge.storage.descriptor import write_storage_descriptor
    write_storage_descriptor(batch_root, StorageConfigSpec(), batch_root)
