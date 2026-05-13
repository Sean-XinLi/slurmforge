from __future__ import annotations

from slurmforge.emit.pipeline_gate import render_pipeline_gate_sbatch
from slurmforge.emit.stage import write_stage_submit_files
from slurmforge.executor.launcher import build_shell_script
from slurmforge.executor.stage import execute_stage_task
from slurmforge.io import SchemaVersion
from slurmforge.planner.stage_batch import compile_stage_batch_for_kind
from slurmforge.planner.train_eval_pipeline import compile_train_eval_pipeline_plan
from slurmforge.resolver.train_eval_pipeline import (
    resolve_stage_inputs_for_train_eval_pipeline,
)
from slurmforge.resolver.explicit.stage_batch import upstream_bindings_from_train_batch
from slurmforge.spec import load_experiment_spec
from slurmforge.submission.generation import prepare_stage_submission
from slurmforge.submission.state import read_submission_state
from slurmforge.submission.submit_manifest import load_submit_manifest
from slurmforge.submission.submitter import submit_prepared_stage_batch

from tests.helpers import write_demo_project

__all__ = [
    "SchemaVersion",
    "build_shell_script",
    "compile_stage_batch_for_kind",
    "compile_train_eval_pipeline_plan",
    "execute_stage_task",
    "load_experiment_spec",
    "load_submit_manifest",
    "prepare_stage_submission",
    "read_submission_state",
    "render_pipeline_gate_sbatch",
    "resolve_stage_inputs_for_train_eval_pipeline",
    "submit_prepared_stage_batch",
    "upstream_bindings_from_train_batch",
    "write_demo_project",
    "write_stage_submit_files",
]
