from __future__ import annotations

from slurmforge.emit import (
    load_stage_submit_manifest,
    render_controller_sbatch,
    write_stage_submit_files,
)
from slurmforge.executor import build_shell_script, execute_stage_task
from slurmforge.io import SchemaVersion
from slurmforge.planner import compile_train_eval_pipeline_plan, compile_stage_batch_for_kind
from slurmforge.resolver import resolve_stage_inputs_for_train_eval_pipeline, upstream_bindings_from_train_batch
from slurmforge.spec import load_experiment_spec
from slurmforge.storage.layout import (
    write_train_eval_pipeline_layout,
    write_stage_batch_layout,
)
from slurmforge.storage.loader import load_stage_batch_plan, load_stage_outputs
from slurmforge.submission.generation import create_submit_generation
from slurmforge.submission.ledger import read_submission_ledger, write_submission_ledger
from slurmforge.submission import (
    prepare_stage_submission,
    read_submission_state,
    submit_prepared_stage_batch,
)

from tests.helpers import write_demo_project

__all__ = [
    "SchemaVersion",
    "build_shell_script",
    "compile_train_eval_pipeline_plan",
    "compile_stage_batch_for_kind",
    "create_submit_generation",
    "execute_stage_task",
    "load_experiment_spec",
    "load_stage_batch_plan",
    "load_stage_outputs",
    "load_stage_submit_manifest",
    "prepare_stage_submission",
    "read_submission_ledger",
    "read_submission_state",
    "render_controller_sbatch",
    "resolve_stage_inputs_for_train_eval_pipeline",
    "submit_prepared_stage_batch",
    "upstream_bindings_from_train_batch",
    "write_demo_project",
    "write_train_eval_pipeline_layout",
    "write_stage_batch_layout",
    "write_stage_submit_files",
    "write_submission_ledger",
]
