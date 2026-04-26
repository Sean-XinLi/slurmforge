from __future__ import annotations

import json
import io
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import yaml

from slurmforge.emit import (
    load_stage_submit_manifest,
    render_controller_sbatch,
    write_stage_submit_files,
)
from slurmforge.resolver import resolve_stage_inputs_for_pipeline, upstream_bindings_from_train_batch
from slurmforge.executor import build_shell_script, execute_stage_task
from slurmforge.io import SchemaVersion
from slurmforge.planner import compile_pipeline_plan, compile_stage_batch_for_kind
from slurmforge.spec import load_experiment_spec
from slurmforge.submission import (
    create_submit_generation,
    prepare_stage_submission,
    read_submission_ledger,
    read_submission_state,
    submit_prepared_stage_batch,
    write_submission_ledger,
)
from slurmforge.storage import load_stage_batch_plan, load_stage_outputs, write_pipeline_layout, write_stage_batch_layout


from tests.conftest import write_demo_project  # noqa: E402


__all__ = [
    "Namespace",
    "Path",
    "SchemaVersion",
    "StageBatchSystemTestCase",
    "build_shell_script",
    "compile_pipeline_plan",
    "compile_stage_batch_for_kind",
    "create_submit_generation",
    "execute_stage_task",
    "io",
    "json",
    "load_experiment_spec",
    "load_stage_batch_plan",
    "load_stage_outputs",
    "load_stage_submit_manifest",
    "patch",
    "prepare_stage_submission",
    "read_submission_ledger",
    "read_submission_state",
    "redirect_stderr",
    "redirect_stdout",
    "render_controller_sbatch",
    "replace",
    "resolve_stage_inputs_for_pipeline",
    "submit_prepared_stage_batch",
    "tempfile",
    "unittest",
    "upstream_bindings_from_train_batch",
    "write_demo_project",
    "write_pipeline_layout",
    "write_stage_batch_layout",
    "write_stage_submit_files",
    "write_submission_ledger",
    "yaml",
]


class StageBatchSystemTestCase(unittest.TestCase):
    pass
