from __future__ import annotations

from .models import ConfigOption

RUN_SINGLE = "single"
RUN_GRID = "grid"
RUN_CASES = "cases"
RUN_MATRIX = "matrix"

ENTRY_PYTHON_SCRIPT = "python_script"
ENTRY_COMMAND = "command"

LAUNCHER_SINGLE = "single"
LAUNCHER_PYTHON = "python"
LAUNCHER_TORCHRUN = "torchrun"
LAUNCHER_SRUN = "srun"
LAUNCHER_MPIRUN = "mpirun"
LAUNCHER_COMMAND = "command"

LAUNCHER_MODE_SINGLE_NODE = "single_node"
LAUNCHER_MODE_MULTI_NODE = "multi_node"

INPUT_SOURCE_UPSTREAM_OUTPUT = "upstream_output"
INPUT_SOURCE_EXTERNAL_PATH = "external_path"

INPUT_EXPECTS_PATH = "path"
INPUT_EXPECTS_MANIFEST = "manifest"
INPUT_EXPECTS_VALUE = "value"

INPUT_INJECT_PATH = "path"
INPUT_INJECT_VALUE = "value"
INPUT_INJECT_JSON = "json"

OUTPUT_KIND_FILE = "file"
OUTPUT_KIND_FILES = "files"
OUTPUT_KIND_METRIC = "metric"
OUTPUT_KIND_MANIFEST = "manifest"

OUTPUT_SELECT_LATEST_STEP = "latest_step"
OUTPUT_SELECT_FIRST = "first"
OUTPUT_SELECT_LAST = "last"

ARTIFACT_STRATEGY_COPY = "copy"
ARTIFACT_STRATEGY_HARDLINK = "hardlink"
ARTIFACT_STRATEGY_SYMLINK = "symlink"
ARTIFACT_STRATEGY_REGISTER_ONLY = "register_only"
ARTIFACT_FALLBACK_NULL = "null"

DISPATCH_POLICY_SERIALIZE_GROUPS = "serialize_groups"
DISPATCH_POLICY_ERROR = "error"
DISPATCH_POLICY_BEST_EFFORT = "best_effort"

EMAIL_EVENT_BATCH_FINISHED = "batch_finished"
EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED = "train_eval_pipeline_finished"
EMAIL_MODE_SUMMARY = "summary"

GPU_SIZING_ESTIMATOR_HEURISTIC = "heuristic"

RUN_TYPES = (
    ConfigOption(RUN_SINGLE, "Plan one run."),
    ConfigOption(RUN_GRID, "Plan every combination from top-level axes."),
    ConfigOption(RUN_CASES, "Plan named hand-authored run variants."),
    ConfigOption(RUN_MATRIX, "Plan named cases, each with its own grid."),
)

ENTRY_TYPES = (
    ConfigOption(ENTRY_PYTHON_SCRIPT, "Run a Python file."),
    ConfigOption(ENTRY_COMMAND, "Run a shell command."),
)

LAUNCHER_TYPES = (
    ConfigOption(LAUNCHER_SINGLE, "Run one process directly."),
    ConfigOption(LAUNCHER_PYTHON, "Launch through Python."),
    ConfigOption(LAUNCHER_TORCHRUN, "Launch distributed PyTorch."),
    ConfigOption(LAUNCHER_SRUN, "Launch through Slurm srun."),
    ConfigOption(LAUNCHER_MPIRUN, "Launch through MPI."),
    ConfigOption(LAUNCHER_COMMAND, "Launch a raw command."),
)

LAUNCHER_MODES = (
    ConfigOption(LAUNCHER_MODE_SINGLE_NODE, "Launch on one node."),
    ConfigOption(LAUNCHER_MODE_MULTI_NODE, "Launch across multiple nodes."),
)

INPUT_SOURCE_KINDS = (
    ConfigOption(INPUT_SOURCE_UPSTREAM_OUTPUT, "Read an output from a previous stage."),
    ConfigOption(INPUT_SOURCE_EXTERNAL_PATH, "Read an explicit user-provided path."),
)

INPUT_EXPECTS = (
    ConfigOption(INPUT_EXPECTS_PATH, "Inject a filesystem path."),
    ConfigOption(INPUT_EXPECTS_MANIFEST, "Inject a manifest payload."),
    ConfigOption(INPUT_EXPECTS_VALUE, "Inject a scalar value."),
)

INPUT_INJECT_MODES = (
    ConfigOption(INPUT_INJECT_PATH, "Pass the resolved input path."),
    ConfigOption(INPUT_INJECT_VALUE, "Pass the resolved input value."),
    ConfigOption(INPUT_INJECT_JSON, "Pass the resolved input encoded as JSON."),
)

OUTPUT_KINDS = (
    ConfigOption(OUTPUT_KIND_FILE, "One managed file."),
    ConfigOption(OUTPUT_KIND_FILES, "Multiple discovered files."),
    ConfigOption(OUTPUT_KIND_METRIC, "A metric value read from JSON."),
    ConfigOption(OUTPUT_KIND_MANIFEST, "A manifest JSON file."),
)

OUTPUT_SELECTORS = (
    ConfigOption(
        OUTPUT_SELECT_LATEST_STEP, "Pick the path with the highest step number."
    ),
    ConfigOption(OUTPUT_SELECT_FIRST, "Pick the first sorted match."),
    ConfigOption(OUTPUT_SELECT_LAST, "Pick the last sorted match."),
)

ARTIFACT_STRATEGIES = (
    ConfigOption(ARTIFACT_STRATEGY_COPY, "Copy managed artifacts into the run store."),
    ConfigOption(
        ARTIFACT_STRATEGY_HARDLINK, "Hardlink managed artifacts into the run store."
    ),
    ConfigOption(
        ARTIFACT_STRATEGY_SYMLINK, "Symlink managed artifacts into the run store."
    ),
    ConfigOption(
        ARTIFACT_STRATEGY_REGISTER_ONLY,
        "Track artifact paths without copying files.",
    ),
)

ARTIFACT_FALLBACK_STRATEGIES = (
    ConfigOption(ARTIFACT_FALLBACK_NULL, "Disable fallback handling."),
    ConfigOption(
        ARTIFACT_STRATEGY_COPY, "Copy artifacts when the primary strategy fails."
    ),
    ConfigOption(ARTIFACT_STRATEGY_HARDLINK, "Hardlink artifacts when supported."),
    ConfigOption(ARTIFACT_STRATEGY_SYMLINK, "Symlink artifacts when supported."),
    ConfigOption(
        ARTIFACT_STRATEGY_REGISTER_ONLY, "Record artifacts without copying files."
    ),
)

DISPATCH_POLICIES = (
    ConfigOption(
        DISPATCH_POLICY_SERIALIZE_GROUPS, "Queue array groups within GPU budget."
    ),
    ConfigOption(DISPATCH_POLICY_ERROR, "Reject plans that exceed the GPU budget."),
    ConfigOption(
        DISPATCH_POLICY_BEST_EFFORT, "Submit groups without strict serialization."
    ),
)

EMAIL_EVENTS = (
    ConfigOption(
        EMAIL_EVENT_BATCH_FINISHED,
        "Send after a stage batch reaches terminal state.",
    ),
    ConfigOption(
        EMAIL_EVENT_TRAIN_EVAL_PIPELINE_FINISHED,
        "Send after a train/eval pipeline reaches terminal state.",
    ),
)

EMAIL_MODES = (ConfigOption(EMAIL_MODE_SUMMARY, "Send a compact workflow summary."),)

GPU_SIZING_ESTIMATORS = (
    ConfigOption(
        GPU_SIZING_ESTIMATOR_HEURISTIC,
        "Estimate GPU count from target memory and hardware profile.",
    ),
)

__all__ = [name for name in globals() if name.isupper()]
