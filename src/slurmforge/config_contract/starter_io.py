from __future__ import annotations

CHECKPOINT_INPUT_NAME = "checkpoint"
CHECKPOINT_OUTPUT_NAME = "checkpoint"
CHECKPOINT_FLAG = "checkpoint_path"
CHECKPOINT_ENV = "SFORGE_INPUT_CHECKPOINT"
CHECKPOINT_GLOB = "checkpoints/**/*.pt"
CHECKPOINT_DIR = "checkpoints"
CHECKPOINT_SUFFIX = ".pt"

ACCURACY_OUTPUT_NAME = "accuracy"
ACCURACY_FILE = "eval/metrics.json"
ACCURACY_FIELD = "accuracy"
ACCURACY_JSON_PATH = "$.accuracy"

EVAL_SPLIT_DEFAULT = "validation"
