from __future__ import annotations

import json
from pathlib import Path

from ..config_contract.option_sets import (
    INPUT_EXPECTS_PATH,
    INPUT_INJECT_PATH,
    INPUT_SOURCE_UPSTREAM_OUTPUT,
    OUTPUT_SELECT_LATEST_STEP,
)
from ..config_contract.starter_io import (
    CHECKPOINT_DIR,
    CHECKPOINT_ENV,
    CHECKPOINT_FLAG,
    CHECKPOINT_INPUT_NAME,
    CHECKPOINT_OUTPUT_NAME,
)
from ..config_contract.workflows import STAGE_EVAL, STAGE_TRAIN
from ..io import SchemaVersion
from .markers import replace_between_markers

SUBMISSION_BINDING_JSON_START = "<!-- SUBMISSION_BINDING_JSON_START -->"
SUBMISSION_BINDING_JSON_END = "<!-- SUBMISSION_BINDING_JSON_END -->"
SUBMISSION_INPUT_YAML_START = "<!-- SUBMISSION_INPUT_YAML_START -->"
SUBMISSION_INPUT_YAML_END = "<!-- SUBMISSION_INPUT_YAML_END -->"


def render_submission_doc(current: str, *, path: Path) -> str:
    rendered = replace_between_markers(
        current,
        SUBMISSION_BINDING_JSON_START,
        SUBMISSION_BINDING_JSON_END,
        render_submission_binding_json(),
        path=path,
    )
    return replace_between_markers(
        rendered,
        SUBMISSION_INPUT_YAML_START,
        SUBMISSION_INPUT_YAML_END,
        render_submission_input_yaml(),
        path=path,
    )


def render_submission_binding_json() -> str:
    checkpoint_path = f"/abs/path/{CHECKPOINT_DIR}/step_12000.pt"
    payload = {
        "schema_version": SchemaVersion.INPUT_BINDINGS,
        "stage_instance_id": f"{STAGE_EVAL}/run_001",
        "bindings": {
            CHECKPOINT_INPUT_NAME: {
                "schema_version": SchemaVersion.INPUT_CONTRACT,
                "input_name": CHECKPOINT_INPUT_NAME,
                "source": {
                    "schema_version": SchemaVersion.INPUT_CONTRACT,
                    "kind": INPUT_SOURCE_UPSTREAM_OUTPUT,
                    "stage": STAGE_TRAIN,
                    "output": CHECKPOINT_OUTPUT_NAME,
                    "path": "",
                },
                "expects": INPUT_EXPECTS_PATH,
                "required": True,
                "resolved": {
                    "schema_version": SchemaVersion.INPUT_CONTRACT,
                    "kind": "path",
                    "path": checkpoint_path,
                    "value": None,
                    "digest": "sha256...",
                    "source_output_kind": "file",
                    "producer_stage_instance_id": f"{STAGE_TRAIN}/run_001",
                },
                "inject": {
                    "flag": CHECKPOINT_FLAG,
                    "env": CHECKPOINT_ENV,
                    "mode": INPUT_INJECT_PATH,
                },
                "resolution": {
                    "schema_version": SchemaVersion.INPUT_CONTRACT,
                    "kind": INPUT_SOURCE_UPSTREAM_OUTPUT,
                    "state": "resolved",
                    "reason": "",
                    "source_root": "",
                    "producer_root": f"/abs/path/{STAGE_TRAIN}_batch",
                    "producer_run_dir": f"/abs/path/{STAGE_TRAIN}_batch/runs/run_001",
                    "producer_stage_instance_id": f"{STAGE_TRAIN}/run_001",
                    "producer_run_id": "run_001",
                    "producer_stage_name": STAGE_TRAIN,
                    "output_name": CHECKPOINT_OUTPUT_NAME,
                    "output_path": checkpoint_path,
                    "output_digest": "sha256...",
                    "producer_digest": "",
                    "digest": "",
                    "selection_reason": OUTPUT_SELECT_LATEST_STEP,
                    "searched_root": "",
                    "resolved_from_lineage_root": "",
                    "source_exists": None,
                    "source_role": "",
                    "path_kind": "file",
                    "lineage_ref": "",
                    "expected_digest": "",
                },
            }
        },
    }
    return f"```json\n{json.dumps(payload, indent=2)}\n```"


def render_submission_input_yaml() -> str:
    return "\n".join(
        (
            "```yaml",
            "inputs:",
            f"  {CHECKPOINT_INPUT_NAME}:",
            "    source:",
            f"      kind: {INPUT_SOURCE_UPSTREAM_OUTPUT}",
            f"      stage: {STAGE_TRAIN}",
            f"      output: {CHECKPOINT_OUTPUT_NAME}",
            f"    expects: {INPUT_EXPECTS_PATH}",
            "    required: true",
            "    inject:",
            f"      flag: {CHECKPOINT_FLAG}",
            f"      env: {CHECKPOINT_ENV}",
            f"      mode: {INPUT_INJECT_PATH}",
            "```",
        )
    )
