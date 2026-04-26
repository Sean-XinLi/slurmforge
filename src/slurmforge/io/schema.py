from __future__ import annotations

from typing import Any


class SchemaVersion:
    """Centralized registry of on-disk schema versions for slurmforge records."""

    PLAN = 1
    INPUT_CONTRACT = 1
    INPUT_BINDINGS = 1
    OUTPUT_CONTRACT = 1
    OUTPUT_RECORD = 1
    STATUS = 1
    ROOT_REF = 1
    RUNTIME_CONTRACT = 1
    SUBMISSION_LEDGER = 1
    MATERIALIZATION_STATUS = 1
    CONTROLLER_JOB = 1
    LINEAGE = 1
    SOURCE_PLAN = 1
    INPUT_VERIFICATION = 1
    SUBMIT_MANIFEST = 1
    BATCH_MANIFEST = 1
    PIPELINE_MANIFEST = 1
    GROUPS = 1
    BUDGET_PLAN = 1
    BLOCKED_RUNS = 1
    CONTROLLER_STATE = 1
    CONTROLLER_STATUS = 1
    SCHEDULER_OBSERVATION = 1
    DRY_RUN_AUDIT = 1


def require_schema(payload: dict[str, Any], *, name: str, version: int) -> int:
    if "schema_version" not in payload:
        raise ValueError(f"{name}.schema_version is required")
    actual = int(payload["schema_version"])
    if actual != version:
        raise ValueError(f"{name}.schema_version is not supported: {actual}")
    return actual
