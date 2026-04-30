from __future__ import annotations

from typing import Any

from ..errors import RecordContractError


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
    LINEAGE = 1
    SOURCE_PLAN = 1
    INPUT_VERIFICATION = 1
    SUBMIT_MANIFEST = 1
    BATCH_MANIFEST = 1
    PIPELINE_MANIFEST = 1
    GROUPS = 1
    BUDGET_PLAN = 1
    BLOCKED_RUNS = 1
    WORKFLOW_STATE = 1
    WORKFLOW_STATUS = 1
    GATE_LEDGER = 1
    STAGE_CATALOG = 1
    RUNTIME_BATCHES = 1
    SCHEDULER_OBSERVATION = 1
    DRY_RUN_AUDIT = 1
    NOTIFICATION = 1
    RESOURCE_ESTIMATE = 1


def require_schema(payload: dict[str, Any], *, name: str, version: int) -> int:
    if "schema_version" not in payload:
        raise RecordContractError(f"{name}.schema_version is required")
    actual = int(payload["schema_version"])
    if actual != version:
        raise RecordContractError(f"{name}.schema_version is not supported: {actual}")
    return actual
