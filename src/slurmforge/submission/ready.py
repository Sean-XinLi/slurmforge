from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..inputs import input_verification_path
from ..io import read_json
from ..plans.stage import StageBatchPlan
from ..storage.loader import load_execution_stage_batch_plan
from ..storage.materialization import read_materialization_status
from .ledger import read_submission_ledger
from .models import PreparedSubmission, SubmissionLedger


def load_ready_prepared_submission(prepared: PreparedSubmission) -> tuple[StageBatchPlan, SubmissionLedger]:
    batch_root = Path(prepared.batch_root)
    if prepared.materialization_state != "ready":
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` was not produced as ready: "
            f"{prepared.materialization_state}"
        )
    materialization = read_materialization_status(batch_root)
    if materialization is None:
        raise ConfigContractError(f"Materialization status is missing for prepared submission `{batch_root}`")
    if materialization.state != "ready":
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` is not ready: materialization state is `{materialization.state}`"
        )
    if materialization.batch_id != prepared.batch_id or materialization.stage_name != prepared.stage_name:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` materialization belongs to "
            f"{materialization.stage_name}/{materialization.batch_id}, not {prepared.stage_name}/{prepared.batch_id}"
        )
    if materialization.submit_manifest_path and Path(materialization.submit_manifest_path) != prepared.manifest_path:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` points at `{prepared.manifest_path}`, "
            f"but materialization records `{materialization.submit_manifest_path}`"
        )
    if not prepared.manifest_path.exists():
        raise ConfigContractError(f"Prepared submission manifest does not exist: {prepared.manifest_path}")
    ledger = read_submission_ledger(batch_root)
    if ledger is None:
        raise ConfigContractError(f"Submission ledger is missing for prepared submission `{batch_root}`")
    if ledger.batch_id != prepared.batch_id or ledger.stage_name != prepared.stage_name:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` ledger belongs to "
            f"{ledger.stage_name}/{ledger.batch_id}, not {prepared.stage_name}/{prepared.batch_id}"
        )
    if ledger.generation_id != prepared.generation_id:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` has generation `{prepared.generation_id}`, "
            f"but ledger records `{ledger.generation_id}`"
        )
    for record in ledger.groups.values():
        if not Path(record.sbatch_path).exists():
            raise ConfigContractError(f"Prepared sbatch file does not exist: {record.sbatch_path}")
    batch = load_execution_stage_batch_plan(batch_root)
    for instance in batch.stage_instances:
        verification_path = input_verification_path(batch_root / instance.run_dir_rel)
        if not verification_path.exists():
            raise ConfigContractError(f"Input verification report is missing: {verification_path}")
        verification = read_json(verification_path)
        if verification.get("state") == "failed":
            raise ConfigContractError(f"Input verification failed for prepared submission: {verification_path}")
        if verification.get("phase") != "submit":
            raise ConfigContractError(f"Input verification report is not a submit-phase report: {verification_path}")
    return batch, ledger
