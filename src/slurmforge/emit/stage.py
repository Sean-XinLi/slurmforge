from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import SchemaVersion, content_digest, read_json, write_json
from ..plans.stage import StageBatchPlan
from .sbatch_helpers import _q
from .stage_render import (
    render_stage_group_sbatch,
    render_stage_notification_barrier_sbatch,
    render_stage_notification_sbatch,
)


def _submit_root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root) / "submit"


def _generation_id(batch: StageBatchPlan) -> str:
    payload = {
        "batch_id": batch.batch_id,
        "stage_name": batch.stage_name,
        "selected_runs": batch.selected_runs,
        "groups": [
            {
                "group_id": group.group_id,
                "stage_instance_ids": group.stage_instance_ids,
                "array_throttle": group.array_throttle,
                "resources": group.resources,
            }
            for group in batch.group_plans
        ],
        "dependencies": batch.budget_plan.dependencies,
    }
    digest = content_digest(payload, prefix=12)
    return f"gen_{digest}"


def _generation_dir(batch: StageBatchPlan, generation_id: str) -> Path:
    return _submit_root(batch) / "generations" / generation_id


def _manifest_path(batch_root: Path) -> Path:
    return batch_root / "submit" / "submit_manifest.json"


def _notification_enabled(batch: StageBatchPlan, event: str) -> bool:
    email = batch.notification_plan.email
    return email.enabled and event in set(email.events)


def write_stage_notification_submit_file(
    batch: StageBatchPlan, event: str = "batch_finished"
) -> Path:
    manifest = load_stage_submit_manifest(Path(batch.submission_root))
    generation_id = str(manifest["generation_id"])
    path = (
        _submit_root(batch) / "notifications" / generation_id / f"notify_{event}.sbatch"
    )
    for item in manifest.get("notifications", ()):
        if str(item.get("event") or "") == event and item.get("sbatch_path"):
            path = Path(str(item["sbatch_path"]))
            break
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_stage_notification_sbatch(batch, event, generation_id=generation_id),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def write_stage_notification_barrier_file(
    batch: StageBatchPlan,
    event: str,
    *,
    barrier_index: int,
) -> Path:
    manifest = load_stage_submit_manifest(Path(batch.submission_root))
    generation_id = str(manifest["generation_id"])
    path = (
        _submit_root(batch)
        / "notifications"
        / generation_id
        / f"barrier_{event}_{barrier_index:03d}.sbatch"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_stage_notification_barrier_sbatch(
            batch,
            event,
            generation_id=generation_id,
            barrier_index=barrier_index,
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def write_stage_submit_files(batch: StageBatchPlan) -> tuple[Path, ...]:
    root = Path(batch.submission_root)
    generation_id = _generation_id(batch)
    submit_dir = _generation_dir(batch, generation_id)
    (submit_dir / "logs").mkdir(parents=True, exist_ok=True)
    (_submit_root(batch) / "logs" / generation_id).mkdir(parents=True, exist_ok=True)
    sbatch_paths: list[Path] = []
    for group in batch.group_plans:
        path = submit_dir / f"{group.group_id}.sbatch"
        path.write_text(
            render_stage_group_sbatch(batch, group, generation_id=generation_id),
            encoding="utf-8",
        )
        path.chmod(0o755)
        sbatch_paths.append(path)
    submit_script = submit_dir / "submit.sh"
    submit_lines = ["#!/usr/bin/env bash", "set -euo pipefail"]
    job_vars: dict[str, str] = {}
    dependencies = {item.to_group: item for item in batch.budget_plan.dependencies}
    for path in sbatch_paths:
        group_id = path.stem
        var_name = f"JOB_{group_id.upper()}"
        dep = dependencies.get(group_id)
        if dep is None:
            submit_lines.append(f'{var_name}="$(sbatch --parsable {_q(str(path))})"')
        else:
            from_vars = [job_vars[str(item)] for item in dep.from_groups if item]
            dependency_expr = ":".join(f"${{{item}}}" for item in from_vars)
            submit_lines.append(
                f'{var_name}="$(sbatch --parsable --dependency={dep.type}:{dependency_expr} {_q(str(path))})"'
            )
        submit_lines.append(f'printf "%s\\n" "{group_id}=${{{var_name}}}"')
        job_vars[group_id] = var_name
    notification_entries: list[dict[str, str]] = []
    if _notification_enabled(batch, "batch_finished"):
        notify_path = submit_dir / "notify_batch_finished.sbatch"
        notify_path.write_text(
            render_stage_notification_sbatch(
                batch, "batch_finished", generation_id=generation_id
            ),
            encoding="utf-8",
        )
        notify_path.chmod(0o755)
        notification_entries.append(
            {
                "event": "batch_finished",
                "sbatch_path": str(notify_path),
            }
        )
    submit_script.write_text("\n".join(submit_lines) + "\n", encoding="utf-8")
    submit_script.chmod(0o755)
    manifest = {
        "schema_version": SchemaVersion.SUBMIT_MANIFEST,
        "batch_id": batch.batch_id,
        "stage_name": batch.stage_name,
        "generation_id": generation_id,
        "generation_dir": str(submit_dir),
        "submit_script": str(submit_script),
        "groups": [
            {
                "group_id": group.group_id,
                "group_index": group.group_index,
                "sbatch_path": str(submit_dir / f"{group.group_id}.sbatch"),
                "array_size": group.array_size,
                "stage_instance_ids": group.stage_instance_ids,
            }
            for group in batch.group_plans
        ],
        "dependencies": batch.budget_plan.dependencies,
        "notifications": notification_entries,
    }
    write_json(_manifest_path(root), manifest)
    wrapper = _submit_root(batch) / "submit.sh"
    wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'exec {_q(str(submit_script))} "$@"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return tuple(sbatch_paths)


def load_stage_submit_manifest(batch_root: Path) -> dict[str, Any]:
    return read_json(_manifest_path(batch_root))


def _stage_submit_paths_from_manifest(batch_root: Path) -> tuple[Path, ...]:
    manifest = load_stage_submit_manifest(batch_root)
    return tuple(Path(str(item["sbatch_path"])) for item in manifest.get("groups", ()))
