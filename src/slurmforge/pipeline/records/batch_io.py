from __future__ import annotations

import json
from pathlib import Path

from .batch_paths import bind_run_plan_to_batch
from .codecs.run_plan import deserialize_run_plan
from .models.run_plan import RunPlan


def _load_manifest_run_plans(runs_manifest_path: Path, *, batch_root: Path) -> list[RunPlan]:
    plans: list[RunPlan] = []
    for line in runs_manifest_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise TypeError(f"Invalid run record line in {runs_manifest_path}")
        plans.append(bind_run_plan_to_batch(batch_root, deserialize_run_plan(payload)))
    return plans


def _load_record_run_plans(batch_root: Path) -> list[RunPlan]:
    records = sorted((batch_root / "records").glob("group_*/task_*.json"))
    plans: list[RunPlan] = []
    for record_path in records:
        payload = json.loads(record_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError(f"Invalid run record file: {record_path}")
        plans.append(bind_run_plan_to_batch(batch_root, deserialize_run_plan(payload)))
    return plans


def load_batch_run_plans(batch_root: Path) -> list[RunPlan]:
    resolved_root = batch_root.resolve()
    runs_manifest_path = resolved_root / "meta" / "runs_manifest.jsonl"
    if runs_manifest_path.exists():
        return _load_manifest_run_plans(runs_manifest_path, batch_root=resolved_root)
    plans = _load_record_run_plans(resolved_root)
    if plans:
        return plans
    raise FileNotFoundError(f"No run records found under batch_root={resolved_root}")
