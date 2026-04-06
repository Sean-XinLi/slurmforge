from __future__ import annotations

from pathlib import Path

from ...records import resolve_dispatch_record_path
from ..models import SourceRef
from .models import RetryCandidate


def build_retry_source_ref(
    *,
    source_batch_root: Path,
    candidate: RetryCandidate,
) -> SourceRef:
    resolved_record_path = resolve_dispatch_record_path(source_batch_root, candidate.plan.dispatch)
    if resolved_record_path is not None:
        record_path = resolved_record_path.resolve()
        return SourceRef(
            config_path=record_path,
            config_label=f"retry run {candidate.plan.run_id} (source_record_path={record_path})",
            planning_root=str(candidate.snapshot.replay_spec.planning_root or "").strip() or None,
            source_batch_root=source_batch_root.resolve(),
            source_run_id=candidate.plan.run_id,
            source_record_path=record_path,
        )

    stored_record_path = (candidate.plan.dispatch.record_path or "").strip()
    if stored_record_path:
        label = f"retry run {candidate.plan.run_id} (source_record_path={stored_record_path}, missing)"
    else:
        label = f"retry run {candidate.plan.run_id} (source_record_path=missing)"
    return SourceRef(
        config_path=None,
        config_label=label,
        planning_root=str(candidate.snapshot.replay_spec.planning_root or "").strip() or None,
        source_batch_root=source_batch_root.resolve(),
        source_run_id=candidate.plan.run_id,
        source_record_path=None,
    )
