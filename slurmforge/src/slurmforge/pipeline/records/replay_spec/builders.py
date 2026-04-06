from __future__ import annotations

import copy
from typing import Any

from ...config.utils import ensure_dict
from .model import ReplaySpec


def sanitize_replay_cfg(replay_cfg: dict[str, Any], name: str) -> dict[str, Any]:
    return copy.deepcopy(ensure_dict(replay_cfg, name))


def build_replay_spec(
    replay_cfg: dict[str, Any],
    *,
    planning_root: str,
    source_batch_root: str | None = None,
    source_run_id: str | None = None,
    source_record_path: str | None = None,
) -> ReplaySpec:
    return ReplaySpec(
        replay_cfg=sanitize_replay_cfg(
            replay_cfg,
            "replay_spec.replay_cfg",
        ),
        planning_root=str(planning_root),
        source_batch_root=None if source_batch_root in (None, "") else str(source_batch_root),
        source_run_id=None if source_run_id in (None, "") else str(source_run_id),
        source_record_path=None if source_record_path in (None, "") else str(source_record_path),
    )
