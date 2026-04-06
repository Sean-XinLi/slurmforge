from __future__ import annotations

import copy
from typing import Any

from ...config.utils import ensure_dict
from .checkpoint import resolve_retry_checkpoint
from .models import RetryCandidate


def base_retry_cfg(candidate: RetryCandidate) -> dict[str, Any]:
    return copy.deepcopy(ensure_dict(candidate.snapshot.replay_spec.replay_cfg, "replay_spec.replay_cfg"))


def inject_retry_resume(run_cfg: dict[str, Any], candidate: RetryCandidate) -> None:
    checkpoint = resolve_retry_checkpoint(candidate)
    if checkpoint is None:
        return

    run_cfg.setdefault("env", {})
    env_cfg = ensure_dict(run_cfg.get("env"), "env")
    env_cfg.setdefault("extra_env", {})
    extra_env = ensure_dict(env_cfg.get("extra_env"), "env.extra_env")
    extra_env["AI_INFRA_RESUME_FROM_CHECKPOINT"] = str(checkpoint)

    run_cfg.setdefault("run", {})
    run_cfg_body = ensure_dict(run_cfg.get("run"), "run")
    run_cfg_body["resume_from_checkpoint"] = str(checkpoint)


def prepare_retry_cfg(candidate: RetryCandidate) -> dict[str, Any]:
    run_cfg = base_retry_cfg(candidate)
    inject_retry_resume(run_cfg, candidate)
    return run_cfg
