from __future__ import annotations

from pathlib import Path
from typing import Any

from ...models import BatchSharedSpec
from ..experiment import normalize_experiment_contract
from .expansion import iter_authoring_static_validation_cfgs
from .shared import prepare_authoring_batch_input


def validate_static_authoring_experiment_cfg(
    cfg: dict[str, Any],
    *,
    config_path: Path,
    batch_shared: BatchSharedSpec,
    context_name: str,
) -> None:
    try:
        normalize_experiment_contract(
            cfg,
            config_path=config_path,
            batch_shared=batch_shared,
        )
    except (TypeError, ValueError) as exc:
        if context_name == "base":
            raise
        raise type(exc)(f"{exc} (while validating {context_name})") from exc


def validate_authoring_config(
    cfg: dict[str, Any],
    config_path: Path,
    *,
    project_root: Path | None = None,
) -> None:
    prepared = prepare_authoring_batch_input(
        cfg,
        config_path=config_path,
        project_root=project_root,
    )
    for context_name, candidate_cfg in iter_authoring_static_validation_cfgs(
        base_cfg=prepared.base_cfg,
        sweep_spec=prepared.sweep_spec,
    ):
        validate_static_authoring_experiment_cfg(
            candidate_cfg,
            config_path=config_path,
            batch_shared=prepared.shared,
            context_name=context_name,
        )
