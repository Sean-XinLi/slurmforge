from __future__ import annotations

from pathlib import Path
from typing import Any

from ....errors import ConfigContractError


def validate_batch_contract(
    *,
    spec: Any,
    shared: Any,
    config_path: Path,
) -> None:
    if spec.project != shared.project or spec.experiment_name != shared.experiment_name:
        raise ConfigContractError(f"{config_path}: sweep run resolved to a different batch identity")
    if spec.output != shared.output:
        raise ConfigContractError(f"{config_path}: sweep run resolved to a different batch output configuration")
    if spec.notify != shared.notify:
        raise ConfigContractError(f"{config_path}: sweep run resolved to a different batch notify configuration")
    if spec.storage != shared.storage:
        raise ConfigContractError(f"{config_path}: sweep run resolved to a different batch storage configuration")
