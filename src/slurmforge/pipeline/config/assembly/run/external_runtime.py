from __future__ import annotations

from pathlib import Path
from typing import Any

from .....errors import ConfigContractError
from ...models import ExternalRuntimeConfig
from ...runtime import LauncherConfig
from ...utils import ensure_dict


def normalize_external_runtime(
    value: Any,
    *,
    config_path: Path | str,
    field_name: str,
) -> ExternalRuntimeConfig:
    data = ensure_dict(value, field_name)
    nnodes = int(data.get("nnodes", 1) or 1)
    nproc_per_node = int(data.get("nproc_per_node", 1) or 1)
    if nnodes < 1:
        raise ConfigContractError(f"{config_path}: {field_name}.nnodes must be >= 1")
    if nproc_per_node < 1:
        raise ConfigContractError(f"{config_path}: {field_name}.nproc_per_node must be >= 1")
    return ExternalRuntimeConfig(
        nnodes=nnodes,
        nproc_per_node=nproc_per_node,
    )


def validate_external_command_launcher(
    launcher_cfg: LauncherConfig,
    raw_launcher_cfg: dict[str, Any],
    *,
    config_path: Path | str,
    context_name: str,
    runtime_field_name: str,
    launcher_field_name: str,
) -> None:
    launcher_mode = str(launcher_cfg.mode or "").strip().lower()
    if launcher_mode not in {"", "auto", "single"}:
        raise ConfigContractError(
            f"{config_path}: {context_name} does not use slurmforge launcher orchestration; "
            f"keep {launcher_field_name}.mode unset/auto/single and declare topology in {runtime_field_name}"
        )

    distributed_raw = ensure_dict(raw_launcher_cfg.get("distributed"), f"{launcher_field_name}.distributed")
    for key in ("nnodes", "nproc_per_node"):
        raw_value = distributed_raw.get(key)
        if raw_value in {None, "", "auto"}:
            continue
        if int(raw_value) != 1:
            raise ConfigContractError(
                f"{config_path}: {context_name} ignores {launcher_field_name}.distributed.{key}; "
                f"declare topology in {runtime_field_name} instead"
            )
