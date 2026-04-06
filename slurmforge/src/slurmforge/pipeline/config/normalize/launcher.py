from __future__ import annotations

from typing import Any

from ....errors import ConfigContractError
from ...utils import deep_merge
from ..runtime import DEFAULT_LAUNCHER, DistributedConfig, LauncherConfig
from ..utils import _is_auto_value, ensure_dict
from .shared import ensure_normalized_config


def normalize_launcher(cfg: dict[str, Any]) -> LauncherConfig:
    merged = deep_merge(DEFAULT_LAUNCHER, ensure_dict(cfg, "launcher"))
    if isinstance(merged.get("ddp"), dict):
        merged["distributed"] = deep_merge(merged["distributed"], merged["ddp"])
    if "extra_torchrun_args" not in merged["distributed"]:
        merged["distributed"]["extra_torchrun_args"] = []
    port_offset_raw = merged["distributed"].get("port_offset")
    if _is_auto_value(port_offset_raw):
        merged["distributed"]["port_offset"] = None
    else:
        merged["distributed"]["port_offset"] = int(port_offset_raw)
        if merged["distributed"]["port_offset"] < 0:
            raise ConfigContractError("launcher.distributed.port_offset must be >= 0")
    distributed = ensure_dict(merged.get("distributed"), "launcher.distributed")
    return LauncherConfig(
        mode=str(merged["mode"]),
        python_bin=str(merged["python_bin"]),
        workdir=str(merged["workdir"]),
        distributed=DistributedConfig(
            nnodes=int(distributed["nnodes"]),
            nproc_per_node=None
            if _is_auto_value(distributed.get("nproc_per_node"))
            else int(distributed["nproc_per_node"]),
            master_port=int(distributed["master_port"]),
            port_offset=None
            if _is_auto_value(distributed.get("port_offset"))
            else int(distributed["port_offset"]),
            extra_torchrun_args=[str(x) for x in list(distributed.get("extra_torchrun_args") or [])],
        ),
    )


def ensure_launcher_config(value: Any, name: str = "launcher") -> LauncherConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=LauncherConfig,
        normalizer=normalize_launcher,
    )
