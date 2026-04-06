from __future__ import annotations

from typing import Any

from ..models import DistributedConfig, LauncherConfig


def serialize_distributed_config(config: DistributedConfig) -> dict[str, Any]:
    return {
        "nnodes": int(config.nnodes),
        "nproc_per_node": None if config.nproc_per_node is None else int(config.nproc_per_node),
        "master_port": int(config.master_port),
        "port_offset": None if config.port_offset is None else int(config.port_offset),
        "extra_torchrun_args": [str(item) for item in config.extra_torchrun_args],
    }


def serialize_launcher_config(config: LauncherConfig) -> dict[str, Any]:
    return {
        "mode": str(config.mode),
        "python_bin": str(config.python_bin),
        "workdir": str(config.workdir),
        "distributed": serialize_distributed_config(config.distributed),
    }
