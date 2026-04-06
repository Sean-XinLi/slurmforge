from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..config.normalize import ensure_launcher_config
from ..config.runtime import LauncherConfig
from .types import LaunchRuntime, ShellToken


class LaunchStrategy(ABC):
    name = "base"

    @abstractmethod
    def build_prefix(
        self,
        launcher_cfg: LauncherConfig | dict[str, Any],
        run_idx: int,
    ) -> tuple[list[ShellToken], LaunchRuntime]:
        raise NotImplementedError


class TorchrunStrategy(LaunchStrategy):
    name = "ddp"

    def build_prefix(
        self,
        launcher_cfg: LauncherConfig | dict[str, Any],
        run_idx: int,
    ) -> tuple[list[ShellToken], LaunchRuntime]:
        launcher = ensure_launcher_config(launcher_cfg)
        dist_cfg = launcher.distributed
        nnodes = int(dist_cfg.nnodes)
        nproc = int(dist_cfg.nproc_per_node or 1)
        base_port = int(dist_cfg.master_port)
        port_offset_raw = dist_cfg.port_offset
        port_offset = int(port_offset_raw) if port_offset_raw not in {None, ""} else 0
        extra = list(dist_cfg.extra_torchrun_args or [])
        raw_port = base_port + port_offset + run_idx
        port = 1024 + ((raw_port - 1024) % (65535 - 1024))
        prefix = [
            ShellToken("torchrun"),
            ShellToken(f"--nnodes={nnodes}"),
            ShellToken(f"--nproc_per_node={nproc}"),
            ShellToken("--node_rank=${SLURM_NODEID:-0}", raw=True),
            ShellToken("--master_addr=${MASTER_ADDR}", raw=True),
            ShellToken(f"--master_port={port}"),
        ]
        prefix.extend(ShellToken(str(x)) for x in extra)
        return prefix, LaunchRuntime(nnodes=nnodes, nproc_per_node=nproc, master_port=port)


class PythonStrategy(LaunchStrategy):
    name = "single"

    def build_prefix(
        self,
        launcher_cfg: LauncherConfig | dict[str, Any],
        run_idx: int,
    ) -> tuple[list[ShellToken], LaunchRuntime]:
        launcher = ensure_launcher_config(launcher_cfg)
        python_bin = str(launcher.python_bin)
        return [ShellToken(python_bin)], LaunchRuntime(nnodes=1, nproc_per_node=1, master_port=None)


STRATEGIES = {
    "ddp": TorchrunStrategy(),
    "single": PythonStrategy(),
}
