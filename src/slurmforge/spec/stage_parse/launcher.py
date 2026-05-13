from __future__ import annotations

from typing import Any

from ...config_contract.keys import reject_unknown_config_keys
from ...config_contract.registry import default_for
from ...errors import ConfigContractError
from ..models import LauncherRendezvousSpec, LauncherSpec, TorchrunLauncherSpec
from ..parse_common import optional_mapping


def parse_launcher(raw: Any, *, name: str) -> LauncherSpec:
    data = optional_mapping(raw, f"stages.{name}.launcher")
    reject_unknown_config_keys(data, parent=f"stages.{name}.launcher")
    rendezvous = optional_mapping(
        data.get("rendezvous"), f"stages.{name}.launcher.rendezvous"
    )
    reject_unknown_config_keys(rendezvous, parent=f"stages.{name}.launcher.rendezvous")
    launcher_type = str(data.get("type") or default_for("stages.*.launcher.type"))
    return LauncherSpec(
        type=launcher_type,
        torchrun=TorchrunLauncherSpec(
            mode="" if data.get("mode") in (None, "") else str(data.get("mode")),
            nnodes=data.get("nnodes", default_for("stages.*.launcher.nnodes")),
            nproc_per_node=data.get(
                "nproc_per_node",
                default_for("stages.*.launcher.nproc_per_node"),
            ),
            rendezvous=LauncherRendezvousSpec(
                backend=str(
                    rendezvous.get(
                        "backend",
                        default_for("stages.*.launcher.rendezvous.backend"),
                    )
                    or default_for("stages.*.launcher.rendezvous.backend")
                ),
                endpoint=str(
                    rendezvous.get(
                        "endpoint",
                        default_for("stages.*.launcher.rendezvous.endpoint"),
                    )
                    or default_for("stages.*.launcher.rendezvous.endpoint")
                ),
                port=rendezvous.get(
                    "port",
                    default_for("stages.*.launcher.rendezvous.port"),
                ),
            ),
            srun_args=_string_tuple(
                data.get("srun_args"), name=f"stages.{name}.launcher.srun_args"
            ),
        ),
        args=_string_tuple(data.get("args"), name=f"stages.{name}.launcher.args"),
    )


def _string_tuple(value: Any, *, name: str) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if not isinstance(value, list):
        raise ConfigContractError(f"`{name}` must be a list")
    if not all(isinstance(item, str) and item for item in value):
        raise ConfigContractError(f"`{name}` must contain non-empty strings")
    return tuple(value)
