from __future__ import annotations

from ..models.runtime import ExternalRuntimeConfig


def serialize_external_runtime_config(config: ExternalRuntimeConfig) -> dict[str, int]:
    return {
        "nnodes": int(config.nnodes),
        "nproc_per_node": int(config.nproc_per_node),
    }
