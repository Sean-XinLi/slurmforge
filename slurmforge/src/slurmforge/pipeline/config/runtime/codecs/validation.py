from __future__ import annotations

from typing import Any

from ..models import ValidationConfig


def serialize_validation_config(config: ValidationConfig) -> dict[str, Any]:
    return {
        "cli_args": str(config.cli_args),
        "topology_errors": str(config.topology_errors),
        "resource_warnings": str(config.resource_warnings),
        "runtime_preflight": str(config.runtime_preflight),
    }
