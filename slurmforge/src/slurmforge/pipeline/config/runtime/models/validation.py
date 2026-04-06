from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationConfig:
    cli_args: str = "warn"
    topology_errors: str = "error"
    resource_warnings: str = "warn"
    runtime_preflight: str = "error"
