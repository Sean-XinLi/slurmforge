from __future__ import annotations

from ...config.normalize import DEFAULT_LAUNCHER, ensure_launcher_config
from ...config.runtime import serialize_launcher_config
from ...config.runtime import ValidationConfig
from ..enums import RuntimeProbe


def runtime_probe(validation_cfg: ValidationConfig) -> RuntimeProbe:
    if str(validation_cfg.runtime_preflight or "error").strip().lower() == "off":
        return RuntimeProbe.NONE
    return RuntimeProbe.CUDA


def launcher_override_dict(launcher_cfg) -> dict[str, object]:
    base = serialize_launcher_config(ensure_launcher_config(launcher_cfg))
    overrides: dict[str, object] = {}
    for key in ("mode", "python_bin", "workdir"):
        if base.get(key) != DEFAULT_LAUNCHER.get(key):
            overrides[key] = base.get(key)
    distributed_overrides: dict[str, object] = {}
    default_dist = dict(DEFAULT_LAUNCHER.get("distributed") or {})
    for key, value in dict(base.get("distributed") or {}).items():
        if value != default_dist.get(key):
            distributed_overrides[key] = value
    if distributed_overrides:
        overrides["distributed"] = distributed_overrides
    return overrides
