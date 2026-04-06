from __future__ import annotations

from typing import Any

from ....errors import ConfigContractError
from ...utils import deep_merge
from .slurm_deps import normalize_dependency_kind
from ..scalars import normalize_bool
from ..runtime import DEFAULT_NOTIFY, NotifyConfig
from ..utils import ensure_dict
from .shared import ensure_normalized_config


def normalize_notify(cfg: dict[str, Any]) -> NotifyConfig:
    merged = deep_merge(DEFAULT_NOTIFY, ensure_dict(cfg, "notify"))
    enabled = normalize_bool(merged.get("enabled", False), name="notify.enabled")
    email_raw = merged.get("email", "")
    if email_raw is None:
        email = ""
    elif isinstance(email_raw, str):
        email = email_raw.strip()
    else:
        raise ConfigContractError("notify.email must be a string when provided")
    when_raw = merged.get("when", "afterany")
    when = normalize_dependency_kind(when_raw, field_name="notify.when")
    if enabled and not email:
        raise ConfigContractError("notify.email must be set when notify.enabled=true")
    return NotifyConfig(enabled=enabled, email=email, when=when)


def ensure_notify_config(value: Any, name: str = "notify") -> NotifyConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=NotifyConfig,
        normalizer=normalize_notify,
    )
