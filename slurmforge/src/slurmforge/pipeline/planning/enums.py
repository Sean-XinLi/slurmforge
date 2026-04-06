from __future__ import annotations

from enum import Enum
from typing import TypeVar

from ...errors import PlanningError


class _ValueEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


E = TypeVar("E", bound=_ValueEnum)


def coerce_enum(enum_type: type[E], value: object, *, field_name: str) -> E:
    if isinstance(value, enum_type):
        return value
    normalized = str(value or "").strip().lower()
    allowed = ", ".join(item.value for item in enum_type)
    if not normalized:
        raise PlanningError(f"{field_name} must be one of: {allowed}")
    try:
        return enum_type(normalized)
    except ValueError as exc:
        raise PlanningError(f"{field_name} must be one of: {allowed}") from exc


class DiagnosticSeverity(_ValueEnum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class DiagnosticCategory(_ValueEnum):
    CLI_ARGS = "cli_args"
    CONFIG = "config"
    TOPOLOGY = "topology"
    RESOURCE = "resource"
    SUMMARY = "summary"
    PLANNING = "planning"


class StageKind(_ValueEnum):
    TRAIN = "train"
    EVAL = "eval"


class InvocationKind(_ValueEnum):
    MODEL_CLI = "model_cli"
    ADAPTER = "adapter"
    EXTERNAL_COMMAND = "external_command"
    EVAL_SCRIPT = "eval_script"


class LauncherKind(_ValueEnum):
    SINGLE = "single"
    DDP = "ddp"
    EXTERNAL = "external"


class RuntimeProbe(_ValueEnum):
    NONE = "none"
    CUDA = "cuda"
