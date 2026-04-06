from __future__ import annotations

from .array_assignment import ensure_array_assignment, serialize_array_assignment
from .metadata import ensure_generated_by, serialize_generated_by

# NOTE: run_plan and run_snapshot are imported lazily to break a circular
# dependency chain:  models.dispatch -> codecs.array_assignment -> (this
# __init__) -> codecs.run_plan -> models.dispatch.
__all__ = [
    "deserialize_dispatch_info",
    "deserialize_run_plan",
    "deserialize_run_snapshot",
    "ensure_array_assignment",
    "ensure_generated_by",
    "serialize_array_assignment",
    "serialize_dispatch_info",
    "serialize_generated_by",
    "serialize_run_plan",
    "serialize_run_snapshot",
]

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "deserialize_dispatch_info": (".run_plan", "deserialize_dispatch_info"),
    "deserialize_run_plan": (".run_plan", "deserialize_run_plan"),
    "serialize_dispatch_info": (".run_plan", "serialize_dispatch_info"),
    "serialize_run_plan": (".run_plan", "serialize_run_plan"),
    "deserialize_run_snapshot": (".run_snapshot", "deserialize_run_snapshot"),
    "serialize_run_snapshot": (".run_snapshot", "serialize_run_snapshot"),
}


def __getattr__(name: str) -> object:
    if name in _LAZY_IMPORTS:
        module_path, attr = _LAZY_IMPORTS[name]
        import importlib

        mod = importlib.import_module(module_path, __name__)
        value = getattr(mod, attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
