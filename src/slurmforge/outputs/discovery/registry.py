from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ...errors import ConfigContractError
from .context import OutputDiscoveryContext
from .handlers.file import discover_file_output
from .handlers.files import discover_files_output
from .handlers.manifest import discover_manifest_output
from .handlers.metric import discover_metric_output
from .models import OutputDiscoveryItem

OutputDiscoveryHandler = Callable[[str, Any, OutputDiscoveryContext], OutputDiscoveryItem]

_HANDLERS: dict[str, OutputDiscoveryHandler] = {
    "file": discover_file_output,
    "files": discover_files_output,
    "manifest": discover_manifest_output,
    "metric": discover_metric_output,
}


def handler_for_kind(kind: str) -> OutputDiscoveryHandler:
    try:
        return _HANDLERS[kind]
    except KeyError as exc:
        raise ConfigContractError(f"Unsupported output kind: {kind}") from exc
