from __future__ import annotations

from pathlib import Path

from .status_format import render_status_lines_from_model
from .status_read_model import build_status_read_model


def render_status_lines(
    *,
    root: Path,
    status_query: str = "all",
    stage: str | None = None,
    reconcile: bool = False,
    missing_output_grace_seconds: int = 300,
) -> list[str]:
    return render_status_lines_from_model(
        build_status_read_model(
            root=root,
            status_query=status_query,
            stage=stage,
            reconcile=reconcile,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
    )
