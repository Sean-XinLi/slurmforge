from __future__ import annotations

from pathlib import Path

from .parse_sections import load_raw_config, parse_experiment_spec
from .validation import validate_experiment_spec


def load_experiment_spec(
    config_path: Path,
    *,
    cli_overrides: tuple[str, ...] = (),
    project_root: Path | None = None,
):
    resolved = config_path.resolve()
    root = project_root.resolve() if project_root is not None else resolved.parent
    raw = load_raw_config(resolved, cli_overrides)
    spec = parse_experiment_spec(raw, config_path=resolved, project_root=root)
    validate_experiment_spec(spec)
    return spec
