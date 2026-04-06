from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Sequence

from ....errors import ConfigContractError
from ...planning import build_batch_identity
from ..models import SourceRunInput


def resolve_replay_project_root(
    source_inputs: Sequence[SourceRunInput],
    *,
    project_root_override: Path | None,
) -> Path:
    if project_root_override is not None:
        return project_root_override.resolve()

    planning_roots = sorted(
        {
            input_item.source.planning_root
            for input_item in source_inputs
            if input_item.source.planning_root is not None and str(input_item.source.planning_root).strip()
        }
    )
    if not planning_roots:
        raise ConfigContractError("Replay requires --project_root because replay records do not include planning_root")
    if len(planning_roots) != 1:
        raise ConfigContractError("Replay selection spans multiple planning_root values; rerun with --project_root")

    resolved = Path(planning_roots[0]).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(
            f"Stored planning_root does not exist anymore: {resolved}. Rerun with --project_root to relocate the project root."
        )
    return resolved


def explicit_replay_batch_name(
    spec: Any,
    *,
    parsed_overrides: Sequence[tuple[str, Any]],
) -> str | None:
    if not any(key == "output.batch_name" for key, _value in parsed_overrides):
        return None
    return spec.output.batch_name


def resolve_replay_batch_identity(
    spec: Any,
    *,
    project_root: Path,
    default_batch_name: str,
    parsed_overrides: Sequence[tuple[str, Any]],
) -> Any:
    return build_batch_identity(
        project_root=project_root,
        project=spec.project,
        experiment_name=spec.experiment_name,
        base_output_dir=spec.output.base_output_dir,
        configured_batch_name=explicit_replay_batch_name(spec, parsed_overrides=parsed_overrides),
        default_batch_name=default_batch_name,
    )


def augment_manifest_extras_context(
    manifest_extras: dict[str, Any] | None,
    *,
    context_key: str | None,
    project_root: Path,
    source_inputs: Sequence[SourceRunInput],
    cli_overrides: Sequence[str],
) -> dict[str, Any]:
    extras = copy.deepcopy(manifest_extras or {})
    if context_key is None:
        return extras
    replay_source = extras.get(context_key)
    if replay_source is None:
        return extras
    if not isinstance(replay_source, dict):
        raise ConfigContractError(f"manifest_extras.{context_key} must be a mapping when provided")
    replay_source["planning_root"] = str(project_root)
    replay_source["cli_overrides"] = list(cli_overrides)
    replay_source["selected_run_count"] = len(source_inputs)
    replay_source["selected_run_ids"] = [item.source.source_run_id for item in source_inputs]
    replay_source["selected_run_indices"] = [
        item.original_run_index for item in source_inputs if item.original_run_index is not None
    ]
    extras[context_key] = replay_source
    return extras
