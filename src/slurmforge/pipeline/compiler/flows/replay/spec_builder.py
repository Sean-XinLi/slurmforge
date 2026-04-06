from __future__ import annotations

from ....config.api import ExperimentSpec
from ....config.assembly.replay import build_replay_experiment_spec
from ....sources.models import SourceRunInput
from ....sources.replay import apply_cli_overrides
from ...state import MaterializedSourceBundle, ReplayMaterializedState


def build_replay_spec(
    materialized: MaterializedSourceBundle,
    source_input: SourceRunInput,
) -> ExperimentSpec:
    context = materialized.context
    assert isinstance(context, ReplayMaterializedState)
    source_ref = source_input.source
    resolved_cfg = apply_cli_overrides(source_input.run_cfg, parsed_overrides=context.parsed_overrides)
    return build_replay_experiment_spec(
        resolved_cfg,
        project_root=context.project_root,
        config_path=source_ref.config_path,
        config_label=source_ref.config_label,
    )
