from __future__ import annotations

import copy
import itertools
import re
from typing import Any

from ..errors import ConfigContractError
from ..io import content_digest
from ..schema import RunDefinition
from .models import ExperimentSpec


def normalize_run_path(raw: dict[str, Any], path: str) -> str:
    first = path.split(".", 1)[0]
    stages = raw.get("stages")
    if isinstance(stages, dict) and first in stages and not path.startswith("stages."):
        return f"stages.{path}"
    return path


def stage_name_for_kind(spec: ExperimentSpec, kind: str) -> str:
    matches = [name for name, stage in spec.enabled_stages.items() if stage.kind == kind]
    if not matches:
        raise ConfigContractError(f"No enabled `{kind}` stage exists in this spec")
    if kind in matches:
        return kind
    if len(matches) > 1:
        joined = ", ".join(sorted(matches))
        raise ConfigContractError(f"Multiple enabled `{kind}` stages exist ({joined}); address a stage by name")
    return matches[0]


def iter_run_overrides(spec: ExperimentSpec) -> tuple[dict[str, Any], ...]:
    if spec.runs.type == "single":
        return ({},)
    if spec.runs.type == "cases":
        return tuple(copy.deepcopy(case.set) for case in spec.runs.cases)
    keys = [key for key, _values in spec.runs.axes]
    value_sets = [tuple(values) for _key, values in spec.runs.axes]
    return tuple(dict(zip(keys, combo)) for combo in itertools.product(*value_sets))


def run_id_for(index: int, run_overrides: dict[str, Any], spec_digest: str) -> str:
    seed = {"index": index, "run_overrides": run_overrides, "spec_snapshot_digest": spec_digest}
    digest = content_digest(seed, prefix=10)
    return f"run_{index:04d}_{digest}"


def _validate_case_run_id(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise ConfigContractError(
            "`runs.cases[].name` may only contain letters, numbers, underscores, dots, and dashes"
        )
    return name


def expand_run_definitions(spec: ExperimentSpec) -> tuple[RunDefinition, ...]:
    runs: list[RunDefinition] = []
    overrides = iter_run_overrides(spec)
    for index, run_overrides in enumerate(overrides, start=1):
        run_id = (
            _validate_case_run_id(spec.runs.cases[index - 1].name)
            if spec.runs.type == "cases"
            else run_id_for(index, run_overrides, spec.spec_snapshot_digest)
        )
        runs.append(
            RunDefinition(
                run_id=run_id,
                run_index=index,
                run_overrides=copy.deepcopy(run_overrides),
                spec_snapshot_digest=spec.spec_snapshot_digest,
            )
        )
    return tuple(runs)


def stage_source_input_name(spec: ExperimentSpec, *, stage_name: str) -> str:
    stage = spec.enabled_stages[stage_name]
    if not stage.inputs:
        raise ConfigContractError(f"`stages.{stage_name}.inputs` must declare at least one input")
    required = [name for name, input_spec in sorted(stage.inputs.items()) if input_spec.required]
    if len(required) == 1:
        return required[0]
    if len(stage.inputs) == 1:
        return next(iter(stage.inputs))
    raise ConfigContractError(
        f"`stages.{stage_name}` has multiple inputs; pass --input-name explicitly"
    )
