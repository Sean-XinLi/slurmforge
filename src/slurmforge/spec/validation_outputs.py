from __future__ import annotations

from ..config_contract.option_sets import (
    OUTPUT_KIND_FILE,
    OUTPUT_KIND_FILES,
    OUTPUT_KIND_MANIFEST,
    OUTPUT_KIND_METRIC,
)
from ..errors import ConfigContractError
from .models import StageSpec
from .validation_common import reject_newline


def validate_stage_outputs_contract(stage: StageSpec) -> None:
    for output_name, output in stage.outputs.outputs.items():
        output_path = f"stages.{stage.name}.outputs.{output_name}"
        if output.kind in {OUTPUT_KIND_FILE, OUTPUT_KIND_FILES}:
            if not output.discover.globs:
                raise ConfigContractError(f"`{output_path}.discover.globs` is required")
            for pattern in output.discover.globs:
                reject_newline(pattern, field=f"{output_path}.discover.globs")
        elif output.kind == OUTPUT_KIND_METRIC:
            if not output.file:
                raise ConfigContractError(f"`{output_path}.file` is required")
            reject_newline(output.file, field=f"{output_path}.file")
            if not output.json_path.startswith("$"):
                raise ConfigContractError(
                    f"`{output_path}.json_path` must start with `$`"
                )
        elif output.kind == OUTPUT_KIND_MANIFEST:
            if not output.file:
                raise ConfigContractError(f"`{output_path}.file` is required")
            reject_newline(output.file, field=f"{output_path}.file")
