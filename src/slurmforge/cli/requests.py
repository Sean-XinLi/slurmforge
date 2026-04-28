from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..errors import ConfigContractError


@dataclass(frozen=True)
class EvalInputSourceRequest:
    kind: Literal["from_train_batch", "from_run", "checkpoint"]
    value: str
    input_name: str | None = None


def eval_source_from_args(args) -> EvalInputSourceRequest | None:
    sources = [
        ("from_train_batch", getattr(args, "from_train_batch", None)),
        ("from_run", getattr(args, "from_run", None)),
        ("checkpoint", getattr(args, "checkpoint", None)),
    ]
    selected = [(kind, value) for kind, value in sources if value]
    if len(selected) > 1:
        raise ConfigContractError("eval accepts exactly one input source")
    if not selected:
        if getattr(args, "input_name", None):
            raise ConfigContractError(
                "--input-name is only valid with an eval input source"
            )
        return None
    kind, value = selected[0]
    return EvalInputSourceRequest(
        kind=kind,  # type: ignore[arg-type]
        value=str(value),
        input_name=getattr(args, "input_name", None),
    )
