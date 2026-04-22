"""Resolve batch-scoped fields from per-spec candidates.

A batch must carry a single value for batch-scoped fields such as
``resources.max_available_gpus`` and ``dispatch.group_overflow_policy``.
During compilation each spec contributes a candidate; ``resolve_batch_scope_unique``
collapses those candidates to one value, erroring cleanly when the selected
runs disagree.

CLI overrides (``--set resources.max_available_gpus=X``) are applied upstream
to each run's raw cfg before its ``ExperimentSpec`` is built, so by the time
candidates are collected they already reflect the override.  No separate
"override branch" is needed at resolve time.
"""
from __future__ import annotations

from typing import Sequence, TypeVar

from ...errors import ConfigContractError


_T = TypeVar("_T")


def resolve_batch_scope_unique(
    candidates: Sequence[_T],
    *,
    field_path: str,
) -> _T:
    """Return the single unique value across all candidates.

    Parameters
    ----------
    candidates
        One entry per successfully-compiled spec.  Must be non-empty when
        the batch contains any run; callers guarantee this.
    field_path
        Dotted path surfaced in the error message, e.g.
        ``"resources.max_available_gpus"``.

    Raises
    ------
    ConfigContractError
        When the candidates do not all agree on one value.  The message
        points the user at ``--set <field_path>=<value>`` as the remedy.
    """
    if not candidates:
        raise ConfigContractError(
            f"cannot resolve {field_path}: no candidates were contributed"
        )
    unique = sorted(set(candidates), key=lambda value: (type(value).__name__, str(value)))
    if len(unique) == 1:
        return unique[0]
    rendered = ", ".join(repr(value) for value in unique)
    raise ConfigContractError(
        f"selected runs have multiple {field_path} values: [{rendered}]. "
        f"Use --set {field_path}=<value> to override for the new batch."
    )
