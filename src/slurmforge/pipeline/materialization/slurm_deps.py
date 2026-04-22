"""Slurm sbatch dependency flag generation.

Slurm accepts a single ``--dependency`` flag whose value is a comma-separated
list of ``kind:id[:id...]`` clauses.  Passing two ``--dependency`` flags is
not additive — Slurm treats the later one as a replacement, silently
discarding the earlier dependencies.  Callers that need to merge external
user-supplied dependencies with tool-generated ones (e.g. serial-group
chaining) MUST build a single merged flag via ``build_sbatch_dependency_clauses``.
"""
from __future__ import annotations

from typing import Mapping, Sequence

from ..config.normalize.slurm_deps import SLURM_DEPENDENCY_KINDS


def build_sbatch_dependency_clauses(dependencies: Mapping[str, Sequence[str]]) -> list[str]:
    """Return the canonical ``kind:id[:id...]`` clauses for a dependency map.

    This is the low-level primitive.  Callers that need to merge dependencies
    with tool-generated ones should append to the returned list and then
    ``--dependency=`` + ``,`` -join the result themselves.
    """
    clauses: list[str] = []
    for dep_kind in SLURM_DEPENDENCY_KINDS:
        values = [str(item).strip() for item in dependencies.get(dep_kind, []) if str(item).strip()]
        if values:
            clauses.append(f"{dep_kind}:{':'.join(values)}")
    return clauses


def build_sbatch_dependency_flag(dependencies: Mapping[str, Sequence[str]]) -> str:
    """Return a single ``--dependency=<clauses> `` flag (with trailing space) or ``""``.

    For callers that never need to merge additional clauses.  If you need to
    combine with tool-generated dependencies, use ``build_sbatch_dependency_clauses``
    instead and emit ONE merged ``--dependency=`` flag.
    """
    clauses = build_sbatch_dependency_clauses(dependencies)
    if not clauses:
        return ""
    return f"--dependency={','.join(clauses)} "
