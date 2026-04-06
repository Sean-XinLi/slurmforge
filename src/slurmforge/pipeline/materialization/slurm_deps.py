"""Slurm sbatch dependency flag generation."""
from __future__ import annotations

from typing import Mapping, Sequence

from ..config.normalize.slurm_deps import SLURM_DEPENDENCY_KINDS


def build_sbatch_dependency_flag(dependencies: Mapping[str, Sequence[str]]) -> str:
    clauses: list[str] = []
    for dep_kind in SLURM_DEPENDENCY_KINDS:
        values = [str(item).strip() for item in dependencies.get(dep_kind, []) if str(item).strip()]
        if values:
            clauses.append(f"{dep_kind}:{':'.join(values)}")
    if not clauses:
        return ""
    return f"--dependency={','.join(clauses)} "
