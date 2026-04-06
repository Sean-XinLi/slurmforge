"""Slurmforge exception hierarchy.

All public exceptions are defined here at the package root — not inside
``pipeline/`` — so that ``sweep/`` can import them without creating a
circular dependency on ``pipeline/``.

Dependency constraint::

    slurmforge/errors.py        ← canonical location
         ↑                ↑
    sweep/ imports     pipeline/ imports

Moving these exceptions into ``pipeline/`` would force ``sweep/`` to depend
on ``pipeline/``, which is intentionally avoided to keep ``sweep/`` a
lightweight, standalone module.
"""
from __future__ import annotations


class ConfigContractError(ValueError):
    """Raised when user-provided config data violates the declared contract."""


class PlanningError(ValueError):
    """Raised when execution planning encounters invalid or unsupported input."""


class InternalCompilerError(RuntimeError):
    """Raised when batch compilation hits an unexpected framework bug."""
