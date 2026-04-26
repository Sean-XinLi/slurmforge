"""Slurmforge exception hierarchy."""
from __future__ import annotations


class ConfigContractError(ValueError):
    """Raised when user-provided config data violates the declared contract."""


class InputContractError(ValueError):
    """Raised when resolved stage inputs violate the execution contract."""


class RuntimeContractError(ValueError):
    """Raised when the declared executor or user runtime cannot be used."""
