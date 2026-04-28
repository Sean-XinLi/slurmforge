"""Slurmforge exception hierarchy."""
from __future__ import annotations


class UserFacingError(Exception):
    """Raised for errors that should be shown without a traceback at the CLI boundary."""


class UsageError(UserFacingError):
    """Raised when user-supplied command inputs or workflow selections are invalid."""


class ConfigContractError(UserFacingError):
    """Raised when user-provided config data violates the declared contract."""


class InputContractError(UserFacingError):
    """Raised when resolved stage inputs violate the execution contract."""


class RuntimeContractError(UserFacingError):
    """Raised when the declared executor or user runtime cannot be used."""
