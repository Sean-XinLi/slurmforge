from __future__ import annotations

from ..errors import UserFacingError


class StarterWriteError(UserFacingError):
    """Raised when starter files cannot be written without overwriting existing paths."""


class StarterTemplateError(RuntimeError):
    """Raised when a starter template definition is internally inconsistent."""
