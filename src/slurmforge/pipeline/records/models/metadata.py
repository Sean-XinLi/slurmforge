from __future__ import annotations

from dataclasses import dataclass

from ....identity import PACKAGE_NAME, __version__


@dataclass(frozen=True)
class GeneratedBy:
    name: str = PACKAGE_NAME
    version: str = __version__
