from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.dont_write_bytecode = True


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    import yaml  # noqa: F401
except ModuleNotFoundError as exc:
    raise RuntimeError(
        "Tests require PyYAML. Install project dependencies into the active Python environment before running pytest."
    ) from exc

from tests._support import remove_generated_artifact_dirs, slurmforge_root


def pytest_sessionfinish(session, exitstatus) -> None:
    remove_generated_artifact_dirs(slurmforge_root())
