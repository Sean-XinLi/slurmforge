from __future__ import annotations

import errno
import shutil
from pathlib import Path

from .context import MaterializationLayout


def commit_staging(layout: MaterializationLayout) -> None:
    try:
        Path(layout.staging_root).rename(layout.final_batch_root)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        shutil.move(str(layout.staging_root), str(layout.final_batch_root))
