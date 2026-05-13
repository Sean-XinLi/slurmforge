from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def stage_finished(stage_instance_id: str):
    from slurmforge.control.workflow import AdvanceHint

    return AdvanceHint(
        event="stage-instance-finished",
        stage_instance_id=stage_instance_id,
    )


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
