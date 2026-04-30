from __future__ import annotations

from pathlib import Path

from ..root_model.runs import iter_all_stage_run_dirs
from ..storage.plan_reader import plan_for_run_dir


def project_root_from_pipeline(pipeline_root: Path) -> Path:
    for run_dir in iter_all_stage_run_dirs(pipeline_root):
        plan = plan_for_run_dir(run_dir)
        if plan is not None and plan.lineage.get("project_root"):
            return Path(str(plan.lineage["project_root"])).resolve()
    return pipeline_root
