"""BatchStorageHandle — unified entry point for reading an existing batch."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ..pipeline.records.batch_paths import resolve_run_dir

if TYPE_CHECKING:
    from ..pipeline.config.api import StorageConfigSpec
    from .contracts import ExecutionStore, PlanningStore
    from .lifecycle import ExecutionLifecycle
    from .models import RunExecutionView


@dataclass(frozen=True)
class BatchStorageHandle:
    """Composite handle — planning + execution + lifecycle.

    ``list_batch_run_views`` is the only place that joins planning and
    execution data.  ``ExecutionStore`` never touches planning.
    """

    batch_root: Path
    storage_config: StorageConfigSpec
    planning: PlanningStore
    execution: ExecutionStore
    lifecycle: ExecutionLifecycle

    def list_batch_run_views(self) -> tuple[RunExecutionView, ...]:
        from .models import RunExecutionView

        plans = self.planning.load_batch_run_plans(self.batch_root)

        # Build run_id → run_dir mapping.  Let resolve_run_dir raise on
        # corrupt data — a garbage run_dir_rel in the DB is a real error,
        # not something to silently skip.
        run_id_to_run_dir: dict[str, Path] = {}
        for plan in plans:
            run_id_to_run_dir[plan.run_id] = resolve_run_dir(self.batch_root, plan)

        attempted = self.execution.load_latest_attempts(self.batch_root, run_id_to_run_dir)

        views: list[RunExecutionView] = []
        for plan in plans:
            attempt = attempted.get(plan.run_id)
            run_dir = run_id_to_run_dir[plan.run_id]
            views.append(RunExecutionView(
                run_id=plan.run_id,
                run_index=plan.run_index,
                group_index=plan.dispatch.array_group or 0,
                task_index=plan.dispatch.array_task_index or 0,
                run_dir_rel=plan.run_dir_rel or "",
                run_dir=str(run_dir),
                latest_result_dir=attempt.latest_result_dir if attempt else "",
                latest_status=attempt.latest_status if attempt else None,
            ))
        return tuple(views)
