from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...errors import PlanningError
from ..config.utils import ensure_path_segment, resolve_path


@dataclass(frozen=True)
class BatchIdentity:
    project_root: Path
    base_output_dir: Path
    project: str
    experiment_name: str
    batch_name: str

    def __post_init__(self) -> None:
        project_root = Path(self.project_root).expanduser().resolve()
        base_output_dir = Path(self.base_output_dir).expanduser()
        if not base_output_dir.is_absolute():
            base_output_dir = (project_root / base_output_dir).resolve()
        else:
            base_output_dir = base_output_dir.resolve()
        project = ensure_path_segment(self.project, name="BatchIdentity.project")
        experiment_name = ensure_path_segment(self.experiment_name, name="BatchIdentity.experiment_name")
        batch_name = ensure_path_segment(self.batch_name, name="BatchIdentity.batch_name")
        object.__setattr__(self, "project_root", project_root)
        object.__setattr__(self, "base_output_dir", base_output_dir)
        object.__setattr__(self, "project", project)
        object.__setattr__(self, "experiment_name", experiment_name)
        object.__setattr__(self, "batch_name", batch_name)

    @property
    def batch_root(self) -> Path:
        return self.base_output_dir / self.project / self.experiment_name / f"batch_{self.batch_name}"

    @property
    def sbatch_dir(self) -> Path:
        return self.batch_root / "sbatch"


def build_batch_identity(
    *,
    project_root: Path,
    project: str,
    experiment_name: str,
    base_output_dir: str,
    configured_batch_name: str | None = None,
    default_batch_name: str | None = None,
) -> BatchIdentity:
    batch_name = str(configured_batch_name or default_batch_name or "").strip()
    if not batch_name:
        raise PlanningError("Batch planning requires output.batch_name or a default_batch_name")
    base_output_dir_path = resolve_path(project_root, base_output_dir, "./runs")
    return BatchIdentity(
        project_root=project_root,
        base_output_dir=base_output_dir_path,
        project=project,
        experiment_name=experiment_name,
        batch_name=batch_name,
    )
