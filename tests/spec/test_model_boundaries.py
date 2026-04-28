from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from pathlib import Path


class SpecModelBoundaryTests(StageBatchSystemTestCase):
    def test_spec_models_are_split_by_concern(self) -> None:
        import slurmforge.spec.models as models
        from slurmforge.spec.models.experiment import ExperimentSpec
        from slurmforge.spec.models.runtime import RuntimeSpec
        from slurmforge.spec.models.stages import StageSpec

        self.assertFalse(Path("src/slurmforge/spec/models.py").exists())
        self.assertTrue(Path("src/slurmforge/spec/models/experiment.py").exists())
        self.assertIs(models.ExperimentSpec, ExperimentSpec)
        self.assertIs(models.RuntimeSpec, RuntimeSpec)
        self.assertIs(models.StageSpec, StageSpec)
