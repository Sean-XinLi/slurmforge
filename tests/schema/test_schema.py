from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import write_demo_project
from tests.support.std import Path, tempfile


class SchemaTypeTests(StageBatchSystemTestCase):
    def test_run_definition_lives_in_schema_and_is_reexported_by_plans(self) -> None:
        from slurmforge.schema import RunDefinition as SchemaRunDefinition
        from slurmforge.plans import RunDefinition as PlanRunDefinition

        self.assertIs(SchemaRunDefinition, PlanRunDefinition)

    def test_source_input_name_prefers_single_required_input(self) -> None:
        from slurmforge.spec import load_experiment_spec, stage_source_input_name

        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))

            self.assertEqual(stage_source_input_name(spec, stage_name="eval"), "checkpoint")
