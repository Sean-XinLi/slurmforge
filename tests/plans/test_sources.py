from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase


class SourcePlanTests(StageBatchSystemTestCase):
    def test_prior_batch_source_plan_uses_source_schema(self) -> None:
        from slurmforge.io import SchemaVersion
        from slurmforge.plans.sources import PriorBatchLineage, StageBatchSource
        from slurmforge.plans.sources import prior_batch_lineage_to_dict

        source = StageBatchSource(kind="prior_batch", source_root="/tmp/root", stage="eval", query="state=failed")
        lineage = PriorBatchLineage(
            source_root=source.source_root,
            stage=source.stage,
            query=source.query,
            selected_run_ids=("run_0001",),
            selected_stage_instance_ids=("eval/run_0001",),
        )

        self.assertEqual(source.schema_version, SchemaVersion.SOURCE_PLAN)
        self.assertEqual(prior_batch_lineage_to_dict(lineage)["kind"], "prior_batch")
