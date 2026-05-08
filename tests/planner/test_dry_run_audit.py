from __future__ import annotations

from pathlib import Path
import tempfile

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)


class DryRunAuditTests(StageBatchSystemTestCase):
    def test_dry_run_audit_reports_unresolved_upstream_eval_as_valid_deferred_input(
        self,
    ) -> None:
        from slurmforge.planner.audit import build_dry_run_audit
        from slurmforge.planner.audit_models import (
            StageBatchDryRunValidation,
            dry_run_audit_to_dict,
        )

        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))
            batch = compile_stage_batch_for_kind(spec, kind="eval")

            audit = build_dry_run_audit(spec, batch, command="eval", full=False)

            self.assertEqual(audit.state, "valid")
            self.assertEqual(audit.plan_kind, "stage_batch")
            self.assertIsInstance(audit.validation, StageBatchDryRunValidation)
            validation = audit.validation
            unresolved = validation.unresolved_inputs
            self.assertEqual(len(unresolved), 1)
            self.assertTrue(unresolved[0].deferred)
            payload = dry_run_audit_to_dict(audit)
            self.assertTrue(
                payload["validation"]["unresolved_inputs"][0]["deferred"]
            )

            full_audit = build_dry_run_audit(spec, batch, command="eval", full=True)
            self.assertIsNotNone(full_audit.resource_estimate)
            self.assertEqual(
                full_audit.resource_estimate.stages[0].stage_name, "eval"
            )
