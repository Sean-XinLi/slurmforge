from __future__ import annotations

from tests.support import *  # noqa: F401,F403


class InputVerifierTests(StageBatchSystemTestCase):
    def _stage_instance(self, root: Path):
        spec = load_experiment_spec(write_demo_project(root))
        batch = compile_stage_batch_for_kind(spec, kind="train")
        return batch.stage_instances[0]

    def test_required_unresolved_input_fails(self) -> None:
        from slurmforge.inputs import verify_stage_instance_inputs
        from slurmforge.schema import InputBinding, InputSource, ResolvedInput

        with tempfile.TemporaryDirectory() as tmp:
            instance = self._stage_instance(Path(tmp))
            binding = InputBinding(
                input_name="checkpoint",
                source=InputSource(kind="external_path", path="missing.pt"),
                expects="path",
                resolved=ResolvedInput(kind="unresolved"),
                inject={"mode": "path", "required": True},
            )

            report = verify_stage_instance_inputs(instance, (binding,), phase="preflight")

            self.assertEqual(report.state, "failed")
            self.assertEqual(report.records[0].failure_class, "input_contract_error")
            self.assertIn("required input did not resolve", report.records[0].reason)

    def test_digest_mismatch_fails(self) -> None:
        from slurmforge.inputs import verify_stage_instance_inputs
        from slurmforge.schema import InputBinding, InputSource, ResolvedInput

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            instance = self._stage_instance(root)
            checkpoint = root / "checkpoint.pt"
            checkpoint.write_text("actual", encoding="utf-8")
            binding = InputBinding(
                input_name="checkpoint",
                source=InputSource(kind="external_path", path=str(checkpoint)),
                expects="path",
                resolved=ResolvedInput(kind="path", path=str(checkpoint)),
                inject={"mode": "path", "required": True},
                resolution={"expected_digest": "0" * 64},
            )

            report = verify_stage_instance_inputs(instance, (binding,), phase="preflight")

            self.assertEqual(report.state, "failed")
            self.assertIn("digest mismatch", report.records[0].reason)
            self.assertTrue(report.records[0].digest)

    def test_resolved_kind_must_match_expectation(self) -> None:
        from slurmforge.inputs import verify_stage_instance_inputs
        from slurmforge.schema import InputBinding, InputSource, ResolvedInput

        with tempfile.TemporaryDirectory() as tmp:
            instance = self._stage_instance(Path(tmp))
            binding = InputBinding(
                input_name="checkpoint",
                source=InputSource(kind="external_path"),
                expects="path",
                resolved=ResolvedInput(kind="value", value="not-a-path"),
                inject={"mode": "path", "required": True},
            )

            report = verify_stage_instance_inputs(instance, (binding,), phase="preflight")

            self.assertEqual(report.state, "failed")
            self.assertIn("does not satisfy expects `path`", report.records[0].reason)
