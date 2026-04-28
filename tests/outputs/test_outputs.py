from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    write_demo_project,
    write_stage_batch_layout,
)
from tests.support.std import Path, json, patch, tempfile, yaml


class OutputTests(StageBatchSystemTestCase):
    def test_invalid_artifact_strategy_is_config_contract_error(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.outputs.artifact_store import manage_file
        from slurmforge.plans import ArtifactStorePlan

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "checkpoint.pt"
            source.write_text("checkpoint\n", encoding="utf-8")

            with self.assertRaisesRegex(ConfigContractError, "Unsupported artifact store strategy"):
                manage_file(
                    str(source),
                    attempt_dir=root / "attempt",
                    kind="file",
                    store_plan=ArtifactStorePlan(strategy="bad-strategy", verify_digest=False),
                )

    def test_invalid_metric_json_path_is_config_contract_error(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.outputs.selection import json_path_value

        with self.assertRaisesRegex(ConfigContractError, "unsupported metric json_path"):
            json_path_value({"accuracy": 0.9}, "accuracy")

    def test_artifact_store_hardlink_strategy_is_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={"artifact_store": {"strategy": "hardlink", "fallback_strategy": "copy", "verify_digest": True}},
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            self.assertEqual(execute_stage_task(Path(batch.submission_root), 1, 0), 0)
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            manifest = json.loads((run_dir / "attempts" / "0001" / "artifacts" / "artifact_manifest.json").read_text())
            self.assertIn(manifest["artifacts"][0]["strategy"], {"hardlink", "copy"})
            self.assertTrue(manifest["artifacts"][0]["verified"])

    def test_artifact_digest_mismatch_fails_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            with patch("slurmforge.outputs.artifact_store.file_digest", side_effect=["source", "managed"]):
                self.assertNotEqual(execute_stage_task(Path(batch.submission_root), 1, 0), 0)
            run_dir = Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel
            status = json.loads((run_dir / "status.json").read_text())
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failure_class"], "artifact_integrity_error")

    def test_required_artifact_contract_failure_blocks_stage_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            cfg = yaml.safe_load(cfg_path.read_text())
            cfg["stages"]["train"]["outputs"]["required_log"] = {
                "kind": "files",
                "discover": {"globs": ["required/**/*.log"]},
                "required": True,
            }
            cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            spec = load_experiment_spec(cfg_path)
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(train_batch, spec_snapshot=spec.raw)

            self.assertNotEqual(execute_stage_task(Path(train_batch.submission_root), 1, 0), 0)
            run_dir = Path(train_batch.submission_root) / train_batch.stage_instances[0].run_dir_rel
            status = json.loads((run_dir / "status.json").read_text())
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["failure_class"], "missing_output")
            self.assertIn("required output `required_log` was not produced", status["reason"])
            self.assertFalse((run_dir / "stage_outputs.json").exists())
            self.assertTrue((run_dir / "attempts" / "0001" / "artifacts" / "artifact_manifest.json").exists())
