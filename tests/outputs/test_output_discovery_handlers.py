from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
)
import json
import tempfile
from dataclasses import replace
from pathlib import Path


class OutputDiscoveryHandlerTests(StageBatchSystemTestCase):
    def test_file_output_selects_latest_step(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workdir = root / "work"
            (workdir / "checkpoints").mkdir(parents=True)
            (workdir / "checkpoints" / "model_1.pt").write_text(
                "old\n", encoding="utf-8"
            )
            latest = workdir / "checkpoints" / "model_2.pt"
            latest.write_text("new\n", encoding="utf-8")

            result = self._discover(
                root, workdir, self._output_contract("checkpoint", "file")
            )

            ref = result.stage_outputs.outputs["checkpoint"]
            self.assertIsNone(result.failure_reason)
            self.assertEqual(ref.kind, "file")
            self.assertEqual(ref.selection_reason, "latest_step")
            self.assertEqual(ref.source_path, str(latest.resolve()))

    def test_files_output_writes_manifest_with_all_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workdir = root / "work"
            (workdir / "eval").mkdir(parents=True)
            (workdir / "eval" / "a.csv").write_text("a\n", encoding="utf-8")
            (workdir / "eval" / "b.csv").write_text("b\n", encoding="utf-8")

            result = self._discover(
                root, workdir, self._output_contract("reports", "files")
            )

            ref = result.stage_outputs.outputs["reports"]
            manifest = json.loads(Path(ref.path).read_text(encoding="utf-8"))
            self.assertIsNone(result.failure_reason)
            self.assertEqual(ref.kind, "files")
            self.assertEqual(ref.cardinality, "many")
            self.assertEqual(ref.selection_reason, "all_matches_manifest")
            self.assertEqual(len(manifest["refs"]), 2)
            self.assertEqual(len(result.artifact_manifest.artifacts), 2)

    def test_metric_output_resolves_json_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workdir = root / "work"
            workdir.mkdir()
            (workdir / "metrics.json").write_text(
                '{"accuracy": 0.9}\n', encoding="utf-8"
            )

            result = self._discover(
                root, workdir, self._output_contract("accuracy", "metric")
            )

            ref = result.stage_outputs.outputs["accuracy"]
            self.assertIsNone(result.failure_reason)
            self.assertEqual(ref.kind, "metric")
            self.assertEqual(ref.value, 0.9)
            self.assertEqual(ref.selection_reason, "json_path:$.accuracy")

    def test_metric_config_error_is_reported_as_missing_required_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workdir = root / "work"
            workdir.mkdir()
            (workdir / "metrics.json").write_text(
                '{"accuracy": 0.9}\n', encoding="utf-8"
            )

            result = self._discover(
                root,
                workdir,
                self._output_contract("accuracy", "metric", json_path="accuracy"),
            )

            self.assertNotIn("accuracy", result.stage_outputs.outputs)
            self.assertIn(
                "required output `accuracy` did not resolve",
                result.failure_reason or "",
            )
            self.assertIn("unsupported metric json_path", result.failure_reason or "")

    def test_manifest_output_records_manifest_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workdir = root / "work"
            workdir.mkdir()
            manifest_file = workdir / "manifest.json"
            manifest_file.write_text('{"items": []}\n', encoding="utf-8")

            result = self._discover(
                root, workdir, self._output_contract("index", "manifest")
            )

            ref = result.stage_outputs.outputs["index"]
            self.assertIsNone(result.failure_reason)
            self.assertEqual(ref.kind, "manifest")
            self.assertEqual(ref.selection_reason, "manifest_file")
            self.assertEqual(ref.source_path, str(manifest_file.resolve()))

    def test_unknown_output_kind_is_contract_error(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.outputs.discovery.registry import handler_for_kind

        with self.assertRaisesRegex(ConfigContractError, "Unsupported output kind"):
            handler_for_kind("unknown")

    def _discover(self, root: Path, workdir: Path, output_contract: object) -> object:
        from slurmforge.outputs import discover_stage_outputs

        spec = load_experiment_spec(write_demo_project(root / "project"))
        instance = compile_stage_batch_for_kind(spec, kind="train").stage_instances[0]
        instance = replace(instance, output_contract=output_contract)
        return discover_stage_outputs(
            instance,
            workdir,
            attempt_id="attempt-1",
            attempt_dir=root / "attempt",
        )

    def _output_contract(
        self,
        output_name: str,
        kind: str,
        *,
        json_path: str = "$.accuracy",
    ) -> object:
        from slurmforge.contracts import (
            FileOutputDiscoveryRule,
            OutputDiscoveryRule,
            StageOutputContract,
            StageOutputSpec,
        )

        if kind == "file":
            spec = StageOutputSpec(
                name=output_name,
                kind=kind,
                required=True,
                discover=FileOutputDiscoveryRule(
                    globs=("checkpoints/*.pt",), select="latest_step"
                ),
            )
        elif kind == "files":
            spec = StageOutputSpec(
                name=output_name,
                kind=kind,
                required=True,
                discover=OutputDiscoveryRule(globs=("eval/*.csv",)),
            )
        elif kind == "metric":
            spec = StageOutputSpec(
                name=output_name,
                kind=kind,
                required=True,
                file="metrics.json",
                json_path=json_path,
            )
        else:
            spec = StageOutputSpec(
                name=output_name,
                kind=kind,
                required=True,
                file="manifest.json",
            )
        return StageOutputContract(outputs={output_name: spec})
