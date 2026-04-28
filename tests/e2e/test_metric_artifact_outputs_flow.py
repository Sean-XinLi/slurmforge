from __future__ import annotations

from pathlib import Path
import json
import tempfile

import yaml

from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import (
    load_stage_outputs,
    materialize_stage_batch_for_test,
)
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
)


class MetricArtifactOutputFlowTests(StageBatchSystemTestCase):
    def test_metric_and_manifest_outputs_are_first_class_output_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            (root / "eval.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "import argparse, json, os",
                        "p = argparse.ArgumentParser()",
                        "p.add_argument('--checkpoint_path')",
                        "args = p.parse_args()",
                        "assert args.checkpoint_path == os.environ['SFORGE_INPUT_CHECKPOINT']",
                        "out = Path('eval')",
                        "out.mkdir(exist_ok=True)",
                        "(out / 'metrics.csv').write_text('metric,value\\nacc,1\\n')",
                        "(out / 'metrics.json').write_text(json.dumps({'accuracy': 0.98}))",
                        "(out / 'manifest.json').write_text(json.dumps({'rows': 1}))",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            cfg = yaml.safe_load(cfg_path.read_text())
            cfg["stages"]["eval"]["outputs"] = {
                "accuracy": {
                    "kind": "metric",
                    "file": "eval/metrics.json",
                    "json_path": "$.accuracy",
                    "required": True,
                },
                "eval_report": {
                    "kind": "manifest",
                    "file": "eval/manifest.json",
                    "required": True,
                },
            }
            cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            spec = load_experiment_spec(cfg_path)
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )
            runs, bindings = upstream_bindings_from_train_batch(
                spec, Path(train_batch.submission_root)
            )
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref="test",
            )
            materialize_stage_batch_for_test(eval_batch, spec_snapshot=spec.raw)
            self.assertEqual(
                execute_stage_task(Path(eval_batch.submission_root), 1, 0), 0
            )
            eval_run_dir = (
                Path(eval_batch.submission_root)
                / eval_batch.stage_instances[0].run_dir_rel
            )
            outputs = load_stage_outputs(eval_run_dir)
            assert outputs is not None
            self.assertEqual(outputs["outputs"]["accuracy"]["kind"], "metric")
            self.assertEqual(outputs["outputs"]["accuracy"]["value"], 0.98)
            self.assertTrue(outputs["outputs"]["accuracy"]["managed"])
            self.assertTrue(outputs["outputs"]["accuracy"]["digest"])
            self.assertTrue(
                outputs["outputs"]["accuracy"]["source_path"].endswith(
                    "eval/metrics.json"
                )
            )
            self.assertTrue(Path(outputs["outputs"]["accuracy"]["path"]).exists())
            self.assertEqual(outputs["outputs"]["eval_report"]["kind"], "manifest")
            self.assertTrue(Path(outputs["outputs"]["eval_report"]["path"]).exists())
            manifest = json.loads(
                (
                    eval_run_dir
                    / "attempts"
                    / "0001"
                    / "artifacts"
                    / "artifact_manifest.json"
                ).read_text()
            )
            artifact_kinds = {item["kind"] for item in manifest["artifacts"]}
            self.assertIn("metric", artifact_kinds)
            self.assertIn("manifest", artifact_kinds)
