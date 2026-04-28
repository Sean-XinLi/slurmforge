from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    load_stage_outputs,
    materialize_stage_batch_for_test,
)
import json
import tempfile
import yaml
from pathlib import Path


class MetricOutputsFlowTests(StageBatchSystemTestCase):
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

    def test_metric_output_feeds_value_input_without_path_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            (root / "train.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "import argparse, json",
                        "p = argparse.ArgumentParser()",
                        "p.add_argument('--lr')",
                        "args = p.parse_args()",
                        "out = Path('checkpoints')",
                        "out.mkdir(exist_ok=True)",
                        "(out / f'step_{str(args.lr).replace(\".\", \"\")}.pt').write_text('ckpt')",
                        "metrics = Path('metrics')",
                        "metrics.mkdir(exist_ok=True)",
                        "(metrics / 'train.json').write_text(json.dumps({'score': 0.98}))",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (root / "eval.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "import argparse, os",
                        "p = argparse.ArgumentParser()",
                        "p.add_argument('--score')",
                        "args = p.parse_args()",
                        "assert args.score == '0.98'",
                        "assert os.environ['SFORGE_INPUT_SCORE'] == '0.98'",
                        "out = Path('eval')",
                        "out.mkdir(exist_ok=True)",
                        "(out / 'metrics.csv').write_text('metric,value\\nscore,' + args.score + '\\n')",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            cfg = yaml.safe_load(cfg_path.read_text())
            cfg["stages"]["train"]["outputs"]["score"] = {
                "kind": "metric",
                "file": "metrics/train.json",
                "json_path": "$.score",
                "required": True,
            }
            cfg["stages"]["eval"]["inputs"] = {
                "score": {
                    "source": {
                        "kind": "upstream_output",
                        "stage": "train",
                        "output": "score",
                    },
                    "expects": "value",
                    "required": True,
                    "inject": {
                        "flag": "score",
                        "env": "SFORGE_INPUT_SCORE",
                        "mode": "value",
                    },
                }
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
            binding = bindings[runs[0].run_id][0]
            self.assertEqual(binding.resolved.kind, "value")
            self.assertEqual(binding.resolved.value, 0.98)
            self.assertTrue(binding.resolved.digest)

            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref="test",
            )
            materialize_stage_batch_for_test(eval_batch, spec_snapshot=spec.raw)
            eval_run_dir = (
                Path(eval_batch.submission_root)
                / eval_batch.stage_instances[0].run_dir_rel
            )
            payload = json.loads((eval_run_dir / "input_bindings.json").read_text())
            self.assertNotIn("resolved_path", payload["bindings"]["score"])
            self.assertEqual(payload["bindings"]["score"]["resolved"]["kind"], "value")
            self.assertEqual(payload["bindings"]["score"]["resolved"]["value"], 0.98)

            self.assertEqual(
                execute_stage_task(Path(eval_batch.submission_root), 1, 0), 0
            )
            verification = json.loads(
                (eval_run_dir / "input_verification.json").read_text()
            )
            self.assertEqual(verification["records"][0]["resolved_kind"], "value")
            self.assertEqual(verification["records"][0]["state"], "verified")
            self.assertTrue(verification["records"][0]["value_digest"])
