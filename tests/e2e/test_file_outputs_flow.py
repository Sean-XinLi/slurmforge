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
import tempfile
import yaml
from pathlib import Path


class FileOutputsFlowTests(StageBatchSystemTestCase):
    def test_file_collection_output_can_feed_downstream_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            (root / "train.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "import argparse",
                        "p = argparse.ArgumentParser()",
                        "p.add_argument('--lr')",
                        "args = p.parse_args()",
                        "out = Path('checkpoints')",
                        "out.mkdir(exist_ok=True)",
                        "(out / f'step_{str(args.lr).replace(\".\", \"\")}.pt').write_text('ckpt')",
                        "logs = Path('logs')",
                        "logs.mkdir(exist_ok=True)",
                        "(logs / 'train.log').write_text('ok')",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            cfg = yaml.safe_load(cfg_path.read_text())
            cfg["stages"]["train"]["outputs"]["train_logs"] = {
                "kind": "files",
                "required": True,
                "discover": {"globs": ["logs/**/*.log"]},
            }
            cfg["stages"]["eval"]["inputs"] = {
                "logs_manifest": {
                    "source": {
                        "kind": "upstream_output",
                        "stage": "train",
                        "output": "train_logs",
                    },
                    "expects": "manifest",
                    "required": True,
                    "inject": {
                        "flag": "logs_manifest",
                        "env": "SFORGE_INPUT_LOGS_MANIFEST",
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
            train_run_dir = (
                Path(train_batch.submission_root)
                / train_batch.stage_instances[0].run_dir_rel
            )
            outputs = load_stage_outputs(train_run_dir)
            assert outputs is not None
            logs_output = outputs["outputs"]["train_logs"]
            self.assertEqual(logs_output["kind"], "files")
            self.assertEqual(logs_output["cardinality"], "many")
            self.assertTrue(Path(logs_output["path"]).exists())

            runs, bindings = upstream_bindings_from_train_batch(
                spec,
                Path(train_batch.submission_root),
                input_name="logs_manifest",
            )
            self.assertEqual(len(runs), 1)
            binding = bindings[runs[0].run_id][0]
            self.assertEqual(binding.input_name, "logs_manifest")
            self.assertEqual(binding.resolved.kind, "manifest")
            self.assertEqual(binding.resolved.path, logs_output["path"])
