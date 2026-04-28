from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    write_demo_project,
)
import io
import json
import tempfile
import yaml
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class PlanCliTests(StageBatchSystemTestCase):
    def test_plan_subcommands_keep_eval_source_arguments_off_train(self) -> None:
        from slurmforge.launcher import build_parser

        parser = build_parser()
        stderr = io.StringIO()
        with (
            redirect_stdout(io.StringIO()),
            redirect_stderr(stderr),
            self.assertRaises(SystemExit),
        ):
            parser.parse_args(
                [
                    "plan",
                    "train",
                    "--config",
                    "unused.yaml",
                    "--checkpoint",
                    "/tmp/checkpoint.pt",
                ]
            )
        self.assertIn("unrecognized arguments: --checkpoint", stderr.getvalue())

        args = parser.parse_args(
            [
                "plan",
                "eval",
                "--config",
                "unused.yaml",
                "--checkpoint",
                "/tmp/checkpoint.pt",
            ]
        )
        self.assertEqual(args.command, "plan")
        self.assertEqual(args.plan_command, "eval")
        self.assertEqual(args.checkpoint, "/tmp/checkpoint.pt")

    def test_plan_dry_run_previews_and_default_emits(self) -> None:
        from slurmforge.cli.plan import handle_plan

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            args = Namespace(
                plan_command="train",
                config=str(cfg_path),
                set=[],
                project_root=None,
                dry_run=True,
                output=None,
            )
            handle_plan(args)
            self.assertFalse((root / "runs").exists())
            args.dry_run = False
            handle_plan(args)
            self.assertTrue(any((root / "runs").glob("**/submit_manifest.json")))
            ledgers = list((root / "runs").glob("**/ledger.json"))
            self.assertEqual(len(ledgers), 1)
            ledger = json.loads(ledgers[0].read_text())
            self.assertEqual(ledger["state"], "planned")

    def test_estimate_command_renders_resource_summary(self) -> None:
        from slurmforge.cli.estimate import handle_estimate

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(root)
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_estimate(
                    Namespace(
                        config=str(cfg_path),
                        set=[],
                        project_root=None,
                        json=False,
                        output=None,
                    )
                )

            text = stdout.getvalue()
            self.assertIn(
                "[ESTIMATE] project=demo experiment=stage_pipeline runs=1", text
            )
            self.assertIn("Stage train:", text)
            self.assertIn("peak_concurrent_gpus", text)

    def test_estimate_command_renders_heterogeneous_gpu_sizing(self) -> None:
        from slurmforge.cli.estimate import handle_estimate

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "hardware": {
                        "gpu_types": {
                            "a100_80gb": {
                                "memory_gb": 80,
                                "usable_memory_fraction": 0.90,
                                "max_gpus_per_node": 8,
                            }
                        }
                    },
                    "sizing": {
                        "gpu": {"defaults": {"safety_factor": 1.0, "round_to": 1}}
                    },
                    "runs": {
                        "type": "cases",
                        "cases": [
                            {
                                "name": "small",
                                "set": {"train.gpu_sizing.target_memory_gb": 80},
                            },
                            {
                                "name": "large",
                                "set": {"train.gpu_sizing.target_memory_gb": 192},
                            },
                        ],
                    },
                    "dispatch": {
                        "max_available_gpus": 8,
                        "overflow_policy": "serialize_groups",
                    },
                },
            )
            payload = yaml.safe_load(cfg_path.read_text())
            payload["stages"]["train"]["resources"]["gpu_type"] = "a100_80gb"
            payload["stages"]["train"]["resources"]["gpus_per_node"] = "auto"
            payload["stages"]["train"]["gpu_sizing"] = {
                "estimator": "heuristic",
                "target_memory_gb": 80,
                "min_gpus_per_job": 1,
            }
            cfg_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                handle_estimate(
                    Namespace(
                        config=str(cfg_path),
                        set=[],
                        project_root=None,
                        json=False,
                        output=None,
                    )
                )

            text = stdout.getvalue()
            self.assertIn("sizing[1].resolved_gpus_per_node: 2", text)
            self.assertIn("sizing[2].resolved_gpus_per_node: 3", text)
