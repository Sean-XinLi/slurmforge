from __future__ import annotations

import tempfile
import unittest
import warnings
from pathlib import Path

import yaml

from slurmforge.pipeline.compiler import AuthoringSourceRequest, collect_source, compile_source
from slurmforge.pipeline.compiler.reports import (
    raise_for_failures,
    report_has_failures,
    report_planned_run_count,
    report_total_runs,
)
from slurmforge.pipeline.config.normalize import (
    normalize_artifacts,
    normalize_cluster,
    normalize_env,
    normalize_launcher,
    normalize_notify,
    normalize_resources,
)
from slurmforge.pipeline.config.api import build_batch_spec, build_replay_experiment_spec


def check_batch_contract(
    cfg: dict,
    config_path: Path,
    *,
    project_root: Path | None = None,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        resolved_config_path = Path(tmp) / config_path.name
        resolved_config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
        report = compile_source(
            AuthoringSourceRequest(
                config_path=resolved_config_path,
                project_root=project_root,
            ),
            phase="config",
        )
        raise_for_failures(report)


class ConfigTests(unittest.TestCase):
    def test_collect_source_returns_source_only_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\nexperiment_name: exp\nrun: {args: {}}\n", encoding="utf-8")

            report = collect_source(AuthoringSourceRequest(config_path=cfg_path))

        self.assertEqual(report.request.config_path, cfg_path)
        self.assertFalse(hasattr(report, "context"))
        self.assertEqual(report.checked_inputs, 1)

    def test_compile_source_reports_yaml_parse_errors_as_source_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "broken.yaml"
            cfg_path.write_text("project: [broken\n", encoding="utf-8")

            report = compile_source(
                AuthoringSourceRequest(config_path=cfg_path),
            )

        self.assertTrue(report_has_failures(report))
        self.assertEqual(report_total_runs(report), 0)
        self.assertEqual(report.batch_diagnostics[0].stage, "source")
        self.assertIn("Failed to parse YAML config", report.batch_diagnostics[0].message)

    def test_compile_source_reports_invalid_cli_overrides_as_source_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\nexperiment_name: exp\nrun: {args: {}}\n", encoding="utf-8")

            report = compile_source(
                AuthoringSourceRequest(
                    config_path=cfg_path,
                    cli_overrides=("bad-override",),
                ),
            )

        self.assertTrue(report_has_failures(report))
        self.assertEqual(report_total_runs(report), 0)
        self.assertEqual(report.batch_diagnostics[0].stage, "source")
        self.assertIn("bad-override", report.batch_diagnostics[0].message)

    def test_compile_source_reports_batch_identity_failures_without_internal_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "experiment.yaml"
            cfg_path.write_text("project: demo\nexperiment_name: exp\nrun: {args: {}}\n", encoding="utf-8")

            report = compile_source(
                AuthoringSourceRequest(
                    config_path=cfg_path,
                    default_batch_name=None,
                ),
                phase="planning",
            )

        self.assertTrue(report_has_failures(report))
        self.assertEqual(report_planned_run_count(report), 0)
        self.assertEqual(len(report.batch_diagnostics), 1)
        self.assertEqual(report.batch_diagnostics[0].stage, "batch")
        self.assertEqual(report.batch_diagnostics[0].code, "batch_identity_error")
        self.assertIn("output.batch_name", report.batch_diagnostics[0].message)

    def test_normalize_resources_clamp_warns(self) -> None:
        with warnings.catch_warnings(record=True) as records:
            warnings.simplefilter("always")
            normalized = normalize_resources(
                {
                    "max_available_gpus": 6,
                    "max_gpus_per_job": 8,
                    "min_gpus_per_job": 7,
                }
            )
        self.assertEqual(normalized.max_gpus_per_job, 6)
        self.assertEqual(normalized.min_gpus_per_job, 6)
        messages = [str(w.message) for w in records]
        self.assertTrue(any("max_gpus_per_job is capped" in m for m in messages))
        self.assertTrue(any("min_gpus_per_job is capped" in m for m in messages))

    def test_normalize_env_rejects_invalid_key(self) -> None:
        with self.assertRaises(ValueError):
            normalize_env({"extra_env": {"BAD-KEY": "1"}})

    def test_normalize_artifacts_returns_typed_config(self) -> None:
        normalized = normalize_artifacts({"checkpoint_globs": ["a/**"], "extra_globs": ["b/**"]})
        self.assertEqual(normalized.checkpoint_globs, ["a/**"])
        self.assertEqual(normalized.extra_globs, ["b/**"])

    def test_normalize_launcher_accepts_auto_aliases(self) -> None:
        normalized = normalize_launcher({"distributed": {"nproc_per_node": "auto", "port_offset": "auto"}})
        self.assertIsNone(normalized.distributed.nproc_per_node)
        self.assertIsNone(normalized.distributed.port_offset)

    def test_normalize_cluster_accepts_auto_alias(self) -> None:
        normalized = normalize_cluster({"gpus_per_node": "auto"})
        self.assertIsNone(normalized.gpus_per_node)

    def test_normalize_notify_accepts_string_bool_and_shared_dependency_kind(self) -> None:
        normalized = normalize_notify({"enabled": "true", "email": "you@example.com", "when": "AFTER"})
        self.assertTrue(normalized.enabled)
        self.assertEqual(normalized.email, "you@example.com")
        self.assertEqual(normalized.when, "after")

    def test_validate_top_config_rejects_non_string_model_registry_file(self) -> None:
        with self.assertRaisesRegex(ValueError, "model_registry\\.registry_file must be a string"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "model_registry": {"registry_file": 123},
                    "run": {"args": {}},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_includes_config_path_for_invalid_section_type(self) -> None:
        with self.assertRaisesRegex(ValueError, "experiment\\.yaml: run must be a mapping"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": "not-a-mapping",
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_enabled_notify_without_email(self) -> None:
        with self.assertRaisesRegex(ValueError, "notify\\.email must be set when notify\\.enabled=true"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "notify": {"enabled": True},
                    "run": {"args": {}},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_invalid_validation_cli_args_policy(self) -> None:
        with self.assertRaisesRegex(ValueError, "validation\\.cli_args must be one of: off, warn, error"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "validation": {"cli_args": "loud"},
                    "run": {"args": {}},
                },
                Path("experiment.yaml"),
            )

    def test_build_batch_spec_requires_eval_external_runtime_for_eval_command(self) -> None:
        with self.assertRaisesRegex(ValueError, "eval\\.command requires eval\\.external_runtime"):
            build_batch_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom", "script": "train.py"},
                    "run": {"args": {"lr": 0.001}},
                    "eval": {"enabled": True, "command": "bash eval.sh"},
                },
                Path("experiment.yaml"),
                project_root=Path(".").resolve(),
            )

    def test_validate_top_config_rejects_unsafe_batch_identity_segments(self) -> None:
        cases = [
            (
                {
                    "project": "../demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                },
                "project.*single path segment",
            ),
            (
                {
                    "project": "demo",
                    "experiment_name": "exp/run",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                },
                "experiment_name.*single path segment",
            ),
            (
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                    "output": {"batch_name": "../retry"},
                },
                "output\\.batch_name.*single path segment",
            ),
        ]
        for cfg, pattern in cases:
            with self.subTest(cfg=cfg):
                with self.assertRaisesRegex(ValueError, pattern):
                    check_batch_contract(cfg, Path("experiment.yaml"))

    def test_validate_top_config_early_rejects_invalid_top_level_sections(self) -> None:
        cases = [
            (
                {"cluster": {"nodes": "bad"}},
                "invalid literal for int",
            ),
            (
                {"env": {"extra_env": {"BAD-KEY": "1"}}},
                "env\\.extra_env key `BAD-KEY` is invalid",
            ),
            (
                {"resources": {"max_available_gpus": 0}},
                "resources\\.max_available_gpus must be >= 1",
            ),
            (
                {"launcher": {"distributed": {"port_offset": -1}}},
                "launcher\\.distributed\\.port_offset must be >= 0",
            ),
        ]
        for extra_cfg, pattern in cases:
            with self.subTest(extra_cfg=extra_cfg):
                with self.assertRaisesRegex(ValueError, pattern):
                    check_batch_contract(
                        {
                            "project": "demo",
                            "experiment_name": "exp",
                            "model": {"name": "custom"},
                            "run": {"args": {}},
                            **extra_cfg,
                        },
                        Path("experiment.yaml"),
                    )

    def test_validate_top_config_rejects_non_list_model_registry_extra_models(self) -> None:
        with self.assertRaisesRegex(ValueError, "model_registry\\.extra_models must be a list"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "model_registry": {"extra_models": {"name": "x"}},
                    "run": {"args": {}},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_duplicate_model_registry_extra_models(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "model_registry\\.extra_models defines duplicate model names: custom_model",
        ):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom_model"},
                    "model_registry": {
                        "extra_models": [
                            {"name": "custom_model", "script": "train_a.py"},
                            {"name": "custom_model", "script": "train_b.py"},
                        ]
                    },
                    "run": {"args": {}},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_unknown_top_level_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains unsupported keys"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                    "resouces": {"max_available_gpus": 4},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_replay_only_model_catalog(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains unsupported keys"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom", "script": "train.py"},
                    "run": {"args": {}},
                    "resolved_model_catalog": {"models": []},
                },
                Path("experiment.yaml"),
            )

    def test_build_replay_experiment_spec_rejects_duplicate_resolved_model_catalog_names(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "resolved_model_catalog\\.models defines duplicate model names: custom_model",
        ):
            build_replay_experiment_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom_model", "script": "train.py"},
                    "resolved_model_catalog": {
                        "models": [
                            {"name": "custom_model", "script": "train_a.py"},
                            {"name": "custom_model", "script": "train_b.py"},
                        ]
                    },
                    "run": {"args": {}},
                },
                project_root=Path(".").resolve(),
                config_path=Path("replay.yaml"),
            )

    def test_build_replay_experiment_spec_uses_explicit_config_label_for_in_memory_replay(self) -> None:
        with self.assertRaisesRegex(ValueError, "retry run r1: run must be a mapping"):
            build_replay_experiment_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "resolved_model_catalog": {"models": []},
                    "run": "not-a-mapping",
                },
                project_root=Path(".").resolve(),
                config_label="retry run r1",
            )

    def test_validate_top_config_rejects_unknown_nested_run_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "run contains unsupported keys"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}, "argss": {"lr": 0.1}},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_unknown_nested_launcher_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "launcher\\.distributed contains unsupported keys"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                    "launcher": {"distributed": {"nproc_per_node": 1, "ports": 29500}},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_legacy_sweep_method_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "sweep contains unsupported keys"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                    "sweep": {
                        "enabled": True,
                        "method": "grid",
                        "parameters": {"run.args.lr": [0.1, 0.2]},
                    },
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_enabled_matrix_without_cases_or_axes(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires at least one of sweep\\.shared_axes or sweep\\.cases"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                    "sweep": {"enabled": True},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_batch_scoped_sweep_paths(self) -> None:
        cases = [
            (
                {
                    "enabled": True,
                    "shared_axes": {"project": ["demo_a", "demo_b"]},
                },
                "sweep\\.shared_axes\\.project",
            ),
            (
                {
                    "enabled": True,
                    "cases": [
                        {
                            "name": "rename_exp",
                            "set": {"experiment_name": "exp_v2"},
                        }
                    ],
                },
                "sweep\\.cases\\[0\\]\\.set\\.experiment_name",
            ),
            (
                {
                    "enabled": True,
                    "cases": [
                        {
                            "name": "move_output",
                            "axes": {"output.base_output_dir": ["./runs_a", "./runs_b"]},
                        }
                    ],
                },
                "sweep\\.cases\\[0\\]\\.axes\\.output\\.base_output_dir",
            ),
            (
                {
                    "enabled": True,
                    "cases": [
                        {
                            "name": "notify_variant",
                            "set": {"notify.email": "ops@example.com"},
                        }
                    ],
                },
                "sweep\\.cases\\[0\\]\\.set\\.notify\\.email",
            ),
        ]
        for sweep_cfg, pattern in cases:
            with self.subTest(sweep_cfg=sweep_cfg):
                with self.assertRaisesRegex(ValueError, pattern):
                    check_batch_contract(
                        {
                            "project": "demo",
                            "experiment_name": "exp",
                            "model": {"name": "custom"},
                            "run": {"args": {}},
                            "sweep": sweep_cfg,
                        },
                        Path("experiment.yaml"),
                    )

    def test_validate_top_config_rejects_unknown_sweep_override_path(self) -> None:
        with self.assertRaisesRegex(ValueError, "run\\.argss"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom", "script": "train.py"},
                    "run": {"args": {}},
                    "sweep": {
                        "enabled": True,
                        "shared_axes": {"run.argss.lr": [0.1, 0.2]},
                    },
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_command_mode_distributed_launcher_settings(self) -> None:
        with self.assertRaisesRegex(ValueError, "command mode does not use slurmforge launcher orchestration"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "run": {"command": "python train.py"},
                    "launcher": {"mode": "ddp"},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_warns_on_command_mode_raw(self) -> None:
        with warnings.catch_warnings(record=True) as records:
            warnings.simplefilter("always")
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "run": {
                        "command": "python train.py --name $USER",
                        "command_mode": "raw",
                        "external_runtime": {"nnodes": 1, "nproc_per_node": 1},
                    },
                },
                Path("experiment.yaml"),
            )
        messages = [str(item.message) for item in records]
        self.assertTrue(any("run.command_mode=raw executes run.command as raw shell text" in message for message in messages))

    def test_build_batch_spec_warns_on_command_mode_raw(self) -> None:
        with warnings.catch_warnings(record=True) as records:
            warnings.simplefilter("always")
            build_batch_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "run": {
                        "command": "python train.py --name $USER",
                        "command_mode": "raw",
                        "external_runtime": {"nnodes": 1, "nproc_per_node": 1},
                    },
                    "output": {"batch_name": "demo_batch"},
                },
                Path("experiment.yaml"),
            )
        messages = [str(item.message) for item in records]
        self.assertTrue(any("run.command_mode=raw executes run.command as raw shell text" in message for message in messages))

    def test_build_batch_spec_warns_on_eval_command_mode_raw(self) -> None:
        with warnings.catch_warnings(record=True) as records:
            warnings.simplefilter("always")
            build_batch_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom", "script": "train.py"},
                    "run": {"args": {"lr": 0.001}},
                    "eval": {
                        "enabled": True,
                        "command": "bash eval.sh | sed 's/x/y/'",
                        "command_mode": "raw",
                        "external_runtime": {"nnodes": 1, "nproc_per_node": 1},
                    },
                },
                Path("experiment.yaml"),
                project_root=Path(".").resolve(),
            )
        messages = [str(item.message) for item in records]
        self.assertTrue(any("eval.command_mode=raw executes eval.command as raw shell text" in message for message in messages))

    def test_validate_top_config_rejects_explicit_eval_checkpoint_policy_without_checkpoint(self) -> None:
        with self.assertRaisesRegex(ValueError, "eval\\.train_outputs\\.explicit_checkpoint must be set"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom", "script": "train.py"},
                    "run": {"args": {"lr": 0.001}},
                    "eval": {
                        "enabled": True,
                        "script": "eval.py",
                        "train_outputs": {"checkpoint_policy": "explicit"},
                    },
                },
                Path("experiment.yaml"),
            )

    def test_build_replay_experiment_spec_preserves_eval_train_outputs_contract(self) -> None:
        spec = build_replay_experiment_spec(
            {
                "project": "demo",
                "experiment_name": "exp",
                "resolved_model_catalog": {"models": []},
                "model": {"name": "custom", "script": "train.py"},
                "run": {"args": {"lr": 0.001}},
                "eval": {
                    "enabled": True,
                    "script": "eval.py",
                    "train_outputs": {
                        "required": False,
                        "checkpoint_policy": "best",
                    },
                },
            },
            project_root=Path(".").resolve(),
            config_path=Path("replay.yaml"),
        )

        self.assertFalse(spec.eval.train_outputs.required)
        self.assertEqual(spec.eval.train_outputs.checkpoint_policy, "best")

    def test_validate_top_config_rejects_removed_output_dispatch_mode_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "output contains unsupported keys"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom", "script": "train.py"},
                    "run": {"args": {}},
                    "output": {"dispatch_mode": "array"},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_rejects_invalid_output_dependencies(self) -> None:
        with self.assertRaisesRegex(ValueError, "output\\.dependencies contains unsupported keys"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom"},
                    "run": {"args": {}},
                    "output": {"dependencies": {"aftersuccess": ["101"]}},
                },
                Path("experiment.yaml"),
            )

    def test_build_replay_experiment_spec_rejects_authoring_only_model_registry(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains unsupported keys"):
            build_replay_experiment_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "run": {"args": {}},
                    "launcher": {},
                    "cluster": {},
                    "env": {},
                    "resources": {},
                    "artifacts": {},
                    "eval": {},
                    "output": {},
                    "notify": {},
                    "validation": {},
                    "model_registry": {},
                },
                project_root=Path(".").resolve(),
                config_path=Path("replay.yaml"),
            )

    def test_validate_top_config_does_not_resolve_model_registry_files(self) -> None:
        with self.assertRaisesRegex(ValueError, "model_registry\\.registry_file not found"):
            check_batch_contract(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom_model"},
                    "model_registry": {"registry_file": "missing_models.yaml"},
                    "run": {"args": {"lr": 0.1}},
                },
                Path("experiment.yaml"),
            )

    def test_validate_top_config_expands_sweep_combinations_and_reports_case_errors(self) -> None:
        cfg = {
            "project": "demo",
            "experiment_name": "exp",
            "model": {"name": "custom", "script": "train.py"},
            "run": {"args": {"lr": 0.1}},
            "sweep": {
                "enabled": True,
                "shared_axes": {
                    "run.command": ["python train.py"],
                },
                "cases": [
                    {
                        "name": "adapter_conflict",
                        "set": {
                            "run.adapter.script": "adapter.py",
                        },
                    }
                ],
            },
        }

        with self.assertRaisesRegex(ValueError, "run\\.command and run\\.adapter\\.script cannot be used together"):
            check_batch_contract(cfg, Path("experiment.yaml"))
