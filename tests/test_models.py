from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from slurmforge.model_support.catalog import build_model_catalog, resolve_model_spec
from slurmforge.model_support.catalog import registry_loader as registry_loader_module
from slurmforge.pipeline.config.api import (
    build_batch_spec,
    build_experiment_spec,
    build_replay_experiment_spec,
    serialize_model_config,
    serialize_replay_experiment_spec,
)


class ModelCatalogTests(unittest.TestCase):
    def test_model_catalog_has_no_project_builtins_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            catalog = build_model_catalog(project_root, model_registry_cfg={})
            with self.assertRaisesRegex(
                ValueError,
                "Unknown model.name=convbert.*model_registry\\.registry_file / model_registry\\.extra_models",
            ):
                resolve_model_spec(catalog, {"name": "convbert"}, project_root=project_root)

    def test_build_model_catalog_loads_registry_file_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            registry_file = tmp_path / "models.yaml"
            registry_file.write_text("models: []\n", encoding="utf-8")
            script_path = tmp_path / "train_custom.py"
            script_path.write_text("print('ok')\n", encoding="utf-8")

            with patch.object(
                registry_loader_module.yaml,
                "safe_load",
                return_value={
                    "models": [
                        {
                            "name": "custom_model",
                            "script": "train_custom.py",
                            "yaml": "",
                            "ddp_supported": False,
                            "ddp_required": False,
                            "estimator_profile": "default",
                        }
                    ]
                },
            ):
                catalog = build_model_catalog(tmp_path, model_registry_cfg={"registry_file": "models.yaml"})
                spec = resolve_model_spec(catalog, {"name": "custom_model"}, project_root=tmp_path)

        self.assertEqual(spec.name, "custom_model")
        self.assertEqual(spec.script, script_path.resolve())
        self.assertEqual(spec.estimator_profile, "default")
        self.assertFalse(spec.ddp_supported)

    def test_build_model_catalog_extra_models_override_registry_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            registry_file = tmp_path / "models.yaml"
            registry_file.write_text(
                "models:\n"
                "  - name: custom_model\n"
                "    script: train_registry.py\n",
                encoding="utf-8",
            )
            registry_script = tmp_path / "train_registry.py"
            override_script = tmp_path / "train_override.py"
            registry_script.write_text("print('registry')\n", encoding="utf-8")
            override_script.write_text("print('override')\n", encoding="utf-8")

            catalog = build_model_catalog(
                tmp_path,
                model_registry_cfg={
                    "registry_file": "models.yaml",
                    "extra_models": [
                        {
                            "name": "custom_model",
                            "script": "train_override.py",
                            "ddp_supported": False,
                        }
                    ],
                },
            )
            spec = resolve_model_spec(catalog, {"name": "custom_model"}, project_root=tmp_path)

        self.assertEqual(spec.script, override_script.resolve())
        self.assertFalse(spec.ddp_supported)

    def test_build_model_catalog_rejects_duplicate_registry_file_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            registry_file = tmp_path / "models.yaml"
            registry_file.write_text(
                "models:\n"
                "  - name: custom_model\n"
                "    script: train_a.py\n"
                "  - name: custom_model\n"
                "    script: train_b.py\n",
                encoding="utf-8",
            )
            (tmp_path / "train_a.py").write_text("print('a')\n", encoding="utf-8")
            (tmp_path / "train_b.py").write_text("print('b')\n", encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError,
                "model_registry\\.registry_file defines duplicate model names: custom_model",
            ):
                build_model_catalog(tmp_path, model_registry_cfg={"registry_file": "models.yaml"})

    def test_direct_model_script_defaults_to_ddp_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            script_path = tmp_path / "train_custom.py"
            script_path.write_text("print('ok')\n", encoding="utf-8")

            catalog = build_model_catalog(tmp_path, model_registry_cfg={})
            spec = resolve_model_spec(
                catalog,
                {"name": "custom_model", "script": "train_custom.py"},
                project_root=tmp_path,
            )

        self.assertEqual(spec.script, script_path.resolve())
        self.assertTrue(spec.ddp_supported)

    def test_replay_model_catalog_rewrites_absolute_paths_relative_to_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            old_workspace = tmp_path / "workspace_old"
            new_workspace = tmp_path / "workspace_new"
            old_project_root = old_workspace / "project"
            old_shared_root = old_workspace / "shared"
            new_project_root = new_workspace / "project"
            new_shared_root = new_workspace / "shared"
            old_project_root.mkdir(parents=True, exist_ok=True)
            old_shared_root.mkdir(parents=True, exist_ok=True)
            new_project_root.mkdir(parents=True, exist_ok=True)
            new_shared_root.mkdir(parents=True, exist_ok=True)

            old_script = old_shared_root / "train_custom.py"
            new_script = new_shared_root / "train_custom.py"
            old_script.write_text("print('old')\n", encoding="utf-8")
            new_script.write_text("print('new')\n", encoding="utf-8")

            spec = build_experiment_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom_model"},
                    "model_registry": {
                        "extra_models": [
                            {
                                "name": "custom_model",
                                "script": str(old_script),
                            }
                        ]
                    },
                    "run": {"args": {"lr": 0.001}},
                },
                old_project_root / "experiment.yaml",
                project_root=old_project_root,
            )

            replay_cfg = serialize_replay_experiment_spec(spec, project_root=old_project_root)
            catalog_entry = replay_cfg["resolved_model_catalog"]["models"][0]
            self.assertFalse(catalog_entry["script"].startswith("/"))

            replayed = build_replay_experiment_spec(
                replay_cfg,
                project_root=new_project_root,
                config_path=new_project_root / "replay.yaml",
            )
            resolved = resolve_model_spec(replayed.model_catalog, {"name": "custom_model"}, project_root=new_project_root)

        self.assertEqual(resolved.script, new_script.resolve())

    def test_replay_cfg_canonicalizes_direct_model_and_run_paths_relative_to_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            old_workspace = tmp_path / "workspace_old"
            new_workspace = tmp_path / "workspace_new"
            old_project_root = old_workspace / "project"
            old_shared_root = old_workspace / "shared"
            new_project_root = new_workspace / "project"
            new_shared_root = new_workspace / "shared"
            old_project_root.mkdir(parents=True, exist_ok=True)
            old_shared_root.mkdir(parents=True, exist_ok=True)
            new_project_root.mkdir(parents=True, exist_ok=True)
            new_shared_root.mkdir(parents=True, exist_ok=True)

            old_script = old_shared_root / "train_custom.py"
            new_script = new_shared_root / "train_custom.py"
            old_eval_script = old_shared_root / "eval_custom.py"
            new_eval_script = new_shared_root / "eval_custom.py"
            old_script.write_text("print('old train')\n", encoding="utf-8")
            new_script.write_text("print('new train')\n", encoding="utf-8")
            old_eval_script.write_text("print('old eval')\n", encoding="utf-8")
            new_eval_script.write_text("print('new eval')\n", encoding="utf-8")

            spec = build_experiment_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom_model", "script": str(old_script)},
                    "run": {
                        "args": {"lr": 0.001},
                        "workdir": str(old_shared_root),
                    },
                    "eval": {
                        "enabled": True,
                        "script": str(old_eval_script),
                        "workdir": str(old_shared_root),
                    },
                },
                old_project_root / "experiment.yaml",
                project_root=old_project_root,
            )

            replay_cfg = serialize_replay_experiment_spec(spec, project_root=old_project_root)
            self.assertEqual(replay_cfg["model"]["script"], "../shared/train_custom.py")
            self.assertEqual(replay_cfg["run"]["workdir"], "../shared")
            self.assertEqual(replay_cfg["eval"]["script"], "../shared/eval_custom.py")
            self.assertEqual(replay_cfg["eval"]["workdir"], "../shared")

            replayed = build_replay_experiment_spec(
                replay_cfg,
                project_root=new_project_root,
                config_path=new_project_root / "replay.yaml",
            )
            resolved = resolve_model_spec(
                replayed.model_catalog,
                serialize_model_config(replayed.model),
                project_root=new_project_root,
            )

        self.assertEqual(resolved.script, new_script.resolve())
        self.assertEqual(replayed.run.workdir, "../shared")
        self.assertEqual(replayed.eval.script, "../shared/eval_custom.py")
        self.assertEqual((new_project_root / replayed.eval.workdir).resolve(), new_shared_root.resolve())

    def test_replay_cfg_persists_dispatch_group_overflow_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            script_path = project_root / "train.py"
            script_path.write_text("print('ok')\n", encoding="utf-8")

            spec = build_experiment_spec(
                {
                    "project": "demo",
                    "experiment_name": "exp",
                    "model": {"name": "custom_model", "script": "train.py"},
                    "run": {"args": {"lr": 0.001}},
                    "dispatch": {"group_overflow_policy": "serial"},
                },
                project_root / "experiment.yaml",
                project_root=project_root,
            )

            replay_cfg = serialize_replay_experiment_spec(spec, project_root=project_root)
            self.assertEqual(replay_cfg["dispatch"]["group_overflow_policy"], "serial")

            replayed = build_replay_experiment_spec(
                replay_cfg,
                project_root=project_root,
                config_path=project_root / "replay.yaml",
            )

        self.assertEqual(replayed.dispatch.group_overflow_policy, "serial")

    def test_build_batch_spec_reuses_model_catalog_for_identical_registry_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            registry_file = tmp_path / "models.yaml"
            registry_file.write_text(
                "models:\n"
                "  - name: custom_model\n"
                "    script: train.py\n",
                encoding="utf-8",
            )
            (tmp_path / "train.py").write_text("print('ok')\n", encoding="utf-8")

            cfg = {
                "project": "demo",
                "experiment_name": "exp",
                "model": {"name": "custom_model"},
                "model_registry": {"registry_file": "models.yaml"},
                "run": {"args": {"lr": 0.001}},
                "sweep": {
                    "enabled": True,
                    "shared_axes": {
                        "run.args.lr": [0.001, 0.002],
                    },
                },
            }

            with patch.object(
                registry_loader_module.yaml,
                "safe_load",
                wraps=registry_loader_module.yaml.safe_load,
            ) as safe_load:
                batch_spec = build_batch_spec(
                    cfg,
                    tmp_path / "experiment.yaml",
                    project_root=tmp_path,
                )

        self.assertEqual(batch_spec.total_runs, 2)
        self.assertEqual(safe_load.call_count, 1)
        self.assertIs(batch_spec.runs[0].spec.model_catalog, batch_spec.runs[1].spec.model_catalog)

    def test_build_replay_experiment_spec_rejects_duplicate_catalog_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

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
                        "run": {"args": {"lr": 0.001}},
                    },
                    project_root=tmp_path,
                    config_path=tmp_path / "replay.yaml",
                )
