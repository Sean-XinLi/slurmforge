from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import yaml


class ExampleCliTests(unittest.TestCase):
    def test_list_example_catalog_includes_descriptions(self) -> None:
        from slurmforge.example_configs import list_example_catalog

        catalog = dict(list_example_catalog())

        self.assertIn("command_minimal", catalog)
        self.assertIn("existing training command", catalog["command_minimal"])
        # script_hpc replaces the old model_cli_script_hpc
        self.assertIn("script_hpc", catalog)
        self.assertIn("script_hpc", catalog)

    def test_init_uses_same_experiment_yaml_as_raw_examples(self) -> None:
        from slurmforge.example_configs import read_example_text
        from slurmforge.starter_catalog import list_starter_specs
        from slurmforge.starter_projects import init_project

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            for spec in list_starter_specs():
                project_root = tmp_path / f"{spec.template_type}_{spec.profile}"
                init_project(spec.template_type, spec.profile, project_root)
                self.assertEqual(
                    (project_root / "experiment.yaml").read_text(encoding="utf-8"),
                    read_example_text(spec.example_name),
                )

    def test_list_example_names_includes_primary_starters(self) -> None:
        from slurmforge.example_configs import list_example_names

        names = list_example_names()

        self.assertIn("command_minimal", names)
        self.assertIn("script_hpc", names)
        self.assertIn("model_registry", names)
        # model_cli_* examples have been removed; script_* are the replacements
        self.assertNotIn("model_cli_script_hpc", names)
        self.assertNotIn("model_cli_script_minimal", names)

    def test_show_examples_prints_yaml_for_script_starter(self) -> None:
        from slurmforge.cli import examples

        buf = io.StringIO()
        with redirect_stdout(buf):
            examples.handle_examples_show(type("Args", (), {"name": "script_starter"})())

        output = buf.getvalue()
        self.assertIn('project: "my_project"', output)
        self.assertIn('experiment_name: "experiment_v1"', output)

    def test_list_examples_prints_name_and_description(self) -> None:
        from slurmforge.cli import examples

        buf = io.StringIO()
        with redirect_stdout(buf):
            examples.handle_examples_list(type("Args", (), {})())

        output = buf.getvalue()
        self.assertIn("command_minimal", output)
        self.assertIn("existing training command", output)

    def test_export_example_requires_force_to_overwrite(self) -> None:
        from slurmforge.example_configs import export_example

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "experiment.yaml"
            target.write_text("old\n", encoding="utf-8")

            with self.assertRaises(FileExistsError):
                export_example("command_minimal", target, force=False)

            export_example("command_minimal", target, force=True)
            self.assertIn('experiment_name: "hello_command"', target.read_text(encoding="utf-8"))

    def test_init_registry_starter_writes_project_skeleton(self) -> None:
        from slurmforge.starter_catalog import get_starter_spec
        from slurmforge.starter_projects import init_project

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "starter"
            written = init_project("registry", "starter", out)
            spec = get_starter_spec("registry", "starter")
            expected_paths = {(out / resource.relative_path).resolve() for resource in spec.resources}

            self.assertTrue(expected_paths.issubset(set(written)))
            self.assertTrue((out / "runs").is_dir())

    def test_script_hpc_example_has_eval_and_sweep_sections(self) -> None:
        from slurmforge.example_configs import read_example_text

        output = read_example_text("script_hpc")
        self.assertIn('project: "my_project"', output)
        self.assertIn('script: "eval.py"', output)
        self.assertIn('sweep:', output)
        # script_hpc uses null sentinels, not placeholder strings
        self.assertIn('partition: ~', output)
        self.assertIn('account: ~', output)

    def test_script_hpc_init_writes_readme_with_profile(self) -> None:
        from slurmforge.starter_projects import init_project

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "starter"
            init_project("script", "hpc", out)

            readme = (out / "README.md").read_text(encoding="utf-8")
            self.assertIn("Template: `script`", readme)
            self.assertIn("Profile: `hpc`", readme)

    def test_init_rejects_non_empty_directory_without_force(self) -> None:
        from slurmforge.starter_projects import init_project

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "starter"
            out.mkdir(parents=True, exist_ok=True)
            (out / "existing.txt").write_text("keep\n", encoding="utf-8")

            with self.assertRaises(FileExistsError):
                init_project("command", "starter", out, force=False)

    def test_main_dispatches_examples_subcommand(self) -> None:
        from slurmforge import launcher

        with patch("slurmforge.cli.examples.handle_examples_list", return_value=None) as handler:
            launcher.main(["examples", "list"])

        self.assertTrue(handler.called)

    def test_main_dispatches_init_script_hpc_subcommand(self) -> None:
        from slurmforge import launcher

        with patch("slurmforge.cli.init.handle_init_template", return_value=None) as handler:
            launcher.main(["init", "script", "--profile", "hpc", "--out", "starter"])

        self.assertEqual(handler.call_args.args[0].template_type, "script")
        self.assertEqual(handler.call_args.args[0].profile, "hpc")
        self.assertEqual(handler.call_args.args[0].out, "starter")

    def test_init_all_type_profile_combos_generate_valid_yaml(self) -> None:
        from slurmforge.starter_catalog import TEMPLATE_TYPES, PROFILES
        from slurmforge.starter_projects import init_project

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            for ttype in TEMPLATE_TYPES:
                for profile in PROFILES:
                    out = tmp_path / f"{ttype}_{profile}"
                    init_project(ttype, profile, out)
                    cfg = yaml.safe_load((out / "experiment.yaml").read_text(encoding="utf-8"))
                    self.assertIsInstance(cfg, dict, f"{ttype}+{profile} must produce a valid YAML mapping")
                    self.assertIn("project", cfg)
                    self.assertIn("experiment_name", cfg)

    def test_init_starter_templates_have_null_sentinels(self) -> None:
        from slurmforge.starter_catalog import TEMPLATE_TYPES, PROFILES
        from slurmforge.starter_projects import init_project
        from slurmforge.pipeline.config.validation.completeness import check_completeness

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            for ttype in TEMPLATE_TYPES:
                for profile in PROFILES:
                    out = tmp_path / f"{ttype}_{profile}"
                    init_project(ttype, profile, out)
                    yaml_path = out / "experiment.yaml"
                    cfg = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
                    issues = check_completeness(cfg, config_path=yaml_path)
                    # Every template must have at least the 3 cluster fields as required nulls
                    null_paths = [i.path for i in issues if hasattr(i, "path") and i.path[0] == "cluster"]
                    self.assertGreaterEqual(
                        len(null_paths), 3,
                        f"{ttype}+{profile} should have partition/account/time_limit as null sentinels"
                    )
