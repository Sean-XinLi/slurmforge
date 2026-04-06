from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from importlib import metadata
from pathlib import Path

import jinja2
import markupsafe
import packaging
import yaml

from slurmforge.starter_catalog import list_starter_specs


CHECKOUT_IGNORE_PATTERNS = (
    ".venv",
    ".venv*",
    ".pytest_cache",
    "__pycache__",
    "*.pyc",
    "*.pyo",
    "*.egg-info",
    "build",
    "dist",
    "venv",
    "venv*",
)

# Required fields by template type — used to fill null sentinels for sforge generate
_REQUIRED_SETS_BY_TYPE: dict[str, list[str]] = {
    "script": [
        "cluster.partition=test_gpu",
        "cluster.account=test_account",
        "cluster.time_limit=01:00:00",
        "model.script=train.py",
    ],
    "command": [
        "cluster.partition=test_gpu",
        "cluster.account=test_account",
        "cluster.time_limit=01:00:00",
        "run.command=echo hello",
    ],
    "registry": [
        "cluster.partition=test_gpu",
        "cluster.account=test_account",
        "cluster.time_limit=01:00:00",
        "model_registry.registry_file=models.yaml",
    ],
    "adapter": [
        "cluster.partition=test_gpu",
        "cluster.account=test_account",
        "cluster.time_limit=01:00:00",
        "run.adapter.script=train_adapter.py",
    ],
}


class InstalledPackageIntegrationTests(unittest.TestCase):
    SEEDED_DISTRIBUTIONS = ("Jinja2", "MarkupSafe", "packaging", "PyYAML")
    SEEDED_MODULES = (jinja2, markupsafe, packaging, yaml)

    def _run(
        self,
        argv: list[str],
        *,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
            cwd=str(cwd) if cwd is not None else None,
        )

    def _copy_checkout(self, repo_root: Path, checkout_root: Path) -> None:
        shutil.copytree(
            repo_root,
            checkout_root,
            ignore=shutil.ignore_patterns(*CHECKOUT_IGNORE_PATTERNS),
        )

    def _site_packages_dir(self, venv_python: Path) -> Path:
        completed = self._run(
            [str(venv_python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"]
        )
        return Path(completed.stdout.strip())

    def _seed_active_runtime_dependencies(self, site_packages: Path) -> None:
        for module in self.SEEDED_MODULES:
            module_path = Path(module.__file__).resolve()
            source = module_path.parent if module_path.name == "__init__.py" else module_path
            destination = site_packages / source.name
            if destination.exists():
                continue
            if source.is_dir():
                shutil.copytree(source, destination)
            else:
                shutil.copy2(source, destination)

        for dist_name in self.SEEDED_DISTRIBUTIONS:
            dist_info = Path(str(metadata.distribution(dist_name)._path)).resolve()
            destination = site_packages / dist_info.name
            if destination.exists():
                continue
            shutil.copytree(dist_info, destination)

    def _find_supported_python(self) -> Path | None:
        candidates: list[Path] = []
        if sys.version_info >= (3, 10):
            candidates.append(Path(sys.executable).resolve())
        for name in ("python3.12", "python3.11", "python3.10"):
            candidate = shutil.which(name)
            if candidate is None:
                continue
            resolved = Path(candidate).resolve()
            if resolved not in candidates:
                candidates.append(resolved)

        for candidate in candidates:
            completed = subprocess.run(
                [
                    str(candidate),
                    "-c",
                    "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            major, minor = (int(part) for part in completed.stdout.strip().split(".", maxsplit=1))
            if (major, minor) >= (3, 10):
                return candidate
        return None

    def _create_install_env(self, venv_dir: Path) -> Path:
        interpreter = self._find_supported_python()
        if interpreter is None:
            self.skipTest("installed-package verification requires a Python 3.10+ interpreter on PATH")
        subprocess.run([str(interpreter), "-m", "venv", str(venv_dir)], check=True)
        bindir = venv_dir / ("Scripts" if os.name == "nt" else "bin")
        venv_python = bindir / ("python.exe" if os.name == "nt" else "python")
        self._seed_active_runtime_dependencies(self._site_packages_dir(venv_python))
        return venv_python

    def _first_record_path(self, batch_root: Path) -> Path:
        return next(batch_root.glob("records/group_*/task_*.json"))

    def _first_result_dir(self, batch_root: Path) -> Path:
        return next((batch_root / "runs").glob("run_*/job-*"))

    def test_source_checkout_install_exposes_real_console_script_workflows(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            work_root = tmp_path / "work"
            work_root.mkdir(parents=True, exist_ok=True)
            checkout_root = tmp_path / "checkout"
            self._copy_checkout(repo_root, checkout_root)

            venv_python = self._create_install_env(tmp_path / "venv")
            subprocess.run(
                [str(venv_python), "-m", "pip", "install", "--no-build-isolation", str(checkout_root)],
                check=True,
            )

            bindir = venv_python.parent
            cli_path = bindir / ("sforge.exe" if os.name == "nt" else "sforge")
            executor_path = bindir / (
                "sforge-run-plan-executor.exe" if os.name == "nt" else "sforge-run-plan-executor"
            )
            artifact_sync_path = bindir / (
                "sforge-artifact-sync.exe" if os.name == "nt" else "sforge-artifact-sync"
            )
            train_outputs_path = bindir / (
                "sforge-write-train-outputs.exe" if os.name == "nt" else "sforge-write-train-outputs"
            )
            attempt_result_path = bindir / (
                "sforge-write-attempt-result.exe" if os.name == "nt" else "sforge-write-attempt-result"
            )

            self.assertTrue(cli_path.exists())
            self.assertTrue(executor_path.exists())
            self.assertTrue(artifact_sync_path.exists())
            self.assertTrue(train_outputs_path.exists())
            self.assertTrue(attempt_result_path.exists())

            # ── CLI help output ──────────────────────────────────────────────
            root_help = self._run([str(cli_path), "--help"], cwd=work_root)
            self.assertIn("Slurm-oriented experiment orchestration CLI", root_help.stdout)
            self.assertIn("sforge init", root_help.stdout)

            init_help = self._run([str(cli_path), "init", "--help"], cwd=work_root)
            self.assertIn("sforge init", init_help.stdout)
            self.assertIn("script", init_help.stdout)
            self.assertIn("command", init_help.stdout)
            self.assertIn("registry", init_help.stdout)
            self.assertIn("adapter", init_help.stdout)

            # ── examples list/show/export ────────────────────────────────────
            list_examples = self._run([str(cli_path), "examples", "list"], cwd=work_root)
            example_lines = list_examples.stdout.splitlines()
            # script_hpc replaces the old model_cli_script_hpc
            self.assertTrue(any(line.startswith("script_hpc") for line in example_lines))
            self.assertTrue(any(line.startswith("model_registry") for line in example_lines))

            show_hpc = self._run([str(cli_path), "examples", "show", "script_hpc"], cwd=work_root)
            self.assertIn('project: "my_project"', show_hpc.stdout)
            self.assertIn('script: "eval.py"', show_hpc.stdout)
            # new templates use null sentinels, not placeholder strings
            self.assertIn('account: ~', show_hpc.stdout)

            exported_hpc = work_root / "hpc.yaml"
            self._run(
                [str(cli_path), "examples", "export", "script_hpc", "--out", str(exported_hpc)],
                cwd=work_root,
            )
            self.assertTrue(exported_hpc.exists())

            # ── installed package resource access ────────────────────────────
            resources_probe = self._run(
                [
                    str(venv_python),
                    "-c",
                    (
                        "from importlib.resources import files; "
                        "print((files('slurmforge') / 'templates' / 'sbatch_array_group.sh.j2').is_file()); "
                        "print((files('slurmforge') / 'examples' / 'script_hpc.yaml').is_file()); "
                        "print((files('slurmforge.starter_templates') / 'README.md.tmpl').is_file())"
                    ),
                ]
            )
            self.assertEqual(resources_probe.stdout.strip().splitlines(), ["True", "True", "True"])

            # ── sforge init + generate for all 8 (type, profile) combos ─────
            batch_roots: dict[str, Path] = {}
            for spec in list_starter_specs():
                spec_key = f"{spec.template_type}_{spec.profile}"
                project_root = work_root / f"starter_{spec_key}"

                # init: new subcommand-tree interface
                self._run(
                    [str(cli_path), "init", spec.template_type, "--profile", spec.profile,
                     "--out", str(project_root)],
                    cwd=work_root,
                )

                config_path = project_root / "experiment.yaml"
                config_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
                self.assertTrue(config_path.exists())
                self.assertTrue((project_root / "runs").is_dir())

                expected_files = [
                    resource.relative_path
                    for resource in spec.resources
                    if resource.kind != "directory" and resource.relative_path != "experiment.yaml"
                ]
                for relative_path in expected_files:
                    self.assertTrue((project_root / relative_path).exists())

                # All templates now use null sentinels for required cluster fields
                self.assertIsNone(config_data["cluster"]["partition"])
                self.assertIsNone(config_data["cluster"]["account"])
                self.assertIsNone(config_data["cluster"]["time_limit"])

                # script/registry templates have null model.script sentinel
                if spec.template_type in ("script", "registry"):
                    pass  # model.script or registry_file is null — that's correct

                # generate: fill required null fields via --set
                batch_name = f"integration_{spec_key}"
                set_args = []
                for kv in _REQUIRED_SETS_BY_TYPE[spec.template_type]:
                    set_args += ["--set", kv]

                self._run(
                    [
                        str(cli_path), "generate",
                        "--config", str(config_path),
                        "--set", f"output.batch_name={batch_name}",
                    ] + set_args,
                    cwd=project_root,
                )

                batch_root = (
                    project_root
                    / "runs"
                    / config_data["project"]
                    / config_data["experiment_name"]
                    / f"batch_{batch_name}"
                )
                self.assertTrue((batch_root / "batch_manifest.json").exists())
                self.assertTrue((batch_root / "sbatch" / "submit_all.sh").exists())
                self.assertTrue((batch_root / "sbatch" / "array_group_01.sbatch.sh").exists())
                manifest_payload = json.loads((batch_root / "batch_manifest.json").read_text(encoding="utf-8"))
                self.assertEqual(manifest_payload["generated_by"]["name"], "slurmforge")
                batch_roots[spec_key] = batch_root

            # ── status and rerun preview (using script_starter as primary) ───
            primary_key = "script_starter"
            primary_project_root = work_root / f"starter_{primary_key}"
            primary_batch_root = batch_roots[primary_key]

            status_result = self._run(
                [str(cli_path), "status", "--from", str(primary_batch_root), "--status", "all"],
                cwd=primary_project_root,
            )
            self.assertIn("[STATUS]", status_result.stdout)
            self.assertIn("missing=1", status_result.stdout)

            rerun_preview = self._run(
                [str(cli_path), "rerun", "--from", str(primary_batch_root), "--status", "all", "--dry_run"],
                cwd=primary_project_root,
            )
            self.assertIn("[RETRY]", rerun_preview.stdout)
            self.assertIn("selected_runs=1", rerun_preview.stdout)

            # ── executor: run the record from script_starter batch ───────────
            executor_result = self._run(
                [str(executor_path), "--record", str(self._first_record_path(primary_batch_root))],
                cwd=primary_project_root,
            )
            self.assertIn("training finished", executor_result.stdout)

            job_result_dir = self._first_result_dir(primary_batch_root)
            self.assertTrue((job_result_dir / "train_summary.json").exists())
            self.assertTrue((job_result_dir / "checkpoints" / "last.ckpt").exists())
            self.assertTrue((job_result_dir / "logs" / "train.log").exists())
            self.assertTrue((job_result_dir / "meta" / "attempt_result.json").exists())

            # ── artifact sync ────────────────────────────────────────────────
            artifact_workdir = work_root / "artifact_source"
            artifact_result_dir = work_root / "artifact_result"
            artifact_workdir.mkdir(parents=True, exist_ok=True)
            (artifact_workdir / "notes.txt").write_text("artifact\n", encoding="utf-8")
            artifact_cli_result = self._run(
                [
                    str(artifact_sync_path),
                    "--workdir", str(artifact_workdir),
                    "--result_dir", str(artifact_result_dir),
                    "--extra_glob", "*.txt",
                ],
                cwd=work_root,
            )
            self.assertIn("[artifact_sync] wrote manifest:", artifact_cli_result.stdout)
            self.assertTrue((artifact_result_dir / "extra" / "notes.txt").exists())
            artifact_manifest = json.loads(
                (artifact_result_dir / "meta" / "artifact_manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(artifact_manifest["status"], "ok")
            self.assertEqual(artifact_manifest["failure_count"], 0)
            self.assertEqual(artifact_manifest["workdirs"], [str(artifact_workdir.resolve())])
