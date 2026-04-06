from __future__ import annotations

import subprocess
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from slurmforge.templating import build_template_env
from slurmforge.text_safety import slurm_safe_job_name


class TemplatingTests(unittest.TestCase):
    def test_notify_template_shellquotes_project_and_experiment_names(self) -> None:
        rendered = build_template_env().get_template("sbatch_notify.sh.j2").render(
            project='my "$(uname)" project',
            experiment_name="exp $(touch should_not_exist) `echo nope`",
        )

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            script_path = tmp_path / "notify.sh"
            script_path.write_text(rendered, encoding="utf-8")
            script_path.chmod(0o755)

            result = subprocess.run(
                ["/bin/bash", str(script_path)],
                check=True,
                capture_output=True,
                text=True,
                cwd=tmp_path,
            )

            self.assertEqual(
                result.stdout.strip(),
                '[NOTIFY] project=my "$(uname)" project experiment=exp $(touch should_not_exist) `echo nope`',
            )
            self.assertFalse((tmp_path / "should_not_exist").exists())

    def test_array_template_uses_slurm_safe_job_name_and_shellquoted_body_literals(self) -> None:
        cluster = {
            "partition": "gpu",
            "account": "acct",
            "qos": "",
            "time_limit": "01:00:00",
            "nodes": 1,
            "gpus_per_node": 1,
            "cpus_per_task": 2,
            "mem": "0",
            "constraint": "",
            "extra_sbatch_args": [],
        }
        project = 'my "$(uname)" project'
        experiment_name = "exp $(touch should_not_exist) `echo nope`"

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            records_dir = tmp_path / "records $(touch record_hack)"
            records_dir.mkdir()
            (records_dir / "task_000000.json").write_text("{}", encoding="utf-8")

            rendered = build_template_env().get_template("sbatch_array_group.sh.j2").render(
                array_job_name=slurm_safe_job_name(f"{project}_{experiment_name}_arr001"),
                project=project,
                experiment_name=experiment_name,
                group_index=1,
                array_size=1,
                cluster=deepcopy(cluster),
                runtime_setup_lines=["export AI_INFRA_TEST_ENV=1"],
                records_dir=str(records_dir),
                array_log_dir='logs $(touch array_log_hack)',
                batch_root='batch $(touch batch_root_hack)',
                run_plan_executor_bin="/bin/echo",
            )

            self.assertIn(
                f"#SBATCH --job-name={slurm_safe_job_name(f'{project}_{experiment_name}_arr001')}",
                rendered,
            )
            self.assertNotIn(project, rendered.splitlines()[1])

            script_path = tmp_path / "array.sh"
            script_path.write_text(rendered, encoding="utf-8")
            script_path.chmod(0o755)

            result = subprocess.run(
                ["/bin/bash", str(script_path)],
                check=True,
                capture_output=True,
                text=True,
                cwd=tmp_path,
                env={"SLURM_ARRAY_TASK_ID": "0"},
            )

            self.assertIn("[ARRAY] batch_root=batch $(touch batch_root_hack)", result.stdout)
            self.assertIn(str(records_dir / "task_000000.json"), result.stdout)
            self.assertFalse((tmp_path / "should_not_exist").exists())
            self.assertFalse((tmp_path / "record_hack").exists())
            self.assertFalse((tmp_path / "batch_root_hack").exists())
            self.assertFalse((tmp_path / "array_log_hack").exists())
