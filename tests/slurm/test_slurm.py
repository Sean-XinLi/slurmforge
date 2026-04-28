from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from pathlib import Path


class SlurmParsingTests(StageBatchSystemTestCase):
    def test_parse_sbatch_job_id_accepts_standard_and_parsable_output(self) -> None:
        from slurmforge.slurm import parse_sbatch_job_id

        self.assertEqual(parse_sbatch_job_id("Submitted batch job 123\n"), "123")
        self.assertEqual(parse_sbatch_job_id("456\n"), "456")

    def test_parse_array_task_rows(self) -> None:
        from slurmforge.slurm import parse_sacct_rows, parse_squeue_rows

        sacct = parse_sacct_rows(
            "100_3|100_3|100|3|COMPLETED|0:0|\n100.batch|100.batch|||COMPLETED|0:0|\n"
        )
        self.assertEqual(sacct["100_3"].array_job_id, "100")
        self.assertEqual(sacct["100_3"].array_task_id, 3)
        self.assertNotIn("100.batch", sacct)

        squeue = parse_squeue_rows("200_4|RUNNING|Resources\n")
        self.assertEqual(squeue["200_4"].state, "RUNNING")
        self.assertEqual(squeue["200_4"].array_task_id, 4)

    def test_fake_slurm_tracks_array_tasks(self) -> None:
        from tests.support.slurm import FakeSlurmClient

        client = FakeSlurmClient()
        job_id = client.submit(Path("group.sbatch"))
        client.set_array_task_state(job_id, 0, "COMPLETED")

        observed = client.query_observed_jobs([job_id])
        self.assertIn(job_id, observed)
        self.assertIn(f"{job_id}_0", observed)
