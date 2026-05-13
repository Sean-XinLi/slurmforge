from __future__ import annotations

import copy
import tempfile
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase
from tests.support.internal_records import materialize_stage_batch_for_test
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    write_demo_project,
    write_stage_submit_files,
)


class SubmitManifestContractTests(StageBatchSystemTestCase):
    def test_submit_manifest_reader_accepts_current_writer_shape(self) -> None:
        from slurmforge.submission.submit_manifest import load_submit_manifest

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            write_stage_submit_files(batch)

            manifest = load_submit_manifest(Path(batch.submission_root))

            self.assertEqual(manifest.batch_id, batch.batch_id)
            self.assertEqual(manifest.stage_name, batch.stage_name)
            self.assertEqual(manifest.groups[0].group_id, "group_001")
            self.assertTrue(Path(manifest.submit_script).exists())

    def test_submit_manifest_reader_rejects_incomplete_records(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.submission.submit_manifest import submit_manifest_from_dict

        payload = {
            "schema_version": 1,
            "batch_id": "batch",
            "stage_name": "train",
            "generation_id": "gen",
            "generation_dir": "/tmp/gen",
            "submit_script": "/tmp/gen/submit.sh",
            "groups": [
                {
                    "group_id": "group_001",
                    "group_index": 0,
                    "sbatch_path": "/tmp/gen/group_001.sbatch",
                    "array_size": 1,
                    "stage_instance_ids": ["run_1.train"],
                }
            ],
            "dependencies": [
                {
                    "from_groups": ["group_001"],
                    "to_group": "group_002",
                    "type": "afterany",
                    "from_wave": "wave_001",
                    "to_wave": "wave_002",
                }
            ],
            "notifications": [
                {
                    "event": "batch_finished",
                    "sbatch_path": "/tmp/gen/notify_batch_finished.sbatch",
                }
            ],
        }

        cases = {
            "schema_version": {**payload, "schema_version": "1"},
            "generation_id": {
                key: value
                for key, value in payload.items()
                if key != "generation_id"
            },
            "array_size": {
                **payload,
                "groups": [
                    {
                        **payload["groups"][0],
                        "array_size": "1",
                    }
                ],
            },
            "stage_instance_ids": {
                **payload,
                "groups": [
                    {
                        **payload["groups"][0],
                        "stage_instance_ids": ("run_1.train",),
                    }
                ],
            },
        }

        for expected, invalid in cases.items():
            with self.subTest(field=expected):
                with self.assertRaisesRegex(RecordContractError, expected):
                    submit_manifest_from_dict(copy.deepcopy(invalid))
