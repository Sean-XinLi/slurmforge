from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_stage_batch_for_kind,
    load_experiment_spec,
    prepare_stage_submission,
    submit_prepared_stage_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    create_submit_generation,
    read_submission_ledger,
    materialize_stage_batch_for_test,
)
import json
import tempfile
from dataclasses import replace
from pathlib import Path


class SubmitGenerationTests(StageBatchSystemTestCase):
    def test_submit_uses_ledger_manifest_and_ignores_stale_sbatch_files(self) -> None:
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {
                        "type": "grid",
                        "axes": {"train.resources.constraint": ["a", "b"]},
                    },
                    "dispatch": {
                        "max_available_gpus": 2,
                        "overflow_policy": "serialize_groups",
                    },
                },
            )
            batch = compile_stage_batch_for_kind(
                load_experiment_spec(cfg_path), kind="train"
            )
            materialize_stage_batch_for_test(
                batch, spec_snapshot=load_experiment_spec(cfg_path).raw
            )
            prepared = prepare_stage_submission(batch)
            materialization = json.loads(
                (
                    Path(batch.submission_root) / "materialization_status.json"
                ).read_text()
            )
            self.assertEqual(materialization["state"], "ready")
            self.assertTrue(
                materialization["submit_manifest_path"].endswith("submit_manifest.json")
            )
            ledger = read_submission_ledger(Path(batch.submission_root))
            assert ledger is not None
            self.assertEqual(ledger.state, "planned")
            stale = Path(batch.submission_root) / "submit" / "group_999.sbatch"
            stale.write_text("#!/usr/bin/env bash\nexit 9\n", encoding="utf-8")

            client = FakeSlurmClient()
            group_job_ids = submit_prepared_stage_batch(prepared, client=client)

            self.assertEqual(set(group_job_ids), {"group_001", "group_002"})
            submitted_paths = {path for path, _dep, _job_id in client.submissions}
            self.assertNotIn(stale, submitted_paths)
            self.assertEqual(
                {path.name for path in submitted_paths},
                {"group_001.sbatch", "group_002.sbatch"},
            )
            ledger = read_submission_ledger(Path(batch.submission_root))
            assert ledger is not None
            self.assertEqual(ledger.state, "submitted")
            self.assertEqual(set(ledger.groups), {"group_001", "group_002"})

            with self.assertRaisesRegex(Exception, "already has submitted group"):
                submit_prepared_stage_batch(prepared, client=FakeSlurmClient())

    def test_submit_generation_creation_is_pure_emit_not_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)

            generation = create_submit_generation(batch)

            self.assertTrue(Path(generation.manifest_path).exists())
            self.assertIsNone(read_submission_ledger(Path(batch.submission_root)))
            materialization = json.loads(
                (
                    Path(batch.submission_root) / "materialization_status.json"
                ).read_text()
            )
            self.assertEqual(materialization["state"], "planned")

    def test_submission_public_api_hides_unsafe_emit_generation(self) -> None:
        import slurmforge.emit as emit
        import slurmforge.submission as submission

        self.assertEqual(submission.__all__, [])
        self.assertFalse(hasattr(submission, "create_submit_generation"))
        self.assertFalse(hasattr(submission, "read_submission_ledger"))
        self.assertFalse(hasattr(submission, "write_submission_ledger"))
        self.assertFalse(hasattr(submission, "finalizer_dependency_group_ids"))
        self.assertFalse(hasattr(submission, "initialize_submission_ledger"))
        self.assertFalse(hasattr(submission, "append_submission_event"))
        self.assertFalse(hasattr(submission, "submit_stage_batch_with_ledger"))
        self.assertFalse(hasattr(submission, "prepare_stage_submission"))
        self.assertFalse(hasattr(submission, "submit_prepared_stage_batch"))
        self.assertFalse(hasattr(submission, "read_submission_state"))
        self.assertEqual(emit.__all__, [])
        self.assertFalse(hasattr(emit, "write_stage_submit_files"))
        self.assertFalse(hasattr(emit, "load_stage_submit_manifest"))
        self.assertFalse(hasattr(emit, "render_stage_group_sbatch"))
        self.assertFalse(hasattr(emit, "submit_sbatch_files"))
        self.assertFalse(hasattr(emit, "render_pipeline_gate_sbatch"))
        self.assertFalse(hasattr(emit, "write_pipeline_gate_submit_file"))
        generation_source = Path("src/slurmforge/submission/generation.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("from ..emit.stage import", generation_source)
        self.assertNotIn("emit._stage", generation_source)
        resubmit_source = Path("src/slurmforge/cli/resubmit.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("find_bound_input", resubmit_source)
        self.assertNotIn("InputBinding", resubmit_source)
        self.assertNotIn("write_json", resubmit_source)
        self.assertNotIn("materialize_stage_batch", resubmit_source)
        self.assertNotIn("compile_stage_batch_from_prior_source", resubmit_source)
        self.assertIn("build_prior_source_stage_batch", resubmit_source)

    def test_submit_requires_ready_prepared_submission(self) -> None:
        from tests.support.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(batch, spec_snapshot=spec.raw)
            prepared = prepare_stage_submission(batch)

            with self.assertRaisesRegex(Exception, "not produced as ready"):
                submit_prepared_stage_batch(
                    replace(prepared, materialization_state="blocked"),
                    client=FakeSlurmClient(),
                )
