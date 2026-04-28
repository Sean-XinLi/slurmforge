from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.sforge import (
    compile_stage_batch_for_kind,
    create_submit_generation,
    load_experiment_spec,
    load_stage_submit_manifest,
    prepare_stage_submission,
    read_submission_ledger,
    submit_prepared_stage_batch,
    write_demo_project,
    write_stage_batch_layout,
    write_stage_submit_files,
)
from tests.support.std import Path, json, replace, tempfile


class SubmissionTests(StageBatchSystemTestCase):
    def test_gpu_budget_uses_global_waves_without_exceeding_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {"type": "grid", "axes": {"train.resources.gpus_per_node": [1, 2]}},
                    "dispatch": {"max_available_gpus": 2, "overflow_policy": "serialize_groups"},
                },
            )
            batch = compile_stage_batch_for_kind(load_experiment_spec(cfg_path), kind="train")
            self.assertEqual(batch.budget_plan.policy_applied, "global_waves")
            self.assertEqual(batch.budget_plan.dependencies[0].type, "afterany")
            self.assertLessEqual(max(wave.total_wave_gpus for wave in batch.budget_plan.waves), 2)
            write_stage_batch_layout(batch, spec_snapshot=load_experiment_spec(cfg_path).raw)
            write_stage_submit_files(batch)
            manifest = load_stage_submit_manifest(Path(batch.submission_root))
            submit_text = Path(manifest["submit_script"]).read_text()
            self.assertIn("--dependency=afterany", submit_text)

    def test_gpu_budget_does_not_double_count_parallel_group_throttles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {"type": "grid", "axes": {"train.resources.constraint": ["a", "b"]}},
                    "dispatch": {"max_available_gpus": 2, "overflow_policy": "serialize_groups"},
                },
            )
            batch = compile_stage_batch_for_kind(load_experiment_spec(cfg_path), kind="train")
            self.assertEqual(len(batch.budget_plan.waves), 1)
            wave = batch.budget_plan.waves[0]
            self.assertEqual(wave.total_wave_gpus, 2)
            self.assertEqual([item.array_throttle for item in wave.groups], [1, 1])

    def test_submit_uses_ledger_manifest_and_ignores_stale_sbatch_files(self) -> None:
        from slurmforge.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "runs": {"type": "grid", "axes": {"train.resources.constraint": ["a", "b"]}},
                    "dispatch": {"max_available_gpus": 2, "overflow_policy": "serialize_groups"},
                },
            )
            batch = compile_stage_batch_for_kind(load_experiment_spec(cfg_path), kind="train")
            write_stage_batch_layout(batch, spec_snapshot=load_experiment_spec(cfg_path).raw)
            prepared = prepare_stage_submission(batch)
            materialization = json.loads((Path(batch.submission_root) / "materialization_status.json").read_text())
            self.assertEqual(materialization["state"], "ready")
            self.assertTrue(materialization["submit_manifest_path"].endswith("submit_manifest.json"))
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
            self.assertEqual({path.name for path in submitted_paths}, {"group_001.sbatch", "group_002.sbatch"})
            ledger = read_submission_ledger(Path(batch.submission_root))
            assert ledger is not None
            self.assertEqual(ledger.state, "submitted")
            self.assertEqual(set(ledger.groups), {"group_001", "group_002"})

            with self.assertRaisesRegex(Exception, "already has submitted group"):
                submit_prepared_stage_batch(prepared, client=FakeSlurmClient())

    def test_batch_notification_finalizer_submits_after_terminal_groups(self) -> None:
        from slurmforge.notifications.records import read_notification_record
        from slurmforge.submission.finalizer import (
            finalizer_dependency_group_ids,
            submit_stage_batch_finalizer,
        )
        from slurmforge.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg_path = write_demo_project(
                root,
                extra={
                    "notifications": {
                        "email": {
                            "enabled": True,
                            "to": ["you@example.com"],
                            "on": ["batch_finished"],
                            "mode": "summary",
                        }
                    },
                },
            )
            spec = load_experiment_spec(cfg_path)
            batch = compile_stage_batch_for_kind(spec, kind="train")
            self.assertEqual(finalizer_dependency_group_ids(batch), ("group_001",))
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            prepared = prepare_stage_submission(batch)
            client = FakeSlurmClient()
            group_job_ids = submit_prepared_stage_batch(prepared, client=client)

            record = submit_stage_batch_finalizer(batch, group_job_ids, client=client)

            assert record is not None
            self.assertEqual(record.state, "submitted")
            self.assertEqual(record.scheduler_job_id, "1002")
            self.assertEqual(client.submissions[-1][0].name, "notify_batch_finished.sbatch")
            self.assertEqual(client.submissions[-1][1], f"afterany:{group_job_ids['group_001']}")
            persisted = read_notification_record(Path(batch.submission_root), "batch_finished")
            assert persisted is not None
            self.assertEqual(persisted.scheduler_job_id, record.scheduler_job_id)

    def test_submit_generation_creation_is_pure_emit_not_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)

            generation = create_submit_generation(batch)

            self.assertTrue(Path(generation.manifest_path).exists())
            self.assertIsNone(read_submission_ledger(Path(batch.submission_root)))
            materialization = json.loads((Path(batch.submission_root) / "materialization_status.json").read_text())
            self.assertEqual(materialization["state"], "planned")

    def test_submission_public_api_hides_unsafe_emit_generation(self) -> None:
        import slurmforge.emit as emit
        import slurmforge.submission as submission

        # submission/__init__ is the canonical entry for public submission behavior.
        self.assertFalse(hasattr(submission, "create_submit_generation"))
        self.assertFalse(hasattr(submission, "read_submission_ledger"))
        self.assertFalse(hasattr(submission, "write_submission_ledger"))
        self.assertFalse(hasattr(submission, "finalizer_dependency_group_ids"))
        self.assertFalse(hasattr(submission, "initialize_submission_ledger"))
        self.assertFalse(hasattr(submission, "append_submission_event"))
        self.assertFalse(hasattr(submission, "submit_stage_batch_with_ledger"))
        self.assertTrue(hasattr(submission, "prepare_stage_submission"))
        self.assertTrue(hasattr(submission, "submit_prepared_stage_batch"))
        self.assertTrue(hasattr(submission, "read_submission_state"))
        # emit publicly exposes the sbatch-render API consumed by submission/.
        self.assertTrue(hasattr(emit, "write_stage_submit_files"))
        self.assertTrue(hasattr(emit, "load_stage_submit_manifest"))
        self.assertFalse(hasattr(emit, "render_stage_group_sbatch"))
        self.assertFalse(hasattr(emit, "submit_sbatch_files"))
        self.assertFalse(hasattr(emit, "submit_controller"))
        self.assertTrue(hasattr(emit, "write_controller_submit_file"))
        generation_source = Path("src/slurmforge/submission/generation.py").read_text(encoding="utf-8")
        self.assertIn("from ..emit import", generation_source)
        self.assertNotIn("emit._stage", generation_source)
        resubmit_source = Path("src/slurmforge/cli/resubmit.py").read_text(encoding="utf-8")
        self.assertNotIn("find_bound_input", resubmit_source)
        self.assertNotIn("InputBinding", resubmit_source)
        self.assertNotIn("write_json", resubmit_source)
        self.assertNotIn("materialize_stage_batch", resubmit_source)
        self.assertNotIn("compile_stage_batch_from_prior_source", resubmit_source)
        self.assertIn("build_prior_source_stage_batch", resubmit_source)

    def test_submit_requires_ready_prepared_submission(self) -> None:
        from slurmforge.slurm import FakeSlurmClient

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            prepared = prepare_stage_submission(batch)

            with self.assertRaisesRegex(Exception, "not produced as ready"):
                submit_prepared_stage_batch(
                    replace(prepared, materialization_state="blocked"),
                    client=FakeSlurmClient(),
                )

    def test_group_submit_failure_writes_diagnostic(self) -> None:
        from slurmforge.slurm import FakeSlurmClient

        class FailingSlurm(FakeSlurmClient):
            def submit(self, path, *, dependency=None):
                raise RuntimeError("sbatch unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            prepared = prepare_stage_submission(batch)

            with self.assertRaisesRegex(RuntimeError, "sbatch unavailable"):
                submit_prepared_stage_batch(prepared, client=FailingSlurm())

            batch_root = Path(batch.submission_root)
            diagnostic = batch_root / "submissions" / "diagnostics" / "group_001_submit_traceback.log"
            self.assertTrue(diagnostic.exists())
            diagnostic_text = diagnostic.read_text(encoding="utf-8")
            self.assertIn("RuntimeError: sbatch unavailable", diagnostic_text)
            self.assertIn("Traceback", diagnostic_text)
            ledger = read_submission_ledger(batch_root)
            assert ledger is not None
            self.assertEqual(ledger.state, "failed")
            self.assertEqual(ledger.groups["group_001"].state, "failed")
            self.assertEqual(ledger.groups["group_001"].reason, "sbatch unavailable")

    def test_standalone_submission_writes_ledger_and_reconcile_uses_it(self) -> None:
        from slurmforge.slurm import FakeSlurmClient
        from slurmforge.status import read_stage_status
        from slurmforge.submission import reconcile_batch_submission

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            batch = compile_stage_batch_for_kind(spec, kind="train")
            write_stage_batch_layout(batch, spec_snapshot=spec.raw)
            client = FakeSlurmClient()
            prepared = prepare_stage_submission(batch)
            group_job_ids = submit_prepared_stage_batch(prepared, client=client)
            self.assertTrue((Path(batch.submission_root) / "submissions" / "ledger.json").exists())
            for job_id in group_job_ids.values():
                client.set_job_state(job_id, "COMPLETED")
            reconcile_batch_submission(
                Path(batch.submission_root),
                client=client,
                missing_output_grace_seconds=300,
            )
            status = read_stage_status(Path(batch.submission_root) / batch.stage_instances[0].run_dir_rel)
            assert status is not None
            self.assertEqual(status.state, "running")
