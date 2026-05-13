from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import (
    compile_train_eval_pipeline_plan,
    compile_stage_batch_for_kind,
    execute_stage_task,
    load_experiment_spec,
    upstream_bindings_from_train_batch,
    write_demo_project,
)
from tests.support.internal_records import (
    materialize_train_eval_pipeline_for_test,
    materialize_stage_batch_for_test,
)
import tempfile
from pathlib import Path


class LineageTests(StageBatchSystemTestCase):
    def test_stage_batch_lineage_records_bound_inputs_and_source_roots(self) -> None:
        from slurmforge.lineage.query import (
            find_bound_input,
            iter_lineage_source_roots,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = load_experiment_spec(write_demo_project(root))
            train_batch = compile_stage_batch_for_kind(spec, kind="train")
            materialize_stage_batch_for_test(train_batch, spec_snapshot=spec.raw)
            self.assertEqual(
                execute_stage_task(Path(train_batch.submission_root), 1, 0), 0
            )
            runs, bindings = upstream_bindings_from_train_batch(
                spec, Path(train_batch.submission_root)
            )
            eval_batch = compile_stage_batch_for_kind(
                spec,
                kind="eval",
                runs=runs,
                input_bindings_by_run=bindings,
                source_ref=f"train_batch:{train_batch.submission_root}",
            )
            materialize_stage_batch_for_test(eval_batch, spec_snapshot=spec.raw)
            eval_root = Path(eval_batch.submission_root)

            roots = tuple(iter_lineage_source_roots(eval_root))
            record = find_bound_input(
                eval_root, run_id=runs[0].run_id, input_name="checkpoint"
            )

            self.assertIn(Path(train_batch.submission_root).resolve(), roots)
            assert record is not None
            self.assertEqual(record.stage_name, "eval")
            self.assertEqual(
                record.resolution.producer_root,
                str(Path(train_batch.submission_root).resolve()),
            )

    def test_pipeline_lineage_lists_stage_batch_roots(self) -> None:
        from slurmforge.lineage.query import iter_lineage_source_roots

        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))
            plan = compile_train_eval_pipeline_plan(spec)
            materialize_train_eval_pipeline_for_test(plan, spec_snapshot=spec.raw)
            roots = set(iter_lineage_source_roots(Path(plan.root_dir)))

            self.assertEqual(
                roots,
                {
                    Path(batch.submission_root).resolve()
                    for batch in plan.stage_batches.values()
                },
            )

    def test_lineage_index_reader_returns_typed_records_and_rejects_invalid(
        self,
    ) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.contracts import InputResolution
        from slurmforge.io import to_jsonable
        from slurmforge.lineage.records import (
            StageBatchLineageRecord,
            TrainEvalPipelineLineageRecord,
            lineage_index_from_dict,
        )

        pipeline_payload = {
            "schema_version": 1,
            "kind": "train_eval_pipeline_lineage",
            "root": "/tmp/pipeline",
            "pipeline_id": "pipe",
            "pipeline_kind": "train_eval",
            "stage_order": ["train", "eval"],
            "run_ids": ["run_1"],
            "spec_snapshot_digest": "digest",
            "stage_batches": {
                "train": {
                    "batch_id": "batch",
                    "root": "/tmp/train",
                    "stage_name": "train",
                    "source_ref": "source",
                }
            },
            "source_roots": ["/tmp/train"],
        }
        stage_payload = {
            "schema_version": 1,
            "kind": "stage_batch_lineage",
            "root": "/tmp/eval",
            "batch_id": "batch",
            "stage_name": "eval",
            "source_ref": "source",
            "spec_snapshot_digest": "digest",
            "run_ids": ["run_1"],
            "stage_instances": [
                {
                    "stage_instance_id": "run_1.eval",
                    "run_id": "run_1",
                    "stage_name": "eval",
                    "run_dir_rel": "runs/run_1",
                }
            ],
            "source_roots": ["/tmp/train"],
            "input_sources": [
                {
                    "stage_instance_id": "run_1.eval",
                    "run_id": "run_1",
                    "stage_name": "eval",
                    "input_name": "checkpoint",
                    "source": {
                        "schema_version": 1,
                        "kind": "upstream_output",
                        "stage": "train",
                        "output": "checkpoint",
                        "path": "",
                    },
                    "expects": "path",
                    "resolved": {
                        "schema_version": 1,
                        "kind": "path",
                        "path": "/tmp/checkpoint.pt",
                        "value": None,
                        "digest": "abc",
                        "source_output_kind": "file",
                        "producer_stage_instance_id": "run_1.train",
                    },
                    "resolution": to_jsonable(
                        InputResolution(kind="upstream_output", state="resolved")
                    ),
                }
            ],
        }

        pipeline_record = lineage_index_from_dict(pipeline_payload)
        stage_record = lineage_index_from_dict(stage_payload)

        self.assertIsInstance(pipeline_record, TrainEvalPipelineLineageRecord)
        assert isinstance(pipeline_record, TrainEvalPipelineLineageRecord)
        self.assertEqual(pipeline_record.stage_batches["train"].stage_name, "train")
        self.assertIsInstance(stage_record, StageBatchLineageRecord)
        assert isinstance(stage_record, StageBatchLineageRecord)
        self.assertEqual(stage_record.input_sources[0].source.output, "checkpoint")
        self.assertEqual(stage_record.input_sources[0].resolved.path, "/tmp/checkpoint.pt")

        cases = {
            "schema_version": {**pipeline_payload, "schema_version": "1"},
            "source_roots": {**pipeline_payload, "source_roots": ("/tmp/train",)},
            "root": {
                **pipeline_payload,
                "stage_batches": {
                    "train": {
                        key: value
                        for key, value in pipeline_payload["stage_batches"][
                            "train"
                        ].items()
                        if key != "root"
                    }
                },
            },
            "resolved": {
                **stage_payload,
                "input_sources": [
                    {
                        key: value
                        for key, value in stage_payload["input_sources"][0].items()
                        if key != "resolved"
                    }
                ],
            },
            "does not match": {
                **pipeline_payload,
                "stage_batches": {
                    "renamed": pipeline_payload["stage_batches"]["train"]
                },
            },
            "source": {
                **stage_payload,
                "input_sources": [
                    {
                        **stage_payload["input_sources"][0],
                        "source": "bad",
                    }
                ],
            },
        }

        for expected, invalid in cases.items():
            with self.subTest(field=expected):
                with self.assertRaisesRegex(RecordContractError, expected):
                    lineage_index_from_dict(invalid)
