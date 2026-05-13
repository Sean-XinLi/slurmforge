from __future__ import annotations

import json
import tempfile
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


class ControlSubmissionLedgerTests(StageBatchSystemTestCase):
    def test_malformed_control_submission_ledger_is_rejected(self) -> None:
        from slurmforge.control.control_submission_ledger import (
            read_control_submission_ledger,
        )
        from slurmforge.errors import RecordContractError

        with tempfile.TemporaryDirectory() as tmp:
            pipeline_root = Path(tmp)
            control_dir = pipeline_root / "control"
            control_dir.mkdir()
            ledger_path = control_dir / "control_submissions.json"

            valid_record = {
                "key": "dispatch_catchup_gate:target",
                "kind": "dispatch_catchup_gate",
                "target_kind": "dispatch",
                "target_id": "target",
                "state": "submitted",
                "sbatch_paths": ["gate.sbatch"],
                "scheduler_job_ids": ["1001"],
                "barrier_job_ids": [],
                "dependency_job_ids": [],
                "reason": "",
                "started_at": "2026-01-01T00:00:00Z",
                "submitted_at": "2026-01-01T00:00:01Z",
                "failed_at": "",
            }
            cases = {
                "submissions_not_object": {
                    "schema_version": 1,
                    "submissions": [],
                },
                "payload_key_mismatch": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "key": "dispatch_catchup_gate:other",
                        }
                    },
                },
                "invalid_kind": {
                    "schema_version": 1,
                    "submissions": {
                        "bad:target": {
                            **valid_record,
                            "key": "bad:target",
                            "kind": "bad",
                        }
                    },
                },
                "invalid_state": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "state": "done",
                        }
                    },
                },
                "submitted_without_scheduler_ids": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "scheduler_job_ids": [],
                        }
                    },
                },
                "failed_without_reason": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "state": "failed",
                            "scheduler_job_ids": [],
                            "reason": "",
                        }
                    },
                },
                "empty_sbatch_paths": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "sbatch_paths": [],
                        }
                    },
                },
                "empty_sbatch_path_item": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "sbatch_paths": [""],
                        }
                    },
                },
                "empty_scheduler_id_item": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "scheduler_job_ids": [""],
                        }
                    },
                },
                "empty_barrier_id_item": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "barrier_job_ids": [""],
                        }
                    },
                },
                "empty_dependency_id_item": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "dependency_job_ids": [""],
                        }
                    },
                },
                "expected_key_mismatch": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:wrong": {
                            **valid_record,
                            "key": "dispatch_catchup_gate:wrong",
                        }
                    },
                },
                "scheduler_ids_not_array": {
                    "schema_version": 1,
                    "submissions": {
                        "dispatch_catchup_gate:target": {
                            **valid_record,
                            "scheduler_job_ids": "1001",
                        }
                    },
                },
            }

            for name, payload in cases.items():
                with self.subTest(name=name):
                    ledger_path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(RecordContractError):
                        read_control_submission_ledger(pipeline_root)

    def test_workflow_status_rejects_unknown_control_job_kind(self) -> None:
        from slurmforge.errors import RecordContractError
        from slurmforge.io import SchemaVersion
        from slurmforge.storage.workflow_status_records import workflow_status_from_dict

        payload = {
            "schema_version": SchemaVersion.WORKFLOW_STATUS,
            "state": "streaming",
            "updated_at": "2026-01-01T00:00:00Z",
            "reason": "",
            "control_jobs": {
                "unknown:target": {
                    "key": "unknown:target",
                    "kind": "unknown",
                    "target_kind": "dispatch",
                    "target_id": "target",
                    "state": "submitted",
                    "sbatch_paths": ["gate.sbatch"],
                    "scheduler_job_ids": ["1001"],
                    "barrier_job_ids": [],
                    "dependency_job_ids": [],
                    "reason": "",
                    "started_at": "2026-01-01T00:00:00Z",
                    "submitted_at": "2026-01-01T00:00:01Z",
                    "failed_at": "",
                }
            },
            "stage_jobs": {},
        }

        with self.assertRaises(RecordContractError):
            workflow_status_from_dict(payload)

    def test_control_job_record_rejects_non_string_tuple_items(self) -> None:
        from slurmforge.control_job_contract import (
            ControlJobRecord,
            validate_control_job_record,
        )
        from slurmforge.errors import RecordContractError

        base = {
            "key": "dispatch_catchup_gate:target",
            "kind": "dispatch_catchup_gate",
            "target_kind": "dispatch",
            "target_id": "target",
            "state": "submitted",
            "sbatch_paths": ("gate.sbatch",),
            "scheduler_job_ids": ("1001",),
            "barrier_job_ids": (),
            "dependency_job_ids": (),
        }
        cases = {
            "list_sbatch_field": {"sbatch_paths": ["gate.sbatch"]},
            "list_scheduler_field": {"scheduler_job_ids": ["1001"]},
            "path_sbatch_item": {"sbatch_paths": (Path("gate.sbatch"),)},
            "integer_scheduler_item": {"scheduler_job_ids": (1001,)},
            "integer_barrier_item": {"barrier_job_ids": (1002,)},
            "integer_dependency_item": {"dependency_job_ids": (1003,)},
        }

        for name, override in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(RecordContractError):
                    validate_control_job_record(
                        ControlJobRecord(**{**base, **override}),
                        label="control submission",
                    )
