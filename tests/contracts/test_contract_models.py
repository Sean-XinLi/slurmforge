from __future__ import annotations

import tempfile
from pathlib import Path

from tests.support.case import StageBatchSystemTestCase
from tests.support.public import write_demo_project


class ContractTests(StageBatchSystemTestCase):
    def test_input_binding_from_dict_round_trips_contract_fields(self) -> None:
        from slurmforge.contracts import input_binding_from_dict

        binding = input_binding_from_dict(
            {
                "schema_version": 1,
                "input_name": "checkpoint",
                "source": {
                    "schema_version": 1,
                    "kind": "external_path",
                    "stage": "",
                    "output": "",
                    "path": "/tmp/checkpoint.pt",
                },
                "expects": "path",
                "resolved": {
                    "schema_version": 1,
                    "kind": "path",
                    "path": "/tmp/checkpoint.pt",
                    "value": None,
                    "digest": "abc",
                    "source_output_kind": "",
                    "producer_stage_instance_id": "",
                },
                "inject": {"flag": "checkpoint", "mode": "path"},
                "resolution": {"kind": "external_path"},
            }
        )

        self.assertEqual(binding.input_name, "checkpoint")
        self.assertEqual(binding.source.kind, "external_path")
        self.assertEqual(binding.resolved.kind, "path")
        self.assertEqual(binding.inject["flag"], "checkpoint")

    def test_run_definition_lives_in_contracts_and_is_reexported_by_plans(self) -> None:
        from slurmforge.contracts import RunDefinition as ContractRunDefinition
        from slurmforge.contracts import RunDefinition as PlanRunDefinition

        run = ContractRunDefinition(
            run_id="run_1",
            run_index=1,
            run_overrides={"train.entry.args.lr": 0.01},
            spec_snapshot_digest="digest",
        )

        self.assertIs(ContractRunDefinition, PlanRunDefinition)
        self.assertEqual(run.run_id, "run_1")

    def test_stage_output_contract_parse_and_from_dict_share_type(self) -> None:
        from slurmforge.contracts import StageOutputContract
        from slurmforge.contracts.outputs import (
            stage_output_contract_from_dict,
        )
        from slurmforge.spec.stage_parse.outputs import parse_stage_output_config

        contract = parse_stage_output_config(
            {
                "checkpoint": {
                    "kind": "file",
                    "required": True,
                    "discover": {
                        "globs": ["checkpoints/*.pt"],
                        "select": "latest_step",
                    },
                }
            },
            stage_name="train",
        )
        restored = stage_output_contract_from_dict(
            {
                "schema_version": contract.schema_version,
                "outputs": {
                    name: {
                        "schema_version": output.schema_version,
                        "name": output.name,
                        "kind": output.kind,
                        "required": output.required,
                        "discover": {
                            "schema_version": output.discover.schema_version,
                            "globs": list(output.discover.globs),
                            "select": getattr(output.discover, "select", None),
                        },
                        "file": output.file,
                        "json_path": output.json_path,
                    }
                    for name, output in contract.outputs.items()
                },
            }
        )

        self.assertIsInstance(contract, StageOutputContract)
        self.assertEqual(restored.outputs["checkpoint"].discover.select, "latest_step")

    def test_input_binding_from_dict_rejects_incomplete_records(self) -> None:
        from slurmforge.contracts import input_binding_from_dict
        from slurmforge.errors import RecordContractError

        payload = {
            "schema_version": 1,
            "input_name": "checkpoint",
            "source": {
                "schema_version": 1,
                "kind": "external_path",
                "stage": "",
                "output": "",
                "path": "/tmp/checkpoint.pt",
            },
            "expects": "path",
            "resolved": {
                "schema_version": 1,
                "kind": "path",
                "path": "/tmp/checkpoint.pt",
                "value": None,
                "digest": "abc",
                "source_output_kind": "",
                "producer_stage_instance_id": "",
            },
            "inject": {"flag": "checkpoint", "mode": "path"},
            "resolution": {"kind": "external_path"},
        }

        for field in ("resolved", "inject", "resolution"):
            with self.subTest(field=field):
                invalid = dict(payload)
                del invalid[field]
                with self.assertRaisesRegex(RecordContractError, field):
                    input_binding_from_dict(invalid)

        invalid_source = {
            **payload,
            "source": {
                **payload["source"],
                "schema_version": "1",
            },
        }
        with self.assertRaisesRegex(RecordContractError, "schema_version"):
            input_binding_from_dict(invalid_source)

    def test_stage_output_contract_from_dict_rejects_incomplete_records(self) -> None:
        from slurmforge.contracts.outputs import stage_output_contract_from_dict
        from slurmforge.errors import RecordContractError

        payload = {
            "schema_version": 1,
            "outputs": {
                "checkpoint": {
                    "schema_version": 1,
                    "name": "checkpoint",
                    "kind": "file",
                    "required": True,
                    "discover": {
                        "schema_version": 1,
                        "globs": ["checkpoints/*.pt"],
                        "select": "latest_step",
                    },
                    "file": "",
                    "json_path": "",
                }
            },
        }

        invalid_schema = {**payload, "schema_version": "1"}
        with self.assertRaisesRegex(RecordContractError, "schema_version"):
            stage_output_contract_from_dict(invalid_schema)

        invalid_required = {
            **payload,
            "outputs": {
                "checkpoint": {
                    **payload["outputs"]["checkpoint"],
                    "required": "true",
                }
            },
        }
        with self.assertRaisesRegex(RecordContractError, "required"):
            stage_output_contract_from_dict(invalid_required)

        invalid_discover = {
            **payload,
            "outputs": {
                "checkpoint": {
                    **payload["outputs"]["checkpoint"],
                    "discover": {
                        **payload["outputs"]["checkpoint"]["discover"],
                        "globs": ("checkpoints/*.pt",),
                    },
                }
            },
        }
        with self.assertRaisesRegex(RecordContractError, "globs"):
            stage_output_contract_from_dict(invalid_discover)

    def test_stage_output_contract_rejects_latest_selector_alias(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.spec.stage_parse.outputs import parse_stage_output_config

        alias = "latest"
        with self.assertRaisesRegex(ConfigContractError, "latest_step, first, or last"):
            parse_stage_output_config(
                {
                    "checkpoint": {
                        "kind": "file",
                        "discover": {
                            "globs": ["checkpoints/*.pt"],
                            "select": alias,
                        },
                    }
                },
                stage_name="train",
            )

    def test_invalid_output_kind_is_config_contract_error(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.spec.stage_parse.outputs import parse_stage_output_config

        with self.assertRaisesRegex(
            ConfigContractError, "`stages.train.outputs.bad.kind`"
        ):
            parse_stage_output_config({"bad": {"kind": "unknown"}}, stage_name="train")

    def test_stage_output_config_rejects_persisted_record_schema_version(self) -> None:
        from slurmforge.errors import ConfigContractError
        from slurmforge.spec.stage_parse.outputs import parse_stage_output_config

        with self.assertRaisesRegex(ConfigContractError, "schema_version"):
            parse_stage_output_config(
                {
                    "checkpoint": {
                        "schema_version": 1,
                        "kind": "file",
                        "discover": {"globs": ["checkpoints/*.pt"]},
                    }
                },
                stage_name="train",
            )

    def test_notification_summary_input_lives_in_contracts(self) -> None:
        from slurmforge.contracts import (
            NotificationRunStatusInput,
            NotificationSummaryInput,
        )
        from slurmforge.notifications.models import (
            NotificationSummaryInput as NotificationModelSummaryInput,
        )

        payload = NotificationSummaryInput(
            event="stage_batch_finished",
            root_kind="stage_batch",
            root="/tmp/root",
            project="demo",
            experiment="baseline",
            object_id="batch",
            state="success",
            run_statuses=(NotificationRunStatusInput(run_id="run_1", state="success"),),
        )

        self.assertIs(NotificationSummaryInput, NotificationModelSummaryInput)
        self.assertEqual(payload.run_statuses[0].state, "success")

    def test_source_input_name_prefers_single_required_input(self) -> None:
        from slurmforge.spec import load_experiment_spec
        from slurmforge.spec.queries import stage_source_input_name

        with tempfile.TemporaryDirectory() as tmp:
            spec = load_experiment_spec(write_demo_project(Path(tmp)))

            self.assertEqual(
                stage_source_input_name(spec, stage_name="eval"), "checkpoint"
            )
