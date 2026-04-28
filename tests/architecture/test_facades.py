from __future__ import annotations

from tests.support.case import StageBatchSystemTestCase


class FacadeTests(StageBatchSystemTestCase):
    def test_public_facades_do_not_export_internal_helpers(self) -> None:
        import slurmforge.notifications as notifications
        import slurmforge.plans as plans
        import slurmforge.planner as planner
        import slurmforge.resolver as resolver
        import slurmforge.spec as spec
        import slurmforge.starter as starter
        import slurmforge.submission as submission

        spec_internal = {
            "FileOutputDiscoveryRule",
            "OutputDiscoveryRule",
            "iter_run_overrides",
            "load_raw_config",
            "normalize_run_path",
            "parse_stage_output_contract",
            "run_id_for",
            "StageOutputContract",
            "StageOutputSpec",
            "expand_run_definitions",
            "stage_name_for_kind",
            "stage_source_input_name",
        }
        plans_internal = {
            "ArtifactStorePlan",
            "BeforeStepPlan",
            "BudgetDependencyPlan",
            "BudgetGroupPlan",
            "BudgetPlan",
            "BudgetWaveGroupPlan",
            "BudgetWavePlan",
            "ControlResourcesPlan",
            "EmailNotificationPlan",
            "EntryPlan",
            "EnvironmentPlan",
            "EnvironmentSourcePlan",
            "ExecutorRuntimePlan",
            "FinalizerPlan",
            "GroupPlan",
            "LauncherPlan",
            "NotificationPlan",
            "OutputRef",
            "PriorBatchLineage",
            "PythonRuntimePlan",
            "RendezvousPlan",
            "ResourcePlan",
            "RunDefinition",
            "RuntimePlan",
            "SelectedStageRun",
            "SourcedStageBatchPlan",
            "StageBatchPlan",
            "StageBatchSource",
            "StageInstancePlan",
            "StageOutputsRecord",
            "TRAIN_EVAL_PIPELINE_KIND",
            "TrainEvalControllerPlan",
            "TrainEvalPipelinePlan",
            "UserRuntimePlan",
            "group_plan_from_dict",
            "output_ref_from_dict",
            "prior_batch_lineage_to_dict",
            "stage_batch_plan_from_dict",
            "stage_instance_plan_from_dict",
            "stage_outputs_record_from_dict",
            "train_eval_pipeline_plan_from_dict",
        }
        submission_internal = {
            "GroupSubmissionRecord",
            "SubmissionLedger",
            "SubmitGeneration",
            "create_submit_generation",
            "dependency_for",
            "finalizer_dependency_group_ids",
            "load_ready_prepared_submission",
            "mark_stage_batch_queued",
            "read_submission_ledger",
            "write_submission_ledger",
        }
        planner_internal = set()
        resolver_internal = {
            "default_stage_input_bindings",
            "input_inject",
            "resolve_stage_inputs_from_prior_source",
            "stage_source_input_name",
        }
        notifications_internal = {
            "append_notification_event",
            "notification_record_path",
            "read_notification_record",
            "write_notification_record",
        }
        starter_internal = {
            "DEFAULT_CHECKPOINT_PATH",
            "DEFAULT_OUTPUT",
            "TEMPLATE_EVAL_CHECKPOINT",
            "TEMPLATE_TRAIN_EVAL",
            "TEMPLATE_TRAIN_ONLY",
            "existing_starter_files",
        }
        for name in spec_internal:
            self.assertFalse(hasattr(spec, name), name)
            self.assertNotIn(name, spec.__all__)
        for name in plans_internal:
            self.assertFalse(hasattr(plans, name), name)
            self.assertNotIn(name, plans.__all__)
        for name in submission_internal:
            self.assertFalse(hasattr(submission, name), name)
            self.assertNotIn(name, submission.__all__)
        for name in planner_internal:
            self.assertFalse(hasattr(planner, name), name)
            self.assertNotIn(name, planner.__all__)
        for name in resolver_internal:
            self.assertFalse(hasattr(resolver, name), name)
            self.assertNotIn(name, resolver.__all__)
        for name in notifications_internal:
            self.assertFalse(hasattr(notifications, name), name)
            self.assertNotIn(name, notifications.__all__)
        for name in starter_internal:
            self.assertFalse(hasattr(starter, name), name)
            self.assertNotIn(name, starter.__all__)
