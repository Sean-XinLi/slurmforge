from __future__ import annotations

from pathlib import Path

from tests.support.case import StageBatchSystemTestCase


EXPECTED_SUPPORTED_FIELDS = (
    "project",
    "experiment",
    "storage.root",
    "artifact_store.strategy",
    "artifact_store.fallback_strategy",
    "artifact_store.verify_digest",
    "artifact_store.fail_on_verify_error",
    "hardware.gpu_types",
    "hardware.gpu_types.*.memory_gb",
    "hardware.gpu_types.*.usable_memory_fraction",
    "hardware.gpu_types.*.max_gpus_per_node",
    "hardware.gpu_types.*.slurm.constraint",
    "environments.*.modules",
    "environments.*.source",
    "environments.*.source[].path",
    "environments.*.source[].args",
    "environments.*.env",
    "runtime.executor.python.bin",
    "runtime.executor.python.min_version",
    "runtime.executor.module",
    "runtime.user.*.python.bin",
    "runtime.user.*.python.min_version",
    "runtime.user.*.env",
    "sizing.gpu.defaults",
    "sizing.gpu.defaults.safety_factor",
    "sizing.gpu.defaults.round_to",
    "notifications.email.enabled",
    "notifications.email.to",
    "notifications.email.on",
    "notifications.email.mode",
    "notifications.email.from",
    "notifications.email.sendmail",
    "notifications.email.subject_prefix",
    "runs.type",
    "runs.axes",
    "runs.cases",
    "runs.cases[].name",
    "runs.cases[].set",
    "runs.cases[].axes",
    "dispatch.max_available_gpus",
    "dispatch.overflow_policy",
    "orchestration.controller",
    "orchestration.controller.partition",
    "orchestration.controller.cpus",
    "orchestration.controller.mem",
    "orchestration.controller.time_limit",
    "orchestration.controller.environment",
    "stages.train.entry.script",
    "stages.eval.entry.script",
    "stages.*.kind",
    "stages.*.enabled",
    "stages.*.environment",
    "stages.*.runtime",
    "stages.*.entry.type",
    "stages.*.entry.script",
    "stages.*.entry.command",
    "stages.*.entry.workdir",
    "stages.*.entry.args",
    "stages.*.before",
    "stages.*.before[].name",
    "stages.*.before[].run",
    "stages.*.resources",
    "stages.*.resources.partition",
    "stages.*.resources.account",
    "stages.*.resources.qos",
    "stages.*.resources.time_limit",
    "stages.*.resources.gpu_type",
    "stages.*.resources.nodes",
    "stages.*.resources.gpus_per_node",
    "stages.*.resources.cpus_per_task",
    "stages.*.resources.mem",
    "stages.*.resources.constraint",
    "stages.*.resources.extra_sbatch_args",
    "stages.*.launcher.type",
    "stages.*.launcher.mode",
    "stages.*.launcher.nnodes",
    "stages.*.launcher.nproc_per_node",
    "stages.*.launcher.rendezvous",
    "stages.*.launcher.rendezvous.backend",
    "stages.*.launcher.rendezvous.endpoint",
    "stages.*.launcher.rendezvous.port",
    "stages.*.launcher.master_port",
    "stages.*.launcher.args",
    "stages.*.launcher.srun_args",
    "stages.*.gpu_sizing",
    "stages.*.gpu_sizing.estimator",
    "stages.*.gpu_sizing.target_memory_gb",
    "stages.*.gpu_sizing.min_gpus_per_job",
    "stages.*.gpu_sizing.max_gpus_per_job",
    "stages.*.gpu_sizing.safety_factor",
    "stages.*.gpu_sizing.round_to",
    "stages.*.depends_on",
    "stages.train.outputs.checkpoint",
    "stages.eval.inputs.checkpoint",
    "stages.eval.outputs.accuracy",
    "stages.*.inputs.*.source.kind",
    "stages.*.inputs.*.source.stage",
    "stages.*.inputs.*.source.output",
    "stages.*.inputs.*.source.path",
    "stages.*.inputs.*.expects",
    "stages.*.inputs.*.required",
    "stages.*.inputs.*.inject.mode",
    "stages.*.inputs.*.inject.flag",
    "stages.*.inputs.*.inject.env",
    "stages.*.outputs.*.kind",
    "stages.*.outputs.*.required",
    "stages.*.outputs.*.file",
    "stages.*.outputs.*.json_path",
    "stages.*.outputs.*.discover.globs",
    "stages.*.outputs.*.discover.select",
)


class ConfigSchemaCoverageTests(StageBatchSystemTestCase):
    def test_schema_covers_current_user_visible_config_surface(self) -> None:
        from slurmforge.config_schema import all_fields

        actual = {field.path for field in all_fields()}
        missing = sorted(set(EXPECTED_SUPPORTED_FIELDS) - actual)
        self.assertEqual(missing, [])

    def test_schema_has_no_duplicate_or_legacy_paths(self) -> None:
        from slurmforge.config_schema import all_fields

        paths = [field.path for field in all_fields()]
        duplicates = sorted({path for path in paths if paths.count(path) > 1})
        self.assertEqual(duplicates, [])
        self.assertNotIn("orchestration.controller.resources", paths)
        self.assertNotIn("runtime.user.default.python.bin", paths)
        self.assertNotIn("environments.default.modules", paths)

    def test_generated_config_reference_lists_every_schema_field(self) -> None:
        from slurmforge.config_schema import all_fields

        config_doc = Path("docs/config.md").read_text(encoding="utf-8")
        missing = [
            field.path for field in all_fields() if f"`{field.path}`" not in config_doc
        ]
        self.assertEqual(missing, [])
