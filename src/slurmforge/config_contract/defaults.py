from __future__ import annotations

from . import default_values as _default_values
from .registry import default_for

AUTO_VALUE = _default_values.AUTO_VALUE
DEFAULT_CONFIG_FILENAME = _default_values.DEFAULT_CONFIG_FILENAME
DEFAULT_OUTPUT_DIR = _default_values.DEFAULT_OUTPUT_DIR
DEFAULT_PARTITION = _default_values.DEFAULT_PARTITION
DEFAULT_ENVIRONMENT_NAME = _default_values.DEFAULT_ENVIRONMENT_NAME
DEFAULT_RUNTIME_NAME = _default_values.DEFAULT_RUNTIME_NAME

DEFAULT_PROJECT = default_for("project")
DEFAULT_EXPERIMENT = default_for("experiment")
DEFAULT_STORAGE_ROOT = default_for("storage.root")

DEFAULT_PYTHON_BIN = default_for("runtime.executor.python.bin")
DEFAULT_PYTHON_MIN_VERSION = default_for("runtime.executor.python.min_version")
DEFAULT_EXECUTOR_MODULE = default_for("runtime.executor.module")
DEFAULT_TRAIN_SCRIPT = default_for("stages.train.entry.script")
DEFAULT_EVAL_SCRIPT = default_for("stages.eval.entry.script")
DEFAULT_CHECKPOINT_PATH = default_for("stages.*.inputs.*.source.path")

DEFAULT_RUN_TYPE = default_for("runs.type")

DEFAULT_ARTIFACT_STORE_STRATEGY = default_for("artifact_store.strategy")
DEFAULT_ARTIFACT_STORE_FALLBACK_STRATEGY = default_for(
    "artifact_store.fallback_strategy"
)
DEFAULT_ARTIFACT_STORE_VERIFY_DIGEST = default_for("artifact_store.verify_digest")
DEFAULT_ARTIFACT_STORE_FAIL_ON_VERIFY_ERROR = default_for(
    "artifact_store.fail_on_verify_error"
)

DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS = default_for("dispatch.max_available_gpus")
DEFAULT_DISPATCH_OVERFLOW_POLICY = default_for("dispatch.overflow_policy")

DEFAULT_CONTROL_PARTITION = default_for("orchestration.control.partition")
DEFAULT_CONTROL_CPUS = default_for("orchestration.control.cpus")
DEFAULT_CONTROL_MEM = default_for("orchestration.control.mem")
DEFAULT_CONTROL_TIME_LIMIT = default_for("orchestration.control.time_limit")
DEFAULT_CONTROL_ENVIRONMENT = default_for("orchestration.control.environment")

DEFAULT_STAGE_ENABLED = default_for("stages.*.enabled")
DEFAULT_STAGE_ENTRY_TYPE = default_for("stages.*.entry.type")
DEFAULT_STAGE_ENTRY_WORKDIR = default_for("stages.*.entry.workdir")
DEFAULT_STAGE_LAUNCHER_TYPE = default_for("stages.*.launcher.type")
DEFAULT_STAGE_ENVIRONMENT = default_for("stages.*.environment")
DEFAULT_STAGE_RUNTIME = default_for("stages.*.runtime")

DEFAULT_STAGE_RESOURCES_PARTITION = default_for("stages.*.resources.partition")
DEFAULT_STAGE_RESOURCES_NODES = default_for("stages.*.resources.nodes")
DEFAULT_STAGE_RESOURCES_GPUS_PER_NODE = default_for("stages.*.resources.gpus_per_node")
DEFAULT_STAGE_RESOURCES_CPUS_PER_TASK = default_for("stages.*.resources.cpus_per_task")
DEFAULT_STAGE_RESOURCES_TIME_LIMIT = default_for("stages.*.resources.time_limit")

DEFAULT_LAUNCHER_MODE = default_for("stages.*.launcher.mode")
DEFAULT_LAUNCHER_NNODES = default_for("stages.*.launcher.nnodes")
DEFAULT_LAUNCHER_NPROC_PER_NODE = default_for("stages.*.launcher.nproc_per_node")
DEFAULT_RENDEZVOUS_BACKEND = default_for("stages.*.launcher.rendezvous.backend")
DEFAULT_RENDEZVOUS_ENDPOINT = default_for("stages.*.launcher.rendezvous.endpoint")
DEFAULT_RENDEZVOUS_PORT = default_for("stages.*.launcher.rendezvous.port")

DEFAULT_GPU_SIZING_MIN_GPUS_PER_JOB = default_for(
    "stages.*.gpu_sizing.min_gpus_per_job"
)
DEFAULT_GPU_SIZING_SAFETY_FACTOR = default_for("sizing.gpu.defaults.safety_factor")
DEFAULT_GPU_SIZING_ROUND_TO = default_for("sizing.gpu.defaults.round_to")

DEFAULT_INPUT_EXPECTS = default_for("stages.*.inputs.*.expects")
DEFAULT_INPUT_INJECT_MODE = default_for("stages.*.inputs.*.inject.mode")
DEFAULT_OUTPUT_REQUIRED = default_for("stages.*.outputs.*.required")
DEFAULT_OUTPUT_JSON_PATH = "$"
DEFAULT_OUTPUT_DISCOVER_SELECT = default_for("stages.*.outputs.*.discover.select")

DEFAULT_EMAIL_ENABLED = default_for("notifications.email.enabled")
DEFAULT_EMAIL_EVENTS = default_for("notifications.email.on")
DEFAULT_EMAIL_MODE = default_for("notifications.email.mode")
DEFAULT_EMAIL_FROM = default_for("notifications.email.from")
DEFAULT_EMAIL_SENDMAIL = default_for("notifications.email.sendmail")
DEFAULT_EMAIL_SUBJECT_PREFIX = default_for("notifications.email.subject_prefix")

__all__ = [
    name for name in globals() if name == "AUTO_VALUE" or name.startswith("DEFAULT_")
]
