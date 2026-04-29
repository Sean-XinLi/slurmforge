from __future__ import annotations

from typing import Final

from ...config_contract.defaults import (
    DEFAULT_CONTROLLER_CPUS,
    DEFAULT_CONTROLLER_ENVIRONMENT,
    DEFAULT_CONTROLLER_MEM,
    DEFAULT_CONTROLLER_TIME_LIMIT,
    DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS,
    DEFAULT_DISPATCH_OVERFLOW_POLICY,
    DEFAULT_PARTITION,
)
from ...config_contract.options import DISPATCH_POLICIES
from ...config_contract.workflows import ALL_STARTER_TEMPLATES, TEMPLATE_TRAIN_EVAL
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="dispatch.max_available_gpus",
        title="Global GPU budget",
        short_help="GPU budget used to serialize Slurm array groups when a plan exceeds available GPUs.",
        when_to_change="Set this to the practical cluster budget you want this workflow to consume.",
        section="Dispatch",
        level="intermediate",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_DISPATCH_MAX_AVAILABLE_GPUS,
    ),
    ConfigField(
        path="dispatch.overflow_policy",
        title="GPU overflow policy",
        short_help="Controls planner behavior when run groups exceed the declared GPU budget.",
        when_to_change="Use error for strict admission control, or best_effort when the scheduler should absorb overflow.",
        section="Dispatch",
        level="intermediate",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_DISPATCH_OVERFLOW_POLICY,
        options=DISPATCH_POLICIES,
    ),
    ConfigField(
        path="orchestration.controller",
        title="Controller resources",
        short_help="Slurm resources for the lightweight workflow controller job.",
        when_to_change="Change this when the controller needs a different partition, time limit, or environment.",
        section="Dispatch",
        level="advanced",
        templates=(TEMPLATE_TRAIN_EVAL,),
        default_display="partition=gpu, cpus=1, mem=2G, time_limit=01:00:00",
    ),
    ConfigField(
        path="orchestration.controller.partition",
        title="Controller partition",
        short_help="Slurm partition for the lightweight train/eval controller job.",
        when_to_change="Change this when controller jobs need a different queue from stage jobs.",
        section="Dispatch",
        level="advanced",
        templates=(TEMPLATE_TRAIN_EVAL,),
        default_display=DEFAULT_PARTITION,
    ),
    ConfigField(
        path="orchestration.controller.cpus",
        title="Controller CPU count",
        short_help="CPU count requested by the lightweight workflow controller job.",
        when_to_change="Increase this only if controller scheduling or planning overhead requires it.",
        section="Dispatch",
        level="advanced",
        value_type="integer",
        templates=(TEMPLATE_TRAIN_EVAL,),
        default_value=DEFAULT_CONTROLLER_CPUS,
    ),
    ConfigField(
        path="orchestration.controller.mem",
        title="Controller memory",
        short_help="Memory requested by the lightweight workflow controller job.",
        when_to_change="Increase this when controller planning or notification work needs more memory.",
        section="Dispatch",
        level="advanced",
        templates=(TEMPLATE_TRAIN_EVAL,),
        default_value=DEFAULT_CONTROLLER_MEM,
    ),
    ConfigField(
        path="orchestration.controller.time_limit",
        title="Controller time limit",
        short_help="Slurm time limit requested by the lightweight workflow controller job.",
        when_to_change="Increase this when the controller must wait for long train/eval pipeline stages.",
        section="Dispatch",
        level="advanced",
        value_type="duration",
        templates=(TEMPLATE_TRAIN_EVAL,),
        default_value=DEFAULT_CONTROLLER_TIME_LIMIT,
    ),
    ConfigField(
        path="orchestration.controller.environment",
        title="Controller environment",
        short_help="Environment profile loaded before the lightweight workflow controller runs.",
        when_to_change="Change this when controller jobs need cluster modules or setup scripts.",
        section="Dispatch",
        level="advanced",
        templates=(TEMPLATE_TRAIN_EVAL,),
        default_value=DEFAULT_CONTROLLER_ENVIRONMENT,
    ),
)
