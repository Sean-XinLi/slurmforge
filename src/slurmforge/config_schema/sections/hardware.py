from __future__ import annotations

from typing import Final

from ...defaults import ALL_STARTER_TEMPLATES
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="hardware.gpu_types",
        title="GPU type catalog",
        short_help="Named GPU profiles available to workflow stages.",
        when_to_change="Define this when GPU sizing or stage placement should reason about accelerator memory and Slurm constraints.",
        section="Hardware",
        level="advanced",
        value_type="mapping",
        templates=ALL_STARTER_TEMPLATES,
        default="{}",
    ),
    ConfigField(
        path="hardware.gpu_types.*.memory_gb",
        title="GPU memory",
        short_help="Nominal memory in GB for one GPU of this type.",
        when_to_change="Set this to let GPU sizing estimate the number of GPUs needed for a stage.",
        section="Hardware",
        level="advanced",
        value_type="float",
        templates=ALL_STARTER_TEMPLATES,
        default="0",
    ),
    ConfigField(
        path="hardware.gpu_types.*.usable_memory_fraction",
        title="Usable GPU memory fraction",
        short_help="Fraction of nominal GPU memory treated as available to jobs.",
        when_to_change="Lower this when framework overhead, fragmentation, or site policy means full GPU memory is not usable.",
        section="Hardware",
        level="advanced",
        value_type="float",
        templates=ALL_STARTER_TEMPLATES,
        default="0",
    ),
    ConfigField(
        path="hardware.gpu_types.*.max_gpus_per_node",
        title="Max GPUs per node",
        short_help="Maximum number of GPUs available per node for this GPU type.",
        when_to_change="Set this when automatic GPU sizing must respect per-node accelerator capacity.",
        section="Hardware",
        level="advanced",
        value_type="integer",
        templates=ALL_STARTER_TEMPLATES,
        default="null",
    ),
    ConfigField(
        path="hardware.gpu_types.*.slurm.constraint",
        title="GPU Slurm constraint",
        short_help="Slurm constraint associated with this GPU type.",
        when_to_change="Set this when choosing a gpu_type should imply a cluster-specific node constraint.",
        section="Hardware",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default="null",
    ),
)
