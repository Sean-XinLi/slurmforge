"""
Catalog of starter project templates organized by (template_type, profile).

Template types  — the user's training invocation style:
  script    → train.py with CLI args
  command   → complete shell command
  registry  → shared team model registry
  adapter   → interface bridge script

Profiles  — cluster complexity:
  starter   → single GPU, minimal config (default)
  hpc       → multi-GPU, sweep, eval, full cluster config
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .errors import ConfigContractError


ResourceKind = Literal["directory", "example", "template"]

TEMPLATE_TYPES: tuple[str, ...] = ("script", "command", "registry", "adapter")
PROFILES: tuple[str, ...] = ("starter", "hpc")


@dataclass(frozen=True)
class StarterResource:
    relative_path: str
    kind: ResourceKind
    source_name: str | None = None
    replacements: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class StarterSpec:
    template_type: str
    profile: str
    example_name: str           # YAML file in examples/ (without .yaml)
    post_init_guidance: str
    scaffold_resources: tuple[StarterResource, ...] = ()

    @property
    def _readme_replacements(self) -> tuple[tuple[str, str], ...]:
        return (
            ("__TEMPLATE_TYPE__", self.template_type),
            ("__PROFILE__", self.profile),
            ("__POST_INIT_GUIDANCE__", self.post_init_guidance),
        )

    @property
    def resources(self) -> tuple[StarterResource, ...]:
        return (
            StarterResource(
                "experiment.yaml",
                kind="example",
                source_name=self.example_name,
            ),
            *self.scaffold_resources,
            StarterResource(
                "README.md",
                kind="template",
                source_name="README.md.tmpl",
                replacements=self._readme_replacements,
            ),
            StarterResource("runs/", kind="directory"),
        )


# ---------------------------------------------------------------------------
# Template catalog — (template_type, profile) → StarterSpec
# ---------------------------------------------------------------------------

_CATALOG: dict[tuple[str, str], StarterSpec] = {
    ("script", "starter"): StarterSpec(
        template_type="script",
        profile="starter",
        example_name="script_starter",
        post_init_guidance=(
            "Fill in the 4 ~ fields in experiment.yaml (cluster.partition, "
            "cluster.account, cluster.time_limit, model.script), then run "
            "'sforge validate' and 'sforge generate'."
        ),
        scaffold_resources=(
            StarterResource("train.py", kind="template", source_name="model_cli_train.py.tmpl"),
        ),
    ),
    ("script", "hpc"): StarterSpec(
        template_type="script",
        profile="hpc",
        example_name="script_hpc",
        post_init_guidance=(
            "Fill in the required ~ fields (cluster.partition, cluster.account, "
            "cluster.time_limit, model.script). Adjust gpus_per_node and "
            "launcher.distributed.nproc_per_node to match. "
            "Enable sweep or eval when ready."
        ),
        scaffold_resources=(
            StarterResource("train.py", kind="template", source_name="hpc_train.py.tmpl"),
            StarterResource("eval.py", kind="template", source_name="eval.py.tmpl"),
        ),
    ),
    ("command", "starter"): StarterSpec(
        template_type="command",
        profile="starter",
        example_name="command_starter",
        post_init_guidance=(
            "Fill in the 4 ~ fields: cluster.partition, cluster.account, "
            "cluster.time_limit, and run.command (your full training command). "
            "No training script is generated — point run.command at your existing script."
        ),
        scaffold_resources=(
            StarterResource("train.py", kind="template", source_name="command_train.py.tmpl"),
        ),
    ),
    ("command", "hpc"): StarterSpec(
        template_type="command",
        profile="hpc",
        example_name="command_hpc",
        post_init_guidance=(
            "Fill in cluster.partition, cluster.account, cluster.time_limit, "
            "and run.command. Enable sweep if you want to run a hyperparameter grid."
        ),
        scaffold_resources=(
            StarterResource("train.py", kind="template", source_name="command_train.py.tmpl"),
        ),
    ),
    ("registry", "starter"): StarterSpec(
        template_type="registry",
        profile="starter",
        example_name="registry_starter",
        post_init_guidance=(
            "Fill in cluster.partition, cluster.account, cluster.time_limit, and "
            "model_registry.registry_file. Edit models.yaml to register your model. "
            "Set model.name to match a key in that registry."
        ),
        scaffold_resources=(
            StarterResource("models.yaml", kind="example", source_name="model_registry"),
            StarterResource("train.py", kind="template", source_name="model_cli_train.py.tmpl"),
        ),
    ),
    ("registry", "hpc"): StarterSpec(
        template_type="registry",
        profile="hpc",
        example_name="registry_hpc",
        post_init_guidance=(
            "Fill in cluster.partition, cluster.account, cluster.time_limit, and "
            "model_registry.registry_file. Expand models.yaml with your team's "
            "model entrypoints. Enable sweep or eval when ready."
        ),
        scaffold_resources=(
            StarterResource("models.yaml", kind="example", source_name="model_registry"),
            StarterResource("train.py", kind="template", source_name="hpc_train.py.tmpl"),
            StarterResource("eval.py", kind="template", source_name="eval.py.tmpl"),
        ),
    ),
    ("adapter", "starter"): StarterSpec(
        template_type="adapter",
        profile="starter",
        example_name="adapter_starter",
        post_init_guidance=(
            "Fill in cluster.partition, cluster.account, cluster.time_limit, and "
            "run.adapter.script. Replace the generated train_adapter.py with your "
            "real bridge logic."
        ),
        scaffold_resources=(
            StarterResource("train_adapter.py", kind="template", source_name="adapter_train.py.tmpl"),
        ),
    ),
    ("adapter", "hpc"): StarterSpec(
        template_type="adapter",
        profile="hpc",
        example_name="adapter_hpc",
        post_init_guidance=(
            "Fill in cluster.partition, cluster.account, cluster.time_limit, and "
            "run.adapter.script. Replace the generated train_adapter.py with your "
            "bridge logic. Enable eval or sweep if needed."
        ),
        scaffold_resources=(
            StarterResource("train_adapter.py", kind="template", source_name="adapter_train.py.tmpl"),
            StarterResource("eval.py", kind="template", source_name="eval.py.tmpl"),
        ),
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_starter_spec(template_type: str, profile: str) -> StarterSpec:
    key = (str(template_type).strip(), str(profile).strip())
    if key not in _CATALOG:
        valid = ", ".join(f"{t}+{p}" for t, p in sorted(_CATALOG))
        raise ConfigContractError(
            f"Unknown template combination ({template_type!r}, {profile!r}). "
            f"Valid combinations: {valid}"
        )
    return _CATALOG[key]


def list_starter_specs() -> list[StarterSpec]:
    return list(_CATALOG.values())
