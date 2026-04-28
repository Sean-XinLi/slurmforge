from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class FieldOption:
    value: str
    description: str


@dataclass(frozen=True)
class FieldCatalog:
    path: str
    options: tuple[FieldOption, ...]


FIELD_CATALOGS: Final[tuple[FieldCatalog, ...]] = (
    FieldCatalog(
        path="artifact_store.fallback_strategy",
        options=(
            FieldOption("null", "Disable fallback handling."),
            FieldOption("copy", "Copy artifacts when the primary strategy fails."),
            FieldOption("hardlink", "Hardlink artifacts when supported."),
            FieldOption("symlink", "Symlink artifacts when supported."),
            FieldOption("register_only", "Record artifacts without copying files."),
        ),
    ),
    FieldCatalog(
        path="artifact_store.strategy",
        options=(
            FieldOption("copy", "Copy managed artifacts into the run store."),
            FieldOption("hardlink", "Hardlink managed artifacts into the run store."),
            FieldOption("symlink", "Symlink managed artifacts into the run store."),
            FieldOption("register_only", "Track artifact paths without copying files."),
        ),
    ),
    FieldCatalog(
        path="dispatch.overflow_policy",
        options=(
            FieldOption("serialize_groups", "Queue array groups within GPU budget."),
            FieldOption("error", "Reject plans that exceed the GPU budget."),
            FieldOption("best_effort", "Submit groups without strict serialization."),
        ),
    ),
    FieldCatalog(
        path="notifications.email.mode",
        options=(
            FieldOption("summary", "Send a compact workflow summary."),
        ),
    ),
    FieldCatalog(
        path="notifications.email.on",
        options=(
            FieldOption("batch_finished", "Send after a stage batch reaches terminal state."),
            FieldOption(
                "train_eval_pipeline_finished",
                "Send after a train/eval pipeline reaches terminal state.",
            ),
        ),
    ),
    FieldCatalog(
        path="runs.type",
        options=(
            FieldOption("single", "Plan one run."),
            FieldOption("grid", "Plan every combination from top-level axes."),
            FieldOption("cases", "Plan named hand-authored run variants."),
            FieldOption("matrix", "Plan named cases, each with its own grid."),
        ),
    ),
    FieldCatalog(
        path="stages.*.entry.type",
        options=(
            FieldOption("python_script", "Run a Python file."),
            FieldOption("command", "Run a shell command."),
        ),
    ),
    FieldCatalog(
        path="stages.*.inputs.*.expects",
        options=(
            FieldOption("path", "Inject a filesystem path."),
            FieldOption("manifest", "Inject a manifest payload."),
            FieldOption("value", "Inject a scalar value."),
        ),
    ),
    FieldCatalog(
        path="stages.*.inputs.*.inject.mode",
        options=(
            FieldOption("path", "Pass the resolved input path."),
            FieldOption("value", "Pass the resolved input value."),
            FieldOption("json", "Pass the resolved input encoded as JSON."),
        ),
    ),
    FieldCatalog(
        path="stages.*.inputs.*.source.kind",
        options=(
            FieldOption("upstream_output", "Read an output from a previous stage."),
            FieldOption("external_path", "Read an explicit user-provided path."),
        ),
    ),
    FieldCatalog(
        path="stages.*.launcher.mode",
        options=(
            FieldOption("single_node", "Launch on one node."),
            FieldOption("multi_node", "Launch across multiple nodes."),
        ),
    ),
    FieldCatalog(
        path="stages.*.launcher.type",
        options=(
            FieldOption("single", "Run one process directly."),
            FieldOption("python", "Launch through Python."),
            FieldOption("torchrun", "Launch distributed PyTorch."),
            FieldOption("srun", "Launch through Slurm srun."),
            FieldOption("mpirun", "Launch through MPI."),
            FieldOption("command", "Launch a raw command."),
        ),
    ),
    FieldCatalog(
        path="stages.*.outputs.*.discover.select",
        options=(
            FieldOption("latest_step", "Pick the path with the highest step number."),
            FieldOption("first", "Pick the first sorted match."),
            FieldOption("last", "Pick the last sorted match."),
        ),
    ),
    FieldCatalog(
        path="stages.*.outputs.*.kind",
        options=(
            FieldOption("file", "One managed file."),
            FieldOption("files", "Multiple discovered files."),
            FieldOption("metric", "A metric value read from JSON."),
            FieldOption("manifest", "A manifest JSON file."),
        ),
    ),
)

FIELD_CATALOG_BY_PATH: Final[dict[str, FieldCatalog]] = {
    catalog.path: catalog for catalog in FIELD_CATALOGS
}

FIELD_OPTIONS: Final[dict[str, tuple[str, ...]]] = {
    catalog.path: tuple(option.value for option in catalog.options)
    for catalog in FIELD_CATALOGS
}


def catalog_for(field: str) -> FieldCatalog:
    return FIELD_CATALOG_BY_PATH[field]


def options_for(field: str) -> tuple[str, ...]:
    return tuple(option.value for option in catalog_for(field).options)


def options_csv(field: str) -> str:
    return ", ".join(options_for(field))


def options_sentence(field: str) -> str:
    return _sentence_join(options_for(field))


def options_comment(field: str, *, indent: int) -> str:
    return f"{' ' * indent}# Options: {options_csv(field)}."


def option_table() -> str:
    rows = ["| Field | Options | Meaning |", "| --- | --- | --- |"]
    for catalog in sorted(FIELD_CATALOGS, key=lambda item: item.path):
        options = options_csv(catalog.path).replace(", ", "`, `")
        descriptions = "<br>".join(
            f"`{option.value}`: {option.description}" for option in catalog.options
        )
        rows.append(f"| `{catalog.path}` | `{options}` | {descriptions} |")
    return "\n".join(rows)


def _sentence_join(values: tuple[str, ...]) -> str:
    if len(values) <= 1:
        return "".join(values)
    return f"{', '.join(values[:-1])}, or {values[-1]}"
