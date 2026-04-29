from __future__ import annotations

from ...config_schema import render_first_edit_list
from ...defaults import (
    DEFAULT_CONFIG_FILENAME,
    DEFAULT_STORAGE_ROOT,
    TEMPLATE_EVAL_CHECKPOINT,
    TEMPLATE_TRAIN_EVAL,
    TEMPLATE_TRAIN_ONLY,
)
from ..models import InitRequest, StarterCommandSet, StarterReadmePlan


def starter_readme_plan(
    request: InitRequest,
    *,
    dry_run_command: str,
    submit_command: str,
    notes: tuple[str, ...] = (),
) -> StarterReadmePlan:
    return StarterReadmePlan(
        template=request.template,
        commands=StarterCommandSet(
            validate=f"sforge validate --config {DEFAULT_CONFIG_FILENAME}",
            dry_run=dry_run_command,
            submit=submit_command,
        ),
        notes=notes,
    )


def render_starter_readme(plan: StarterReadmePlan) -> str:
    notes_text = "\n".join(f"- {note}" for note in plan.notes)
    if notes_text:
        notes_text = f"\nNotes:\n\n{notes_text}\n"
    first_edit_fields = render_first_edit_list(plan.template)
    return f"""# SlurmForge Starter

Template: `{plan.template}`

## Next Commands

```bash
{plan.commands.validate}
{plan.commands.dry_run}
{plan.commands.submit}
```

## Generated Files

- `{DEFAULT_CONFIG_FILENAME}`: executable starter config.
- `CONFIG.sforge.md`: config guide scoped to this starter template.
- `README.sforge.md`: this workflow guide.
- Stage scripts: replace demo model code while keeping the SlurmForge contract sections.

## Output Locations

- Dry-run audits print to stdout unless you pass `--output`.
- Submitted plans and run artifacts land under `{DEFAULT_STORAGE_ROOT}/<project>/<experiment>/...`.
- Stage attempts write per-run records under the generated batch root's `runs/<run_id>/attempts/<attempt>/`.
{notes_text}
## Connect Your Model

Generated scripts are split into three sections:

- `SECTION A - SlurmForge contract`: keep the injected args and environment contract; add your own CLI args here.
- `SECTION B - Your model code`: replace the demo model, data loading, and stage logic.
- `SECTION C - Output contract`: keep the file shapes that the YAML declares.

{_stage_contract(plan.template)}

## Edit These First

{first_edit_fields}

## Config Reference

- `CONFIG.sforge.md`: fields used by this starter.
- `docs/config.md`: complete SlurmForge config reference.

The starter values are deliberately small. Replace scripts, resources, runtime paths, and output contracts before using this for real work.
"""


def _stage_contract(template: str) -> str:
    if template == TEMPLATE_TRAIN_EVAL:
        return (
            "The train stage must leave a `.pt` checkpoint under `checkpoints/`. "
            "The eval stage receives that checkpoint as `--checkpoint_path` and "
            "`SFORGE_INPUT_CHECKPOINT`, then writes `eval/metrics.json` with a "
            "numeric `accuracy` field."
        )
    if template == TEMPLATE_TRAIN_ONLY:
        return (
            "The train stage must leave a `.pt` checkpoint under `checkpoints/`. "
            "That file shape is the output contract declared by `experiment.yaml`."
        )
    if template == TEMPLATE_EVAL_CHECKPOINT:
        return (
            "The eval stage receives the checkpoint selected by `--checkpoint` as "
            "`--checkpoint_path` and `SFORGE_INPUT_CHECKPOINT`, then writes "
            "`eval/metrics.json` with a numeric `accuracy` field."
        )
    return "Keep the file shapes declared by `experiment.yaml`."
