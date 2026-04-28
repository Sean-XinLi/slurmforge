from __future__ import annotations

from ..defaults import DEFAULT_STORAGE_ROOT
from ..models import InitRequest, StarterCommandSet, StarterReadmePlan
from ..config_comments import option_table


def starter_readme_plan(
    request: InitRequest,
    *,
    dry_run_command: str,
    submit_command: str,
    editable_fields: tuple[str, ...],
    notes: tuple[str, ...] = (),
) -> StarterReadmePlan:
    config_name = request.output.name
    return StarterReadmePlan(
        template=request.template,
        commands=StarterCommandSet(
            validate=f"sforge validate --config {config_name}",
            dry_run=dry_run_command,
            submit=submit_command,
        ),
        editable_fields=editable_fields,
        notes=notes,
    )


def render_starter_readme(plan: StarterReadmePlan) -> str:
    notes_text = "\n".join(f"- {note}" for note in plan.notes)
    if notes_text:
        notes_text = f"\nNotes:\n\n{notes_text}\n"
    fields = "\n".join(f"- `{field}`" for field in plan.editable_fields)
    return f"""# SlurmForge Starter

Template: `{plan.template}`

## Next Commands

```bash
{plan.commands.validate}
{plan.commands.dry_run}
{plan.commands.submit}
```

## Output Locations

- Dry-run audits print to stdout unless you pass `--output`.
- Submitted plans and run artifacts land under `{DEFAULT_STORAGE_ROOT}/<project>/<experiment>/...`.
- Stage attempts write per-run records under the generated batch root's `runs/<run_id>/attempts/<attempt>/`.
{notes_text}
## Common Fields To Edit

{fields}

## Common Field Options

{option_table()}

The starter values are deliberately small. Replace scripts, resources, runtime paths, and output contracts before using this for real work.
"""
