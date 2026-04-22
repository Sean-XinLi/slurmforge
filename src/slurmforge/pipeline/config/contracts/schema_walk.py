"""Walk a validation ``ObjectSchema`` to enumerate every leaf path.

Used by the meta-test to assert every schema field appears in the contract
registry.  Also useful for diagnostics / debugging.

Leaf emission rules
-------------------
- ``ScalarField`` leaves emit their fully-qualified dotted path.
- ``DynamicMapping`` leaves emit ``path.*`` to express "any key under this
  prefix is allowed" — matches how the contract registry's wildcard entries
  are written.
- ``ObjectSchema`` recurses; its own name is NOT emitted.
"""
from __future__ import annotations

from ..validation.definitions import DynamicMapping, ObjectSchema, ScalarField


def walk_schema_leaf_paths(schema: ObjectSchema, _prefix: str = "") -> tuple[str, ...]:
    paths: list[str] = []
    for name, field_spec in schema.fields.items():
        path = f"{_prefix}.{name}" if _prefix else name
        if isinstance(field_spec, ObjectSchema):
            paths.extend(walk_schema_leaf_paths(field_spec, path))
        elif isinstance(field_spec, DynamicMapping):
            paths.append(f"{path}.*")
        elif isinstance(field_spec, ScalarField):
            paths.append(path)
        else:
            # Unknown leaf type — still emit the path so the meta-test
            # surfaces the mismatch rather than silently skipping.
            paths.append(path)
    return tuple(paths)
