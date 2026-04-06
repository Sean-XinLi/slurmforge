from __future__ import annotations

import json
from typing import Any

from ...model_support.argparse_introspect import resolve_cli_arg_action


def to_cli_args(args: dict[str, Any], *, arg_actions: dict[str, str] | None = None) -> list[str]:
    out: list[str] = []
    actions = arg_actions or {}
    for key, value in args.items():
        if value is None:
            continue
        flag = f"--{key}"
        action_text = (resolve_cli_arg_action(key, actions) or "").strip()
        action_name, action_meta = (action_text.split(":", 1) + [""])[:2]
        action_name = action_name.lower()
        resolved_flag = action_meta if action_meta else flag
        if isinstance(value, bool):
            if action_name == "booleanoptionalaction" and not action_meta:
                action_meta = resolved_flag
            if action_name == "store_true":
                if value:
                    out.append(resolved_flag)
                continue
            if action_name == "store_false":
                if not value:
                    out.append(resolved_flag)
                continue
            if action_name == "booleanoptionalaction":
                positive_flag = action_meta if action_meta else resolved_flag
                if value:
                    out.append(positive_flag)
                else:
                    out.append(f"--no-{positive_flag.lstrip('-')}")
                continue

            out.extend([resolved_flag, "true" if value else "false"])
            continue

        out.append(resolved_flag)
        if isinstance(value, (dict, list)):
            out.append(json.dumps(value, sort_keys=True, separators=(",", ":")))
            continue
        out.append(str(value))
    return out
