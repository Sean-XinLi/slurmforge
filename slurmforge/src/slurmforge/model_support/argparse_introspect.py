from __future__ import annotations

import ast
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class CliArgIntrospection:
    supported: frozenset[str]
    actions: tuple[tuple[str, str], ...]
    error: str | None = None


def _normalize_flag(flag: str) -> str:
    return flag.strip().lstrip("-")


def _flag_variants(flag: str) -> set[str]:
    core = _normalize_flag(flag)
    return {
        core,
        core.replace("-", "_"),
        core.replace("_", "-"),
    }


def _canonical_optional_flag(flag: str) -> str:
    token = flag.strip()
    if token.startswith("--"):
        return token
    return f"--{_normalize_flag(token)}"


def _action_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


@lru_cache(maxsize=128)
def _extract_cli_arg_metadata(script_path: str) -> CliArgIntrospection:
    path = Path(script_path)
    if not path.exists():
        return CliArgIntrospection(supported=frozenset(), actions=(), error=f"script not found: {path}")

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return CliArgIntrospection(supported=frozenset(), actions=(), error=f"{type(exc).__name__}: {exc}")

    supported: set[str] = set()
    actions: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "add_argument":
            continue

        flags: list[str] = []
        dest: str | None = None
        action = "value"

        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                flags.append(arg.value.strip())

        for kw in node.keywords:
            if kw.arg == "dest" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                dest = kw.value.value.strip()
            elif kw.arg == "action":
                parsed = _action_name(kw.value)
                if parsed:
                    action = parsed.strip().lower()

        if not flags:
            continue

        canonical_flag = _canonical_optional_flag(flags[0])
        action_value = f"{action}:{canonical_flag}"

        for flag in flags:
            variants = _flag_variants(flag)
            supported.update(variants)
            for variant in variants:
                actions.setdefault(variant, action_value)
        if dest:
            variants = _flag_variants(dest)
            supported.update(variants)
            for variant in variants:
                actions.setdefault(variant, action_value)

    return CliArgIntrospection(
        supported=frozenset(supported),
        actions=tuple(sorted(actions.items())),
        error=None,
    )


def extract_supported_cli_keys(script_path: str) -> set[str]:
    metadata = _extract_cli_arg_metadata(script_path)
    return set(metadata.supported)


def extract_cli_arg_actions(script_path: str) -> dict[str, str]:
    metadata = _extract_cli_arg_metadata(script_path)
    return dict(metadata.actions)


def extract_cli_arg_error(script_path: str) -> str | None:
    return _extract_cli_arg_metadata(script_path).error


def resolve_cli_arg_action(key: str, actions: dict[str, str]) -> str | None:
    for variant in _flag_variants(key):
        if variant in actions:
            return actions[variant]
    return None


def key_supported(key: str, supported: set[str]) -> bool:
    if not supported:
        return True
    return bool(_flag_variants(key) & supported)
