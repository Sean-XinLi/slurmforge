from __future__ import annotations

import json
import hashlib
import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from ..errors import RecordContractError


def to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {key: to_jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    return value


def stable_json(payload: Any) -> str:
    return json.dumps(
        to_jsonable(payload), sort_keys=True, separators=(",", ":"), default=str
    )


def content_digest(payload: Any, *, prefix: int | None = None) -> str:
    digest = hashlib.sha256(stable_json(payload).encode("utf-8")).hexdigest()
    return digest[:prefix] if prefix is not None else digest


def write_json_value(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_text(
        json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def write_json_object(path: Path, payload: Any, *, label: str | None = None) -> None:
    value = to_jsonable(payload)
    if not isinstance(value, dict):
        name = label or str(path)
        raise RecordContractError(f"{name} must be a JSON object")
    write_json_value(path, value)


def read_json_value(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_json_object(path: Path, *, label: str | None = None) -> dict[str, Any]:
    payload = read_json_value(path)
    if not isinstance(payload, dict):
        name = label or str(path)
        raise RecordContractError(f"{name} must be a JSON object")
    return dict(payload)
