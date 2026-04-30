from __future__ import annotations

DEFAULT_CONFIG_FILENAME = "experiment.yaml"
DEFAULT_OUTPUT_DIR = "."

AUTO_VALUE = "auto"

DEFAULT_ENVIRONMENT_NAME = "default"
DEFAULT_RUNTIME_NAME = "default"

__all__ = [
    name for name in globals() if name == "AUTO_VALUE" or name.startswith("DEFAULT_")
]
