from __future__ import annotations

from typing import Final

from ..config_contract.registry import CONFIG_FIELDS as CONTRACT_CONFIG_FIELDS
from .models import ConfigField

CONFIG_FIELDS: Final[tuple[ConfigField, ...]] = CONTRACT_CONFIG_FIELDS
