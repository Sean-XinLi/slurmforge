from __future__ import annotations

from typing import Final

from ...config_contract.fields.stage_io_base import FIELDS as BASE_FIELDS
from ...config_contract.fields.stage_io_starter import FIELDS as STARTER_FIELDS
from ...config_contract.models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (*STARTER_FIELDS, *BASE_FIELDS)
