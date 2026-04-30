from __future__ import annotations

from typing import Final

from ..models import ConfigField
from .stage_io_base import FIELDS as BASE_FIELDS
from .stage_io_starter import FIELDS as STARTER_FIELDS

FIELDS: Final[tuple[ConfigField, ...]] = (*STARTER_FIELDS, *BASE_FIELDS)
