from __future__ import annotations

from typing import Final

from .models import ConfigField
from .sections.dispatch import FIELDS as DISPATCH_FIELDS
from .sections.hardware import FIELDS as HARDWARE_FIELDS
from .sections.identity import FIELDS as IDENTITY_FIELDS
from .sections.launcher import FIELDS as LAUNCHER_FIELDS
from .sections.notifications import FIELDS as NOTIFICATIONS_FIELDS
from .sections.resources import FIELDS as RESOURCES_FIELDS
from .sections.runtime import FIELDS as RUNTIME_FIELDS
from .sections.runs import FIELDS as RUNS_FIELDS
from .sections.sizing import FIELDS as SIZING_FIELDS
from .sections.stage_gpu import FIELDS as STAGE_GPU_FIELDS
from .sections.stage_io import FIELDS as STAGE_IO_FIELDS
from .sections.stages import FIELDS as STAGES_FIELDS
from .sections.storage import FIELDS as STORAGE_FIELDS

CONFIG_FIELDS: Final[tuple[ConfigField, ...]] = (
    *IDENTITY_FIELDS,
    *STORAGE_FIELDS,
    *HARDWARE_FIELDS,
    *RUNTIME_FIELDS,
    *SIZING_FIELDS,
    *RUNS_FIELDS,
    *DISPATCH_FIELDS,
    *RESOURCES_FIELDS,
    *STAGES_FIELDS,
    *LAUNCHER_FIELDS,
    *STAGE_GPU_FIELDS,
    *STAGE_IO_FIELDS,
    *NOTIFICATIONS_FIELDS,
)
