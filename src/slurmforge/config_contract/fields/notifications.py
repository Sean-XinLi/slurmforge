from __future__ import annotations

from typing import Final

from ..option_sets import (
    EMAIL_EVENT_BATCH_FINISHED,
    EMAIL_EVENTS,
    EMAIL_WHEN_AFTERANY,
    EMAIL_WHENS,
)
from ..workflows import ALL_STARTER_TEMPLATES
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="notifications.email.enabled",
        title="Email notifications",
        short_help="Enables Slurm-native email notifications for terminal workflow events.",
        when_to_change="Enable after Slurm mail is configured on the cluster and recipient addresses are configured.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=False,
    ),
    ConfigField(
        path="notifications.email.recipients",
        title="Email recipients",
        short_help="Recipients for Slurm-native email notifications when email is enabled.",
        when_to_change="Set this before enabling notifications.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_display="[]",
    ),
    ConfigField(
        path="notifications.email.events",
        title="Email notification events",
        short_help="Terminal workflow events that trigger Slurm mail jobs.",
        when_to_change="Use batch_finished for stage batches and train_eval_pipeline_finished for streaming pipelines.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=(EMAIL_EVENT_BATCH_FINISHED,),
        default_display=EMAIL_EVENT_BATCH_FINISHED,
        options=EMAIL_EVENTS,
    ),
    ConfigField(
        path="notifications.email.when",
        title="Email notification dependency",
        short_help="Slurm dependency condition used by notification jobs.",
        when_to_change="Keep afterany for completion notifications, or use afterok/afternotok for success/failure-only notifications.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=EMAIL_WHEN_AFTERANY,
        options=EMAIL_WHENS,
    ),
)
