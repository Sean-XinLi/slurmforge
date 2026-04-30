from __future__ import annotations

from typing import Final

from ..default_values import (
    DEFAULT_EMAIL_ENABLED,
    DEFAULT_EMAIL_EVENTS,
    DEFAULT_EMAIL_FROM,
    DEFAULT_EMAIL_MODE,
    DEFAULT_EMAIL_SENDMAIL,
    DEFAULT_EMAIL_SUBJECT_PREFIX,
)
from ..option_sets import EMAIL_EVENTS, EMAIL_MODES
from ..workflows import ALL_STARTER_TEMPLATES
from ..models import ConfigField

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="notifications.email.enabled",
        title="Email notifications",
        short_help="Enables email summary notifications for terminal workflow events.",
        when_to_change="Enable after sendmail works on the cluster and recipient addresses are configured.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_EMAIL_ENABLED,
    ),
    ConfigField(
        path="notifications.email.to",
        title="Email recipients",
        short_help="Recipients for email notifications when email is enabled.",
        when_to_change="Set this before enabling notifications.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_display="[]",
    ),
    ConfigField(
        path="notifications.email.from",
        title="Email sender",
        short_help="Sender address used by email delivery.",
        when_to_change="Change this to the approved sender identity for your cluster.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_EMAIL_FROM,
    ),
    ConfigField(
        path="notifications.email.sendmail",
        title="Sendmail binary",
        short_help="Local sendmail-compatible binary used to deliver email.",
        when_to_change="Change this if the cluster exposes sendmail at a non-default path.",
        section="Notifications",
        level="advanced",
        value_type="path",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_EMAIL_SENDMAIL,
    ),
    ConfigField(
        path="notifications.email.subject_prefix",
        title="Email subject prefix",
        short_help="Prefix added to SlurmForge notification email subjects.",
        when_to_change="Change this to identify a project, team, or cluster in inboxes.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_EMAIL_SUBJECT_PREFIX,
    ),
    ConfigField(
        path="notifications.email.on",
        title="Email notification events",
        short_help="Terminal workflow events that trigger email summaries.",
        when_to_change="Use batch_finished for stage batches and train_eval_pipeline_finished for streaming pipelines.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_EMAIL_EVENTS,
        default_display=", ".join(DEFAULT_EMAIL_EVENTS),
        options=EMAIL_EVENTS,
    ),
    ConfigField(
        path="notifications.email.mode",
        title="Email notification mode",
        short_help="Controls email content shape.",
        when_to_change="Keep summary unless a future notification mode is added.",
        section="Notifications",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default_value=DEFAULT_EMAIL_MODE,
        options=EMAIL_MODES,
    ),
)
