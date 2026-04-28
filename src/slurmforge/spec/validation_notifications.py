from __future__ import annotations

from ..errors import ConfigContractError
from ..field_options import options_for, options_sentence
from .models import ExperimentSpec
from .validation_common import reject_newline


def validate_notifications_contract(spec: ExperimentSpec) -> None:
    email = spec.notifications.email
    if not email.enabled:
        return
    if not email.to:
        raise ConfigContractError("`notifications.email.to` is required when email notifications are enabled")
    if not email.events:
        raise ConfigContractError("`notifications.email.on` must include at least one event")
    if email.mode not in options_for("notifications.email.mode"):
        raise ConfigContractError(
            f"`notifications.email.mode` must be {options_sentence('notifications.email.mode')}"
        )
    for index, recipient in enumerate(email.to):
        if "@" not in recipient:
            raise ConfigContractError(f"`notifications.email.to[{index}]` must be an email address")
        reject_newline(recipient, field=f"notifications.email.to[{index}]")
    for event in email.events:
        if event not in options_for("notifications.email.on"):
            raise ConfigContractError(f"`notifications.email.on` contains unsupported event: {event}")
    reject_newline(email.from_address, field="notifications.email.from")
    reject_newline(email.sendmail, field="notifications.email.sendmail")
    reject_newline(email.subject_prefix, field="notifications.email.subject_prefix")
