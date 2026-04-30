from __future__ import annotations

from ..config_contract.registry import options_for, options_sentence
from ..errors import ConfigContractError
from .models import ExperimentSpec
from .validation_common import reject_newline


def validate_notifications_contract(spec: ExperimentSpec) -> None:
    email = spec.notifications.email
    if not email.enabled:
        return
    if not email.recipients:
        raise ConfigContractError(
            "`notifications.email.recipients` is required when email notifications are enabled"
        )
    if not email.events:
        raise ConfigContractError(
            "`notifications.email.events` must include at least one event"
        )
    if email.when not in options_for("notifications.email.when"):
        raise ConfigContractError(
            f"`notifications.email.when` must be {options_sentence('notifications.email.when')}"
        )
    for index, recipient in enumerate(email.recipients):
        if "@" not in recipient:
            raise ConfigContractError(
                f"`notifications.email.recipients[{index}]` must be an email address"
            )
        reject_newline(recipient, field=f"notifications.email.recipients[{index}]")
    for event in email.events:
        if event not in options_for("notifications.email.events"):
            raise ConfigContractError(
                f"`notifications.email.events` contains unsupported event: {event}"
            )
    reject_newline(email.when, field="notifications.email.when")
