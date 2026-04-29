from __future__ import annotations

from typing import Final

from ...defaults import (
    ALL_STARTER_TEMPLATES,
    DEFAULT_STORAGE_ROOT,
)
from ..models import ConfigField, ConfigOption

FIELDS: Final[tuple[ConfigField, ...]] = (
    ConfigField(
        path="storage.root",
        title="Storage root",
        short_help="Root directory for plans, logs, status records, and managed artifacts.",
        when_to_change="Change this when runs should be written to a shared filesystem or scratch mount.",
        section="Storage",
        level="common",
        templates=ALL_STARTER_TEMPLATES,
        default=DEFAULT_STORAGE_ROOT,
        first_edit=True,
    ),
    ConfigField(
        path="artifact_store.strategy",
        title="Artifact storage strategy",
        short_help="Controls how declared outputs are placed in the managed run store.",
        when_to_change="Keep copy for the starter; change only after validating filesystem support and retention policy.",
        section="Storage",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default="copy",
        options=(
            ConfigOption("copy", "Copy managed artifacts into the run store."),
            ConfigOption("hardlink", "Hardlink managed artifacts into the run store."),
            ConfigOption("symlink", "Symlink managed artifacts into the run store."),
            ConfigOption(
                "register_only", "Track artifact paths without copying files."
            ),
        ),
    ),
    ConfigField(
        path="artifact_store.fallback_strategy",
        title="Artifact fallback strategy",
        short_help="Fallback behavior when the primary artifact storage strategy fails.",
        when_to_change="Use this only when the primary strategy depends on filesystem features that may be unavailable.",
        section="Storage",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default="null",
        options=(
            ConfigOption("null", "Disable fallback handling."),
            ConfigOption("copy", "Copy artifacts when the primary strategy fails."),
            ConfigOption("hardlink", "Hardlink artifacts when supported."),
            ConfigOption("symlink", "Symlink artifacts when supported."),
            ConfigOption("register_only", "Record artifacts without copying files."),
        ),
    ),
    ConfigField(
        path="artifact_store.verify_digest",
        title="Artifact digest verification",
        short_help="Verifies managed output digests after artifact storage.",
        when_to_change="Keep enabled unless artifact verification is too expensive for the target filesystem.",
        section="Storage",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default="true",
    ),
    ConfigField(
        path="artifact_store.fail_on_verify_error",
        title="Artifact verification failure policy",
        short_help="Fails the run when artifact verification cannot prove integrity.",
        when_to_change="Disable only when incomplete verification should be recorded as a warning instead of failing the run.",
        section="Storage",
        level="advanced",
        templates=ALL_STARTER_TEMPLATES,
        default="true",
    ),
)
