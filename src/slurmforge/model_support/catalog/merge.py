from __future__ import annotations

from ...errors import ConfigContractError
from .models import ModelCatalogEntry, ResolvedModelCatalog


def duplicate_model_names(entries: tuple[ModelCatalogEntry, ...] | list[ModelCatalogEntry]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for entry in entries:
        if entry.name in seen:
            duplicates.add(entry.name)
        else:
            seen.add(entry.name)
    return tuple(sorted(duplicates))


def ensure_unique_entries(
    entries: list[ModelCatalogEntry],
    *,
    source_name: str,
) -> dict[str, ModelCatalogEntry]:
    duplicates = duplicate_model_names(entries)
    if duplicates:
        joined = ", ".join(duplicates)
        raise ConfigContractError(f"{source_name} defines duplicate model names: {joined}")
    return {entry.name: entry for entry in entries}


def catalog_from_entry_map(entry_map: dict[str, ModelCatalogEntry]) -> ResolvedModelCatalog:
    return ResolvedModelCatalog(entries=tuple(sorted(entry_map.values(), key=lambda item: item.name)))


def merge_catalog_layers(
    *,
    base_entries: tuple[ModelCatalogEntry, ...] | list[ModelCatalogEntry],
    override_entries: tuple[ModelCatalogEntry, ...] | list[ModelCatalogEntry],
    base_name: str,
    override_name: str,
) -> ResolvedModelCatalog:
    merged_entries = ensure_unique_entries(list(base_entries), source_name=base_name)
    merged_entries.update(ensure_unique_entries(list(override_entries), source_name=override_name))
    return catalog_from_entry_map(merged_entries)


def entry_by_name(entries: tuple[ModelCatalogEntry, ...], name: str) -> ModelCatalogEntry | None:
    for entry in entries:
        if entry.name == name:
            return entry
    return None
