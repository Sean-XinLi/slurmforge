from __future__ import annotations

import importlib.resources as resources


def list_package_files(package: str, *, suffix: str = "") -> list[str]:
    if hasattr(resources, "files"):
        return sorted(
            entry.name
            for entry in resources.files(package).iterdir()
            if entry.is_file() and (not suffix or entry.name.endswith(suffix))
        )

    names: list[str] = []
    for entry_name in resources.contents(package):
        if suffix and not entry_name.endswith(suffix):
            continue
        if resources.is_resource(package, entry_name):
            names.append(entry_name)
    return sorted(names)


def read_package_text(package: str, resource_name: str, *, encoding: str = "utf-8") -> str:
    if hasattr(resources, "files"):
        return resources.files(package).joinpath(resource_name).read_text(encoding=encoding)
    return resources.read_text(package, resource_name, encoding=encoding)
