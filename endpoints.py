from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

PLUGIN_SLUG = "BrainDriveLibraryPlugin"


def _resolve_legacy_endpoints_path() -> Path:
    current = Path(__file__).resolve()
    candidates = []
    for parent in [current.parent, *current.parents]:
        candidates.append(parent / "BrainDriveLibraryService" / "v0" / "endpoints.py")
        candidates.append(
            parent
            / "backend"
            / "plugins"
            / "shared"
            / "BrainDriveLibraryService"
            / "v0"
            / "endpoints.py"
        )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Unable to locate legacy BrainDriveLibraryService endpoints module."
    )


def _load_legacy_module() -> ModuleType:
    module_path = _resolve_legacy_endpoints_path()
    module_name = f"braindrive_library_legacy_endpoints_{abs(hash(str(module_path)))}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec for {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    setattr(module, "PLUGIN_SLUG", PLUGIN_SLUG)
    return module


_legacy = _load_legacy_module()

for _name, _value in vars(_legacy).items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _value

__all__ = [
    name
    for name in globals()
    if not name.startswith("_")
]
