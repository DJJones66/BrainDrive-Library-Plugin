#!/usr/bin/env python3
"""
Shared lifecycle helpers for BrainDrive community plugins.

Provides optional utilities like installation validation, file copying,
and database checks that sit between BrainDrive's BaseLifecycleManager
and plugin-specific lifecycle managers.
"""

import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

try:
  from app.plugins.base_lifecycle_manager import BaseLifecycleManager  # type: ignore
except ImportError:
  import sys

  current_dir = Path(__file__).resolve().parent
  backend_plugins_path = current_dir.parent.parent / "backend" / "app" / "plugins"
  if backend_plugins_path.exists():
    sys.path.insert(0, str(backend_plugins_path))
    from base_lifecycle_manager import BaseLifecycleManager  # type: ignore
  else:
    raise

logger = structlog.get_logger()


class CommunityPluginLifecycleBase(BaseLifecycleManager):
  """Helper lifecycle manager with common utilities for BrainDrive community plugins."""

  copy_exclude_patterns = (
    "node_modules",
    "package-lock.json",
    ".git",
    "__pycache__",
    "*.pyc",
    ".DS_Store",
    "Thumbs.db",
    "*.log"
  )

  required_files = (
    "package.json",
    "tsconfig.json",
    "webpack.config.js",
    "src/index.tsx",
    "dist/remoteEntry.js"
  )

  plugins_base_dir: Optional[str] = None
  _plugin_root: Optional[Path] = None
  plugin_data: Dict[str, Any]
  module_data: List[Dict[str, Any]]

  # ---------------------------------------------------------------------------
  # Metadata accessors
  # ---------------------------------------------------------------------------

  @property
  def PLUGIN_DATA(self) -> Dict[str, Any]:
    return self.plugin_data

  @property
  def MODULE_DATA(self) -> List[Dict[str, Any]]:
    return self.module_data

  def get_plugin_info(self) -> Dict[str, Any]:
    return self.plugin_data

  async def get_plugin_metadata(self) -> Dict[str, Any]:
    return self.plugin_data

  async def get_module_metadata(self) -> List[Dict[str, Any]]:
    return self.module_data

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  @property
  def plugin_root(self) -> Path:
    if self._plugin_root is None:
      self._plugin_root = Path(__file__).resolve().parent
    return self._plugin_root

  def set_plugin_root(self, root: Path) -> None:
    self._plugin_root = root

  def _extract_major_version(self, version: str) -> str:
    normalized = str(version or "").strip().lstrip("v")
    if not normalized:
      return "1"

    digits = []
    for char in normalized:
      if char.isdigit():
        digits.append(char)
      else:
        break

    major = "".join(digits)
    return major or "1"

  def _version_sort_key(self, directory_name: str) -> tuple:
    normalized = str(directory_name or "").strip().lstrip("v")
    base, _, suffix = normalized.partition("-")

    values: List[int] = []
    for part in base.split("."):
      digits = []
      for char in part:
        if char.isdigit():
          digits.append(char)
        else:
          break
      values.append(int("".join(digits) or "0"))

    while len(values) < 3:
      values.append(0)

    # Prefer stable releases over pre-release suffixes for alias targets.
    stability_weight = 1 if not suffix else 0
    return tuple([*values, stability_weight, suffix])

  def _build_major_endpoints_alias_content(self, version_dir_name: str) -> str:
    return (
      "from __future__ import annotations\n\n"
      "import importlib.util\n"
      "from pathlib import Path\n"
      "from types import ModuleType\n\n\n"
      "def _resolve_target_endpoints_path() -> Path:\n"
      f"    return Path(__file__).resolve().parent.parent / \"{version_dir_name}\" / \"endpoints.py\"\n\n\n"
      "def _load_target_module() -> ModuleType:\n"
      "    module_path = _resolve_target_endpoints_path()\n"
      "    if not module_path.exists():\n"
      "        raise FileNotFoundError(\n"
      "            f\"BrainDrive major alias target missing: {module_path}\"\n"
      "        )\n\n"
      "    module_name = f\"braindrive_library_major_alias_{abs(hash(str(module_path)))}\"\n"
      "    spec = importlib.util.spec_from_file_location(module_name, module_path)\n"
      "    if spec is None or spec.loader is None:\n"
      "        raise RuntimeError(f\"Failed to load module spec for {module_path}\")\n\n"
      "    module = importlib.util.module_from_spec(spec)\n"
      "    spec.loader.exec_module(module)\n"
      "    return module\n\n\n"
      "_target = _load_target_module()\n\n"
      "for _name, _value in vars(_target).items():\n"
      "    if _name.startswith(\"__\"):\n"
      "        continue\n"
      "    globals()[_name] = _value\n\n"
      "__all__ = [name for name in globals() if not name.startswith(\"_\")]\n"
    )

  def _ensure_major_endpoints_alias(self, target_dir: Path) -> None:
    target_dir = Path(target_dir).resolve()
    target_endpoints = target_dir / "endpoints.py"
    if not target_endpoints.exists():
      return

    plugin_root = target_dir.parent
    major = self._extract_major_version(target_dir.name)
    major_dir = plugin_root / f"v{major}"
    major_dir.mkdir(parents=True, exist_ok=True)

    version_prefix = f"v{major}."
    candidates: List[Path] = []
    for entry in plugin_root.iterdir():
      if not entry.is_dir():
        continue
      if entry.name == major_dir.name:
        continue
      if not entry.name.startswith(version_prefix):
        continue
      if not (entry / "endpoints.py").exists():
        continue
      candidates.append(entry)

    if not candidates:
      return

    candidates.sort(key=lambda path: self._version_sort_key(path.name), reverse=True)
    target_version_dir = candidates[0]

    alias_path = major_dir / "endpoints.py"
    alias_content = self._build_major_endpoints_alias_content(target_version_dir.name)
    if alias_path.exists():
      existing_content = alias_path.read_text(encoding="utf-8")
      if existing_content == alias_content:
        return

    alias_path.write_text(alias_content, encoding="utf-8")
    logger.info(
      "Community lifecycle: refreshed major-version endpoints alias",
      plugin_slug=self.plugin_data.get("plugin_slug"),
      major_alias=str(alias_path),
      target=str(target_version_dir / "endpoints.py"),
    )

  # ---------------------------------------------------------------------------
  # Optional lifecycle helpers
  # ---------------------------------------------------------------------------

  async def _check_existing_plugin(self, user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Check if plugin already exists for a user."""
    try:
      query = text("""
        SELECT id
        FROM plugin
        WHERE user_id = :user_id
        AND plugin_slug = :plugin_slug
      """)
      result = await db.execute(query, {
        "user_id": user_id,
        "plugin_slug": self.plugin_data["plugin_slug"]
      })
      row = result.fetchone()
      exists = row is not None
      plugin_id = row.id if row else None
      return {"exists": exists, "plugin_id": plugin_id}
    except Exception as error:  # pragma: no cover
      logger.error("Community lifecycle: error checking plugin existence", error=str(error))
      return {"exists": False, "error": str(error)}

  async def _copy_plugin_files_impl(self, user_id: str, target_dir: Path, update: bool = False) -> Dict[str, Any]:
    """Copy plugin source files to the shared directory."""
    try:
      source_dir = self.plugin_root
      target_dir = Path(target_dir).resolve()

      def do_copy():
        if update and target_dir.exists():
          shutil.rmtree(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        ignore = shutil.ignore_patterns(*self.copy_exclude_patterns)
        shutil.copytree(source_dir, target_dir, dirs_exist_ok=True, ignore=ignore)

      await asyncio.to_thread(do_copy)
      self._ensure_major_endpoints_alias(target_dir)
      return {"success": True, "copied_to": str(target_dir)}
    except Exception as error:  # pragma: no cover
      logger.error("Community lifecycle: file copy failed", error=str(error))
      return {"success": False, "error": str(error)}

  async def _validate_installation_impl(self, user_id: str, plugin_dir: Path) -> Dict[str, Any]:
    """Validate essential plugin files after installation."""
    try:
      plugin_dir = Path(plugin_dir)
      missing = [path for path in self.required_files if not (plugin_dir / path).exists()]
      if missing:
        return {"valid": False, "error": f"Missing required files: {', '.join(missing)}"}

      package_json = plugin_dir / "package.json"
      try:
        with package_json.open("r", encoding="utf-8") as handle:
          json.load(handle)
      except (json.JSONDecodeError, FileNotFoundError) as exc:
        return {"valid": False, "error": f"Invalid package.json: {exc}"}

      bundle = plugin_dir / "dist" / "remoteEntry.js"
      if bundle.exists() and bundle.stat().st_size == 0:
        return {"valid": False, "error": "Bundle file dist/remoteEntry.js is empty"}

      return {"valid": True}
    except Exception as error:  # pragma: no cover
      logger.error("Community lifecycle: validation failed", error=str(error))
      return {"valid": False, "error": str(error)}

  async def _get_plugin_health_impl(self, user_id: str, plugin_dir: Path) -> Dict[str, Any]:
    """Provide a lightweight health summary for diagnostics."""
    try:
      plugin_dir = Path(plugin_dir)
      bundle = plugin_dir / "dist" / "remoteEntry.js"
      package_json = plugin_dir / "package.json"

      details = {
        "bundle_exists": bundle.exists(),
        "bundle_size": bundle.stat().st_size if bundle.exists() else 0,
        "package_json_exists": package_json.exists(),
        "shared_path": str(plugin_dir)
      }

      is_healthy = details["bundle_exists"] and details["bundle_size"] > 0 and details["package_json_exists"]
      return {"healthy": is_healthy, "details": details}
    except Exception as error:  # pragma: no cover
      logger.error("Community lifecycle: health check failed", error=str(error))
      return {"healthy": False, "details": {"error": str(error)}}

  # ---------------------------------------------------------------------------
  # Convenience compatibility wrappers
  # ---------------------------------------------------------------------------

  async def install_plugin(self, user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Compatibility wrapper mirroring legacy installer behaviour."""
    existing = await self._check_existing_plugin(user_id, db)
    if existing.get("exists"):
      return {
        "success": False,
        "error": "Plugin already installed for user",
        "plugin_id": existing.get("plugin_id")
      }

    shared_path = self.shared_path
    shared_path.mkdir(parents=True, exist_ok=True)

    copy_result = await self._copy_plugin_files_impl(user_id, shared_path)
    if not copy_result.get("success"):
      return copy_result

    install_result = await self.install_for_user(user_id, db, shared_path)
    if install_result.get("success"):
      install_result["plugin_slug"] = self.plugin_data["plugin_slug"]
      install_result["plugin_name"] = self.plugin_data["name"]
      install_result["validation"] = await self._validate_installation_impl(user_id, shared_path)
    return install_result

  async def delete_plugin(self, user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Compatibility wrapper for uninstall operations."""
    result = await self.uninstall_for_user(user_id, db)
    if result.get("success"):
      try:
        if self.shared_path.exists():
          shutil.rmtree(self.shared_path)
      except OSError:
        # Non-critical if cleanup fails; leave artefacts for troubleshooting
        logger.warning("Community lifecycle: unable to remove shared path", path=str(self.shared_path))
    return result

  async def get_plugin_status(self, user_id: str, db: AsyncSession) -> Dict[str, Any]:
    """Extend base status response with validation and health diagnostics."""
    status = await super().get_plugin_status(user_id, db)
    if not status.get("exists"):
      return status

    validation = await self._validate_installation_impl(user_id, self.shared_path)
    health = await self._get_plugin_health_impl(user_id, self.shared_path)

    status["validation"] = validation
    status["health"] = health
    return status


__all__ = ["CommunityPluginLifecycleBase"]
