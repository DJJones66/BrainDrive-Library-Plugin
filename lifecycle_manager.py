#!/usr/bin/env python3
"""
BrainDrive Library Plugin lifecycle manager.

Installs the Library Capture + Library Editor modules, creates default pages,
and removes both cleanly on uninstall.
"""

from __future__ import annotations

import asyncio
import datetime
import importlib.util
import json
import os
import shutil
import subprocess
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

CURRENT_DIR = Path(__file__).resolve().parent

HELPER_PATH = CURRENT_DIR / "community_lifecycle_manager.py"
spec = importlib.util.spec_from_file_location(
    "library.community_lifecycle_manager", HELPER_PATH
)
helper_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper_module)
CommunityPluginLifecycleBase = helper_module.CommunityPluginLifecycleBase

logger = structlog.get_logger()

LIBRARY_SERVICE_REPO_URL = "https://github.com/DJJones66/Library-Service"
LIBRARY_SERVICE_RUNTIME_DIR_NAME = "Library-Service"
SERVICES_RUNTIME_ENV_VAR = "BRAINDRIVE_SERVICES_RUNTIME_DIR"
DIRTY_WORKTREE_POLICY_ENV_VAR = "BRAINDRIVE_LIBRARY_RUNTIME_DIRTY_POLICY"
DEFAULT_DIRTY_WORKTREE_POLICY = "stash"

PLUGIN_DATA: Dict[str, Any] = {
    "name": "BrainDrive Library Plugin",
    "description": "Library capture and editor modules for BrainDrive Library workflows.",
    "version": "1.1.1",
    "type": "fullstack",
    "plugin_type": "fullstack",
    "icon": "FolderOpen",
    "category": "productivity",
    "official": True,
    "author": "BrainDrive",
    "compatibility": "1.0.0",
    "scope": "BrainDriveLibraryPlugin",
    "bundle_method": "webpack",
    "bundle_location": "dist/remoteEntry.js",
    "endpoints_file": "endpoints.py",
    "route_prefix": "/",
    "is_local": False,
    "long_description": (
        "A mobile-responsive Capture + Editor plugin for routing quick inputs, "
        "approval-gated writes, transcript ingestion, and direct scoped file editing."
    ),
    "plugin_slug": "BrainDriveLibraryPlugin",
    "source_type": "github",
    "source_url": "https://github.com/BrainDriveAI/BrainDrive-Library-Plugin",
    "required_services_runtime": ["library_service"],
    "backend_dependencies": [],
    "permissions": [
        "api.access",
        "storage.read",
        "storage.write",
        "pageContext.read",
    ],
}

MODULE_DATA: List[Dict[str, Any]] = [
    {
        "name": "LibraryCapture",
        "display_name": "Library Capture",
        "description": "Quick text capture with approval-gated Library routing.",
        "icon": "MessageSquare",
        "category": "ai",
        "priority": 1,
        "props": {
            "initial_greeting": (
                "Capture is ready. Add a note, decision, task, completion update, "
                "or upload a transcript."
            ),
            "prompt_question": "What do you want to capture?",
            "conversation_type": "capture",
            "enable_streaming": True,
            "show_model_selection": True,
            "default_model_key": None,
            "default_model_provider": None,
            "default_model_server_id": None,
            "default_model_name": None,
            "default_model_provider_id": None,
            "default_model_server_name": None,
            "lock_model_selection": False,
            "default_library_scope_enabled": False,
            "default_scope_root": None,
            "default_scope_path": None,
            "default_project_slug": None,
            "default_project_lifecycle": "active",
            "lock_project_scope": False,
            "apply_defaults_on_new_chat": True,
            "input_placeholder": (
                "Capture a note, decision, task, completion, or upload a transcript..."
            ),
            "submit_label": "Capture",
            "show_transcript_upload": True,
            "default_transcript_source": "capture-upload",
            "enable_new_scope_proposals": True,
        },
        "config_fields": {
            "initial_greeting": {
                "type": "text",
                "description": "Initial greeting shown by Capture",
                "default": (
                    "Capture is ready. Add a note, decision, task, completion update, "
                    "or upload a transcript."
                ),
            },
            "prompt_question": {
                "type": "text",
                "description": "Prompt shown near the Capture input",
                "default": "What do you want to capture?",
            },
            "conversation_type": {
                "type": "text",
                "description": "Conversation type namespace for capture",
                "default": "capture",
            },
            "enable_streaming": {
                "type": "boolean",
                "description": "Enable streaming responses by default",
                "default": True,
            },
            "show_model_selection": {
                "type": "boolean",
                "description": "Show model selection dropdown",
                "default": True,
            },
            "default_model_key": {
                "type": "text",
                "description": "Default model in <provider>::<serverId>::<modelName> format",
                "default": None,
            },
            "default_model_provider": {
                "type": "text",
                "description": "Default model provider (for example: ollama, openrouter)",
                "default": None,
            },
            "default_model_server_id": {
                "type": "text",
                "description": "Default model server ID",
                "default": None,
            },
            "default_model_name": {
                "type": "text",
                "description": "Default model name",
                "default": None,
            },
            "default_model_provider_id": {
                "type": "text",
                "description": "Default model provider settings ID",
                "default": None,
            },
            "default_model_server_name": {
                "type": "text",
                "description": "Default model server name",
                "default": None,
            },
            "lock_model_selection": {
                "type": "boolean",
                "description": "Prevent changing model in this module",
                "default": False,
            },
            "default_library_scope_enabled": {
                "type": "boolean",
                "description": "Enable default scope selection",
                "default": False,
            },
            "default_scope_root": {
                "type": "text",
                "description": "Default scope root (life or projects)",
                "default": None,
            },
            "default_scope_path": {
                "type": "text",
                "description": "Default scope path",
                "default": None,
            },
            "default_project_slug": {
                "type": "text",
                "description": "Default project/topic slug",
                "default": None,
            },
            "default_project_lifecycle": {
                "type": "text",
                "description": "Lifecycle used when resolving default scope",
                "default": "active",
            },
            "lock_project_scope": {
                "type": "boolean",
                "description": "Prevent changing selected scope",
                "default": False,
            },
            "apply_defaults_on_new_chat": {
                "type": "boolean",
                "description": "Re-apply defaults when starting a new capture thread",
                "default": True,
            },
            "input_placeholder": {
                "type": "text",
                "description": "Capture input placeholder",
                "default": (
                    "Capture a note, decision, task, completion, or upload a transcript..."
                ),
            },
            "submit_label": {
                "type": "text",
                "description": "Submit button label",
                "default": "Capture",
            },
            "show_transcript_upload": {
                "type": "boolean",
                "description": "Show transcript upload action",
                "default": True,
            },
            "default_transcript_source": {
                "type": "text",
                "description": "Default transcript source metadata",
                "default": "capture-upload",
            },
            "enable_new_scope_proposals": {
                "type": "boolean",
                "description": "Allow proposing new scope/page creation when no match exists",
                "default": True,
            },
        },
        "messages": {},
        "required_services": {
            "api": {
                "methods": ["get", "post", "put", "delete", "postStreaming"],
                "version": "1.0.0",
            },
            "theme": {
                "methods": [
                    "getCurrentTheme",
                    "addThemeChangeListener",
                    "removeThemeChangeListener",
                ],
                "version": "1.0.0",
            },
            "pageContext": {
                "methods": ["getCurrentPageContext", "onPageContextChange"],
                "version": "1.0.0",
            },
            "settings": {
                "methods": ["getSetting", "setSetting", "getSettingDefinitions"],
                "version": "1.0.0",
            },
        },
        "dependencies": [],
        "layout": {
            "minWidth": 6,
            "minHeight": 6,
            "defaultWidth": 12,
            "defaultHeight": 10,
        },
        "tags": ["library", "capture", "approval", "tasks", "transcripts"],
    },
    {
        "name": "LibraryEditor",
        "display_name": "Library Editor",
        "description": "Browse, preview, and edit scoped library files.",
        "icon": "FolderPen",
        "category": "productivity",
        "priority": 2,
        "props": {
            "title": "Library Editor",
            "subtitle": "Navigate and update your scoped library files",
        },
        "config_fields": {
            "title": {
                "type": "text",
                "description": "Module title",
                "default": "Library Editor",
            },
            "subtitle": {
                "type": "text",
                "description": "Module subtitle",
                "default": "Navigate and update your scoped library files",
            },
        },
        "messages": {},
        "required_services": {
            "api": {
                "methods": ["get", "post", "put", "delete"],
                "version": "1.0.0",
            },
            "theme": {
                "methods": [
                    "getCurrentTheme",
                    "addThemeChangeListener",
                    "removeThemeChangeListener",
                ],
                "version": "1.0.0",
            },
            "pageContext": {
                "methods": ["getCurrentPageContext", "onPageContextChange"],
                "version": "1.0.0",
            },
        },
        "dependencies": [],
        "layout": {
            "minWidth": 5,
            "minHeight": 6,
            "defaultWidth": 12,
            "defaultHeight": 10,
        },
        "tags": ["library", "editor", "markdown", "filesystem"],
    },
]

PAGE_SPECS: List[Dict[str, Any]] = [
    {
        "name": "Library Capture",
        "route": "library-capture",
        "module_name": "LibraryCapture",
        "display_name": "Library Capture",
        "description": "Quick text capture with approval-gated library routing.",
        "module_args": {
            "conversation_type": "capture",
            "default_library_scope_enabled": False,
            "default_project_slug": None,
            "default_project_lifecycle": "active",
            "default_scope_root": None,
            "default_scope_path": None,
            "apply_defaults_on_new_chat": True,
            "lock_project_scope": False,
        },
    },
    {
        "name": "Library Editor",
        "route": "library-editor",
        "module_name": "LibraryEditor",
        "display_name": "Library Editor",
        "description": "Browse and edit files in your scoped BrainDrive Library.",
    },
    {
        "name": "WhyFinder",
        "route": "whyfinder",
        "module_name": "LibraryCapture",
        "display_name": "WhyFinder Capture",
        "description": "Focused life page for values and purpose discovery.",
        "module_args": {
            "conversation_type": "life-whyfinder",
            "default_library_scope_enabled": True,
            "default_project_slug": "whyfinder",
            "default_project_lifecycle": "active",
            "default_scope_root": "life",
            "default_scope_path": "life/whyfinder",
            "apply_defaults_on_new_chat": True,
            "lock_project_scope": False,
        },
    },
    {
        "name": "Career",
        "route": "career",
        "module_name": "LibraryCapture",
        "display_name": "Career Capture",
        "description": "Focused life page for career.",
        "module_args": {
            "conversation_type": "life-career",
            "default_library_scope_enabled": True,
            "default_project_slug": "career",
            "default_project_lifecycle": "active",
            "default_scope_root": "life",
            "default_scope_path": "life/career",
            "apply_defaults_on_new_chat": True,
            "lock_project_scope": False,
        },
    },
    {
        "name": "Finances",
        "route": "finances",
        "module_name": "LibraryCapture",
        "display_name": "Finances Capture",
        "description": "Focused life page for finances.",
        "module_args": {
            "conversation_type": "life-finances",
            "default_library_scope_enabled": True,
            "default_project_slug": "finances",
            "default_project_lifecycle": "active",
            "default_scope_root": "life",
            "default_scope_path": "life/finances",
            "apply_defaults_on_new_chat": True,
            "lock_project_scope": False,
        },
    },
    {
        "name": "Fitness",
        "route": "fitness",
        "module_name": "LibraryCapture",
        "display_name": "Fitness Capture",
        "description": "Focused life page for fitness.",
        "module_args": {
            "conversation_type": "life-fitness",
            "default_library_scope_enabled": True,
            "default_project_slug": "fitness",
            "default_project_lifecycle": "active",
            "default_scope_root": "life",
            "default_scope_path": "life/fitness",
            "apply_defaults_on_new_chat": True,
            "lock_project_scope": False,
        },
    },
    {
        "name": "Relationships",
        "route": "relationships",
        "module_name": "LibraryCapture",
        "display_name": "Relationships Capture",
        "description": "Focused life page for relationships.",
        "module_args": {
            "conversation_type": "life-relationships",
            "default_library_scope_enabled": True,
            "default_project_slug": "relationships",
            "default_project_lifecycle": "active",
            "default_scope_root": "life",
            "default_scope_path": "life/relationships",
            "apply_defaults_on_new_chat": True,
            "lock_project_scope": False,
        },
    },
]


class BrainDriveLibraryPluginLifecycleManager(CommunityPluginLifecycleBase):
    """Lifecycle manager for the BrainDrive Library plugin."""

    def __init__(self, plugins_base_dir: Optional[str] = None):
        self.plugin_data = PLUGIN_DATA
        self.module_data = MODULE_DATA
        self.plugins_base_dir = plugins_base_dir
        self.set_plugin_root(Path(__file__).resolve().parent)

        if plugins_base_dir:
            shared_path = (
                Path(plugins_base_dir)
                / "shared"
                / PLUGIN_DATA["plugin_slug"]
                / f"v{PLUGIN_DATA['version']}"
            )
        else:
            shared_path = (
                Path(__file__).resolve().parent.parent.parent
                / "backend"
                / "plugins"
                / "shared"
                / PLUGIN_DATA["plugin_slug"]
                / f"v{PLUGIN_DATA['version']}"
            )

        super().__init__(
            plugin_slug=PLUGIN_DATA["plugin_slug"],
            version=PLUGIN_DATA["version"],
            shared_storage_path=shared_path,
        )

    def _resolve_services_runtime_dir(self) -> Path:
        override = str(os.environ.get(SERVICES_RUNTIME_ENV_VAR, "")).strip()
        if override:
            return Path(override).expanduser().resolve()

        current_file = Path(__file__).resolve()
        for parent in [current_file.parent, *current_file.parents]:
            if parent.name == "backend":
                return (parent / "services_runtime").resolve()
            backend_dir = parent / "backend"
            if backend_dir.is_dir():
                return (backend_dir / "services_runtime").resolve()

        cwd = Path.cwd().resolve()
        for parent in [cwd, *cwd.parents]:
            if parent.name == "backend":
                return (parent / "services_runtime").resolve()
            backend_dir = parent / "backend"
            if backend_dir.is_dir():
                return (backend_dir / "services_runtime").resolve()

        raise RuntimeError(
            "Unable to resolve services_runtime directory. "
            f"Set {SERVICES_RUNTIME_ENV_VAR} to <BrainDriveRoot>/backend/services_runtime."
        )

    @contextmanager
    def _runtime_lock(self, lock_file: Path) -> Iterator[None]:
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_file.open("a+", encoding="utf-8")
        locker = None
        try:
            try:
                import fcntl as locker  # type: ignore

                locker.flock(handle.fileno(), locker.LOCK_EX)
            except Exception:
                locker = None
            yield
        finally:
            if locker is not None:
                try:
                    locker.flock(handle.fileno(), locker.LOCK_UN)
                except Exception:
                    pass
            handle.close()

    def _truncate_output(self, value: str, limit: int = 1500) -> str:
        raw = str(value or "")
        if len(raw) <= limit:
            return raw
        return f"{raw[:limit]}... [truncated {len(raw) - limit} chars]"

    def _run_command(
        self,
        command: List[str],
        cwd: Optional[Path] = None,
        timeout: int = 600,
    ) -> Dict[str, Any]:
        try:
            completed = subprocess.run(
                command,
                cwd=str(cwd) if cwd else None,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            stdout = completed.stdout or ""
            stderr = completed.stderr or ""
            return {
                "success": completed.returncode == 0,
                "returncode": completed.returncode,
                "stdout": self._truncate_output(stdout),
                "stderr": self._truncate_output(stderr),
                "command": " ".join(command),
            }
        except Exception as error:
            return {
                "success": False,
                "returncode": None,
                "stdout": "",
                "stderr": str(error),
                "command": " ".join(command),
            }

    def _sync_library_service_runtime(self, update_existing: bool = False) -> Dict[str, Any]:
        git_bin = shutil.which("git")
        if not git_bin:
            return {"success": False, "error": "git executable not found in PATH."}

        try:
            runtime_root = self._resolve_services_runtime_dir()
            runtime_root.mkdir(parents=True, exist_ok=True)
        except Exception as error:
            return {"success": False, "error": f"Failed to resolve runtime root: {error}"}

        runtime_dir = runtime_root / LIBRARY_SERVICE_RUNTIME_DIR_NAME
        lock_file = runtime_root / ".library-service-runtime.lock"

        with self._runtime_lock(lock_file):
            action = "existing"
            if not runtime_dir.exists():
                clone_result = self._run_command(
                    [git_bin, "clone", LIBRARY_SERVICE_REPO_URL, str(runtime_dir)],
                    timeout=900,
                )
                if not clone_result.get("success"):
                    return {
                        "success": False,
                        "error": "Failed to clone Library-Service runtime repository.",
                        "runtime_dir": str(runtime_dir),
                        "repo_url": LIBRARY_SERVICE_REPO_URL,
                        "clone": clone_result,
                    }
                action = "cloned"

            is_git_repo = (runtime_dir / ".git").exists()
            commit_before: Optional[str] = None
            if is_git_repo:
                commit_result = self._run_command([git_bin, "rev-parse", "HEAD"], cwd=runtime_dir)
                if commit_result.get("success"):
                    commit_before = str(commit_result.get("stdout") or "").strip()

            if not update_existing:
                return {
                    "success": True,
                    "runtime_dir": str(runtime_dir),
                    "repo_url": LIBRARY_SERVICE_REPO_URL,
                    "action": action,
                    "updated": False,
                    "is_git_repo": is_git_repo,
                    "commit": commit_before,
                }

            if not is_git_repo:
                return {
                    "success": False,
                    "error": (
                        "Library-Service runtime directory exists but is not a git repository; "
                        "cannot pull updates."
                    ),
                    "runtime_dir": str(runtime_dir),
                    "repo_url": LIBRARY_SERVICE_REPO_URL,
                    "action": action,
                }

            status_result = self._run_command([git_bin, "status", "--porcelain"], cwd=runtime_dir)
            if not status_result.get("success"):
                return {
                    "success": False,
                    "error": "Failed to inspect Library-Service runtime git status.",
                    "runtime_dir": str(runtime_dir),
                    "repo_url": LIBRARY_SERVICE_REPO_URL,
                    "status": status_result,
                }

            dirty_worktree = bool(str(status_result.get("stdout") or "").strip())
            dirty_policy = (
                str(os.environ.get(DIRTY_WORKTREE_POLICY_ENV_VAR, DEFAULT_DIRTY_WORKTREE_POLICY))
                .strip()
                .lower()
            )
            if dirty_policy not in {"stash", "skip", "fail"}:
                dirty_policy = DEFAULT_DIRTY_WORKTREE_POLICY

            stash_result: Optional[Dict[str, Any]] = None
            stash_created = False
            stash_name: Optional[str] = None

            if dirty_worktree:
                if dirty_policy == "skip":
                    return {
                        "success": True,
                        "runtime_dir": str(runtime_dir),
                        "repo_url": LIBRARY_SERVICE_REPO_URL,
                        "action": action,
                        "updated": False,
                        "skipped_reason": "dirty_worktree",
                        "dirty_worktree": True,
                        "dirty_worktree_policy": dirty_policy,
                        "is_git_repo": True,
                        "commit_before": commit_before,
                    }

                if dirty_policy == "fail":
                    return {
                        "success": False,
                        "runtime_dir": str(runtime_dir),
                        "repo_url": LIBRARY_SERVICE_REPO_URL,
                        "error": "Library-Service runtime worktree is dirty.",
                        "dirty_worktree": True,
                        "dirty_worktree_policy": dirty_policy,
                        "is_git_repo": True,
                        "commit_before": commit_before,
                        "status": status_result,
                    }

                stash_name = (
                    "braindrive-library-runtime-auto-stash-"
                    f"{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                )
                stash_result = self._run_command(
                    [
                        git_bin,
                        "stash",
                        "push",
                        "--include-untracked",
                        "--message",
                        stash_name,
                    ],
                    cwd=runtime_dir,
                    timeout=900,
                )
                if not stash_result.get("success"):
                    return {
                        "success": False,
                        "runtime_dir": str(runtime_dir),
                        "repo_url": LIBRARY_SERVICE_REPO_URL,
                        "error": "Failed to stash local Library-Service runtime changes before update.",
                        "dirty_worktree": True,
                        "dirty_worktree_policy": dirty_policy,
                        "is_git_repo": True,
                        "commit_before": commit_before,
                        "status": status_result,
                        "stash": stash_result,
                    }

                stash_stdout = str(stash_result.get("stdout") or "").lower()
                stash_created = "no local changes to save" not in stash_stdout

            fetch_result = self._run_command(
                [git_bin, "fetch", "--all", "--prune"],
                cwd=runtime_dir,
                timeout=900,
            )
            if not fetch_result.get("success"):
                return {
                    "success": False,
                    "error": "Failed to fetch Library-Service runtime updates.",
                    "runtime_dir": str(runtime_dir),
                    "repo_url": LIBRARY_SERVICE_REPO_URL,
                    "fetch": fetch_result,
                }

            pull_result = self._run_command(
                [git_bin, "pull", "--ff-only"],
                cwd=runtime_dir,
                timeout=900,
            )
            if not pull_result.get("success"):
                return {
                    "success": False,
                    "error": "Failed to fast-forward Library-Service runtime updates.",
                    "runtime_dir": str(runtime_dir),
                    "repo_url": LIBRARY_SERVICE_REPO_URL,
                    "pull": pull_result,
                }

            commit_after: Optional[str] = None
            commit_after_result = self._run_command([git_bin, "rev-parse", "HEAD"], cwd=runtime_dir)
            if commit_after_result.get("success"):
                commit_after = str(commit_after_result.get("stdout") or "").strip()

            return {
                "success": True,
                "runtime_dir": str(runtime_dir),
                "repo_url": LIBRARY_SERVICE_REPO_URL,
                "action": action,
                "updated": bool(commit_before and commit_after and commit_before != commit_after),
                "is_git_repo": True,
                "commit_before": commit_before,
                "commit_after": commit_after,
                "dirty_worktree": dirty_worktree,
                "dirty_worktree_policy": dirty_policy,
                "stash_created": stash_created,
                "stash_name": stash_name,
                "stash": stash_result,
                "fetch": fetch_result,
                "pull": pull_result,
            }

    async def _ensure_library_service_runtime(self, update_existing: bool = False) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self._sync_library_service_runtime,
            update_existing,
        )

    async def _perform_user_installation(
        self, user_id: str, db: AsyncSession, shared_plugin_path: Path
    ) -> Dict[str, Any]:
        del shared_plugin_path
        try:
            runtime_result = await self._ensure_library_service_runtime(
                update_existing=False
            )
            if not runtime_result.get("success"):
                return {
                    "success": False,
                    "error": runtime_result.get("error")
                    or "Failed to prepare shared Library-Service runtime.",
                    "library_service_runtime": runtime_result,
                }

            records = await self._create_database_records(user_id, db)
            if not records["success"]:
                return records

            page_result = await self._create_plugin_pages(
                user_id, db, records["modules_created"]
            )
            if not page_result.get("success"):
                plugin_id = records.get("plugin_id")
                if plugin_id:
                    rollback = await self._delete_database_records(user_id, plugin_id, db)
                    if not rollback.get("success"):
                        logger.error(
                            "Failed to rollback plugin records after page creation failure",
                            user_id=user_id,
                            error=rollback.get("error"),
                        )
                return page_result

            return {
                "success": True,
                "plugin_id": records["plugin_id"],
                "modules_created": records["modules_created"],
                "pages": page_result.get("pages", {}),
                "pages_created": page_result.get("created_count", 0),
                "library_service_runtime": runtime_result,
            }
        except Exception as error:  # pragma: no cover
            logger.error("Library plugin installation failed", error=str(error))
            return {"success": False, "error": str(error)}

    async def _perform_user_uninstallation(
        self, user_id: str, db: AsyncSession
    ) -> Dict[str, Any]:
        try:
            plugin_id = f"{user_id}_{self.plugin_data['plugin_slug']}"
            page_cleanup = await self._delete_plugin_pages(user_id, db)
            if not page_cleanup.get("success"):
                return page_cleanup

            records_cleanup = await self._delete_database_records(user_id, plugin_id, db)
            if not records_cleanup.get("success"):
                return records_cleanup

            runtime_dir = None
            try:
                runtime_dir = str(
                    self._resolve_services_runtime_dir()
                    / LIBRARY_SERVICE_RUNTIME_DIR_NAME
                )
            except Exception:
                runtime_dir = None

            return {
                "success": True,
                "deleted_page_rows": page_cleanup.get("deleted_rows", 0),
                "plugin_id": plugin_id,
                "library_service_runtime": {
                    "preserved": True,
                    "runtime_dir": runtime_dir,
                },
            }
        except Exception as error:  # pragma: no cover
            logger.error("Library plugin uninstallation failed", error=str(error))
            return {"success": False, "error": str(error)}

    async def _perform_user_update(
        self, user_id: str, db: AsyncSession, shared_plugin_path: Path
    ) -> Dict[str, Any]:
        del shared_plugin_path
        try:
            runtime_result = await self._ensure_library_service_runtime(
                update_existing=True
            )
            if not runtime_result.get("success"):
                return {
                    "success": False,
                    "error": runtime_result.get("error")
                    or "Failed to update shared Library-Service runtime.",
                    "library_service_runtime": runtime_result,
                }

            sync_result = await self._sync_records_for_update(user_id, db)
            if not sync_result.get("success"):
                return sync_result

            page_result = await self._create_plugin_pages(
                user_id, db, sync_result.get("module_ids", [])
            )
            if not page_result.get("success"):
                return page_result

            return {
                "success": True,
                "plugin_id": sync_result.get("plugin_id"),
                "module_ids": sync_result.get("module_ids", []),
                "modules_added": sync_result.get("modules_added", []),
                "pages": page_result.get("pages", {}),
                "pages_created": page_result.get("created_count", 0),
                "library_service_runtime": runtime_result,
            }
        except Exception as error:  # pragma: no cover
            logger.error("Library plugin update failed", user_id=user_id, error=str(error))
            return {"success": False, "error": str(error)}

    async def _sync_records_for_update(
        self, user_id: str, db: AsyncSession
    ) -> Dict[str, Any]:
        try:
            plugin_id = f"{user_id}_{self.plugin_data['plugin_slug']}"
            now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            plugin_check_stmt = text(
                """
                SELECT id FROM plugin
                WHERE id = :plugin_id AND user_id = :user_id
                LIMIT 1
                """
            )
            plugin_exists_result = await db.execute(
                plugin_check_stmt,
                {"plugin_id": plugin_id, "user_id": user_id},
            )
            plugin_exists = plugin_exists_result.scalar_one_or_none() is not None

            if not plugin_exists:
                return await self._create_database_records(user_id, db)

            plugin_update_stmt = text(
                """
                UPDATE plugin
                SET name = :name,
                    description = :description,
                    version = :version,
                    type = :type,
                    plugin_type = :plugin_type,
                    icon = :icon,
                    category = :category,
                    compatibility = :compatibility,
                    scope = :scope,
                    bundle_method = :bundle_method,
                    bundle_location = :bundle_location,
                    endpoints_file = :endpoints_file,
                    route_prefix = :route_prefix,
                    required_services_runtime = :required_services_runtime,
                    backend_dependencies = :backend_dependencies,
                    is_local = :is_local,
                    long_description = :long_description,
                    source_type = :source_type,
                    source_url = :source_url,
                    permissions = :permissions,
                    updated_at = :updated_at
                WHERE id = :plugin_id AND user_id = :user_id
                """
            )
            await db.execute(
                plugin_update_stmt,
                {
                    "name": self.plugin_data["name"],
                    "description": self.plugin_data["description"],
                    "version": self.plugin_data["version"],
                    "type": self.plugin_data["type"],
                    "plugin_type": self.plugin_data["plugin_type"],
                    "icon": self.plugin_data["icon"],
                    "category": self.plugin_data["category"],
                    "compatibility": self.plugin_data["compatibility"],
                    "scope": self.plugin_data["scope"],
                    "bundle_method": self.plugin_data["bundle_method"],
                    "bundle_location": self.plugin_data["bundle_location"],
                    "endpoints_file": self.plugin_data["endpoints_file"],
                    "route_prefix": self.plugin_data["route_prefix"],
                    "required_services_runtime": json.dumps(self.plugin_data["required_services_runtime"]),
                    "backend_dependencies": json.dumps(self.plugin_data["backend_dependencies"]),
                    "is_local": self.plugin_data["is_local"],
                    "long_description": self.plugin_data["long_description"],
                    "source_type": self.plugin_data["source_type"],
                    "source_url": self.plugin_data["source_url"],
                    "permissions": json.dumps(self.plugin_data["permissions"]),
                    "updated_at": now,
                    "plugin_id": plugin_id,
                    "user_id": user_id,
                },
            )

            module_check_stmt = text(
                """
                SELECT id FROM module
                WHERE id = :module_id AND user_id = :user_id
                LIMIT 1
                """
            )

            module_update_stmt = text(
                """
                UPDATE module
                SET display_name = :display_name,
                    description = :description,
                    icon = :icon,
                    category = :category,
                    priority = :priority,
                    props = :props,
                    config_fields = :config_fields,
                    messages = :messages,
                    required_services = :required_services,
                    dependencies = :dependencies,
                    layout = :layout,
                    tags = :tags,
                    updated_at = :updated_at
                WHERE id = :module_id AND user_id = :user_id
                """
            )

            module_insert_stmt = text(
                """
                INSERT INTO module (
                  id, plugin_id, name, display_name, description, icon, category,
                  enabled, priority, props, config_fields, messages,
                  required_services, dependencies, layout, tags,
                  created_at, updated_at, user_id
                ) VALUES (
                  :id, :plugin_id, :name, :display_name, :description, :icon, :category,
                  :enabled, :priority, :props, :config_fields, :messages,
                  :required_services, :dependencies, :layout, :tags,
                  :created_at, :updated_at, :user_id
                )
                """
            )

            modules_added: List[str] = []
            module_ids: List[str] = []
            for module in self.module_data:
                module_id = f"{user_id}_{self.plugin_data['plugin_slug']}_{module['name']}"
                module_ids.append(module_id)
                module_exists_result = await db.execute(
                    module_check_stmt,
                    {"module_id": module_id, "user_id": user_id},
                )
                module_exists = module_exists_result.scalar_one_or_none() is not None

                payload = {
                    "module_id": module_id,
                    "user_id": user_id,
                    "display_name": module["display_name"],
                    "description": module["description"],
                    "icon": module["icon"],
                    "category": module["category"],
                    "priority": module["priority"],
                    "props": json.dumps(module["props"]),
                    "config_fields": json.dumps(module["config_fields"]),
                    "messages": json.dumps(module.get("messages", {})),
                    "required_services": json.dumps(module["required_services"]),
                    "dependencies": json.dumps(module.get("dependencies", [])),
                    "layout": json.dumps(module["layout"]),
                    "tags": json.dumps(module["tags"]),
                    "updated_at": now,
                }

                if module_exists:
                    await db.execute(module_update_stmt, payload)
                    continue

                insert_payload = {
                    "id": module_id,
                    "plugin_id": plugin_id,
                    "name": module["name"],
                    "display_name": module["display_name"],
                    "description": module["description"],
                    "icon": module["icon"],
                    "category": module["category"],
                    "enabled": True,
                    "priority": module["priority"],
                    "props": json.dumps(module["props"]),
                    "config_fields": json.dumps(module["config_fields"]),
                    "messages": json.dumps(module.get("messages", {})),
                    "required_services": json.dumps(module["required_services"]),
                    "dependencies": json.dumps(module.get("dependencies", [])),
                    "layout": json.dumps(module["layout"]),
                    "tags": json.dumps(module["tags"]),
                    "created_at": now,
                    "updated_at": now,
                    "user_id": user_id,
                }
                await db.execute(module_insert_stmt, insert_payload)
                modules_added.append(module_id)

            await db.commit()
            return {
                "success": True,
                "plugin_id": plugin_id,
                "module_ids": module_ids,
                "modules_added": modules_added,
            }
        except Exception as error:  # pragma: no cover
            await db.rollback()
            logger.error(
                "Failed to sync Library plugin records for update",
                user_id=user_id,
                error=str(error),
            )
            return {"success": False, "error": str(error)}

    async def _create_database_records(
        self, user_id: str, db: AsyncSession
    ) -> Dict[str, Any]:
        try:
            now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            plugin_id = f"{user_id}_{self.plugin_data['plugin_slug']}"

            plugin_stmt = text(
                """
                INSERT INTO plugin (
                  id, name, description, version, type, plugin_type, enabled, icon, category,
                  status, official, author, compatibility, scope, bundle_method,
                  bundle_location, endpoints_file, route_prefix,
                  required_services_runtime, backend_dependencies,
                  is_local, long_description, plugin_slug,
                  source_type, source_url, permissions, created_at, updated_at, user_id
                ) VALUES (
                  :id, :name, :description, :version, :type, :plugin_type, :enabled, :icon, :category,
                  :status, :official, :author, :compatibility, :scope, :bundle_method,
                  :bundle_location, :endpoints_file, :route_prefix,
                  :required_services_runtime, :backend_dependencies,
                  :is_local, :long_description, :plugin_slug,
                  :source_type, :source_url, :permissions, :created_at, :updated_at, :user_id
                )
                """
            )

            await db.execute(
                plugin_stmt,
                {
                    "id": plugin_id,
                    "name": self.plugin_data["name"],
                    "description": self.plugin_data["description"],
                    "version": self.plugin_data["version"],
                    "type": self.plugin_data["type"],
                    "plugin_type": self.plugin_data["plugin_type"],
                    "enabled": True,
                    "icon": self.plugin_data["icon"],
                    "category": self.plugin_data["category"],
                    "status": "activated",
                    "official": self.plugin_data["official"],
                    "author": self.plugin_data["author"],
                    "compatibility": self.plugin_data["compatibility"],
                    "scope": self.plugin_data["scope"],
                    "bundle_method": self.plugin_data["bundle_method"],
                    "bundle_location": self.plugin_data["bundle_location"],
                    "endpoints_file": self.plugin_data["endpoints_file"],
                    "route_prefix": self.plugin_data["route_prefix"],
                    "required_services_runtime": json.dumps(self.plugin_data["required_services_runtime"]),
                    "backend_dependencies": json.dumps(self.plugin_data["backend_dependencies"]),
                    "is_local": self.plugin_data["is_local"],
                    "long_description": self.plugin_data["long_description"],
                    "plugin_slug": self.plugin_data["plugin_slug"],
                    "source_type": self.plugin_data["source_type"],
                    "source_url": self.plugin_data["source_url"],
                    "permissions": json.dumps(self.plugin_data["permissions"]),
                    "created_at": now,
                    "updated_at": now,
                    "user_id": user_id,
                },
            )

            module_stmt = text(
                """
                INSERT INTO module (
                  id, plugin_id, name, display_name, description, icon, category,
                  enabled, priority, props, config_fields, messages,
                  required_services, dependencies, layout, tags,
                  created_at, updated_at, user_id
                ) VALUES (
                  :id, :plugin_id, :name, :display_name, :description, :icon, :category,
                  :enabled, :priority, :props, :config_fields, :messages,
                  :required_services, :dependencies, :layout, :tags,
                  :created_at, :updated_at, :user_id
                )
                """
            )

            modules_created: List[str] = []
            for module in self.module_data:
                module_id = (
                    f"{user_id}_{self.plugin_data['plugin_slug']}_{module['name']}"
                )
                await db.execute(
                    module_stmt,
                    {
                        "id": module_id,
                        "plugin_id": plugin_id,
                        "name": module["name"],
                        "display_name": module["display_name"],
                        "description": module["description"],
                        "icon": module["icon"],
                        "category": module["category"],
                        "enabled": True,
                        "priority": module["priority"],
                        "props": json.dumps(module["props"]),
                        "config_fields": json.dumps(module["config_fields"]),
                        "messages": json.dumps(module.get("messages", {})),
                        "required_services": json.dumps(module["required_services"]),
                        "dependencies": json.dumps(module.get("dependencies", [])),
                        "layout": json.dumps(module["layout"]),
                        "tags": json.dumps(module["tags"]),
                        "created_at": now,
                        "updated_at": now,
                        "user_id": user_id,
                    },
                )
                modules_created.append(module_id)

            await db.commit()
            return {
                "success": True,
                "plugin_id": plugin_id,
                "modules_created": modules_created,
            }
        except Exception as error:  # pragma: no cover
            await db.rollback()
            logger.error("Failed to create Library plugin DB records", error=str(error))
            return {"success": False, "error": str(error)}

    async def _delete_database_records(
        self, user_id: str, plugin_id: str, db: AsyncSession
    ) -> Dict[str, Any]:
        try:
            module_delete = text(
                """
                DELETE FROM module
                WHERE plugin_id = :plugin_id AND user_id = :user_id
                """
            )
            await db.execute(module_delete, {"plugin_id": plugin_id, "user_id": user_id})

            plugin_delete = text(
                """
                DELETE FROM plugin
                WHERE id = :plugin_id AND user_id = :user_id
                """
            )
            await db.execute(plugin_delete, {"plugin_id": plugin_id, "user_id": user_id})

            await db.commit()
            return {"success": True}
        except Exception as error:  # pragma: no cover
            await db.rollback()
            logger.error("Failed to delete Library plugin DB records", error=str(error))
            return {"success": False, "error": str(error)}

    async def _create_plugin_pages(
        self, user_id: str, db: AsyncSession, modules_created: List[str]
    ) -> Dict[str, Any]:
        try:
            pages: Dict[str, Dict[str, Any]] = {}
            created_count = 0
            for page_spec in PAGE_SPECS:
                page_result = await self._create_single_page(
                    user_id=user_id,
                    db=db,
                    modules_created=modules_created,
                    page_spec=page_spec,
                )
                if not page_result.get("success"):
                    await db.rollback()
                    return page_result

                route = page_spec["route"]
                pages[route] = {
                    "page_id": page_result.get("page_id"),
                    "created": bool(page_result.get("created", False)),
                    "name": page_spec["name"],
                }
                if page_result.get("created"):
                    created_count += 1

            await db.commit()
            return {"success": True, "pages": pages, "created_count": created_count}
        except Exception as error:  # pragma: no cover
            await db.rollback()
            logger.error("Failed to create Library plugin pages", error=str(error))
            return {"success": False, "error": str(error)}

    async def _create_single_page(
        self,
        user_id: str,
        db: AsyncSession,
        modules_created: List[str],
        page_spec: Dict[str, Any],
    ) -> Dict[str, Any]:
        route = page_spec["route"]
        check_stmt = text(
            """
            SELECT id FROM pages
            WHERE creator_id = :user_id AND route = :route
            LIMIT 1
            """
        )
        existing_result = await db.execute(
            check_stmt,
            {"user_id": user_id, "route": route},
        )
        existing = existing_result.fetchone()
        if existing:
            existing_page_id = existing.id if hasattr(existing, "id") else existing[0]
            return {"success": True, "page_id": existing_page_id, "created": False}

        module_name = page_spec["module_name"]
        module_id = await self._resolve_module_id(user_id, db, modules_created, module_name)
        if not module_id:
            return {
                "success": False,
                "error": f"Unable to resolve {module_name} module id.",
            }

        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        content = self._build_page_content(
            module_id=module_id,
            display_name=page_spec["display_name"],
            module_name=module_name,
            module_args=page_spec.get("module_args"),
        )

        page_id = uuid.uuid4().hex
        insert_stmt = text(
            """
            INSERT INTO pages (
              id, name, route, content, creator_id,
              created_at, updated_at, is_published, publish_date, description
            ) VALUES (
              :id, :name, :route, :content, :creator_id,
              :created_at, :updated_at, :is_published, :publish_date, :description
            )
            """
        )

        await db.execute(
            insert_stmt,
            {
                "id": page_id,
                "name": page_spec["name"],
                "route": route,
                "content": json.dumps(content),
                "creator_id": user_id,
                "created_at": now,
                "updated_at": now,
                "is_published": 1,
                "publish_date": now,
                "description": page_spec.get("description", ""),
            },
        )

        return {"success": True, "page_id": page_id, "created": True}

    async def _resolve_module_id(
        self,
        user_id: str,
        db: AsyncSession,
        modules_created: List[str],
        module_name: str,
    ) -> Optional[str]:
        suffix = f"_{module_name}"
        for created_id in modules_created:
            if created_id.endswith(suffix):
                return created_id

        fallback_stmt = text(
            """
            SELECT id FROM module
            WHERE user_id = :user_id
              AND plugin_id = :plugin_id
              AND name = :name
            LIMIT 1
            """
        )
        plugin_id = f"{user_id}_{self.plugin_data['plugin_slug']}"
        fallback_result = await db.execute(
            fallback_stmt,
            {
                "user_id": user_id,
                "plugin_id": plugin_id,
                "name": module_name,
            },
        )
        fallback_row = fallback_result.fetchone()
        if fallback_row:
            return fallback_row.id if hasattr(fallback_row, "id") else fallback_row[0]
        return None

    def _build_page_content(
        self,
        module_id: str,
        display_name: str,
        module_name: str,
        module_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        timestamp_ms = int(datetime.datetime.utcnow().timestamp() * 1000)
        layout_id = f"{module_name}_{module_id}_{timestamp_ms}"
        args: Dict[str, Any] = {
            "moduleId": module_id,
            "displayName": display_name,
        }
        if isinstance(module_args, dict):
            args.update(module_args)

        desktop = {
            "i": layout_id,
            "x": 0,
            "y": 0,
            "w": 12,
            "h": 12,
            "pluginId": self.plugin_data["plugin_slug"],
            "args": dict(args),
        }
        tablet = {
            "i": layout_id,
            "x": 0,
            "y": 0,
            "w": 8,
            "h": 12,
            "pluginId": self.plugin_data["plugin_slug"],
            "args": dict(args),
        }
        mobile = {
            "i": layout_id,
            "x": 0,
            "y": 0,
            "w": 4,
            "h": 12,
            "pluginId": self.plugin_data["plugin_slug"],
            "args": dict(args),
        }
        return {
            "layouts": {
                "desktop": [desktop],
                "tablet": [tablet],
                "mobile": [mobile],
            },
            "modules": {},
        }

    async def _delete_plugin_pages(self, user_id: str, db: AsyncSession) -> Dict[str, Any]:
        try:
            delete_stmt = text(
                """
                DELETE FROM pages
                WHERE creator_id = :user_id
                AND route = :route
                """
            )
            deleted_rows = 0
            for page_spec in PAGE_SPECS:
                result = await db.execute(
                    delete_stmt,
                    {
                        "user_id": user_id,
                        "route": page_spec["route"],
                    },
                )
                deleted_rows += int(result.rowcount or 0)

            await db.commit()
            return {"success": True, "deleted_rows": deleted_rows}
        except Exception as error:  # pragma: no cover
            await db.rollback()
            logger.error("Failed to delete Library plugin pages", error=str(error))
            return {"success": False, "error": str(error)}


async def install_plugin(
    user_id: str, db: AsyncSession, plugins_base_dir: Optional[str] = None
) -> Dict[str, Any]:
    manager = BrainDriveLibraryPluginLifecycleManager(plugins_base_dir)
    return await manager.install_plugin(user_id, db)


async def delete_plugin(
    user_id: str, db: AsyncSession, plugins_base_dir: Optional[str] = None
) -> Dict[str, Any]:
    manager = BrainDriveLibraryPluginLifecycleManager(plugins_base_dir)
    return await manager.delete_plugin(user_id, db)


async def get_plugin_status(
    user_id: str, db: AsyncSession, plugins_base_dir: Optional[str] = None
) -> Dict[str, Any]:
    manager = BrainDriveLibraryPluginLifecycleManager(plugins_base_dir)
    return await manager.get_plugin_status(user_id, db)
