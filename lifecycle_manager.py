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
import inspect
import json
import os
import shutil
import subprocess
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from app.core.job_manager_provider import get_job_manager  # type: ignore
except Exception:  # pragma: no cover - fallback for remote install
    get_job_manager = None
    try:
        import sys

        _current_dir = Path(__file__).resolve().parent
        for parent in (_current_dir, *_current_dir.parents):
            if (parent / "app" / "core" / "job_manager_provider.py").exists():
                sys.path.insert(0, str(parent))
                from app.core.job_manager_provider import get_job_manager  # type: ignore
                break
    except Exception:
        get_job_manager = None

try:
    from app.utils.ollama import normalize_server_base, make_dedupe_key  # type: ignore
except Exception:  # pragma: no cover - fallback for remote install
    def normalize_server_base(url: str) -> str:
        raw = str(url or "").strip().rstrip("/")
        if raw.endswith("/api/pull"):
            raw = raw[: -len("/api/pull")]
        if raw.endswith("/api"):
            raw = raw[: -len("/api")]
        return raw

    def make_dedupe_key(server_base: str, name: str) -> str:
        return f"{server_base}|{name}"

try:
    from app.utils.json_parsing import safe_encrypted_json_parse  # type: ignore
except Exception:  # pragma: no cover - fallback for remote install
    safe_encrypted_json_parse = None

CURRENT_DIR = Path(__file__).resolve().parent

HELPER_PATH = CURRENT_DIR / "community_lifecycle_manager.py"
spec = importlib.util.spec_from_file_location(
    "library.community_lifecycle_manager", HELPER_PATH
)
helper_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper_module)
CommunityPluginLifecycleBase = helper_module.CommunityPluginLifecycleBase

SERVICE_OPS_PATH = CURRENT_DIR / "service_ops.py"
service_ops_spec = importlib.util.spec_from_file_location(
    "library.service_ops", SERVICE_OPS_PATH
)
service_ops_module = (
    importlib.util.module_from_spec(service_ops_spec)
    if service_ops_spec and service_ops_spec.loader
    else None
)
if service_ops_module is not None:
    service_ops_spec.loader.exec_module(service_ops_module)

prepare_service = (
    getattr(service_ops_module, "prepare_service", None)
    if service_ops_module is not None
    else None
)
start_service = (
    getattr(service_ops_module, "start_service", None)
    if service_ops_module is not None
    else None
)
health_check = (
    getattr(service_ops_module, "health_check", None)
    if service_ops_module is not None
    else None
)

logger = structlog.get_logger()

LIBRARY_SERVICE_REPO_URL = "https://github.com/DJJones66/Library-Service"
LIBRARY_SERVICE_RUNTIME_DIR_NAME = "Library-Service"
SERVICES_RUNTIME_ENV_VAR = "BRAINDRIVE_SERVICES_RUNTIME_DIR"
DIRTY_WORKTREE_POLICY_ENV_VAR = "BRAINDRIVE_LIBRARY_RUNTIME_DIRTY_POLICY"
DEFAULT_DIRTY_WORKTREE_POLICY = "stash"
LIBRARY_SERVICE_SETTINGS_DEFINITION_ID = "braindrive_library_service_settings"
LIBRARY_SERVICE_INSTALL_COMMAND = "python service_scripts/install_with_venv.py"
LIBRARY_SERVICE_START_COMMAND = "python service_scripts/start_with_venv.py"
LIBRARY_SERVICE_STOP_COMMAND = "python service_scripts/shutdown_with_venv.py"
LIBRARY_SERVICE_RESTART_COMMAND = "python service_scripts/restart_with_venv.py"
LIBRARY_SERVICE_DEFAULT_HOST = "127.0.0.1"
LIBRARY_SERVICE_DEFAULT_PORT = "18170"
LIBRARY_SERVICE_DEFAULT_HEALTHCHECK_URL = (
    f"http://localhost:{LIBRARY_SERVICE_DEFAULT_PORT}/health"
)
LIBRARY_SERVICE_ENV_INHERIT = "minimal"
DEFAULT_LIBRARY_REQUIRED_ENV_VARS = [
    "PROCESS_HOST",
    "PROCESS_PORT",
    "BRAINDRIVE_LIBRARY_PATH",
    "BRAINDRIVE_LIBRARY_BASE_TEMPLATE_PATH",
    "BRAINDRIVE_LIBRARY_REQUIRE_USER_HEADER",
    SERVICES_RUNTIME_ENV_VAR,
]

LIBRARY_OLLAMA_SETTINGS_DEFINITION_ID = "ollama_servers_settings"
DEFAULT_LIBRARY_OLLAMA_PREFETCH_MODEL = "hf.co/mradermacher/granite-4.0-micro-GGUF:Q8_0"
LIBRARY_PREFETCH_MODEL_ENV_VAR = "BRAINDRIVE_LIBRARY_PREFETCH_MODEL"
LIBRARY_PREFETCH_ENABLED_ENV_VAR = "BRAINDRIVE_LIBRARY_PREFETCH_ENABLED"
LIBRARY_PREFETCH_SERVER_ID_ENV_VAR = "BRAINDRIVE_LIBRARY_PREFETCH_OLLAMA_SERVER_ID"
LIBRARY_PREFETCH_SERVER_URL_ENV_VAR = "BRAINDRIVE_LIBRARY_PREFETCH_OLLAMA_SERVER_URL"


def _expand_model_tokens(model: Dict[str, Any]) -> Set[str]:
    tokens: Set[str] = set()
    for key in ("name", "model", "digest"):
        value = model.get(key)
        if value:
            tokens.add(str(value))

    aliases = model.get("aliases") or []
    if isinstance(aliases, list):
        for alias in aliases:
            if isinstance(alias, str):
                tokens.add(alias)
            elif isinstance(alias, dict):
                for value in alias.values():
                    if value:
                        tokens.add(str(value))

    expanded: Set[str] = set()
    for token in tokens:
        cleaned = str(token).strip().lower()
        if not cleaned:
            continue
        expanded.add(cleaned)
        if ":" in cleaned:
            expanded.add(cleaned.split(":", 1)[0])

    return expanded


def _build_model_token_index(payload: Dict[str, Any]) -> Set[str]:
    tokens: Set[str] = set()
    models = payload.get("models") or []
    if not isinstance(models, list):
        return tokens

    for model in models:
        if isinstance(model, dict):
            tokens.update(_expand_model_tokens(model))

    return tokens


def _model_lookup_tokens(model_name: str) -> Set[str]:
    return _expand_model_tokens({"name": model_name})

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
        lock_backend: Optional[str] = None
        lock_module = None
        try:
            if os.name == "nt":
                try:
                    import msvcrt  # type: ignore

                    # msvcrt.locking requires a non-zero byte range.
                    handle.seek(0, os.SEEK_END)
                    if handle.tell() == 0:
                        handle.write("0")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                    lock_backend = "msvcrt"
                    lock_module = msvcrt
                except Exception as error:
                    logger.warning(
                        "Library runtime lock unavailable on Windows",
                        lock_file=str(lock_file),
                        error=str(error),
                    )
            else:
                try:
                    import fcntl as locker  # type: ignore

                    locker.flock(handle.fileno(), locker.LOCK_EX)
                    lock_backend = "fcntl"
                    lock_module = locker
                except Exception as error:
                    logger.warning(
                        "Library runtime lock unavailable on POSIX",
                        lock_file=str(lock_file),
                        error=str(error),
                    )
            yield
        finally:
            if lock_backend == "msvcrt" and lock_module is not None:
                try:
                    handle.seek(0)
                    lock_module.locking(handle.fileno(), lock_module.LK_UNLCK, 1)
                except Exception:
                    pass
            elif lock_backend == "fcntl" and lock_module is not None:
                try:
                    lock_module.flock(handle.fileno(), lock_module.LOCK_UN)
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

    def _required_env_vars_by_service(self) -> Dict[str, List[str]]:
        if service_ops_module is not None:
            getter = getattr(service_ops_module, "get_required_env_vars_map", None)
            if callable(getter):
                try:
                    values = getter()
                    if isinstance(values, dict):
                        return {
                            str(key): [str(item) for item in (items or [])]
                            for key, items in values.items()
                        }
                except Exception as error:
                    logger.warning(
                        "Failed to derive Library service required_env_vars from service_ops",
                        error=str(error),
                    )

        return {"library_service": list(DEFAULT_LIBRARY_REQUIRED_ENV_VARS)}

    def _service_ops_path_for_jobs(self) -> Path:
        candidate = self.shared_path / "service_ops.py"
        if candidate.exists():
            return candidate
        return SERVICE_OPS_PATH

    @staticmethod
    def _supports_installer_user_id_kwarg(func: Any) -> bool:
        try:
            signature = inspect.signature(func)
        except Exception:
            return False

        if "installer_user_id" in signature.parameters:
            return True

        return any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )

    def _build_runtime_service_rows(self) -> List[Dict[str, Any]]:
        runtime_root = self._resolve_services_runtime_dir()
        runtime_dir = runtime_root / LIBRARY_SERVICE_RUNTIME_DIR_NAME
        required_env_vars = self._required_env_vars_by_service().get(
            "library_service", list(DEFAULT_LIBRARY_REQUIRED_ENV_VARS)
        )

        return [
            {
                "name": "library_service",
                "source_url": LIBRARY_SERVICE_REPO_URL,
                "type": "venv_process",
                "install_command": LIBRARY_SERVICE_INSTALL_COMMAND,
                "start_command": LIBRARY_SERVICE_START_COMMAND,
                "stop_command": LIBRARY_SERVICE_STOP_COMMAND,
                "restart_command": LIBRARY_SERVICE_RESTART_COMMAND,
                "healthcheck_url": LIBRARY_SERVICE_DEFAULT_HEALTHCHECK_URL,
                "definition_id": LIBRARY_SERVICE_SETTINGS_DEFINITION_ID,
                "required_env_vars": required_env_vars,
                "runtime_dir_key": LIBRARY_SERVICE_RUNTIME_DIR_NAME,
                "env_inherit": LIBRARY_SERVICE_ENV_INHERIT,
                "env_overrides": {
                    "PROCESS_HOST": LIBRARY_SERVICE_DEFAULT_HOST,
                    "PROCESS_PORT": LIBRARY_SERVICE_DEFAULT_PORT,
                    SERVICES_RUNTIME_ENV_VAR: str(runtime_root),
                    "BRAINDRIVE_LIBRARY_PATH": str((runtime_dir / "library").resolve()),
                    "BRAINDRIVE_LIBRARY_BASE_TEMPLATE_PATH": str(
                        (runtime_dir / "library_templates" / "Base_Library").resolve()
                    ),
                    "BRAINDRIVE_LIBRARY_REQUIRE_USER_HEADER": "true",
                },
            }
        ]

    async def _upsert_service_runtime_rows(
        self,
        user_id: str,
        plugin_id: str,
        db: AsyncSession,
        *,
        now: str,
    ) -> Dict[str, Any]:
        upsert_stmt = text(
            """
            INSERT INTO plugin_service_runtime (
              id, plugin_id, plugin_slug, name, source_url, type,
              install_command, start_command, stop_command, restart_command,
              healthcheck_url, definition_id, required_env_vars,
              runtime_dir_key, env_inherit, env_overrides, status,
              created_at, updated_at, user_id
            ) VALUES (
              :id, :plugin_id, :plugin_slug, :name, :source_url, :type,
              :install_command, :start_command, :stop_command, :restart_command,
              :healthcheck_url, :definition_id, :required_env_vars,
              :runtime_dir_key, :env_inherit, :env_overrides, :status,
              :created_at, :updated_at, :user_id
            )
            ON CONFLICT(id) DO UPDATE SET
              plugin_id = excluded.plugin_id,
              plugin_slug = excluded.plugin_slug,
              name = excluded.name,
              source_url = excluded.source_url,
              type = excluded.type,
              install_command = excluded.install_command,
              start_command = excluded.start_command,
              stop_command = excluded.stop_command,
              restart_command = excluded.restart_command,
              healthcheck_url = excluded.healthcheck_url,
              definition_id = excluded.definition_id,
              required_env_vars = excluded.required_env_vars,
              runtime_dir_key = excluded.runtime_dir_key,
              env_inherit = excluded.env_inherit,
              env_overrides = excluded.env_overrides,
              status = excluded.status,
              updated_at = excluded.updated_at,
              user_id = excluded.user_id
            """
        )

        created_ids: List[str] = []
        for service_data in self._build_runtime_service_rows():
            service_id = (
                f"{user_id}_{self.plugin_data['plugin_slug']}_{service_data['name']}"
            )
            payload = {
                "id": service_id,
                "plugin_id": plugin_id,
                "plugin_slug": self.plugin_data["plugin_slug"],
                "name": service_data["name"],
                "source_url": service_data.get("source_url"),
                "type": service_data.get("type"),
                "install_command": service_data.get("install_command"),
                "start_command": service_data.get("start_command"),
                "stop_command": service_data.get("stop_command"),
                "restart_command": service_data.get("restart_command"),
                "healthcheck_url": service_data.get("healthcheck_url"),
                "definition_id": service_data.get("definition_id"),
                "required_env_vars": json.dumps(
                    service_data.get("required_env_vars", [])
                ),
                "runtime_dir_key": service_data.get("runtime_dir_key"),
                "env_inherit": service_data.get("env_inherit"),
                "env_overrides": json.dumps(service_data.get("env_overrides") or {}),
                "status": "pending",
                "created_at": now,
                "updated_at": now,
                "user_id": user_id,
            }
            await db.execute(upsert_stmt, payload)
            created_ids.append(service_id)

        return {"success": True, "service_ids": created_ids}

    async def _prepare_services(self, user_id: str) -> Dict[str, Any]:
        if not callable(prepare_service):
            return {
                "skipped": True,
                "reason": "library_service_ops_missing",
            }

        skip = os.environ.get("BRAINDRIVE_LIBRARY_SKIP_SERVICE_INSTALL", "").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        full_install = os.environ.get("BRAINDRIVE_LIBRARY_FULL_INSTALL", "").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        force_recreate = os.environ.get("BRAINDRIVE_LIBRARY_FORCE_VENV", "").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        auto_start = os.environ.get("BRAINDRIVE_LIBRARY_AUTO_START", "1").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        async_mode = os.environ.get("BRAINDRIVE_LIBRARY_ASYNC_INSTALL", "0").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        use_jobs = os.environ.get("BRAINDRIVE_LIBRARY_USE_JOB_MANAGER", "1").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

        if skip:
            return {
                "skipped": True,
                "reason": "BRAINDRIVE_LIBRARY_SKIP_SERVICE_INSTALL env set",
            }

        service_keys = ["library_service"]

        if use_jobs and get_job_manager:
            try:
                job_manager = await get_job_manager()
                job, _ = await job_manager.enqueue_job(
                    job_type="service.install",
                    payload={
                        "service_ops_path": str(self._service_ops_path_for_jobs()),
                        "service_keys": service_keys,
                        "full_install": full_install,
                        "force_recreate": force_recreate,
                        "auto_start": auto_start,
                        "installer_user_id": user_id,
                        "require_user_bootstrap": True,
                    },
                    user_id=user_id,
                    workspace_id=None,
                    idempotency_key=(
                        "library_install_"
                        f"{user_id}_{self.plugin_data['version']}_{uuid.uuid4().hex}"
                    ),
                    max_retries=1,
                )
                return {
                    "skipped": False,
                    "mode": "job",
                    "job_id": job.id,
                    "service_keys": service_keys,
                }
            except Exception as error:
                logger.warning(
                    "Library service install job enqueue failed; falling back",
                    error=str(error),
                )

        async def _install_then_start(service_key: str) -> Dict[str, Any]:
            prepare_kwargs = {
                "full_install": full_install,
                "force_recreate": force_recreate,
            }
            if self._supports_installer_user_id_kwarg(prepare_service):
                prepare_kwargs["installer_user_id"] = user_id

            install_result = await prepare_service(service_key, **prepare_kwargs)
            payload: Dict[str, Any] = {
                "service": service_key,
            }
            if isinstance(install_result, dict):
                payload.update(install_result)
            else:
                payload["result"] = install_result

            if (
                auto_start
                and callable(start_service)
                and isinstance(install_result, dict)
                and bool(install_result.get("success"))
            ):
                payload["start"] = await start_service(service_key)

            return payload

        installs: List[Dict[str, Any]] = []
        if async_mode:
            loop = asyncio.get_running_loop()
            for key in service_keys:
                loop.create_task(_install_then_start(key))
                installs.append({"service": key, "scheduled": True})
            return {
                "skipped": False,
                "mode": "async",
                "installs": installs,
                "auto_start": auto_start,
            }

        for key in service_keys:
            try:
                installs.append(await _install_then_start(key))
            except Exception as error:
                installs.append(
                    {
                        "service": key,
                        "success": False,
                        "error": str(error),
                    }
                )

        return {
            "skipped": False,
            "mode": "sync",
            "installs": installs,
            "auto_start": auto_start,
        }

    def _library_prefetch_enabled(self) -> bool:
        raw = str(
            os.environ.get(LIBRARY_PREFETCH_ENABLED_ENV_VAR, "1")
            or "1"
        ).strip().lower()
        return raw in {"1", "true", "yes", "on"}

    def _library_prefetch_model_name(self) -> str:
        model_name = os.environ.get(
            LIBRARY_PREFETCH_MODEL_ENV_VAR,
            DEFAULT_LIBRARY_OLLAMA_PREFETCH_MODEL,
        )
        return str(model_name or "").strip()

    def _parse_settings_payload(
        self,
        raw_value: Any,
        *,
        setting_id: Optional[str],
        definition_id: str,
    ) -> Optional[Dict[str, Any]]:
        if isinstance(raw_value, dict):
            return raw_value

        if safe_encrypted_json_parse:
            try:
                parsed_value = safe_encrypted_json_parse(
                    raw_value,
                    context=f"library_lifecycle:{definition_id}",
                    setting_id=setting_id or "",
                    definition_id=definition_id,
                )
                if isinstance(parsed_value, dict):
                    return parsed_value
            except Exception as error:
                logger.warning(
                    "Failed to parse settings payload with safe parser",
                    definition_id=definition_id,
                    setting_id=setting_id,
                    error=str(error),
                )

        if isinstance(raw_value, str):
            try:
                parsed_value = json.loads(raw_value)
                if isinstance(parsed_value, dict):
                    return parsed_value
            except Exception as error:
                logger.warning(
                    "Failed to parse settings payload",
                    definition_id=definition_id,
                    setting_id=setting_id,
                    error=str(error),
                )

        return None

    def _select_ollama_server(
        self,
        settings_payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        override_server_url = str(
            os.environ.get(LIBRARY_PREFETCH_SERVER_URL_ENV_VAR, "") or ""
        ).strip()
        if override_server_url:
            return {
                "server_id": None,
                "server_name": "env_override",
                "server_url": normalize_server_base(override_server_url),
                "api_key": None,
                "selection_source": "env_override",
            }

        servers = settings_payload.get("servers") or []
        if not isinstance(servers, list):
            return None

        valid_servers: List[Dict[str, Any]] = []
        for server in servers:
            if not isinstance(server, dict):
                continue
            server_address = str(server.get("serverAddress") or "").strip()
            if not server_address:
                continue
            valid_servers.append(server)

        if not valid_servers:
            return None

        preferred_server_id = str(
            os.environ.get(LIBRARY_PREFETCH_SERVER_ID_ENV_VAR, "") or ""
        ).strip()
        selected: Optional[Dict[str, Any]] = None

        if preferred_server_id:
            for server in valid_servers:
                if str(server.get("id") or "").strip() == preferred_server_id:
                    selected = server
                    break

        if selected is None:
            for server in valid_servers:
                if str(server.get("connectionStatus") or "").strip().lower() == "connected":
                    selected = server
                    break

        if selected is None:
            selected = valid_servers[0]

        server_address = normalize_server_base(str(selected.get("serverAddress") or "").strip())
        if not server_address:
            return None

        return {
            "server_id": str(selected.get("id") or "").strip() or None,
            "server_name": str(selected.get("serverName") or "").strip() or "ollama",
            "server_url": server_address,
            "api_key": str(selected.get("apiKey") or "").strip() or None,
            "selection_source": "settings",
        }

    async def _resolve_ollama_server_for_prefetch(
        self,
        user_id: str,
        db: AsyncSession,
    ) -> Optional[Dict[str, Any]]:
        stmt = text(
            """
            SELECT id, value
            FROM settings_instances
            WHERE definition_id = :definition_id
              AND user_id = :user_id
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        result = await db.execute(
            stmt,
            {
                "definition_id": LIBRARY_OLLAMA_SETTINGS_DEFINITION_ID,
                "user_id": user_id,
            },
        )
        row = result.first()
        if row is None:
            return None

        row_map = row._mapping if hasattr(row, "_mapping") else {}
        payload = self._parse_settings_payload(
            row_map.get("value") if row_map else None,
            setting_id=row_map.get("id") if row_map else None,
            definition_id=LIBRARY_OLLAMA_SETTINGS_DEFINITION_ID,
        )
        if not payload:
            return None

        return self._select_ollama_server(payload)

    async def _check_ollama_server_health(
        self,
        client: httpx.AsyncClient,
        server_base: str,
        headers: Dict[str, str],
    ) -> bool:
        try:
            response = await client.get(f"{server_base}/api/version", headers=headers)
            return response.status_code == 200
        except Exception as error:
            logger.warning(
                "Library model prefetch health check failed",
                server_url=server_base,
                error=str(error),
            )
            return False

    async def _fetch_ollama_tags(
        self,
        client: httpx.AsyncClient,
        server_base: str,
        headers: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        try:
            response = await client.get(f"{server_base}/api/tags", headers=headers)
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else None
        except Exception as error:
            logger.warning(
                "Library model prefetch tags lookup failed",
                server_url=server_base,
                error=str(error),
            )
            return None

    async def _enqueue_library_ollama_prefetch(
        self,
        user_id: str,
        db: AsyncSession,
    ) -> Dict[str, Any]:
        model_name = self._library_prefetch_model_name()
        if not self._library_prefetch_enabled():
            return {
                "status": "skipped_unconfigured",
                "reason": "prefetch_disabled",
                "model_name": model_name,
            }

        if not model_name:
            return {
                "status": "skipped_unconfigured",
                "reason": "model_name_missing",
            }

        server = await self._resolve_ollama_server_for_prefetch(user_id, db)
        if not server:
            return {
                "status": "skipped_unconfigured",
                "reason": "ollama_server_not_configured",
                "model_name": model_name,
            }

        server_url = str(server.get("server_url") or "").strip()
        server_base = normalize_server_base(server_url)
        if not server_base.startswith(("http://", "https://")):
            return {
                "status": "skipped_unconfigured",
                "reason": "invalid_server_url",
                "model_name": model_name,
                "server_url": server_url,
            }

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        api_key = server.get("api_key")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
            is_healthy = await self._check_ollama_server_health(client, server_base, headers)
            if not is_healthy:
                return {
                    "status": "skipped_unhealthy",
                    "reason": "ollama_health_check_failed",
                    "model_name": model_name,
                    "server_url": server_base,
                    "server_id": server.get("server_id"),
                    "server_name": server.get("server_name"),
                }

            tags_payload = await self._fetch_ollama_tags(client, server_base, headers)
            if tags_payload:
                installed_tokens = _build_model_token_index(tags_payload)
                target_tokens = _model_lookup_tokens(model_name)
                if target_tokens.intersection(installed_tokens):
                    return {
                        "status": "skipped_already_installed",
                        "reason": "model_present",
                        "model_name": model_name,
                        "server_url": server_base,
                        "server_id": server.get("server_id"),
                        "server_name": server.get("server_name"),
                    }

        if not get_job_manager:
            return {
                "status": "enqueue_failed",
                "reason": "job_manager_unavailable",
                "model_name": model_name,
                "server_url": server_base,
                "server_id": server.get("server_id"),
                "server_name": server.get("server_name"),
            }

        idempotency_key = (
            f"library_prefetch_{user_id}_{self.plugin_data['plugin_slug']}_"
            f"{make_dedupe_key(server_base, model_name)}"
        )

        try:
            job_manager = await get_job_manager()
            job, created = await job_manager.enqueue_job(
                job_type="ollama.install",
                payload={
                    "model_name": model_name,
                    "server_url": server_base,
                    "force_reinstall": False,
                },
                user_id=user_id,
                workspace_id=None,
                idempotency_key=idempotency_key,
                max_retries=1,
            )
            return {
                "status": "queued",
                "job_id": job.id,
                "deduped": not created,
                "model_name": model_name,
                "server_url": server_base,
                "server_id": server.get("server_id"),
                "server_name": server.get("server_name"),
                "idempotency_key": idempotency_key,
            }
        except Exception as error:
            logger.warning(
                "Failed to enqueue Library model prefetch",
                user_id=user_id,
                model_name=model_name,
                server_url=server_base,
                error=str(error),
            )
            return {
                "status": "enqueue_failed",
                "reason": "enqueue_error",
                "error": str(error),
                "model_name": model_name,
                "server_url": server_base,
                "server_id": server.get("server_id"),
                "server_name": server.get("server_name"),
            }

    async def _collect_service_health(self) -> Dict[str, Any]:
        if not callable(health_check):
            return {
                "skipped": True,
                "reason": "library_service_ops_missing_health_check",
            }

        try:
            status = await health_check("library_service")
            return {
                "skipped": False,
                "services": [{"service": "library_service", **status}],
            }
        except Exception as error:
            return {
                "skipped": False,
                "services": [
                    {
                        "service": "library_service",
                        "success": False,
                        "error": str(error),
                    }
                ],
            }

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

            service_installs = await self._prepare_services(user_id)
            model_prefetch = await self._enqueue_library_ollama_prefetch(user_id, db)
            service_health = None
            if service_installs.get("mode") == "sync" and service_installs.get("auto_start"):
                service_health = await self._collect_service_health()

            return {
                "success": True,
                "plugin_id": records["plugin_id"],
                "modules_created": records["modules_created"],
                "service_runtime_rows": records.get("service_runtime_rows", []),
                "pages": page_result.get("pages", {}),
                "pages_created": page_result.get("created_count", 0),
                "library_service_runtime": runtime_result,
                "service_installs": service_installs,
                "model_prefetch": model_prefetch,
                "service_health": service_health,
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

            service_installs = await self._prepare_services(user_id)
            model_prefetch = await self._enqueue_library_ollama_prefetch(user_id, db)
            service_health = None
            if service_installs.get("mode") == "sync" and service_installs.get("auto_start"):
                service_health = await self._collect_service_health()

            return {
                "success": True,
                "plugin_id": sync_result.get("plugin_id"),
                "module_ids": sync_result.get("module_ids", []),
                "modules_added": sync_result.get("modules_added", []),
                "service_runtime_rows": sync_result.get("service_runtime_rows", []),
                "pages": page_result.get("pages", {}),
                "pages_created": page_result.get("created_count", 0),
                "library_service_runtime": runtime_result,
                "service_installs": service_installs,
                "model_prefetch": model_prefetch,
                "service_health": service_health,
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

            service_rows_result = await self._upsert_service_runtime_rows(
                user_id=user_id,
                plugin_id=plugin_id,
                db=db,
                now=now,
            )

            await db.commit()
            return {
                "success": True,
                "plugin_id": plugin_id,
                "module_ids": module_ids,
                "modules_added": modules_added,
                "service_runtime_rows": service_rows_result.get("service_ids", []),
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

            service_rows_result = await self._upsert_service_runtime_rows(
                user_id=user_id,
                plugin_id=plugin_id,
                db=db,
                now=now,
            )

            await db.commit()
            return {
                "success": True,
                "plugin_id": plugin_id,
                "modules_created": modules_created,
                "service_runtime_rows": service_rows_result.get("service_ids", []),
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

            service_delete = text(
                """
                DELETE FROM plugin_service_runtime
                WHERE plugin_id = :plugin_id AND user_id = :user_id
                """
            )
            await db.execute(service_delete, {"plugin_id": plugin_id, "user_id": user_id})

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
