from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from app.core.user_initializer.library_template import resolve_library_root_path
from app.core.database import get_db
from app.plugins.decorators import PluginRequest, plugin_endpoint
from app.services.mcp_registry_service import MCPRegistryService

PLUGIN_SLUG = "BrainDriveLibraryPlugin"
DEFAULT_LIFECYCLE = "active"
EDITOR_MAX_FILE_BYTES = 2 * 1024 * 1024
EDITOR_ALLOWED_EXTENSIONS = {
    ".md",
    ".markdown",
    ".txt",
    ".json",
    ".yaml",
    ".yml",
}
EDITOR_ALLOWED_EXTENSIONS_LABEL = ", ".join(sorted(EDITOR_ALLOWED_EXTENSIONS))
_VALID_USER_ID = re.compile(r"^[A-Za-z0-9_]{3,128}$")

LIFE_TOPIC_ALIASES: Dict[str, str] = {
    "whyfinder": "whyfinder",
    "why": "whyfinder",
    "career": "career",
    "finance": "finances",
    "finances": "finances",
    "fitness": "fitness",
    "relationship": "relationships",
    "relationships": "relationships",
    "people": "relationships",
}

LIFE_TOPIC_TITLES: Dict[str, str] = {
    "finances": "Finances",
    "fitness": "Fitness",
    "relationships": "Relationships",
    "career": "Career",
    "whyfinder": "WhyFinder",
}


def _normalize_user_id(raw_user_id: Any) -> str:
    normalized = _normalize_query_value(raw_user_id).replace("-", "")
    if not normalized or not _VALID_USER_ID.fullmatch(normalized):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Invalid user identity for library scope.",
            },
        )
    return normalized


def _normalize_editor_path(raw_path: Any) -> str:
    normalized = _normalize_query_value(raw_path).replace("\\", "/").strip("/")
    if not normalized:
        return ""

    candidate = PurePosixPath(normalized)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Path traversal is not allowed.",
                "path": str(raw_path),
            },
        )

    return "/".join(part for part in candidate.parts if part not in {"", "."})


def _contains_symlink_path(library_root: Path, normalized_path: str) -> bool:
    if not normalized_path:
        return False

    current = library_root
    for segment in PurePosixPath(normalized_path).parts:
        current = current / segment
        if current.exists() and current.is_symlink():
            return True
    return False


def _resolve_scoped_library_root(user_id: str) -> Path:
    base_root = resolve_library_root_path()
    scoped_root = base_root / "users" / _normalize_user_id(user_id)
    scoped_root.mkdir(parents=True, exist_ok=True)
    return scoped_root


def _resolve_editor_target(library_root: Path, raw_path: Any) -> tuple[Path, str]:
    normalized_path = _normalize_editor_path(raw_path)
    if _contains_symlink_path(library_root, normalized_path):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Symlinked paths are not allowed.",
                "path": str(raw_path),
            },
        )

    if normalized_path:
        target = library_root.joinpath(*PurePosixPath(normalized_path).parts)
    else:
        target = library_root

    resolved_root = library_root.resolve()
    resolved_target = target.resolve(strict=False)

    try:
        resolved_target.relative_to(resolved_root)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Path is outside of the scoped library root.",
                "path": str(raw_path),
            },
        ) from exc

    return target, normalized_path


def _to_iso8601(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat().replace("+00:00", "Z")


def _safe_stat(path: Path) -> tuple[int, str]:
    metadata = path.stat()
    return int(metadata.st_size), _to_iso8601(metadata.st_mtime)


def _is_supported_extension(path: Path) -> bool:
    return path.suffix.lower() in EDITOR_ALLOWED_EXTENSIONS


def _editor_item_payload(library_root: Path, item: Path) -> Dict[str, Any]:
    relative_path = item.relative_to(library_root).as_posix()
    is_directory = item.is_dir()
    extension = "" if is_directory else item.suffix.lower()
    size, modified_at = _safe_stat(item)

    return {
        "name": item.name,
        "path": relative_path,
        "type": "directory" if is_directory else "file",
        "extension": extension,
        "size": 0 if is_directory else size,
        "supported": is_directory or extension in EDITOR_ALLOWED_EXTENSIONS,
        "modified_at": modified_at,
    }


def _atomic_write_text(target_path: Path, content: str) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(
        f".{target_path.name}.tmp-{uuid.uuid4().hex}"
    )
    try:
        temp_path.write_text(content, encoding="utf-8")
        temp_path.replace(target_path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _decode_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Only UTF-8 text files are supported in Library Editor.",
                "path": path.name,
            },
        ) from exc


def _normalize_query_value(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _normalize_lifecycle(value: Any) -> str:
    normalized = _normalize_query_value(value).lower()
    return normalized or DEFAULT_LIFECYCLE


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    slug = slug.strip("-")
    return re.sub(r"-{2,}", "-", slug)


def _resolve_projects_path(raw_path: Any, raw_scope: Any, lifecycle: str) -> str:
    requested_path = _normalize_query_value(raw_path).replace("\\", "/").strip("/")
    requested_scope = _normalize_query_value(raw_scope).lower()

    if requested_path:
        return requested_path
    if requested_scope == "life":
        return "life"
    return f"projects/{lifecycle}"


def _tool_error(execution: Dict[str, Any]) -> Dict[str, Any]:
    error = execution.get("error")
    return error if isinstance(error, dict) else {}


def _tool_error_code(execution: Dict[str, Any]) -> str:
    error = _tool_error(execution)
    code = error.get("code")
    if isinstance(code, str) and code.strip():
        return code.strip()
    return ""


def _nested_tool_error(execution: Dict[str, Any]) -> Dict[str, Any]:
    error = _tool_error(execution)
    details = error.get("details")
    if not isinstance(details, dict):
        return {}
    nested_error = details.get("error")
    return nested_error if isinstance(nested_error, dict) else {}


def _nested_tool_error_code(execution: Dict[str, Any]) -> str:
    nested_error = _nested_tool_error(execution)
    code = nested_error.get("code")
    if isinstance(code, str) and code.strip():
        return code.strip()
    return ""


def _is_tool_not_allowed(execution: Dict[str, Any]) -> bool:
    return _tool_error_code(execution) == "TOOL_NOT_ALLOWED"


def _is_not_found(execution: Dict[str, Any]) -> bool:
    code = _tool_error_code(execution)
    nested_code = _nested_tool_error_code(execution)
    return code == "FILE_NOT_FOUND" or nested_code == "FILE_NOT_FOUND"


def _is_project_exists(execution: Dict[str, Any]) -> bool:
    code = _tool_error_code(execution)
    nested_code = _nested_tool_error_code(execution)
    return code == "PROJECT_EXISTS" or nested_code == "PROJECT_EXISTS"


def _extract_tool_payload(execution: Dict[str, Any]) -> Dict[str, Any]:
    payload = execution.get("data")
    if not isinstance(payload, dict):
        return {}
    nested = payload.get("data")
    if isinstance(nested, dict):
        return nested
    return payload


async def _execute_tool_with_resync(
    service: MCPRegistryService,
    *,
    user_id: str,
    tool_name: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    execution = await service.execute_tool_call(user_id, tool_name, arguments)
    if execution.get("ok"):
        return execution
    if not _is_tool_not_allowed(execution):
        return execution

    for plugin_filter in (PLUGIN_SLUG, None):
        try:
            await service.sync_user_servers(user_id, plugin_slug_filter=plugin_filter)
        except Exception:
            # Best effort sync; preserve the original execution error if retries fail.
            pass

        execution = await service.execute_tool_call(user_id, tool_name, arguments)
        if execution.get("ok"):
            return execution
        if not _is_tool_not_allowed(execution):
            return execution

    return execution


async def _execute_library_tool(
    user_id: str,
    *,
    tool_name: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    async for db in get_db():
        service = MCPRegistryService(db)
        return await _execute_tool_with_resync(
            service,
            user_id=user_id,
            tool_name=tool_name,
            arguments=arguments,
        )

    return {
        "ok": False,
        "error": {
            "code": "DB_UNAVAILABLE",
            "message": "Database session unavailable while resolving library scope.",
        },
    }


def _build_library_entry(raw: Dict[str, Any], *, requested_path: str, lifecycle: str, force_scope_root: Optional[str] = None) -> Optional[Dict[str, Any]]:
    raw_path = _normalize_query_value(raw.get("path")).replace("\\", "/").strip("/")
    raw_name = _normalize_query_value(raw.get("name"))

    if not raw_path and raw_name:
        slug_guess = _slugify(raw_name)
        if requested_path == "life":
            raw_path = f"life/{slug_guess}"
        else:
            raw_path = f"projects/{lifecycle}/{slug_guess}"

    if not raw_path:
        return None

    if force_scope_root:
        scope_root = force_scope_root
    elif raw_path.startswith("life/"):
        scope_root = "life"
    else:
        scope_root = "projects"

    if not raw_name:
        raw_name = PurePosixPath(raw_path).name

    slug = _slugify(PurePosixPath(raw_path).name or raw_name)
    if not slug:
        return None

    lifecycle_value = lifecycle or DEFAULT_LIFECYCLE
    if scope_root == "projects":
        parts = raw_path.split("/")
        if len(parts) >= 3 and parts[0] == "projects" and parts[1]:
            lifecycle_value = parts[1]
    else:
        lifecycle_value = DEFAULT_LIFECYCLE

    return {
        "name": raw_name,
        "slug": slug,
        "lifecycle": lifecycle_value,
        "path": raw_path,
        "scope_root": scope_root,
        "has_agent_md": False,
        "has_spec": False,
        "has_build_plan": False,
        "has_decisions": False,
    }


def _build_projects_payload(
    raw_projects: List[Dict[str, Any]],
    *,
    requested_path: str,
    lifecycle: str,
    force_scope_root: Optional[str] = None,
) -> List[Dict[str, Any]]:
    projects: List[Dict[str, Any]] = []
    for raw in raw_projects:
        if not isinstance(raw, dict):
            continue
        entry = _build_library_entry(
            raw,
            requested_path=requested_path,
            lifecycle=lifecycle,
            force_scope_root=force_scope_root,
        )
        if entry:
            projects.append(entry)

    projects.sort(key=lambda item: item.get("name", "").lower())
    return projects


def _build_context_candidate_paths(slug: str, lifecycle: str) -> List[str]:
    normalized_slug = slug.strip().replace("\\", "/").strip("/")
    if not normalized_slug:
        return []

    if "/" in normalized_slug:
        return [normalized_slug]

    candidates = [f"projects/{lifecycle}/{normalized_slug}"]
    if lifecycle != DEFAULT_LIFECYCLE:
        candidates.append(f"projects/{DEFAULT_LIFECYCLE}/{normalized_slug}")
    candidates.append(f"projects/{normalized_slug}")

    deduped: List[str] = []
    for candidate in candidates:
        if candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _normalize_context_files(raw_files: Any) -> Dict[str, Dict[str, Any]]:
    files: Dict[str, Dict[str, Any]] = {}
    if not isinstance(raw_files, list):
        return files

    for raw in raw_files:
        if not isinstance(raw, dict):
            continue

        path = _normalize_query_value(raw.get("path"))
        key = PurePosixPath(path).name if path else _normalize_query_value(raw.get("name"))
        content = raw.get("content")
        if not key or not isinstance(content, str):
            continue

        files[key] = {
            "content": content,
            "size": len(content.encode("utf-8")),
        }

    return files


def _normalize_life_topic_slug(value: str) -> Optional[str]:
    token = re.sub(r"[^a-z0-9]+", "", _normalize_query_value(value).lower())
    if not token:
        return None
    return LIFE_TOPIC_ALIASES.get(token) or _slugify(token)


def _normalize_scope_path(value: Any) -> Optional[str]:
    raw = _normalize_query_value(value).replace("\\", "/").strip("/")
    if not raw:
        return None

    parts = [part.strip() for part in raw.split("/") if part.strip()]
    if not parts:
        return None

    head = parts[0].lower()
    if head == "life":
        if len(parts) < 2:
            return None
        topic = _normalize_life_topic_slug(parts[1])
        return f"life/{topic}" if topic else None

    if head in {"project", "projects"}:
        if head == "project":
            if len(parts) < 2:
                return None
            slug = _slugify(parts[1])
            return f"projects/{DEFAULT_LIFECYCLE}/{slug}" if slug else None

        if len(parts) >= 3:
            lifecycle = _slugify(parts[1])
            slug = _slugify(parts[2])
            if lifecycle and slug:
                return f"projects/{lifecycle}/{slug}"
            return None

        if len(parts) == 2:
            slug = _slugify(parts[1])
            return f"projects/{DEFAULT_LIFECYCLE}/{slug}" if slug else None

    if len(parts) == 1:
        slug = _slugify(parts[0])
        return f"projects/{DEFAULT_LIFECYCLE}/{slug}" if slug else None

    return None


def _scope_parts(scope_path: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    normalized = _normalize_scope_path(scope_path)
    if not normalized:
        return None, None

    parts = normalized.split("/")
    if len(parts) < 2:
        return None, None

    if parts[0] == "life":
        return "life", parts[1]

    if parts[0] == "projects":
        return "projects", parts[2] if len(parts) >= 3 else parts[1]

    return None, None


def _extract_tasks_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = [payload.get("tasks"), payload.get("items"), payload.get("data")]
    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    return []


def _compact_scope_task(task: Dict[str, Any]) -> Dict[str, Any]:
    scope_path = _normalize_scope_path(
        task.get("scopePath")
        or task.get("scope_path")
        or task.get("path")
        or task.get("scope")
    )
    scope_root, scope_name = _scope_parts(scope_path)

    priority = task.get("priority")
    due = task.get("due")
    title = task.get("title")

    return {
        "id": task.get("id"),
        "title": title.strip() if isinstance(title, str) else "",
        "priority": str(priority).strip() if isinstance(priority, str) and str(priority).strip() else None,
        "due": due.strip() if isinstance(due, str) and due.strip() else None,
        "scope_path": scope_path,
        "scope_root": scope_root,
        "scope_name": scope_name,
    }


def _build_warning(execution: Dict[str, Any], *, code: str, message: str) -> Dict[str, str]:
    error = _tool_error(execution)
    nested = _nested_tool_error(execution)

    resolved_code = code
    resolved_message = message

    nested_code = nested.get("code") if isinstance(nested.get("code"), str) else None
    nested_message = nested.get("message") if isinstance(nested.get("message"), str) else None
    error_code = error.get("code") if isinstance(error.get("code"), str) else None
    error_message = error.get("message") if isinstance(error.get("message"), str) else None

    if nested_code and nested_code.strip():
        resolved_code = nested_code.strip()
    elif error_code and error_code.strip():
        resolved_code = error_code.strip()

    if nested_message and nested_message.strip():
        resolved_message = nested_message.strip()
    elif error_message and error_message.strip():
        resolved_message = error_message.strip()

    return {
        "code": resolved_code,
        "message": resolved_message,
    }


def _raise_tool_error(execution: Dict[str, Any], default_message: str) -> None:
    status_code = 502
    if _is_not_found(execution):
        status_code = 404

    error = _tool_error(execution)
    nested_error = _nested_tool_error(execution)

    detail: Dict[str, Any] = {
        "success": False,
        "message": default_message,
    }
    if error:
        detail["error"] = error
        if isinstance(error.get("message"), str) and error["message"].strip():
            detail["message"] = error["message"].strip()
    if nested_error:
        detail["nested_error"] = nested_error
        if isinstance(nested_error.get("message"), str) and nested_error["message"].strip():
            detail["message"] = nested_error["message"].strip()

    raise HTTPException(status_code=status_code, detail=detail)


async def _read_json_payload(request: PluginRequest) -> Dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Request body must be valid JSON.",
            },
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Request body must be a JSON object.",
            },
        )

    return payload


@plugin_endpoint("/library/projects", methods=["GET"])
async def list_library_projects(request: PluginRequest) -> Dict[str, Any]:
    query = request.request.query_params
    lifecycle = _normalize_lifecycle(query.get("lifecycle"))
    requested_path = _resolve_projects_path(query.get("path"), query.get("scope"), lifecycle)

    execution = await _execute_library_tool(
        request.user_id,
        tool_name="list_projects",
        arguments={"path": requested_path},
    )

    if execution.get("ok"):
        payload = _extract_tool_payload(execution)
        raw_projects = payload.get("projects") if isinstance(payload.get("projects"), list) else []
    elif _is_not_found(execution):
        raw_projects = []
    else:
        _raise_tool_error(execution, "Unable to load library projects.")

    force_scope_root = "life" if requested_path == "life" else None
    projects = _build_projects_payload(
        raw_projects,
        requested_path=requested_path,
        lifecycle=lifecycle,
        force_scope_root=force_scope_root,
    )

    return {
        "success": True,
        "projects": projects,
        "count": len(projects),
        "path": requested_path,
        "lifecycle": lifecycle,
    }


@plugin_endpoint("/library/projects", methods=["POST"])
async def create_library_project(request: PluginRequest) -> Dict[str, Any]:
    payload = await _read_json_payload(request)
    lifecycle = _slugify(_normalize_lifecycle(payload.get("lifecycle"))) or DEFAULT_LIFECYCLE
    requested_name = _normalize_query_value(payload.get("name"))
    requested_slug = _slugify(_normalize_query_value(payload.get("slug")))
    requested_path = _normalize_scope_path(payload.get("path"))

    if requested_path:
        if not requested_path.startswith("projects/"):
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "message": "Only projects scope paths are supported.",
                    "path": requested_path,
                },
            )
        target_path = requested_path
        parts = target_path.split("/")
        if len(parts) >= 3 and parts[1]:
            lifecycle = _slugify(parts[1]) or lifecycle
        slug = _slugify(parts[-1])
    else:
        slug = requested_slug or _slugify(requested_name)
        if not slug:
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "message": "Project slug or name is required.",
                },
            )
        target_path = f"projects/{lifecycle}/{slug}"

    execution = await _execute_library_tool(
        request.user_id,
        tool_name="create_project_scaffold",
        arguments={"path": target_path},
    )

    created = False
    if execution.get("ok"):
        created = True
    elif not _is_project_exists(execution):
        _raise_tool_error(execution, "Unable to create library project.")

    raw_name = requested_name or slug.replace("-", " ").title()
    project_entry = _build_library_entry(
        {"name": raw_name, "path": target_path},
        requested_path=f"projects/{lifecycle}",
        lifecycle=lifecycle,
        force_scope_root="projects",
    )

    return {
        "success": True,
        "created": created,
        "existing": not created,
        "path": target_path,
        "slug": slug,
        "lifecycle": lifecycle,
        "project": project_entry,
    }


@plugin_endpoint("/library/life", methods=["GET"])
async def list_life_scopes(request: PluginRequest) -> Dict[str, Any]:
    execution = await _execute_library_tool(
        request.user_id,
        tool_name="list_projects",
        arguments={"path": "life"},
    )

    if execution.get("ok"):
        payload = _extract_tool_payload(execution)
        raw_scopes = payload.get("projects") if isinstance(payload.get("projects"), list) else []
    elif _is_not_found(execution):
        raw_scopes = []
    else:
        _raise_tool_error(execution, "Unable to load life scopes.")

    scopes = _build_projects_payload(
        raw_scopes,
        requested_path="life",
        lifecycle=DEFAULT_LIFECYCLE,
        force_scope_root="life",
    )

    return {
        "success": True,
        "life": scopes,
        "count": len(scopes),
        "path": "life",
    }


@plugin_endpoint("/library/scope/status", methods=["GET"])
async def get_library_scope_status(request: PluginRequest) -> Dict[str, Any]:
    query = request.request.query_params
    scope_path = _normalize_scope_path(query.get("scope") or query.get("path"))
    if not scope_path:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "A valid scope path is required.",
            },
        )

    scope_root, scope_name = _scope_parts(scope_path)
    if not scope_root or not scope_name:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Unable to resolve scope root/name from the provided path.",
                "scope_path": scope_path,
            },
        )

    warnings: List[Dict[str, str]] = []

    open_tasks: List[Dict[str, Any]] = []
    tasks_execution = await _execute_library_tool(
        request.user_id,
        tool_name="list_tasks",
        arguments={"scope": scope_path, "status": "open"},
    )
    if tasks_execution.get("ok"):
        payload = _extract_tool_payload(tasks_execution)
        open_tasks = [_compact_scope_task(task) for task in _extract_tasks_payload(payload)]
    elif not _is_not_found(tasks_execution):
        warnings.append(
            _build_warning(
                tasks_execution,
                code="OPEN_TASKS_UNAVAILABLE",
                message="Unable to load open tasks for this scope.",
            )
        )

    onboarding: Optional[Dict[str, Any]] = None
    if scope_root == "life":
        topic_slug = _normalize_life_topic_slug(scope_name) or scope_name
        topic_title = LIFE_TOPIC_TITLES.get(topic_slug, topic_slug.replace("-", " ").title())
        onboarding = {
            "topic": topic_slug,
            "title": topic_title,
            "status": "unknown",
            "needs_interview": False,
            "start_prompt": f"Start my {topic_title} onboarding interview.",
        }

        onboarding_execution = await _execute_library_tool(
            request.user_id,
            tool_name="get_onboarding_state",
            arguments={},
        )
        if onboarding_execution.get("ok"):
            payload = _extract_tool_payload(onboarding_execution)
            state = payload.get("state") if isinstance(payload.get("state"), dict) else {}
            starter_topics = state.get("starter_topics") if isinstance(state.get("starter_topics"), dict) else {}
            topic_status = starter_topics.get(topic_slug)
            if isinstance(topic_status, str) and topic_status.strip():
                normalized_status = topic_status.strip().lower()
                onboarding["status"] = normalized_status
                onboarding["needs_interview"] = normalized_status in {"not_started", "in_progress"}
        elif not _is_not_found(onboarding_execution):
            warnings.append(
                _build_warning(
                    onboarding_execution,
                    code="ONBOARDING_STATE_UNAVAILABLE",
                    message="Unable to load onboarding state for this life topic.",
                )
            )

    return {
        "success": True,
        "scope_path": scope_path,
        "scope_root": scope_root,
        "scope_name": scope_name,
        "onboarding": onboarding,
        "open_tasks": {
            "count": len(open_tasks),
            "tasks": open_tasks,
        },
        "warnings": warnings,
    }


@plugin_endpoint("/library/project/{slug}/context", methods=["GET"])
async def get_project_context(request: PluginRequest) -> Dict[str, Any]:
    query = request.request.query_params
    lifecycle = _normalize_lifecycle(query.get("lifecycle"))
    slug = _normalize_query_value(request.request.path_params.get("slug"))
    if not slug:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Project slug is required.",
            },
        )

    candidates = _build_context_candidate_paths(slug, lifecycle)
    if not candidates:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Project slug is required.",
            },
        )

    for candidate_path in candidates:
        execution = await _execute_library_tool(
            request.user_id,
            tool_name="project_context",
            arguments={"path": candidate_path},
        )
        if execution.get("ok"):
            payload = _extract_tool_payload(execution)
            files = _normalize_context_files(payload.get("files"))
            return {
                "success": True,
                "project": slug,
                "lifecycle": lifecycle,
                "path": candidate_path,
                "files": files,
            }
        if _is_not_found(execution):
            continue
        _raise_tool_error(execution, "Unable to load project context.")

    raise HTTPException(
        status_code=404,
        detail={
            "success": False,
            "message": "Project context not found.",
            "project": slug,
            "lifecycle": lifecycle,
        },
    )


@plugin_endpoint("/library/editor/tree", methods=["GET"])
async def list_library_editor_tree(request: PluginRequest) -> Dict[str, Any]:
    query = request.request.query_params
    raw_path = query.get("path")

    try:
        library_root = _resolve_scoped_library_root(request.user_id)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Unable to resolve configured library root path.",
            },
        ) from exc

    target_path, normalized_path = _resolve_editor_target(library_root, raw_path)

    if not target_path.exists():
        raise HTTPException(
            status_code=404,
            detail={
                "success": False,
                "message": "Requested directory does not exist.",
                "path": normalized_path,
            },
        )

    if not target_path.is_dir():
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Path must reference a directory.",
                "path": normalized_path,
            },
        )

    items: List[Dict[str, Any]] = []
    for child in sorted(
        target_path.iterdir(),
        key=lambda entry: (entry.is_file(), entry.name.lower()),
    ):
        items.append(_editor_item_payload(library_root, child))

    parent_path: Optional[str] = None
    if normalized_path:
        parent = PurePosixPath(normalized_path).parent
        parent_path = "" if str(parent) == "." else parent.as_posix()

    return {
        "success": True,
        "path": normalized_path,
        "parent_path": parent_path,
        "count": len(items),
        "items": items,
    }


@plugin_endpoint("/library/editor/file", methods=["GET"])
async def read_library_editor_file(request: PluginRequest) -> Dict[str, Any]:
    raw_path = request.request.query_params.get("path")
    if not _normalize_query_value(raw_path):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "File path is required.",
            },
        )

    try:
        library_root = _resolve_scoped_library_root(request.user_id)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Unable to resolve configured library root path.",
            },
        ) from exc

    target_path, normalized_path = _resolve_editor_target(library_root, raw_path)

    if not target_path.exists():
        raise HTTPException(
            status_code=404,
            detail={
                "success": False,
                "message": "Requested file does not exist.",
                "path": normalized_path,
            },
        )

    if not target_path.is_file():
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Path must reference a file.",
                "path": normalized_path,
            },
        )

    if not _is_supported_extension(target_path):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": (
                    "Unsupported file type for editor preview. "
                    f"Supported file types: {EDITOR_ALLOWED_EXTENSIONS_LABEL}."
                ),
                "path": normalized_path,
            },
        )

    content = _decode_text_file(target_path)
    size, modified_at = _safe_stat(target_path)
    extension = target_path.suffix.lower()

    return {
        "success": True,
        "path": normalized_path,
        "name": target_path.name,
        "extension": extension,
        "content": content,
        "size": size,
        "encoding": "utf-8",
        "modified_at": modified_at,
        "is_markdown": extension in {".md", ".markdown"},
        "supported": True,
    }


@plugin_endpoint("/library/editor/file", methods=["PUT", "POST"])
async def write_library_editor_file(request: PluginRequest) -> Dict[str, Any]:
    payload = await _read_json_payload(request)
    raw_path = payload.get("path")
    content = payload.get("content")

    if not _normalize_query_value(raw_path):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "File path is required.",
            },
        )

    if not isinstance(content, str):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "File content must be a string.",
            },
        )

    encoded_bytes = content.encode("utf-8")
    if len(encoded_bytes) > EDITOR_MAX_FILE_BYTES:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": (
                    f"File exceeds maximum size of {EDITOR_MAX_FILE_BYTES} bytes "
                    "for editor writes."
                ),
            },
        )

    try:
        library_root = _resolve_scoped_library_root(request.user_id)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Unable to resolve configured library root path.",
            },
        ) from exc

    target_path, normalized_path = _resolve_editor_target(library_root, raw_path)
    if not _is_supported_extension(target_path):
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": (
                    "Unsupported file type for editor writes. "
                    f"Supported file types: {EDITOR_ALLOWED_EXTENSIONS_LABEL}."
                ),
                "path": normalized_path,
            },
        )

    if target_path.exists() and not target_path.is_file():
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "message": "Path must reference a file.",
                "path": normalized_path,
            },
        )

    created = not target_path.exists()
    _atomic_write_text(target_path, content)
    _, modified_at = _safe_stat(target_path)

    return {
        "success": True,
        "path": normalized_path,
        "bytes": len(encoded_bytes),
        "created": created,
        "updated_at": modified_at,
    }
