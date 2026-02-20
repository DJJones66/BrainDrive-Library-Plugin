import asyncio
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib import error, request

SERVICES_RUNTIME_ENV_VAR = "BRAINDRIVE_SERVICES_RUNTIME_DIR"
LIBRARY_SERVICE_SOURCE_ENV_VAR = "BRAINDRIVE_LIBRARY_SERVICE_SOURCE"
DEFAULT_LIBRARY_SERVICE_REPO_URL = "https://github.com/DJJones66/Library-Service"

DEFAULT_PROCESS_HOST = "127.0.0.1"
DEFAULT_PROCESS_PORT = "18170"
DEFAULT_REQUIRE_USER_HEADER = "true"
DEFAULT_HEALTH_STARTUP_ATTEMPTS = 20
DEFAULT_HEALTH_STARTUP_DELAY_SECONDS = 0.5
DEFAULT_INSTALL_ATTEMPTS = 2
DEFAULT_RUNTIME_PACKAGES = [
    "fastapi",
    "uvicorn",
    "dulwich",
    "httpx",
]
RUNTIME_IMPORT_CHECK = "import fastapi, uvicorn, dulwich, httpx"

DEFAULT_REQUIRED_ENV_VARS = [
    "PROCESS_HOST",
    "PROCESS_PORT",
    "BRAINDRIVE_LIBRARY_PATH",
    "BRAINDRIVE_LIBRARY_BASE_TEMPLATE_PATH",
    "BRAINDRIVE_LIBRARY_REQUIRE_USER_HEADER",
    SERVICES_RUNTIME_ENV_VAR,
]

_VALID_USER_ID = re.compile(r"^[A-Za-z0-9_]{3,128}$")


def _resolve_repo_root() -> Optional[Path]:
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        has_backend = (parent / "backend").is_dir() or parent.name == "backend"
        has_pluginbuild = (parent / "PluginBuild").is_dir() or parent.name == "PluginBuild"
        if has_backend and has_pluginbuild:
            return parent
    return None


def _resolve_services_runtime_dir() -> Path:
    override = str(os.environ.get(SERVICES_RUNTIME_ENV_VAR, "")).strip()
    if override:
        return Path(override).expanduser().resolve()

    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
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
        "Unable to resolve BrainDrive services_runtime directory. "
        f"Set {SERVICES_RUNTIME_ENV_VAR} to <BrainDriveRoot>/backend/services_runtime."
    )


BASE_RUNTIME_DIR = _resolve_services_runtime_dir()


@dataclass
class ServiceConfig:
    key: str
    label: str
    repo_path: Path
    local_seed_path: Optional[Path]
    repo_url: str
    venv_path: Path
    health_url: str
    scripts_dir: Path


def _default_local_seed_path() -> Optional[Path]:
    repo_root = _resolve_repo_root()
    if not repo_root:
        return None

    candidates = [
        repo_root / "PluginBuild" / "Library-Service",
        repo_root / "backend" / "services_runtime" / "Library-Service",
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return candidate.resolve()
    return None


def _default_repo_url() -> str:
    override = str(os.environ.get(LIBRARY_SERVICE_SOURCE_ENV_VAR, "")).strip()
    if override:
        return override

    local_seed = _default_local_seed_path()
    if local_seed is not None:
        return str(local_seed)

    return DEFAULT_LIBRARY_SERVICE_REPO_URL


SERVICE_CONFIG: Dict[str, ServiceConfig] = {
    "library_service": ServiceConfig(
        key="library_service",
        label="BrainDrive Library Service",
        repo_path=BASE_RUNTIME_DIR / "Library-Service",
        local_seed_path=_default_local_seed_path(),
        repo_url=_default_repo_url(),
        venv_path=BASE_RUNTIME_DIR / "Library-Service" / ".venv",
        health_url=f"http://localhost:{DEFAULT_PROCESS_PORT}/health",
        scripts_dir=BASE_RUNTIME_DIR / "Library-Service" / "service_scripts",
    )
}


def _venv_python(service: ServiceConfig) -> Path:
    if os.name == "nt":
        return service.venv_path / "Scripts" / "python.exe"
    return service.venv_path / "bin" / "python"


def _looks_remote(url: str) -> bool:
    lowered = url.lower()
    return lowered.startswith(("http://", "https://", "ssh://", "git@", "git://"))


def _copy_seed_repo(source_path: Path, target_path: Path) -> None:
    source_path = source_path.resolve()
    target_path = target_path.resolve()
    if not source_path.is_dir():
        raise RuntimeError(f"Local seed path not found: {source_path}")
    if source_path == target_path:
        return
    if target_path in source_path.parents:
        raise RuntimeError(
            f"Target runtime dir '{target_path}' cannot be parent of source '{source_path}'."
        )
    if source_path in target_path.parents:
        raise RuntimeError(
            f"Target runtime dir '{target_path}' cannot be inside source '{source_path}'."
        )

    ignore = shutil.ignore_patterns(
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".forge",
        "logs",
        "library",
        "user_library",
    )
    shutil.copytree(source_path, target_path, dirs_exist_ok=True, ignore=ignore)


def _ensure_repo(service: ServiceConfig) -> None:
    if service.repo_path.exists():
        return

    service.repo_path.parent.mkdir(parents=True, exist_ok=True)

    source_url = str(service.repo_url or "").strip()
    if not source_url:
        raise RuntimeError("No source configured for library service runtime.")

    if _looks_remote(source_url):
        subprocess.run(["git", "clone", source_url, str(service.repo_path)], check=True)
        return

    source_path = Path(source_url).expanduser()
    if not source_path.is_absolute():
        source_path = (Path.cwd() / source_path).resolve()
    _copy_seed_repo(source_path, service.repo_path)


def _normalize_env(env: Optional[Mapping[str, str]]) -> Dict[str, str]:
    merged = os.environ.copy()
    if env:
        merged.update({str(k): str(v) for k, v in env.items()})
    return merged


def _service_default_env_values(service: ServiceConfig) -> Dict[str, str]:
    return {
        SERVICES_RUNTIME_ENV_VAR: str(BASE_RUNTIME_DIR),
        "BRAINDRIVE_LIBRARY_PATH": str((service.repo_path / "library").resolve()),
        "BRAINDRIVE_LIBRARY_BASE_TEMPLATE_PATH": str(
            (service.repo_path / "library_templates" / "Base_Library").resolve()
        ),
        "BRAINDRIVE_LIBRARY_REQUIRE_USER_HEADER": DEFAULT_REQUIRE_USER_HEADER,
        "PROCESS_HOST": DEFAULT_PROCESS_HOST,
        "PROCESS_PORT": DEFAULT_PROCESS_PORT,
    }


def _quote_env_value(value: str) -> str:
    return json.dumps(str(value))


def _parse_env_lines(text: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if key:
            values[key] = value
    return values


def _template_candidates(service: ServiceConfig) -> List[Path]:
    return [
        service.repo_path / ".env-example",
        service.repo_path / ".env.example",
        service.repo_path / ".env.local.example",
        service.repo_path / ".env.local",
    ]


def _parse_env_template(template_path: Path) -> Tuple[List[str], Dict[str, str]]:
    if not template_path.exists():
        return [], {}
    values = _parse_env_lines(template_path.read_text(encoding="utf-8"))
    return list(values.keys()), values


def get_required_env_vars(service_key: str) -> List[str]:
    service = SERVICE_CONFIG[service_key]
    keys: List[str] = []
    for candidate in _template_candidates(service):
        parsed, _ = _parse_env_template(candidate)
        keys.extend(parsed)
    keys.extend(DEFAULT_REQUIRED_ENV_VARS)

    deduped: List[str] = []
    seen = set()
    for key in keys:
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def get_required_env_vars_map() -> Dict[str, List[str]]:
    return {key: get_required_env_vars(key) for key in SERVICE_CONFIG}


def get_service_metadata() -> List[Dict[str, Any]]:
    return [
        {
            "key": service.key,
            "label": service.label,
            "health_url": service.health_url,
            "repo_path": str(service.repo_path),
            "repo_url": service.repo_url,
        }
        for service in SERVICE_CONFIG.values()
    ]


def _ensure_env_file(service: ServiceConfig) -> Path:
    env_path = service.repo_path / ".env"
    if env_path.exists():
        return env_path

    for candidate in _template_candidates(service):
        if candidate.exists():
            shutil.copy(candidate, env_path)
            return env_path

    env_path.write_text("", encoding="utf-8")
    return env_path


def _render_env_content(
    existing_text: str,
    updates: Dict[str, str],
    managed_keys: List[str],
) -> str:
    managed_set = {key for key in managed_keys if key}
    seen = set()
    rendered_lines: List[str] = []

    for raw_line in existing_text.splitlines():
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            rendered_lines.append(line)
            continue

        key = stripped.split("=", 1)[0].strip()
        if key in managed_set:
            rendered_lines.append(f"{key}={_quote_env_value(updates.get(key, ''))}")
            seen.add(key)
        else:
            rendered_lines.append(line)

    for key in managed_keys:
        if key in seen:
            continue
        rendered_lines.append(f"{key}={_quote_env_value(updates.get(key, ''))}")

    return "\n".join(rendered_lines).rstrip() + "\n"


async def materialize_env_file(
    service_key: str,
    values: Optional[Dict[str, str]] = None,
    allowed_keys: Optional[List[str]] = None,
    backup: bool = True,
) -> Dict[str, Any]:
    service = SERVICE_CONFIG[service_key]
    _ensure_repo(service)
    env_path = _ensure_env_file(service)

    managed_values = _service_default_env_values(service)
    if values:
        managed_values.update({str(k): str(v) for k, v in values.items()})

    managed_keys = allowed_keys or list(managed_values.keys())
    existing_text = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
    rendered = _render_env_content(existing_text, managed_values, managed_keys)

    if rendered == existing_text:
        return {
            "changed": False,
            "env_path": str(env_path),
            "managed_keys": managed_keys,
        }

    backup_path = None
    if backup and env_path.exists():
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        backup_path = env_path.with_suffix(env_path.suffix + f".bak.{timestamp}")
        backup_path.write_text(existing_text, encoding="utf-8")

    env_path.write_text(rendered, encoding="utf-8")
    return {
        "changed": True,
        "env_path": str(env_path),
        "backup": str(backup_path) if backup_path else None,
        "managed_keys": managed_keys,
    }


def _resolve_configured_path(raw: str, base: Path) -> Path:
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = (base / candidate).resolve()
    return candidate.resolve()


def _resolve_library_root_from_env(service: ServiceConfig) -> Path:
    env_path = service.repo_path / ".env"
    values = _parse_env_lines(env_path.read_text(encoding="utf-8") if env_path.exists() else "")
    raw_path = values.get("BRAINDRIVE_LIBRARY_PATH")
    if raw_path:
        return _resolve_configured_path(raw_path, service.repo_path)
    return (service.repo_path / "library").resolve()


def _resolve_template_root_from_env(service: ServiceConfig) -> Path:
    env_path = service.repo_path / ".env"
    values = _parse_env_lines(env_path.read_text(encoding="utf-8") if env_path.exists() else "")
    raw_template = values.get("BRAINDRIVE_LIBRARY_BASE_TEMPLATE_PATH")
    if raw_template:
        return _resolve_configured_path(raw_template, service.repo_path)
    return (service.repo_path / "library_templates" / "Base_Library").resolve()


def _normalize_user_id(raw_user_id: str) -> str:
    normalized = str(raw_user_id or "").strip().replace("-", "")
    if not normalized or not _VALID_USER_ID.fullmatch(normalized):
        raise ValueError(f"Invalid installer user id: {raw_user_id!r}")
    return normalized


def _copy_template_idempotent(source_root: Path, destination_root: Path) -> List[str]:
    copied: List[str] = []
    if not source_root.is_dir():
        return copied

    for source in sorted(source_root.rglob("*")):
        relative = source.relative_to(source_root)
        target = destination_root / relative
        if source.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        if not source.is_file():
            continue
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied.append(relative.as_posix())

    return copied


def _apply_schema(service: ServiceConfig, scoped_root: Path) -> List[str]:
    schema_module_path = service.repo_path / "app" / "library_schema.py"
    if not schema_module_path.exists():
        return []

    spec = importlib.util.spec_from_file_location(
        f"library_service_schema_{abs(hash(str(schema_module_path)))}",
        schema_module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import schema module: {schema_module_path}")

    module_name = spec.name or f"library_service_schema_{abs(hash(str(schema_module_path)))}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    ensure_fn = getattr(module, "ensure_scoped_library_structure", None)
    if not callable(ensure_fn):
        return []

    result = ensure_fn(scoped_root, include_digest_period_files=True)
    changed_paths = getattr(result, "changed_paths", None)
    if not changed_paths:
        return []

    normalized: List[str] = []
    for item in changed_paths:
        try:
            normalized.append(Path(item).as_posix())
        except Exception:
            normalized.append(str(item))
    return normalized


def _bootstrap_first_user(service: ServiceConfig, installer_user_id: str) -> Dict[str, Any]:
    normalized_user_id = _normalize_user_id(installer_user_id)
    library_root = _resolve_library_root_from_env(service)
    template_root = _resolve_template_root_from_env(service)
    if not template_root.is_dir():
        raise RuntimeError(
            f"Library template root missing or invalid: {template_root}"
        )

    scoped_root = library_root / "users" / normalized_user_id
    scoped_root.mkdir(parents=True, exist_ok=True)

    copied = _copy_template_idempotent(template_root, scoped_root)
    schema_changed = _apply_schema(service, scoped_root)

    changed = sorted(set([*copied, *schema_changed]))
    return {
        "success": True,
        "user_id": normalized_user_id,
        "scoped_root": str(scoped_root),
        "changed_paths": changed,
    }


async def bootstrap_installer_user(service_key: str, installer_user_id: str) -> Dict[str, Any]:
    service = _get_service(service_key)
    return await asyncio.to_thread(_bootstrap_first_user, service, installer_user_id)


async def _run_python(
    script: Path,
    *,
    env: Optional[Mapping[str, str]] = None,
    cwd: Optional[Path] = None,
) -> Dict[str, Any]:
    if not script.exists():
        return {"success": False, "error": f"missing script: {script}", "cmd": ""}

    python_cmd = (
        sys.executable
        or shutil.which("python3")
        or shutil.which("python")
        or ("py" if os.name == "nt" else "python3")
    )
    cmd = [python_cmd, str(script)]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd) if cwd else None,
        env=_normalize_env(env),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return {
        "success": proc.returncode == 0,
        "code": proc.returncode,
        "cmd": " ".join(cmd),
        "stdout": stdout.decode(errors="replace"),
        "stderr": stderr.decode(errors="replace"),
    }


async def _run_venv_python(
    service: ServiceConfig,
    args: List[str],
    *,
    env: Optional[Mapping[str, str]] = None,
    cwd: Optional[Path] = None,
) -> Dict[str, Any]:
    python_path = _venv_python(service)
    if not python_path.exists():
        return {
            "success": False,
            "code": 1,
            "cmd": "",
            "stdout": "",
            "stderr": f"venv python missing: {python_path}",
        }

    cmd = [str(python_path), *args]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd) if cwd else None,
        env=_normalize_env(env),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return {
        "success": proc.returncode == 0,
        "code": proc.returncode,
        "cmd": " ".join(cmd),
        "stdout": stdout.decode(errors="replace"),
        "stderr": stderr.decode(errors="replace"),
    }


async def _verify_runtime_dependencies(
    service: ServiceConfig,
    *,
    env: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    result = await _run_venv_python(
        service,
        ["-c", RUNTIME_IMPORT_CHECK],
        env=env,
        cwd=service.repo_path,
    )
    return {
        **result,
        "required_imports": list(DEFAULT_RUNTIME_PACKAGES),
    }


async def _repair_runtime_dependencies(
    service: ServiceConfig,
    *,
    env: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    steps: List[Dict[str, Any]] = []

    # First try matching the runtime repo's requirements. If that fails due
    # to optional/dev packages, fall back to minimum runtime deps.
    upgrade_pip = await _run_venv_python(
        service,
        ["-m", "pip", "install", "--upgrade", "pip"],
        env=env,
        cwd=service.repo_path,
    )
    steps.append({"step": "pip_upgrade", **upgrade_pip})

    requirements = service.repo_path / "requirements.txt"
    if requirements.exists():
        requirements_install = await _run_venv_python(
            service,
            ["-m", "pip", "install", "-r", str(requirements)],
            env=env,
            cwd=service.repo_path,
        )
        steps.append({"step": "install_requirements", **requirements_install})

    runtime_install = await _run_venv_python(
        service,
        ["-m", "pip", "install", "--upgrade", *DEFAULT_RUNTIME_PACKAGES],
        env=env,
        cwd=service.repo_path,
    )
    steps.append({"step": "install_runtime_packages", **runtime_install})

    verify_result = await _verify_runtime_dependencies(service, env=env)
    steps.append({"step": "verify_runtime_dependencies", **verify_result})

    return {
        "success": bool(verify_result.get("success")),
        "steps": steps,
        "verify": verify_result,
    }


def _tail_service_log(service: ServiceConfig, *, line_count: int = 120) -> str:
    log_path = service.repo_path / "service_runtime.log"
    if not log_path.exists():
        return ""
    try:
        content = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    lines = content.splitlines()
    return "\n".join(lines[-line_count:])


async def _wait_for_service_health(
    service_key: str,
    *,
    attempts: int = DEFAULT_HEALTH_STARTUP_ATTEMPTS,
    delay_seconds: float = DEFAULT_HEALTH_STARTUP_DELAY_SECONDS,
) -> Dict[str, Any]:
    last_result: Optional[Dict[str, Any]] = None
    for attempt in range(1, attempts + 1):
        result = await health_check(service_key, timeout=3)
        last_result = result
        if result.get("success"):
            return {
                "success": True,
                "attempt": attempt,
                "result": result,
            }
        if attempt < attempts:
            await asyncio.sleep(delay_seconds)
    return {
        "success": False,
        "attempt": attempts,
        "result": last_result or {"success": False, "error": "health check did not run"},
    }


def _annotate_step(step_name: str, attempt: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "step": step_name,
        "attempt": attempt,
        **payload,
    }


def _get_service(service_key: str) -> ServiceConfig:
    service = SERVICE_CONFIG.get(service_key)
    if not service:
        raise KeyError(f"Unknown service key: {service_key}")
    return service


async def pre_start_check(service_key: str) -> Dict[str, Any]:
    service = _get_service(service_key)
    missing: List[str] = []

    if not service.repo_path.exists():
        missing.append("repo")
    if not service.scripts_dir.exists():
        missing.append("service_scripts")
    if not _venv_python(service).exists():
        missing.append("venv_python")

    return {
        "success": not missing,
        "service": service_key,
        "missing": missing,
    }


async def prepare_service(
    service_key: str,
    full_install: bool = False,
    force_recreate: bool = False,
    installer_user_id: Optional[str] = None,
) -> Dict[str, Any]:
    service = _get_service(service_key)
    _ensure_repo(service)

    env_write = await materialize_env_file(service_key, backup=False)

    create_script = service.scripts_dir / "create_venv.py"
    install_script = service.scripts_dir / "install_with_venv.py"
    init_script = service.scripts_dir / "init_db.py"

    script_env = _service_default_env_values(service)

    steps: List[Dict[str, Any]] = []
    install_successful = False
    install_failure_reason = "install"

    for attempt in range(1, DEFAULT_INSTALL_ATTEMPTS + 1):
        attempt_env = dict(script_env)
        if force_recreate or attempt > 1:
            attempt_env["VENV_FORCE_RECREATE"] = "1"

        create_result = await _run_python(create_script, env=attempt_env, cwd=service.repo_path)
        steps.append(_annotate_step("create_venv", attempt, create_result))
        if not create_result.get("success"):
            install_failure_reason = "create_venv"
            continue

        install_result = await _run_python(install_script, env=attempt_env, cwd=service.repo_path)
        steps.append(_annotate_step("install_with_venv", attempt, install_result))

        verify_result = await _verify_runtime_dependencies(service, env=attempt_env)
        steps.append(_annotate_step("verify_runtime_dependencies", attempt, verify_result))

        if install_result.get("success") and verify_result.get("success"):
            install_successful = True
            break

        repair_result = await _repair_runtime_dependencies(service, env=attempt_env)
        steps.append(_annotate_step("repair_runtime_dependencies", attempt, repair_result))
        if repair_result.get("success"):
            install_successful = True
            break

        install_failure_reason = (
            "install_with_venv"
            if not install_result.get("success")
            else "verify_runtime_dependencies"
        )

    if not install_successful:
        return {
            "success": False,
            "step": install_failure_reason,
            "service": service_key,
            "steps": steps,
            "env": env_write,
            "error": (
                "Library service runtime dependencies could not be installed. "
                "See step outputs for the failing command."
            ),
        }

    if full_install and init_script.exists():
        init_result = await _run_python(init_script, env=script_env, cwd=service.repo_path)
        if not init_result.get("success"):
            return {"success": False, "step": "init_db", **init_result}
        steps.append(init_result)

    bootstrap_result = None
    if installer_user_id:
        try:
            bootstrap_result = await bootstrap_installer_user(service_key, installer_user_id)
        except Exception as error:
            return {
                "success": False,
                "step": "bootstrap_user",
                "error": str(error),
                "service": service_key,
                "steps": steps,
                "env": env_write,
            }

    return {
        "success": True,
        "service": service_key,
        "steps": steps,
        "env": env_write,
        "bootstrap_user": bootstrap_result,
    }


async def start_service(service_key: str) -> Dict[str, Any]:
    service = _get_service(service_key)
    _ensure_repo(service)
    await materialize_env_file(service_key, backup=False)

    start_script = service.scripts_dir / "start_with_venv.py"
    script_env = _service_default_env_values(service)
    start_result = await _run_python(start_script, env=script_env, cwd=service.repo_path)
    if not start_result.get("success"):
        return start_result

    health_result = await _wait_for_service_health(service_key)
    if health_result.get("success"):
        return {
            **start_result,
            "health": health_result,
        }

    return {
        "success": False,
        "code": start_result.get("code"),
        "cmd": start_result.get("cmd"),
        "stdout": start_result.get("stdout"),
        "stderr": start_result.get("stderr"),
        "health": health_result,
        "log_tail": _tail_service_log(service),
        "error": "service failed health check after startup",
    }


async def shutdown_service(service_key: str) -> Dict[str, Any]:
    service = _get_service(service_key)
    if not service.repo_path.exists():
        return {"success": True, "skipped": True, "reason": "repo_missing"}
    stop_script = service.scripts_dir / "shutdown_with_venv.py"
    script_env = _service_default_env_values(service)
    return await _run_python(stop_script, env=script_env, cwd=service.repo_path)


async def restart_service(service_key: str) -> Dict[str, Any]:
    await shutdown_service(service_key)
    return await start_service(service_key)


async def health_check(
    service_key: str,
    override_url: Optional[str] = None,
    timeout: int = 5,
) -> Dict[str, Any]:
    service = _get_service(service_key)
    url = override_url or service.health_url

    def _check() -> Dict[str, Any]:
        try:
            with request.urlopen(url, timeout=timeout) as response:
                return {
                    "success": response.status == 200,
                    "status": response.status,
                    "url": url,
                }
        except error.HTTPError as http_error:
            return {
                "success": False,
                "status": http_error.code,
                "url": url,
                "error": str(http_error),
            }
        except Exception as exc:
            return {
                "success": False,
                "url": url,
                "error": str(exc),
            }

    return await asyncio.to_thread(_check)


__all__ = [
    "prepare_service",
    "start_service",
    "shutdown_service",
    "restart_service",
    "pre_start_check",
    "health_check",
    "get_service_metadata",
    "get_required_env_vars",
    "get_required_env_vars_map",
    "materialize_env_file",
    "bootstrap_installer_user",
]
