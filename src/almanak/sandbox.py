"""OS-enforced isolation for the local strategy worker."""

import json
import shutil
import sys
import sysconfig
from pathlib import Path

from almanak.worker_policy import SINGLE_THREADED


def readable_paths() -> tuple[Path, ...]:
    # uv can point the venv through a version alias absent from sys.base_prefix.
    # CPython exposes this path at runtime, but typeshed omits the private attribute.
    base_executable = sys._base_executable  # type: ignore[attr-defined]
    runtime = (
        sys.prefix,
        sys.base_prefix,
        str(Path(base_executable).parent.parent),
        str(Path(__file__).parent.parent),
    )
    system: tuple[str, ...] = ("/System", "/usr/lib", "/usr/share")
    if sys.platform == "linux":
        system = ("/usr/lib", "/usr/lib64", "/lib", "/lib64")
    return tuple(Path(path) for path in (*runtime, *system) if Path(path).exists())


def python_library_path() -> Path | None:
    """Return the interpreter library directory needed before Python can start."""
    configured = sysconfig.get_config_var("LIBDIR")
    if not isinstance(configured, str):
        return None
    path = Path(configured)
    return path if path.is_absolute() and path.is_dir() else None


def validate_key_path(key_file: Path | None, readable: tuple[Path, ...]):
    if key_file is not None and any(
        key_file.resolve().is_relative_to(path.resolve()) for path in readable
    ):
        raise ValueError("signing keys must stay outside strategy-readable runtime directories")


def worker_command(*, key_file: Path | None = None) -> list[str]:
    readable = readable_paths()
    validate_key_path(key_file, readable)
    if sys.platform == "linux":
        return linux_command(readable)
    if sys.platform == "darwin" and Path("/usr/bin/sandbox-exec").exists():
        return macos_command(readable)
    raise RuntimeError("strategy execution requires macOS sandbox-exec or Linux bubblewrap")


def linux_command(readable: tuple[Path, ...]) -> list[str]:
    executable = shutil.which("bwrap", path="/usr/bin:/bin")
    if executable is None:
        raise RuntimeError("Linux strategy execution requires bubblewrap (bwrap)")
    command = [
        executable,
        "--unshare-all",
        "--unshare-user",
        "--die-with-parent",
        "--new-session",
        "--cap-drop",
        "ALL",
        "--clearenv",
    ]
    library = python_library_path()
    if library is not None:
        # The official Python images resolve libpython through the host loader cache.
        # The sandbox intentionally excludes /etc, so give the cleared child the
        # interpreter's configured library directory explicitly.
        command.extend(("--setenv", "LD_LIBRARY_PATH", str(library)))
    command.extend(
        [
            "--setenv",
            "PATH",
            "/usr/bin:/bin",
            "--setenv",
            "PYTHONNOUSERSITE",
            "1",
            "--setenv",
            "PYTHONDONTWRITEBYTECODE",
            "1",
            *(
                item
                for name, value in SINGLE_THREADED.items()
                for item in ("--setenv", name, value)
            ),
            "--chdir",
            "/",
        ]
    )
    for path in readable:
        command.extend(("--ro-bind", str(path), str(path)))
    # Only these devices are exposed; host /dev, /proc and /run stay absent.
    for name in ("null", "urandom", "random"):
        command.extend(("--dev-bind", f"/dev/{name}", f"/dev/{name}"))
    return [*command, "--remount-ro", "/", "--", sys.executable]


def macos_command(readable: tuple[Path, ...]) -> list[str]:
    read_rules = " ".join(
        "(subpath " + json.dumps(str(Path(path).resolve())) + ")" for path in readable
    )
    executable = json.dumps(str(Path(sys.executable).resolve()))
    policy = f"""(version 1)
(deny default)
(allow process-exec (literal {executable}))
(allow file-read-metadata)
(allow file-read* {read_rules} (literal "/")
    (literal "/dev/null") (literal "/dev/urandom") (literal "/dev/random"))
(allow file-write-data (literal "/dev/null"))
(allow sysctl-read)
(allow mach-lookup)
"""
    return ["/usr/bin/sandbox-exec", "-p", policy, sys.executable]
