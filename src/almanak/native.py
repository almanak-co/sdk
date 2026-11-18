"""Dispatch to the bundled Almanak Code binary.

Release wheels carry one binary for their own platform under `bin/`; a source
checkout, or a platform with no published wheel, has none.
"""

import json
import os
import platform
import stat
import sys
from pathlib import Path
from typing import NoReturn

# platform.machine() spellings that name the architecture the release assets and
# the bundled filenames call arm64 and x86_64. An unlisted spelling passes
# through and simply resolves a path that does not exist.
ARCHITECTURES = {"aarch64": "arm64", "amd64": "x86_64"}

# Set across the exec. A command line the agent emits that this CLI does not
# define, such as a V2 `almanak strat new`, must not forward back into the agent
# and relaunch it.
ACTIVE = "ALMANAK_CODE_ACTIVE"

# The agent selects its SDK v3 authoring lane (prompts, sub-agents, workflows and
# sdk-docs/sdk-src resolved from this interpreter's package) from these.
# Values already set in the environment are kept.
GENERATION = "ALMANAK_SDK_GENERATION"
V3_PYTHON = "ALMANAK_SDK_V3_PYTHON"


def forwarded_environment(environ: dict[str, str]) -> dict[str, str]:
    return {GENERATION: "v3", V3_PYTHON: sys.executable, **environ, ACTIVE: "1"}


def binary_name(system: str, machine: str) -> str:
    name = system.lower()
    architecture = ARCHITECTURES.get(machine.lower(), machine.lower())
    suffix = ".exe" if name == "windows" else ""
    return f"almanak-code-{name}-{architecture}{suffix}"


def binary_path() -> Path:
    return (
        Path(__file__).resolve().parent / "bin" / binary_name(platform.system(), platform.machine())
    )


def forwarded_arguments(argv: list[str], known: frozenset[str]) -> list[str] | None:
    """Arguments to forward, or None when the SDK parser owns this invocation.

    A leading option stays with argparse, matching the V2 Click group, which
    resolved its own options before it resolved a command name.
    """
    if not argv:
        return []
    if argv[0].startswith("-") or argv[0] in known:
        return None
    return list(argv)


def make_executable(path: Path) -> None:
    """Wheel extraction does not reliably preserve the execute bit."""
    if os.access(path, os.X_OK):
        return
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def unavailable(path: Path) -> NoReturn:
    print(
        json.dumps(
            {
                "error": "almanak_code_unavailable",
                "message": "this installation has no Almanak Code binary for this platform",
                "path": str(path),
            }
        ),
        file=sys.stderr,
    )
    raise SystemExit(1)


def exec_binary(arguments: list[str]) -> NoReturn:
    path = binary_path()
    if not path.exists():
        unavailable(path)
    make_executable(path)
    os.execve(str(path), [str(path), *arguments], forwarded_environment(dict(os.environ)))


def forward_unrecognized(argv: list[str], known: frozenset[str]) -> None:
    """Hand an unrecognized command line to the binary, or return to argparse.

    Inside a forwarded session the SDK parser always wins, so a V2 command line
    the agent still emits reports the commands this CLI does define instead of
    reopening the agent.
    """
    if os.environ.get(ACTIVE):
        return
    arguments = forwarded_arguments(argv, known)
    if arguments is not None:
        exec_binary(arguments)
