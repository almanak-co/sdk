"""Run `almanak` as a user would: one process per command, no provider key in its environment."""

import json
import os
import shlex
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

# Removed from the child environment, so a command that passes did so without a
# provider key and without the session read gateway.
WITHHELD = ("ALCHEMY_API_KEY", "ALMANAK_AX_GATEWAY_URL", "ALMANAK_AX_GATEWAY_AUTH_FILE")

READ_TIMEOUT_SECONDS = 180


@dataclass(frozen=True)
class Invocation:
    """What one command printed. `timed_out` commands have no exit status."""

    arguments: tuple[str, ...]
    exit_code: int | None
    stdout: str
    stderr: str
    timed_out: bool = False

    @property
    def command(self) -> str:
        return shlex.join(self.arguments)

    def output(self) -> dict[str, Any] | None:
        """The single JSON document a successful command prints on standard output."""
        return parsed(self.stdout)

    def refusal(self) -> dict[str, Any] | None:
        """The last JSON line with `error` on standard error; event lines are skipped."""
        for line in reversed(self.stderr.splitlines()):
            document = parsed(line)
            if document is not None and "error" in document:
                return document
        return None


def parsed(text: str) -> dict[str, Any] | None:
    try:
        document = json.loads(text)
    except ValueError:
        return None
    return document if isinstance(document, dict) else None


def default_command() -> tuple[str, ...]:
    """The `almanak` installed beside this interpreter: the checkout under test."""
    return (str(Path(sys.executable).with_name("almanak")),)


def child_environment(environment: Mapping[str, str]) -> dict[str, str]:
    return {name: value for name, value in environment.items() if name not in WITHHELD}


class Run(Protocol):
    def __call__(
        self, arguments: Sequence[str], timeout: float | None = READ_TIMEOUT_SECONDS
    ) -> Invocation: ...


def runner(command: Sequence[str], cwd: Path) -> Run:
    """A callable that runs `<command> <arguments>` in `cwd` and captures both streams."""

    def run(arguments: Sequence[str], timeout: float | None = READ_TIMEOUT_SECONDS) -> Invocation:
        full = (*command, *arguments)
        try:
            done = subprocess.run(
                full,
                cwd=cwd,
                env=child_environment(os.environ),
                capture_output=True,
                text=True,
                timeout=timeout,
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            return Invocation(full, None, "", "", timed_out=True)
        return Invocation(full, done.returncode, done.stdout, done.stderr)

    return run
