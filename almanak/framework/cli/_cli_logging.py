"""Logging for short-lived CLI commands (`ax`, `strat permissions`).

These commands are called by people and by coding agents that read everything
on stderr. Library warnings raised while they run — token-registry address
collisions, symbol-resolution deprecations, a managed gateway's start-up notes —
describe the SDK or the environment, not the command's result, so on stderr they
only bury the answer. They still matter for debugging, so they go to a rotating
file instead, and stderr keeps ERROR and above.

Long-running processes (`strat run`, the gateway, deployed strategies, the MCP
server) configure their own logging and are not affected.

Controls:
    --verbose on the command          stderr shows INFO and above
    ALMANAK_CLI_LOG_LEVEL=<level>     stderr threshold (debug|info|warning|error|critical)
    ALMANAK_CLI_LOG_FILE=<path>       file destination (default ~/.almanak/logs/cli.log);
                                      set to "" to disable the file
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

from almanak.config.cli_runtime import cli_log_file_from_env, cli_log_level_from_env

_HANDLER_NAME = "almanak-cli"
_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


def cli_stderr_level(verbose: bool = False) -> int:
    """The stderr threshold for this invocation."""
    if verbose:
        return logging.INFO
    return _LEVELS.get(cli_log_level_from_env(), logging.ERROR)


def _log_file() -> Path | None:
    configured = cli_log_file_from_env()
    if configured is not None:
        return Path(configured).expanduser() if configured.strip() else None
    return Path.home() / ".almanak" / "logs" / "cli.log"


def configure_cli_logging(verbose: bool = False) -> None:
    """Route library logs to a file and keep only errors on stderr.

    Idempotent: re-invoking (nested groups, tests) replaces this module's own
    handlers and never touches handlers someone else installed.
    """
    root = logging.getLogger()
    for handler in [h for h in root.handlers if h.get_name() == _HANDLER_NAME]:
        root.removeHandler(handler)
        handler.close()

    stderr_level = cli_stderr_level(verbose)
    stderr = logging.StreamHandler(sys.stderr)
    stderr.set_name(_HANDLER_NAME)
    stderr.setLevel(stderr_level)
    stderr.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    root.addHandler(stderr)
    root_level = stderr_level

    path = _log_file()
    if path is not None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(path, maxBytes=5_000_000, backupCount=2)
        except OSError:
            file_handler = None
        if file_handler is not None:
            file_handler.set_name(_HANDLER_NAME)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
            root.addHandler(file_handler)
            root_level = min(root_level, logging.INFO)

    root.setLevel(root_level)
    # `warnings.warn` (e.g. SymbolTokenResolutionWarning) goes through the same
    # handlers: recorded in the file, shown on stderr only when verbose.
    logging.captureWarnings(True)
