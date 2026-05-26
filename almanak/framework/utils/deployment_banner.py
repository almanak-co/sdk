"""Deployment-start log banner.

Emits a visually distinct multi-line banner plus a structured single-line
sentinel at the start of each deployment, so users can tell where one
deployment's logs end and the next one's begin. Fires once per process, as
the very first log line:

- ``almanak strat run`` (CLI entry) — emits in the strategy container before
  any other startup output, via :func:`emit_cli_banner` (uses ``click.echo``
  so it works before the framework's Python logging is configured).
- Gateway server / managed_serve ``main()`` — strategy-pod and dashboard-pod
  gateway sidecars at process boot, via :func:`emit_gateway_banner` (uses
  ``logger.info`` — structlog is configured first in those entrypoints).

The frontend log viewer parses the ``ALMANAK_DEPLOYMENT_BANNER`` sentinel
line; the surrounding ``=`` rule lines are for raw ``kubectl logs`` users.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

# Imports from ``almanak.framework.deployment.mode`` are deferred to the
# emit functions: the import-leanness tests (``tests/framework/runner/
# test_imports_lean.py``, ``tests/gateway/test_imports_lean.py``) forbid the
# strategy runner and gateway server from pulling ``almanak.framework.
# deployment`` into ``sys.modules`` at module load. The banner fires once
# per boot, so paying the import cost inside the function is free.

_RULE = "=" * 64
_SENTINEL = "ALMANAK_DEPLOYMENT_BANNER"


def _sanitize_sentinel_value(value: str) -> str:
    """Replace whitespace with ``_`` so the space-delimited sentinel parser
    on the frontend (``logParser.ts``) can tokenise key=value pairs without
    needing quoting. The visual ASCII banner above is unaffected — it keeps
    the natural rendering.
    """
    return "_".join(value.split()) if value else value


def _sdk_version_local() -> str:
    """Read the installed SDK version (used when the deployer didn't inject one)."""
    try:
        from almanak._version import __version__

        return __version__
    except Exception:
        return "unknown"


def _resolve_fields(
    *,
    strategy_name: str | None,
    strategy_version: str | None,
) -> dict[str, str]:
    """Pull deployment identity from env, falling back to caller hints.

    Precedence is **env wins over caller hint**: in hosted V2 the deployer
    injects ``ALMANAK_STRATEGY_NAME`` / ``ALMANAK_STRATEGY_VERSION`` from
    the strategy's ``pyproject.toml`` — those are the authoritative
    identifiers and must beat the CLI's ``working_dir`` basename hint
    (the working dir is typically ``/app/src`` in hosted pods, which
    would otherwise surface as ``strategy: src``). The caller hint only
    fills in for local-mode runs where the env vars are unset.
    """
    from ..deployment.mode import (
        deployment_commit_sha,
        deployment_id,
        deployment_sdk_version,
        deployment_strategy_name,
        deployment_strategy_version,
    )

    return {
        "deployment_id": deployment_id() or "local",
        "strategy_name": deployment_strategy_name() or strategy_name or "unknown",
        "strategy_version": deployment_strategy_version() or strategy_version or "unknown",
        "commit_sha": deployment_commit_sha() or "local",
        "sdk_version": deployment_sdk_version() or _sdk_version_local(),
    }


def emit_cli_banner(
    *,
    strategy_name: str | None = None,
    strategy_version: str | None = None,
) -> None:
    """Emit the banner via ``click.echo`` from the CLI entrypoint.

    Used at the top of ``almanak strat run`` so the banner is the very
    first line in the strategy container's logs — before config loading,
    gateway connection, or any other startup output. ``click.echo`` writes
    directly to stdout, which works regardless of whether Python logging
    has been configured yet.
    """
    import click

    _emit(click.echo, **_resolve_fields(strategy_name=strategy_name, strategy_version=strategy_version))


def emit_gateway_banner(logger: logging.Logger) -> None:
    """Emit the deployment-start banner from the gateway sidecar boot path.

    Skips when there is no hosted deployment id — that's local-dev gateway
    boots (e.g. ``almanak strat run``) where the strategy-side banner will
    fire instead and a gateway banner would just be noise.
    """
    fields = _resolve_fields(strategy_name=None, strategy_version=None)
    if fields["deployment_id"] == "local":
        return
    # The gateway has no decorator metadata to fall back on if the deployer
    # didn't inject ALMANAK_STRATEGY_NAME — surface "unknown" rather than
    # "local" so the gateway banner reads coherently.
    if fields["commit_sha"] == "local":
        fields["commit_sha"] = "unknown"

    _emit(logger.info, **fields)


def _emit(
    write: Callable[[str], object],
    *,
    deployment_id: str,
    strategy_name: str,
    strategy_version: str,
    commit_sha: str,
    sdk_version: str,
) -> None:
    import sys

    started_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    write(_RULE)
    write("  ▶  NEW DEPLOYMENT STARTED")
    write(f"     deployment_id   : {deployment_id}")
    write(f"     strategy        : {strategy_name} v{strategy_version}")
    write(f"     commit_sha      : {commit_sha}")
    write(f"     sdk_version     : {sdk_version}")
    write(f"     started_at      : {started_at}")
    write(_RULE)
    write(
        f"{_SENTINEL} "
        f"deployment_id={_sanitize_sentinel_value(deployment_id)} "
        f"strategy={_sanitize_sentinel_value(strategy_name)} "
        f"strategy_version={_sanitize_sentinel_value(strategy_version)} "
        f"commit_sha={_sanitize_sentinel_value(commit_sha)} "
        f"sdk_version={_sanitize_sentinel_value(sdk_version)} "
        f"started_at={_sanitize_sentinel_value(started_at)}"
    )

    # Force a flush so Cloud Logging sees each line with its real emit-time
    # nanosecond timestamp. Without this, K8s stdout block-buffering can
    # bundle the banner lines into one chunk; Cloud Logging then assigns
    # them very close timestamps and the UI sorts them in a non-emit order
    # (visible on stage with v2.16.1-rc6: started_at appearing between
    # strategy and commit_sha).
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass


__all__ = ["emit_cli_banner", "emit_gateway_banner"]
