"""Deployment-start log banner.

Emits a visually distinct multi-line banner plus a structured single-line
sentinel at the start of each deployment, so users can tell where one
deployment's logs end and the next one's begin. Used at two boot points:

- ``StrategyRunner.run_loop`` — strategy container, first thing the runner
  does once it has a strategy in hand.
- Gateway server / managed_serve ``main()`` — strategy-pod and dashboard-pod
  gateway sidecars at process boot.

The frontend log viewer parses the ``ALMANAK_DEPLOYMENT_BANNER`` sentinel
line; the surrounding ``=`` rule lines are for raw ``kubectl logs`` users.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..runner.runner_models import StrategyProtocol

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


def emit_strategy_banner(logger: logging.Logger, strategy: StrategyProtocol) -> None:
    """Emit the deployment-start banner from the strategy container."""
    from ..deployment.mode import deployment_commit_sha, deployment_sdk_version
    from ..runner.runner_gateway import _strategy_display_name

    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    strategy_version = getattr(metadata, "version", "") or "unknown"
    _emit(
        logger,
        deployment_id=getattr(strategy, "deployment_id", "") or "local",
        strategy_name=_strategy_display_name(strategy),
        strategy_version=strategy_version,
        commit_sha=deployment_commit_sha() or "local",
        sdk_version=deployment_sdk_version() or _sdk_version_local(),
    )


def emit_gateway_banner(logger: logging.Logger) -> None:
    """Emit the deployment-start banner from the gateway sidecar boot path.

    Skips when there is no hosted deployment id — that's local-dev gateway
    boots (e.g. ``almanak strat run``) where the strategy-side banner will
    fire instead and a gateway banner would just be noise.
    """
    from ..deployment.mode import (
        deployment_commit_sha,
        deployment_id,
        deployment_sdk_version,
        deployment_strategy_name,
        deployment_strategy_version,
    )

    dep_id = deployment_id()
    if not dep_id:
        return

    _emit(
        logger,
        deployment_id=dep_id,
        strategy_name=deployment_strategy_name() or "unknown",
        strategy_version=deployment_strategy_version() or "unknown",
        commit_sha=deployment_commit_sha() or "unknown",
        sdk_version=deployment_sdk_version() or _sdk_version_local(),
    )


def _emit(
    logger: logging.Logger,
    *,
    deployment_id: str,
    strategy_name: str,
    strategy_version: str,
    commit_sha: str,
    sdk_version: str,
) -> None:
    started_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    logger.info(_RULE)
    logger.info("  ▶  NEW DEPLOYMENT STARTED")
    logger.info(f"     deployment_id   : {deployment_id}")
    logger.info(f"     strategy        : {strategy_name} v{strategy_version}")
    logger.info(f"     commit_sha      : {commit_sha}")
    logger.info(f"     sdk_version     : {sdk_version}")
    logger.info(f"     started_at      : {started_at}")
    logger.info(_RULE)
    logger.info(
        f"{_SENTINEL} "
        f"deployment_id={_sanitize_sentinel_value(deployment_id)} "
        f"strategy={_sanitize_sentinel_value(strategy_name)} "
        f"strategy_version={_sanitize_sentinel_value(strategy_version)} "
        f"commit_sha={_sanitize_sentinel_value(commit_sha)} "
        f"sdk_version={_sanitize_sentinel_value(sdk_version)} "
        f"started_at={_sanitize_sentinel_value(started_at)}"
    )


__all__ = ["emit_strategy_banner", "emit_gateway_banner"]
