"""Deployment-mode detection — single source of truth.

`ALMANAK_IS_HOSTED` is the explicit signal that distinguishes hosted from
local, and `ALMANAK_DEPLOYMENT_ID` carries the id *value* within hosted mode:

- **Hosted**: `ALMANAK_IS_HOSTED` is truthy (after strip). The deployer
  injects it into every pod container, alongside a non-blank
  `ALMANAK_DEPLOYMENT_ID` (the platform deployment id).
- **Local**: `ALMANAK_IS_HOSTED` is unset, empty, whitespace-only, or
  falsey. `ALMANAK_DEPLOYMENT_ID` is ignored — the runner derives the id
  from the execution wallet + chain instead (see
  `almanak/framework/runner/identity.py`).

Splitting the *signal* (`ALMANAK_IS_HOSTED`) from the *value*
(`ALMANAK_DEPLOYMENT_ID`) means a stray id var cannot flip a local run into
hosted mode, and a deployer that sets one var but not the other fails loudly
at boot rather than limping (see blueprint 29 §2.3).

All other env vars (`ALMANAK_GATEWAY_DATABASE_URL`,
`ALMANAK_GATEWAY_AUTH_TOKEN`, `ALMANAK_GATEWAY_ALLOW_INSECURE`) are
configuration *within* a mode — not signals *of* a mode. They must never be
used to derive the deployment mode.

This module is the **only** permitted reader of both `ALMANAK_IS_HOSTED` and
`ALMANAK_DEPLOYMENT_ID` (blueprint 29 §2.3); every other production code
path must consume one of the helpers below.
"""

from __future__ import annotations

import logging
import os
from typing import Literal

logger = logging.getLogger(__name__)

DeploymentMode = Literal["hosted", "local"]

# Values accepted as "truthy" for the boolean ALMANAK_IS_HOSTED signal.
_TRUTHY = frozenset({"1", "true", "yes", "on"})

# Module-level flag: emit the local-mode stray-id warning at most once.
_LOCAL_STRAY_ID_WARNING_EMITTED = False


class FatalBootError(RuntimeError):
    """Raised at runner boot when the deployment-mode env contract is invalid.

    Hosted mode with a blank ``ALMANAK_DEPLOYMENT_ID`` is the canonical
    trigger: a hosted pod with no id cannot stamp deployment-scoped rows and
    must refuse to start rather than write under an empty identity.
    """


def _raw(name: str) -> str:
    """Return the stripped env value for ``name`` ("" if unset)."""
    return (os.environ.get(name) or "").strip()


def is_hosted() -> bool:
    """True iff this process runs as a managed (hosted) deployment.

    Reads only ``ALMANAK_IS_HOSTED`` — the single deployment-mode signal.
    Truthy values (case-insensitive): ``1``, ``true``, ``yes``, ``on``.
    """
    return _raw("ALMANAK_IS_HOSTED").lower() in _TRUTHY


def is_local() -> bool:
    """True iff this process runs as a local SDK install (default for users)."""
    return not is_hosted()


def deployment_id() -> str | None:
    """Return the hosted deployment id, or ``None`` in local mode.

    Hosted mode: returns the stripped ``ALMANAK_DEPLOYMENT_ID`` value, and
    raises :class:`FatalBootError` if it is blank — a hosted pod with no id
    is an invalid deployment.

    Local mode: returns ``None``. The id var is *ignored* (a stray
    ``ALMANAK_DEPLOYMENT_ID`` on a local box must not be mistaken for a
    real hosted identity); a one-time warning is emitted if it is set. The
    runner derives the local ``deployment_id`` from wallet + chain via
    ``almanak.framework.runner.identity.resolve_deployment_id``.
    """
    global _LOCAL_STRAY_ID_WARNING_EMITTED  # noqa: PLW0603

    raw_id = _raw("ALMANAK_DEPLOYMENT_ID")

    if is_hosted():
        if not raw_id:
            raise FatalBootError(
                "hosted deployment (ALMANAK_IS_HOSTED set) but "
                "ALMANAK_DEPLOYMENT_ID is blank — a hosted pod must carry "
                "the platform deployment id. Refusing to start."
            )
        return raw_id

    # Local mode — the id var is configuration *within* hosted mode only.
    if raw_id and not _LOCAL_STRAY_ID_WARNING_EMITTED:
        logger.warning(
            "ALMANAK_DEPLOYMENT_ID is set (%r) but ALMANAK_IS_HOSTED is not "
            "truthy — running in LOCAL mode. The id var is ignored; the "
            "deployment_id is derived from wallet + chain. Set "
            "ALMANAK_IS_HOSTED=true if you intended a hosted run.",
            raw_id,
        )
        _LOCAL_STRAY_ID_WARNING_EMITTED = True
    return None


def deployment_mode() -> DeploymentMode:
    """Return the deployment mode as a string token suitable for logging."""
    return "hosted" if is_hosted() else "local"


def deployment_commit_sha() -> str | None:
    """Return the deployer-injected git commit SHA, or ``None`` if unset.

    Used by the deployment-start banner to surface *which build* a hosted
    pod is running. Set by the deployer (``ALMANAK_COMMIT_SHA``) from
    ``v2_metadata.commit_sha`` on the platform side; unset locally.
    """
    return _raw("ALMANAK_COMMIT_SHA") or None


def deployment_sdk_version() -> str | None:
    """Return the deployer-injected SDK version pin, or ``None`` if unset.

    Set by the deployer from the strategy's ``pyproject.toml``/``uv.lock``
    ``almanak`` pin (``ALMANAK_SDK_VERSION``). Distinct from the strategy's
    own version: this is *which Almanak SDK release* the strategy was
    deployed against.
    """
    return _raw("ALMANAK_SDK_VERSION") or None


def deployment_strategy_name() -> str | None:
    """Return the deployer-injected strategy name, or ``None`` if unset.

    Used as the gateway-side banner fallback (the gateway does not import
    strategy code, so the ``@almanak_strategy(name=…)`` decorator value is
    not visible at gateway boot). The strategy container itself resolves
    the name from ``STRATEGY_METADATA`` via ``_strategy_display_name``.
    """
    return _raw("ALMANAK_STRATEGY_NAME") or None


def deployment_strategy_version() -> str | None:
    """Return the deployer-injected strategy's own version, or ``None`` if unset.

    Set by the deployer from ``pyproject.toml`` ``[project].version`` so the
    gateway-side banner can render the strategy's package version without
    importing strategy code.
    """
    return _raw("ALMANAK_STRATEGY_VERSION") or None
