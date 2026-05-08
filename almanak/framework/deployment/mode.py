"""Deployment-mode detection — single source of truth.

`AGENT_ID` is the explicit signal that distinguishes hosted from local:

- **Hosted**: `AGENT_ID` is set and non-empty (after strip). The deployer
  injects it into every pod container.
- **Local**: `AGENT_ID` is unset, empty, or whitespace-only.

All other env vars (`ALMANAK_GATEWAY_DATABASE_URL`,
`ALMANAK_GATEWAY_AUTH_TOKEN`, `ALMANAK_GATEWAY_ALLOW_INSECURE`) are
configuration *within* a mode — not signals *of* a mode. They must never be
used to derive the deployment mode.

This module is the single permitted reader of the `AGENT_ID` environment
variable (VIB-3759); every other production code path must consume one of
the helpers below.
"""

from __future__ import annotations

import os
from typing import Literal

DeploymentMode = Literal["hosted", "local"]


def agent_id() -> str | None:
    """Return the deployment `AGENT_ID`, or `None` if not in hosted mode.

    Whitespace-only values are treated as unset, matching the legacy
    `_is_managed_deployment()` semantics so behavior is preserved while
    callers migrate to this helper.
    """
    raw = os.environ.get("AGENT_ID")
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def is_hosted() -> bool:
    """True iff this process runs as a managed (hosted) deployment."""
    return agent_id() is not None


def is_local() -> bool:
    """True iff this process runs as a local SDK install (default for users)."""
    return not is_hosted()


def deployment_mode() -> DeploymentMode:
    """Return the deployment mode as a string token suitable for logging."""
    return "hosted" if is_hosted() else "local"
