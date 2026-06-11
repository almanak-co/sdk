"""Strategy-side teardown post-condition registry."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass
class ClosureCheckResult:
    """Outcome of an on-chain closure verification for a single position.

    Attributes:
        closed: True iff the post-condition determined the position is fully
            closed on-chain. False means residual liquidity or debt was
            detected, or the check itself errored out.
        protocol: Protocol the result is for, for logs and operator output.
        position_id: Position identifier checked.
        residual: Protocol-specific residual data.
        error: Set when the check itself failed. Treated as ``closed=False``.
    """

    closed: bool
    protocol: str = ""
    position_id: str = ""
    residual: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class TeardownPostCondition(Protocol):
    """Protocol-specific on-chain closure check."""

    def __call__(
        self,
        position: Any,
        wallet_address: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
    ) -> ClosureCheckResult: ...


_REGISTRY: dict[str, TeardownPostCondition] = {}


def _register_teardown_post_condition(protocol: str, hook: TeardownPostCondition) -> None:
    """Register a post-condition for a protocol (framework-internal).

    Not a connector-facing API: connectors publish post-conditions through
    ``CONNECTOR.teardown_post_condition`` (an ``ImportRef`` on the manifest);
    the framework hydrates them into this registry at import time
    (``almanak.framework.teardown.post_conditions``).

    Re-registering the same hook is idempotent. Replacing an existing hook logs
    a warning so accidental shadowing is visible in logs.
    """
    key = protocol.lower()
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not hook:
        logger.warning(
            "Replacing existing teardown post-condition for protocol %r",
            protocol,
        )
    _REGISTRY[key] = hook


def get_teardown_post_condition(protocol: str) -> TeardownPostCondition | None:
    """Look up a registered post-condition. Returns ``None`` when none."""
    return _REGISTRY.get(protocol.lower())


def has_teardown_post_condition(protocol: str) -> bool:
    """``True`` iff a post-condition is registered for ``protocol``."""
    return protocol.lower() in _REGISTRY


__all__ = [
    "ClosureCheckResult",
    "TeardownPostCondition",
    "get_teardown_post_condition",
    "has_teardown_post_condition",
]
