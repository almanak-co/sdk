"""Strategy-side teardown post-condition registry."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass
class ClosureCheckResult:
    """Outcome of an on-chain closure verification for a single position.

    Three-valued by design (VIB-5573, Empty ≠ Zero): a position is either
    MEASURED-closed, MEASURED-open (residual), or UNMEASURED (the read itself
    could not be completed). Conflating "we could not read" with "we read a
    residual" is a real bug: it lets a transient gateway/RPC blip during the
    post-teardown verify fabricate a residual → ``FAILED`` → hosted shutdown +
    entry latch on a healthy strategy. So a read fault sets ``unmeasured=True``
    (→ ``UNVERIFIED``, honest don't-know) and NEVER masquerades as a residual.
    Only a *positive on-chain measurement* of residual value is ``closed=False``
    (→ ``FAILED``).

    Attributes:
        closed: True iff the post-condition MEASURED the position fully closed
            on-chain. Only meaningful when ``unmeasured`` is False.
        unmeasured: True iff the check could not obtain a trustworthy on-chain
            reading (gateway/RPC fault after bounded read-retry, missing client,
            unresolved address, unsupported vault interface). The composition
            seam lowers this to ``UNVERIFIED`` — never ``FAILED``. When True,
            ``closed`` is ignored and MUST NOT be treated as a residual.
        protocol: Protocol the result is for, for logs and operator output.
        position_id: Position identifier checked.
        residual: Protocol-specific residual data (only set on a MEASURED
            residual, i.e. ``closed=False`` AND ``unmeasured=False``).
        error: Human-readable reason. Set on a read fault (``unmeasured=True``)
            or, rarely, alongside a measured residual for operator context.
    """

    closed: bool
    protocol: str = ""
    position_id: str = ""
    residual: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    unmeasured: bool = False


class TeardownPostCondition(Protocol):
    """Protocol-specific on-chain closure check.

    VIB-5140: ``block`` is an OPTIONAL block reference (the close-tx receipt's
    ``block_number``). Hooks that re-query on-chain state SHOULD pin their
    reads to it so a read replica trailing the writer cannot return PRE-close
    state and false-negative the closure check. ``None`` (the default for any
    caller that omits it) preserves the legacy ``"latest"`` behaviour.
    """

    def __call__(
        self,
        position: Any,
        wallet_address: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        block: int | str | None = None,
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
