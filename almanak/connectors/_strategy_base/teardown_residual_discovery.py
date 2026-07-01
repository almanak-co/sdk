"""Strategy-side teardown residual-discovery registry (VIB-5116).

A **teardown post-condition** (``teardown_post_condition.py``) verifies that a
*known* enumerated position closed on-chain. But some on-chain risk/capital is
held by a venue in a form the strategy never surfaces as a position at all — the
canonical case is a GMX V2 **pending (unfilled) order**: collateral sits in the
OrderVault keyed by the order, the order is not yet a position (the keeper has
not executed it), and no ``position_registry`` row was ever written. Teardown's
position enumeration (``registry_enumeration``) therefore returns nothing and the
capital is stranded while teardown reports a clean ``no_positions`` success
(VIB-5116).

A **teardown residual discovery** closes that gap. It is a connector-owned,
gateway-routed read that discovers this deployment's residual on-chain
risk/capital **independent of the strategy's ``get_open_positions()`` /
``_loop_state``**, so a strategy whose in-memory state says "flat" cannot hide a
committed-but-unfilled order. The framework runs each registered discovery over
the deployment's own wallet (1 gateway : 1 strategy ⇒ wallet-scoped is
deployment-scoped) on the connector's declared chains, and folds the discovered
residuals into the teardown position set so they are counted, coverage-checked,
and on-chain closure-verified like any other position.

Design mirrors ``teardown_post_condition``:

* Connectors publish a discovery through ``CONNECTOR.teardown_residual_discovery``
  (an ``ImportRef`` on the manifest); the framework hydrates it into this
  registry at import time
  (``almanak.framework.teardown.residual_discovery``). The framework never names
  a protocol — the coupling stays in the connector (blueprint 22 / coupling
  ratchet).
* **Empty ≠ Zero, fail-closed (scoped).** A discovery must distinguish a
  *measured* empty read (``ok=True``, no residuals — genuinely nothing committed)
  from an *unmeasured* read (``ok=False`` — gateway/RPC error, decode fault,
  partial data). An unmeasured read must NEVER be silently reported as "no
  residuals" — that is the exact VIB-5116 bug. The framework's response is
  **scoped to whether the deployment actually uses the connector** (its STATIC
  declared ``supported_protocols`` / ``intent_types``): if it does (or the
  declaration is undeterminable), the unmeasured read is a real strand risk and
  the framework surfaces a loud, closure-failing sentinel; if it provably does
  NOT (the connector was only reached via chain-overlap), the framework logs a
  loud WARNING but does not fail-close an unrelated teardown. Either way it is
  loud, never a silent pass.
* Discoveries NEVER raise: any failure returns
  ``ResidualDiscoveryResult(ok=False, error=...)``.

No emojis. No Postgres DDL. No direct network egress — every read goes through
the supplied gateway client.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PendingResidual:
    """A single unit of residual on-chain risk/capital a discovery surfaced.

    Deliberately a connector-side DTO (not the framework ``PositionInfo``) so a
    connector hook never has to import framework teardown models. The framework
    residual-discovery loader converts each ``PendingResidual`` into a
    ``PositionInfo`` for the teardown lane.

    Attributes:
        protocol: Connector slug the residual belongs to (e.g. ``"gmx_v2"``) —
            the framework routes the on-chain closure verify to this protocol's
            teardown post-condition.
        chain: Chain the residual lives on.
        identifier: A stable on-chain identity for the residual (e.g. a GMX order
            key). Used as the teardown ``position_id``; must be non-empty and
            unique per residual so two residuals never collapse in de-dup.
        position_type: Teardown ``PositionType`` value as a string (e.g.
            ``"PERP"``) — kept a string so the connector never imports the
            framework enum. The framework maps it back to the enum.
        value_usd: Best-effort USD value of the committed capital (``Decimal("0")``
            when the discovery cannot price it — Empty ≠ Zero: never fabricated).
        details: Free-form residual detail (market, collateral token/amount, order
            type, a ``kind`` discriminator, …) surfaced to the operator and read
            by the protocol's post-condition. MUST carry ``source`` so the origin
            is auditable.
    """

    protocol: str
    chain: str
    identifier: str
    position_type: str = "PERP"
    value_usd: Decimal = Decimal("0")
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResidualDiscoveryResult:
    """Outcome of a connector teardown residual-discovery read.

    Attributes:
        residuals: The residuals the discovery measured. Empty with ``ok=True``
            means a MEASURED empty read (genuinely nothing committed on-chain).
        ok: ``True`` iff the read was measured (answerable). ``False`` ⇒ the read
            was UNMEASURED (gateway/RPC error, decode fault, partial data); the
            framework must fail-closed on it, never treat it as "no residuals".
        error: Populated when ``ok=False`` (or a non-fatal note). Operator-facing.
    """

    residuals: list[PendingResidual] = field(default_factory=list)
    ok: bool = True
    error: str | None = None


class TeardownResidualDiscovery(Protocol):
    """Connector-owned, gateway-routed discovery of residual on-chain risk/capital.

    Called once per (deployment wallet, chain) at teardown. MUST be gateway-only
    (no direct egress) and MUST NOT raise — return
    ``ResidualDiscoveryResult(ok=False, error=...)`` on any failure so an
    unmeasured read fails the teardown closed instead of silently passing.

    ``block`` is an optional block reference; discoveries that re-read on-chain
    SHOULD pin to it when supplied (a read replica trailing the writer must not
    return stale state). ``rpc_url`` exists only to satisfy connectors that share
    the post-condition dual path; framework code crosses the gateway boundary
    only and does not pass it.
    """

    def __call__(
        self,
        wallet_address: str,
        chain: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        block: int | str | None = None,
    ) -> ResidualDiscoveryResult: ...


_REGISTRY: dict[str, TeardownResidualDiscovery] = {}


def _register_teardown_residual_discovery(protocol: str, hook: TeardownResidualDiscovery) -> None:
    """Register a residual discovery for a protocol (framework-internal).

    Not a connector-facing API: connectors publish discoveries through
    ``CONNECTOR.teardown_residual_discovery``; the framework hydrates them into
    this registry at import time. Re-registering the same hook is idempotent;
    replacing an existing hook logs a warning so accidental shadowing is visible.
    """
    key = protocol.lower()
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not hook:
        logger.warning("Replacing existing teardown residual discovery for protocol %r", protocol)
    _REGISTRY[key] = hook


def get_teardown_residual_discovery(protocol: str) -> TeardownResidualDiscovery | None:
    """Look up a registered residual discovery. Returns ``None`` when none."""
    return _REGISTRY.get(protocol.lower())


def has_teardown_residual_discovery(protocol: str) -> bool:
    """``True`` iff a residual discovery is registered for ``protocol``."""
    return protocol.lower() in _REGISTRY


def registered_residual_discovery_protocols() -> tuple[str, ...]:
    """Protocols with a registered residual discovery (sorted, for iteration)."""
    return tuple(sorted(_REGISTRY))


__all__ = [
    "PendingResidual",
    "ResidualDiscoveryResult",
    "TeardownResidualDiscovery",
    "get_teardown_residual_discovery",
    "has_teardown_residual_discovery",
    "registered_residual_discovery_protocols",
]
