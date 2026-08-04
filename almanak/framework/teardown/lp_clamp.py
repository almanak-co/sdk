"""Bound a fungible-LP close to this deployment's own outstanding liquidity (VIB-6162).

A fungible-LP position is a plain ERC-20 balance in the wallet, so a close that
withdraws ``balanceOf(wallet)`` burns **everything in that pool** — including LP the
strategy never minted: a user's own position, or a sibling deployment's — and books the
proceeds as this strategy's PnL. Reproduced on ``aerodrome:base`` at roughly +10% on a
round trip against 10.35% pre-existing LP.

The division of labour is the load-bearing part of the design:

**Framework knows the ledger. Connector knows the chain.**

This module owns the ledger half only. It folds ``position_events`` into an outstanding
balance for one deployment and one pool. It never resolves a symbolic pool spec to an
address, never reads a chain balance, and never decides what an address-shaped
identifier *denotes* — that is connector knowledge, declared on the manifest
(:class:`~almanak.connectors._connector_descriptor.FungibleLpCloseDecl`), because an
address means the pool on Aerodrome, the LP token on Curve, and the wrapper on Fluid.

Two failure shapes this module is written against, both real:

* **The inert guard.** The first attempt at this ticket resolved only address-shaped
  identifiers. Every strategy emitting Aerodrome's symbolic ``TOKEN0/TOKEN1/pool_type``
  silently got no clamp, while a wei-exact mainnet-fork proof on the address form
  appeared to establish the invariant. Hence: unresolved identity **refuses**, and the
  refusal is loud.
* **The guard that can never succeed.** An earlier round routed the live read through
  ``market.balance()``, which resolves via the token registry; a Solidly-fork pool is
  not a registered token, so it raised deterministically and the clamp skipped 100% of
  closes, stranding every position. Hence: no chain read here at all.

``None`` from this module means **UNMEASURED**, never zero. ``Decimal("0")`` is a
measured zero — a deployment with no outstanding liquidity. Callers must refuse on
``None`` and must never read it as "no limit applies"; that substitution is the original
defect wearing a return value.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.connectors._connector_descriptor import CONNECTOR_REGISTRY, FungibleLpCloseDecl

logger = logging.getLogger(__name__)

#: Event types that move outstanding liquidity. ``SNAPSHOT`` rows carry the CURRENT
#: liquidity once per iteration rather than a delta; folding them in inflates the bound
#: far above what was ever minted, so they are excluded rather than netted.
_OPEN_EVENTS: frozenset[str] = frozenset({"OPEN", "LP_OPEN", "INCREASE"})
_CLOSE_EVENTS: frozenset[str] = frozenset({"CLOSE", "LP_CLOSE", "DECREASE"})


class LpClampUnresolved(Exception):
    """Raised when the close cannot be bounded and must therefore be refused.

    Carries the reason so the teardown lane can log something actionable instead of a
    bare failure. Every construction site names the identifier or the amounts involved.
    """


def clamp_decl_for(protocol: str | None) -> FungibleLpCloseDecl | None:
    """Return the connector's fungible-LP close declaration, or ``None``.

    ``None`` means this venue declares no clamp — a deliberate manifest decision (Curve
    is currently one, pending VIB-6489), not permission to close unbounded.
    """
    if not protocol:
        return None
    return CONNECTOR_REGISTRY.fungible_lp_close_for(str(protocol))


def _resolve_identity(decl: FungibleLpCloseDecl, position_id: Any, pool: Any) -> str | None:
    """Resolve the connector's canonical pool key for this close, or ``None``."""
    if decl.identity is None:
        return None
    resolver = decl.identity.load()
    return resolver(position_id, pool)


def _scale(raw: Any, decl: FungibleLpCloseDecl) -> Decimal | None:
    """Convert a stored ``position_events.liquidity`` value into token units.

    Returns ``None`` for an unmeasured value. **Empty is not zero**: ``None`` and ``""``
    both mean the producer never recorded a quantity, and coercing either to
    ``Decimal("0")`` would silently report "this deployment minted nothing" — which,
    depending on the branch taken downstream, licenses either a no-op or a whole-balance
    burn. Both are wrong, and neither is visible in a passing test.
    """
    if raw is None or raw == "":
        return None
    try:
        value = Decimal(str(raw))
    except (ArithmeticError, ValueError):
        return None
    if decl.units == "raw":
        # decimals is guaranteed non-None for units="raw" by FungibleLpCloseDecl's own
        # validation, so this cannot silently divide by an unknown scale.
        return value / (Decimal(10) ** int(decl.decimals or 0))
    return value


async def read_outstanding_liquidity(
    state_manager: Any,
    deployment_id: str,
    *,
    protocol: str,
    position_id: Any,
    pool: Any = None,
) -> Decimal | None:
    """Return this deployment's outstanding liquidity for the pool, or ``None``.

    ``None`` is UNMEASURED — an unresolvable identifier, an unreadable history, or a
    position whose mint was never recorded. The caller must refuse the close.

    Outstanding is a **running balance**: opens minus closes, not the latest open and
    not the sum of all opens. A strategy that ran ``OPEN -> partial CLOSE -> OPEN``
    holds less than it ever minted, and bounding by the mint total would over-withdraw
    by exactly the amount already closed.
    """
    decl = clamp_decl_for(protocol)
    if decl is None or not decl.clamp:
        return None

    key = _resolve_identity(decl, position_id, pool)
    if key is None:
        raise LpClampUnresolved(
            f"cannot resolve a canonical pool identity for protocol={protocol!r} "
            f"position_id={position_id!r} pool={pool!r}; refusing to close unbounded"
        )

    rows = await state_manager.get_position_events_filtered(
        deployment_id=deployment_id,
        position_types=frozenset({"LP"}),
    )

    # The two sides frequently name the SAME position in shapes no pure function can
    # bridge. Measured on a Base fork: the OPEN event stores the pool ADDRESS
    # (0x67b00b46...) while the close intent carries the SYMBOLIC USDC/DAI/stable.
    # Canonicalization alone therefore matches nothing, and the clamp refuses a close it
    # should have bounded.
    #
    # `position_events` has no `pool` column -- only `position_id` and `protocol` -- so
    # there is no third field to join on. The available disambiguator is scope: rows are
    # already restricted to THIS deployment, and the connector's protocol is known. When
    # that scope contains exactly ONE distinct LP identity, it is unambiguously the one
    # being closed, whatever shape either side wrote.
    #
    # More than one distinct identity means the shapes genuinely cannot be attributed,
    # and the contract is refusal -- never "pick one" and never "sum them all", either of
    # which would bound the close by another pool's liquidity.
    lp_rows = [r for r in rows if str(r.get("event_type") or "").upper() in _OPEN_EVENTS | _CLOSE_EVENTS]
    distinct = {str(r.get("position_id") or "") for r in lp_rows if r.get("position_id")}
    fallback_key: str | None = None
    if len(distinct) == 1:
        only = _resolve_identity(decl, next(iter(distinct)), None)
        if only is not None and only != key:
            fallback_key = only
            logger.info(
                "VIB-6162: matching this deployment's single LP identity %r for a close "
                "naming %r — the open and close lanes wrote different shapes for the "
                "same position",
                fallback_key,
                key,
            )

    outstanding = Decimal(0)
    matched = 0
    for row in rows:
        row_key = _resolve_identity(decl, row.get("position_id"), row.get("pool"))
        if row_key != key and (fallback_key is None or row_key != fallback_key):
            continue
        event = str(row.get("event_type") or "").upper()
        if event not in _OPEN_EVENTS and event not in _CLOSE_EVENTS:
            continue
        amount = _scale(row.get("liquidity"), decl)
        if amount is None:
            # An unmeasured leg makes the whole fold unmeasured. Skipping it would
            # understate outstanding, and understating is not "safe" here -- it is a
            # different wrong number presented with the same confidence.
            #
            # BOTH close lanes consume this fold since VIB-6517 (teardown attach and
            # the runner's iteration-lane attach), so a VIB-6488 fix that changes what
            # CLOSE rows carry changes the bound on every strategy-decided close too.
            raise LpClampUnresolved(
                f"position_events row {row.get('id')!r} for pool {key} has no measured "
                f"liquidity; outstanding cannot be derived (VIB-6488)"
            )
        matched += 1
        outstanding += amount if event in _OPEN_EVENTS else -amount

    if matched == 0:
        raise LpClampUnresolved(
            f"no position_events rows for deployment={deployment_id!r} pool={key}; outstanding is unmeasured, not zero"
        )
    return outstanding


def bound_close_amount(
    outstanding: Decimal | None,
    live_balance: Decimal | None,
    *,
    pool_key: str = "",
) -> Decimal:
    """Return the amount a close may withdraw, or raise :class:`LpClampUnresolved`.

    **This is deliberately not ``min(outstanding, live)``.** That formula was specified
    during design review and is unsafe: with outstanding=100, foreign=50, and 60 of the
    strategy's own LP transferred out of the wallet, the wallet holds 40 own + 50
    foreign and ``min(100, 90) = 90`` burns **all 50 foreign shares** — re-creating the
    exact defect this module exists to remove.

    ``outstanding > live`` means the ledger and the chain disagree about what this
    deployment still holds, so the balance is no longer attributable and the contract is
    refusal — the same rule as an unresolvable identifier. Refusing strands a position
    that a human can then close deliberately; guessing spends someone else's money.
    """
    if outstanding is None:
        raise LpClampUnresolved(f"outstanding is unmeasured for pool {pool_key}; refusing to close unbounded")
    if live_balance is None:
        raise LpClampUnresolved(f"live balance is unmeasured for pool {pool_key}; refusing to close unbounded")
    if outstanding < 0:
        raise LpClampUnresolved(
            f"outstanding for pool {pool_key} folded negative ({outstanding}); "
            f"closes exceed opens in this deployment's history"
        )
    if outstanding > live_balance:
        raise LpClampUnresolved(
            f"tracked outstanding ({outstanding}) exceeds live balance ({live_balance}) "
            f"for pool {pool_key}; the balance is no longer attributable to this "
            f"deployment and the close is refused rather than widened"
        )
    return outstanding


__all__ = [
    "LpClampUnresolved",
    "bound_close_amount",
    "clamp_decl_for",
    "read_outstanding_liquidity",
]
