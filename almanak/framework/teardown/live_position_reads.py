"""Live, per-KNOWN-position chain re-derivation for teardown — VIB-5463 / TD-05.

Blueprint 14:811 requires ``get_open_positions()`` to **"query on-chain state -
do not use cached state."** TD-01 (``registry_enumeration``) made the durable
``position_registry`` the WARM read path for the two cut-over LP primitives, so a
restarted runner re-derives the open LP set from WARM even when in-memory state
was wiped. But two gaps remained, both explicitly deferred to TD-05:

1. **Non-cut-over primitives still trust the cache.** A lending strategy
   (``morpho_looping`` and its siblings) reports its open SUPPLY / BORROW from
   in-memory counters (``_total_collateral`` / ``_total_borrowed``). On a
   wiped / ``--fresh`` / corrupt-WARM restart those counters are zero, so
   ``get_open_positions()`` returns **nothing** and teardown silently strands a
   live on-chain debt + collateral. Note ``--fresh`` deletes *every* durable
   WARM tier — ``position_registry``, ``transaction_ledger`` AND
   ``position_events`` (``_run_setup._FRESH_DEPLOYMENT_ID_TABLES``) — so after a
   ``--fresh`` boot the **only** surviving identity for a lending position is the
   strategy's own **config** (``market_id`` + collateral/borrow tokens, which
   are deterministic) plus the **chain** itself. The fix is therefore to
   re-derive the live amounts of the *config-known* market from chain.

2. **The registry read's failure path was warn-only.** When the registry SQL
   read itself raised (transient gateway / decode fault), ``registry_enumeration``
   logged a WARNING and fell back to the strategy enumeration unverified — see
   the comment it leaves at the ``except Exception`` branch: *"Live re-derivation
   when the registry read fails is owned by TD-05 (VIB-5463)."*

This module is the generalisation of the ``morpho_looping`` pattern the ticket
calls for, plus the per-position LP chain-verify capability TD-06 needs to
eventually trust the registry instead of unioning with the legacy enumeration.

**Plan A only (per-KNOWN-position), never a wallet-wide scan.** Every read here
is scoped to an identity the framework *already knows* — a lending market named
by config, or a single LP NFT ``token_id`` the registry / strategy already
reported. The wallet-wide on-chain discovery that finds *unknown* token ids is
Plan B (``teardown.discovery`` / ``teardown.lp_recovery``), a separate lane.

**Gateway boundary (CLAUDE.md §Gateway boundary).** Lending reads go through
``MarketSnapshot.position_health`` (gateway-routed ``eth_call``); the LP verify
reuses ``teardown.discovery``'s gateway-routed ``positions(tokenId)`` read. No
direct RPC / HTTP is opened here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.market import MarketSnapshot
    from almanak.framework.teardown.models import PositionInfo

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveLendingPosition:
    """Live on-chain state of a single KNOWN lending market (VIB-5463).

    Re-derived from chain via :func:`redrive_lending_position` for a market the
    strategy already knows by config. USD values come straight from the
    gateway-routed ``position_health`` read; token amounts are a best-effort
    USD/price conversion (``None`` when the oracle can't price a leg — Empty ≠
    Zero, never a fabricated amount).

    Attributes:
        collateral_value_usd: USD value of deposited collateral (``Decimal("0")``
            == measured-zero, a real closed-collateral signal).
        debt_value_usd: USD value of outstanding debt (``Decimal("0")`` == no
            debt).
        health_factor: Live HF, or ``None`` when the market reports no debt /
            the read could not compute it.
        collateral_amount: Collateral in token units, or ``None`` when the
            collateral price was unavailable.
        debt_amount: Debt in token units, or ``None`` when the debt-token price
            was unavailable.
    """

    collateral_value_usd: Decimal
    debt_value_usd: Decimal
    health_factor: Decimal | None
    collateral_amount: Decimal | None
    debt_amount: Decimal | None

    def has_exposure(self, *, dust_usd: Decimal = Decimal("0.01")) -> bool:
        """True iff either leg carries more than ``dust_usd`` of value on-chain.

        Used as the teardown DETECTION gate: a market whose live collateral and
        debt are both at/under dust is genuinely closed and must not be surfaced
        as an open position (which would emit a no-op REPAY / WITHDRAW).
        """
        return self.collateral_value_usd > dust_usd or self.debt_value_usd > dust_usd


def _safe_price(market: MarketSnapshot, token: str) -> Decimal | None:
    """Best-effort positive USD price for ``token``; ``None`` on any failure."""
    try:
        raw = market.price(token)
    except Exception:  # noqa: BLE001 — re-derivation must never fault the teardown lane
        return None
    if raw is None:
        return None
    try:
        price = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return price if price > 0 else None


def redrive_lending_position(
    *,
    market: MarketSnapshot,
    protocol: str,
    market_id: str,
    collateral_token: str,
    borrow_token: str,
    collateral_price_usd: Decimal | None = None,
    debt_price_usd: Decimal | None = None,
) -> LiveLendingPosition | None:
    """Re-derive a KNOWN lending position's live state from chain.

    Generalises the ``morpho_looping`` teardown-detection pattern: given a market
    the strategy knows by **config** (``protocol`` + ``market_id`` +
    collateral/borrow token symbols), read the *current* on-chain collateral,
    debt and health factor through the gateway-routed
    :meth:`MarketSnapshot.position_health`. This is what lets teardown honour
    blueprint 14:811 on a wiped / ``--fresh`` restart, where the in-memory
    counters are zero but the on-chain position is still live.

    Args:
        market: A live :class:`MarketSnapshot` (gateway-wired). The caller owns
            building it (``self.create_market_snapshot()`` in a strategy).
        protocol: Lending protocol id (``"morpho_blue"``, ``"aave_v3"``,
            ``"compound_v3"``, …) — passed verbatim to ``position_health``.
        market_id: Protocol market identifier (bytes32 for Morpho; the Comet key
            for Compound; informational for Aave V3 — one pool per chain).
        collateral_token: Collateral token symbol (for the USD→token amount
            conversion only).
        borrow_token: Debt/borrow token symbol (same).
        collateral_price_usd: Optional collateral price override (Morpho
            cross-asset markets require it; otherwise leave ``None`` and the
            snapshot's own oracle is used).
        debt_price_usd: Optional debt-token price override.

    Returns:
        A :class:`LiveLendingPosition` when the chain read succeeded (including a
        cleanly measured all-zero position — a genuinely closed market), or
        ``None`` when the on-chain read was **unavailable** (gateway down,
        unsupported protocol, oracle missing). ``None`` means *unmeasured* —
        the caller MUST fall back to its cached enumeration rather than treat the
        market as closed (Empty ≠ Zero; never strand a position because a read
        blipped).

    Never raises — re-derivation must never fault the teardown lane.
    """
    try:
        health = market.position_health(
            protocol,
            market_id,
            collateral_price_usd=collateral_price_usd,
            debt_price_usd=debt_price_usd,
        )
    except Exception as exc:  # noqa: BLE001 — unavailable read ⇒ caller fail-safes to cache
        logger.info(
            "Teardown live re-derivation: position_health unavailable for "
            "protocol=%s market_id=%s (%s: %s) — caller will fall back to cached "
            "enumeration",
            protocol,
            market_id[:18] if isinstance(market_id, str) else market_id,
            type(exc).__name__,
            exc,
        )
        return None

    if health is None:
        # A double (mock) or an edge-case provider can return None rather than
        # raise. Treat it as UNAVAILABLE (unmeasured), never as a measured-zero
        # closed market — otherwise the all-zero ``getattr`` defaults below would
        # silently report "closed" and the caller would strand a live position.
        logger.info(
            "Teardown live re-derivation: position_health returned None for "
            "protocol=%s market_id=%s — treating as unavailable (cache fallback)",
            protocol,
            market_id,
        )
        return None

    try:
        collateral_value_usd = Decimal(str(getattr(health, "collateral_value_usd", "0") or "0"))
        debt_value_usd = Decimal(str(getattr(health, "debt_value_usd", "0") or "0"))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(
            "Teardown live re-derivation: position_health returned non-numeric "
            "values for protocol=%s market_id=%s — treating as unavailable",
            protocol,
            market_id,
        )
        return None

    raw_hf = getattr(health, "health_factor", None)
    health_factor: Decimal | None
    try:
        health_factor = Decimal(str(raw_hf)) if raw_hf is not None else None
    except (InvalidOperation, ValueError, TypeError):
        health_factor = None

    # Best-effort USD → token-unit conversion. A missing price leaves the amount
    # at None (unmeasured) — the USD value alone is enough for teardown
    # DETECTION; the unwind sizes itself from chain (TD-07 / the leverage-loop
    # helper) and does not depend on these amounts.
    collateral_price = (
        collateral_price_usd
        if collateral_price_usd and collateral_price_usd > 0
        else _safe_price(market, collateral_token)
    )
    debt_price = debt_price_usd if debt_price_usd and debt_price_usd > 0 else _safe_price(market, borrow_token)
    collateral_amount = (collateral_value_usd / collateral_price) if collateral_price else None
    debt_amount = (debt_value_usd / debt_price) if debt_price else None

    return LiveLendingPosition(
        collateral_value_usd=collateral_value_usd,
        debt_value_usd=debt_value_usd,
        health_factor=health_factor,
        collateral_amount=collateral_amount,
        debt_amount=debt_amount,
    )


async def chain_verify_lp_open(
    *,
    gateway_client: Any,
    position: PositionInfo,
    network: str = "",
) -> bool | None:
    """Verify a SINGLE KNOWN LP NFT's open-ness on-chain (VIB-5463 / TD-05).

    The per-KNOWN-position chain-verify capability TD-06 needs to eventually
    trust the ``position_registry`` instead of unioning it with the legacy
    enumeration. Given one LP :class:`PositionInfo` whose ``position_id`` is the
    NFT ``token_id``, read ``positions(tokenId).liquidity`` on the
    NonfungiblePositionManager(s) registered for the position's chain (reusing
    the gateway-routed :mod:`teardown.discovery` primitive — no new egress).

    This is deliberately **per-position**, never a wallet scan: it reads one
    known ``token_id`` and answers "is *this* position still open?", so it can
    distinguish a registry row that is genuinely open from one whose write was
    skipped / that has since been closed — the signal that lets
    "absent from registry" be told apart from "open but write-skipped /
    pre-cutover" (AC3). The actual union→authoritative FLIP is TD-06's; this only
    provides the verdict.

    Args:
        gateway_client: A connected :class:`GatewayClient` (gateway-routed RPC).
        position: The LP position to verify. Only ``position_id`` (NFT token id)
            and ``chain`` are read.
        network: Gateway network override; ``""`` (default) uses the gateway's
            configured network — which on a managed Anvil run is the fork, not
            the live chain.

    Returns:
        ``True``  — the NFT was found and reports ``liquidity > 0`` (open).
        ``False`` — the NFT was found with ``liquidity == 0`` (burned / fully
                    withdrawn → closed).
        ``None``  — UNVERIFIABLE: no gateway, no V3-family NPM on the chain, the
                    token id is not a uint, the NFT was not found on any
                    registered NPM (e.g. a UniV4 ``lp_v4`` position, which is on a
                    different position manager), or every read errored. ``None``
                    means *unknown* — the caller MUST NOT treat it as closed.

    Never raises — verification must never fault the teardown lane.
    """
    if gateway_client is None:
        return None
    if not getattr(gateway_client, "is_connected", True):
        return None

    chain = str(getattr(position, "chain", "") or "").lower()
    if not chain:
        return None
    try:
        token_id = int(str(getattr(position, "position_id", "")))
    except (ValueError, TypeError):
        # Composite / pool-prefixed id, or no bare token id ⇒ not verifiable here.
        return None

    # The import + NPM-registry resolution can raise (ImportError / registry
    # lookup faults); the docstring promises this never faults the teardown lane,
    # so guard them. The per-NPM read inside the loop is already individually
    # guarded so one blip can't mask the others.
    try:
        from almanak.framework.teardown.discovery import _npms_for_chain, _read_position

        npms = _npms_for_chain(chain)
    except Exception:  # noqa: BLE001 — verification must never raise into teardown
        logger.debug("chain_verify_lp_open: NPM resolution failed for chain %s", chain, exc_info=True)
        return None
    if not npms:
        return None

    found_closed = False
    for protocol, npm in npms:
        try:
            discovered = await _read_position(gateway_client, chain, npm, token_id, network=network, protocol=protocol)
        except Exception:  # noqa: BLE001 — try the next NPM; one blip must not mask others
            logger.debug(
                "chain_verify_lp_open: read failed on %s/%s for token %s",
                protocol,
                npm,
                token_id,
                exc_info=True,
            )
            continue
        if discovered is None:
            continue  # not minted on this NPM — try the next registered one
        if discovered.liquidity > 0:
            return True
        # Found on this NPM with zero liquidity ⇒ closed. A token id is minted on
        # exactly one NPM, so this is authoritative; finish the loop only to be
        # robust to a (pathological) duplicate hit reporting liquidity elsewhere.
        found_closed = True

    if found_closed:
        return False
    return None


__all__ = [
    "LiveLendingPosition",
    "chain_verify_lp_open",
    "redrive_lending_position",
]
