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

from almanak.connectors._strategy_base.teardown_post_condition import resolve_nft_token_id

if TYPE_CHECKING:
    from almanak.framework.market import MarketSnapshot
    from almanak.framework.teardown.models import PositionInfo

logger = logging.getLogger(__name__)


def _reviewed_npm_for_position(*, position: PositionInfo, protocol: str, chain: str) -> str | None:
    """Resolve the exact connector-reviewed manager authority for ``position``.

    Classic V3 connectors publish one NPM through ``AbiFamily.V3_NPM``.  A
    multi-generation CL connector instead declares every reviewed manager as
    ordered ``CL_POSITION_MANAGER`` contract kinds.  In that shape the durable
    position identity MUST name its manager; choosing the first declaration
    would query the same numeric token id on the wrong ERC-721 contract.

    An explicit manager is always checked against connector-owned metadata.
    Unknown or ambiguous identity returns ``None`` (unmeasured) and never falls
    back to another manager, preserving teardown continuity without certifying
    the wrong position.
    """
    details = position.details if isinstance(position.details, dict) else {}
    raw_manager = details.get("nft_manager_addr") or details.get("position_manager") or details.get("nft_manager")
    explicit_manager = str(raw_manager).strip() if raw_manager else ""
    slug = protocol.lower()

    try:
        # Bootstraps connector-owned role declarations without importing any
        # concrete connector into the framework.
        import almanak.connectors._strategy_contract_role_registry  # noqa: F401
        from almanak.connectors._strategy_base.address_registry import AddressRegistry
        from almanak.connectors._strategy_base.contract_role_registry import (
            CONTRACT_ROLE_REGISTRY,
            ContractRole,
        )

        kinds = CONTRACT_ROLE_REGISTRY.kinds_for(slug, ContractRole.CL_POSITION_MANAGER)
        if kinds is not None:
            address_protocol = CONTRACT_ROLE_REGISTRY.address_protocol(slug)
            contracts = AddressRegistry.addresses_for(address_protocol, chain)
            reviewed = tuple(address for kind in kinds if isinstance((address := contracts.get(kind)), str) and address)
            if explicit_manager:
                matches = [address for address in reviewed if address.lower() == explicit_manager.lower()]
                return matches[0] if len(matches) == 1 else None
            return reviewed[0] if len(reviewed) == 1 else None

        from almanak.framework.teardown.discovery import npm_for_protocol

        npm = npm_for_protocol(slug, chain)
    except Exception:  # noqa: BLE001 — verification must never raise into teardown
        logger.debug(
            "chain_verify_lp_open: connector manager resolution failed for protocol %s on chain %s",
            protocol,
            chain,
            exc_info=True,
        )
        return None

    if not npm:
        return None
    if explicit_manager and explicit_manager.lower() != npm.lower():
        return None
    return npm


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

    # VIB-6285: the SAME reasoning as the ``health is None`` guard above, applied
    # per FIELD. A health object that exists but whose value field is MISSING or
    # ``None`` used to coerce through the ``getattr(..., "0") or "0"`` defaults to
    # ``Decimal("0")`` — a FABRICATED measured zero. ``_reconcile_lending`` reads
    # that as at/under dust ⇒ ``DIVERGED_CLOSED``, and since W0.1 that verdict is
    # no longer merely "fails to lower confidence": it is affirmative evidence
    # that CERTIFIES the position's protocol group. A missing field would
    # certify a live lending position closed and strand it.
    #
    # Empty ≠ Zero: only a genuinely PRESENT, numeric value is a measured zero.
    # Missing / ``None`` ⇒ unavailable (unmeasured), exactly like ``health is
    # None`` above and the non-numeric branch below. ``_MISSING`` is used rather
    # than a ``None`` default so "attribute absent" and "attribute present but
    # None" both land here without being confused with a real value.
    _MISSING = object()
    raw_values = {
        "collateral_value_usd": getattr(health, "collateral_value_usd", _MISSING),
        "debt_value_usd": getattr(health, "debt_value_usd", _MISSING),
    }
    for field_name, raw in raw_values.items():
        if raw is _MISSING or raw is None:
            logger.warning(
                "Teardown live re-derivation: position_health is missing %s for "
                "protocol=%s market_id=%s — treating as unavailable (unmeasured), NOT a "
                "measured zero; certifying closure off a fabricated zero would strand a "
                "live position (VIB-6285)",
                field_name,
                protocol,
                market_id,
            )
            return None

    try:
        collateral_value_usd = Decimal(str(raw_values["collateral_value_usd"]))
        debt_value_usd = Decimal(str(raw_values["debt_value_usd"]))
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
    NFT ``token_id``, read the position's liquidity on the
    NonfungiblePositionManager **of the position's own protocol** via the
    gateway's typed ``QueryPositionLiquidity`` RPC (no new egress, no new proto).

    **Protocol-scoped, never a cross-NPM walk (VIB-5631).** NPM token ids are
    per-contract monotonic counters: the SAME uint exists independently on every
    V3-fork NPM deployed to a chain. Walking all registered NPMs for a bare
    token id (the pre-VIB-5631 shape) matched a foreign protocol's
    identically-numbered, unrelated position — a burned sushiswap_v3 NFT read
    back as "STILL OPEN" off uniswap_v3's NPM, flipping a provably-clean
    teardown to FAILED. The read is now scoped to the single NPM
    ``position.protocol`` resolves to; a position whose protocol has no
    registered NPM on the chain is ``None`` (unverifiable here), never probed
    against other protocols' NPMs.

    **Tri-state, Empty ≠ Zero (mirrors VIB-5634 for V4).** The gateway read
    distinguishes a MEASURED closure from a read fault: a burned NFT's
    ``positions(tokenId)`` revert ("Invalid token ID") is folded by
    ``query_position_liquidity`` into ``liquidity = 0`` — a measurement — while
    a gateway/RPC fault returns ``None`` (unmeasured). A burned position is
    therefore ``False`` (measured-closed), never conflated with "not found /
    read failed".

    This is deliberately **per-position**, never a wallet scan: it reads one
    known ``token_id`` and answers "is *this* position still open?", so it can
    distinguish a registry row that is genuinely open from one whose write was
    skipped / that has since been closed — the signal that lets
    "absent from registry" be told apart from "open but write-skipped /
    pre-cutover" (AC3). The actual union→authoritative FLIP is TD-06's; this only
    provides the verdict.

    Args:
        gateway_client: A connected :class:`GatewayClient` (gateway-routed RPC).
        position: The LP position to verify. Only ``position_id``, ``details``
            (NFT tokenId resolution — the SAME shared rule the TD-14 hooks
            use, ``resolve_nft_token_id``), ``protocol`` and ``chain`` are
            read.
        network: Accepted for signature stability. The underlying
            ``QueryPositionLiquidity`` RPC always targets the gateway's
            configured network — identical to the ``""`` every production
            caller passes (``_gateway_network`` is never populated); a
            non-empty override cannot be honoured and is logged at DEBUG.

    Returns:
        ``True``  — the position's own NPM reports ``liquidity > 0`` (open).
        ``False`` — MEASURED closed: the position's own NPM reports
                    ``liquidity == 0`` — either the burned-NFT path (the
                    canonical "Invalid token ID" revert, folded to 0 by the
                    gateway read) or a fully-decreased, unburned NFT shell.
        ``None``  — UNVERIFIABLE: no gateway, the position's protocol has no
                    registered NPM on the chain (non-V3-family LP, e.g. a UniV4
                    ``lp_v4`` position on a different position manager), the
                    token id is not a uint, or the read faulted. ``None`` means
                    *unknown* — the caller MUST NOT treat it as closed.

    Never raises — verification must never fault the teardown lane.
    """
    if gateway_client is None:
        return None
    if not getattr(gateway_client, "is_connected", True):
        return None

    chain = str(getattr(position, "chain", "") or "").lower()
    if not chain:
        return None
    # SHARED NFT-id resolution (VIB-5631 parity): identical rule to the
    # TD-14 post-condition hooks — ``details`` keys (nft_position_id / nft_id /
    # token_id / position_id) first, then the ``position_id`` attribute. Before
    # this, Plan-A only parsed a numeric ``position_id``, so a strategy using a
    # human-readable id ("my-lp-1") with the NFT id in ``details`` verified
    # fine in TD-14 but reconciled UNVERIFIABLE here — the two lanes
    # contradicted each other on the same position. Numeric attribute ids
    # resolve exactly as before.
    token_id = resolve_nft_token_id(position)
    if token_id is None:
        # Composite / pool-prefixed id with no numeric detail key, or no bare
        # token id anywhere ⇒ not verifiable here (never a guess).
        return None
    if network:
        logger.debug(
            "chain_verify_lp_open: network override %r ignored — QueryPositionLiquidity "
            "targets the gateway's configured network",
            network,
        )

    protocol = str(getattr(position, "protocol", "") or "")
    npm = _reviewed_npm_for_position(position=position, protocol=protocol, chain=chain)
    if not npm:
        # Not an NFT-based protocol, no reviewed manager on this chain, or a
        # multi-generation position omitted its exact manager identity. This
        # read cannot answer and MUST NOT guess by probing another manager.
        return None

    # Gateway-routed, protocol-scoped, tri-state read. query_position_liquidity
    # folds the burned-NFT "Invalid token ID" revert into liquidity=0 (a MEASURED
    # closure) and returns None on a gateway/RPC fault (unmeasured) — the same
    # read the TD-14 post-condition hook trusts, so the two lanes cannot
    # contradict each other on a burned position.
    try:
        liquidity = gateway_client.query_position_liquidity(
            chain=chain,
            position_manager=npm,
            token_id=token_id,
        )
    except Exception:  # noqa: BLE001 — verification must never raise into teardown
        logger.debug(
            "chain_verify_lp_open: query_position_liquidity raised for %s token %s on %s",
            protocol,
            token_id,
            chain,
            exc_info=True,
        )
        return None
    if liquidity is None:
        return None  # read FAULT — unknown, never "closed" (Empty ≠ Zero)
    return bool(liquidity > 0)


__all__ = [
    "LiveLendingPosition",
    "chain_verify_lp_open",
    "redrive_lending_position",
]
