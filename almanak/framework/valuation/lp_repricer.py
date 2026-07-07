"""Shared on-chain LP repricing engine.

This module owns the single canonical implementation of "value an open V3 /
Slipstream CL LP position from its live on-chain state". It was extracted
verbatim from ``PortfolioValuer._reprice_lp_on_chain_enriched`` (VIB-5664) so
that the snapshot-facing ``MarketSnapshot.lp_position_value`` and the
portfolio valuer share ONE code path — no bespoke tick / liquidity math is
re-implemented anywhere.

The math itself lives in :mod:`almanak.framework.valuation.lp_valuer`
(``value_lp_position``); this module is only the wiring: read the position and
pool slot0 via an ``LPPositionReader``, resolve symbols / decimals / pool
address, price each token, add uncollected fees, and return the enriched
result.

Empty ≠ Zero: every unmeasured input (missing token id, unreadable position,
unresolved symbol / decimals, non-positive price) returns ``None`` — never a
fabricated ``$0`` — so callers can distinguish "measured zero liquidity" from
"could not measure".
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .lp_valuer import value_lp_position

logger = logging.getLogger(__name__)


@dataclass
class LPPositionValueResult:
    """Typed result of :func:`reprice_lp_position`, snapshot-facing.

    ``value_usd`` is the LP token value EXCLUDING uncollected fees;
    ``fees_usd`` is the uncollected-fee USD separately (so a caller can decide
    whether to fold fees into NAV). ``total_usd`` = ``value_usd + fees_usd`` is
    the number the portfolio valuer persists. The raw ``enriched`` dict is kept
    for callers that want the full breakdown.
    """

    value_usd: Decimal
    fees_usd: Decimal
    total_usd: Decimal
    amount0: Decimal
    amount1: Decimal
    in_range: bool
    position_id: str
    liquidity: int
    token0_symbol: str | None = None
    token1_symbol: str | None = None
    enriched: dict[str, Any] = field(default_factory=dict)


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def build_lp_position_value_result(
    reprice_output: tuple[Decimal, dict[str, Any]] | None,
) -> LPPositionValueResult | None:
    """Wrap :func:`reprice_lp_position` output into a typed result.

    ``None`` in → ``None`` out (Empty ≠ Zero: an unmeasured position stays
    unmeasured, never a fabricated zero). The genuinely-empty position case
    (``{"position_id": ..., "liquidity": "0"}``) maps to an all-zero,
    out-of-range result so a caller sees measured-zero.
    """
    if reprice_output is None:
        return None
    total_usd, enriched = reprice_output
    fees_usd = _to_decimal(enriched.get("fees_usd"))
    token0_value = _to_decimal(enriched.get("token0_value_usd"))
    token1_value = _to_decimal(enriched.get("token1_value_usd"))
    # value_usd (LP tokens, no fees) = total - fees; prefer the token-value sum
    # when present (identical by construction), else derive from total.
    value_usd = (token0_value + token1_value) if ("token0_value_usd" in enriched) else (total_usd - fees_usd)
    liquidity_raw = enriched.get("liquidity", "0")
    try:
        liquidity = int(liquidity_raw)
    except (ValueError, TypeError):
        liquidity = 0
    return LPPositionValueResult(
        value_usd=value_usd,
        fees_usd=fees_usd,
        total_usd=total_usd,
        amount0=_to_decimal(enriched.get("amount0")),
        amount1=_to_decimal(enriched.get("amount1")),
        in_range=bool(enriched.get("in_range", False)),
        position_id=str(enriched.get("position_id", "")),
        liquidity=liquidity,
        token0_symbol=enriched.get("token0_symbol"),
        token1_symbol=enriched.get("token1_symbol"),
        enriched=enriched,
    )


# ---------------------------------------------------------------------------
# Small pure helpers (canonical home — PortfolioValuer delegates to these)
# ---------------------------------------------------------------------------


def looks_like_evm_address(value: object) -> bool:
    """Return True iff ``value`` is the 42-char ``0x``-prefixed hex shape.

    VIB-4274 — ``position.details["pool"]`` is type-overloaded across the
    codebase: some producers stash an actual pool contract address
    (``"0x..."``), others stash a human descriptor (``"WETH/USDC/500"``). The
    descriptor shape must be rejected before it reaches an ``eth_call``.
    """
    return (
        isinstance(value, str)
        and value.startswith("0x")
        and len(value) == 42
        and all(c in "0123456789abcdefABCDEF" for c in value[2:])
    )


def resolve_lp_pool_address_from_details(position: Any) -> str | None:
    """Resolve a usable pool address from ``position.details`` for repricing.

    ``pool_address`` first (canonical key for the actual contract), ``pool`` as
    a legacy fallback. Non-hex descriptor strings (e.g. ``"WETH/USDC/500"``)
    are rejected so the caller falls through to the price-ratio-tick
    approximation instead of feeding a descriptor into ``eth_call``.
    """
    pool_address = position.details.get("pool_address") or position.details.get("pool")
    if pool_address and not looks_like_evm_address(pool_address):
        return None
    return pool_address or None


def extract_token_id(position: Any) -> int | None:
    """Extract the numeric NFT token id from position data (or ``None``)."""
    pid = position.position_id
    if not pid:
        return None

    try:
        token_id = int(pid)
        if token_id >= 0:
            return token_id
    except (ValueError, TypeError):
        pass

    for key in ("token_id", "tokenId", "nft_id"):
        val = position.details.get(key)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass

    return None


def resolve_token_symbol(token_address: str, position: Any, field_name: str) -> str | None:
    """Resolve a token address to a symbol.

    Prefers the authoritative on-chain address via ``TokenResolver``, then
    falls back to strategy-reported metadata, then (for token0/token1) the
    ``tokens`` list.
    """
    try:
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        resolved = resolver.resolve(token_address, position.chain)
        if resolved and resolved.symbol:
            return resolved.symbol
    except Exception:
        pass

    symbol = position.details.get(field_name)
    if symbol:
        return symbol

    if field_name in ("token0", "token1"):
        tokens = position.details.get("tokens", [])
        idx = 0 if field_name == "token0" else 1
        if len(tokens) > idx:
            return tokens[idx]

    return None


def default_decimals_fn(symbol: str, chain: str) -> int | None:
    """Resolve token decimals. Returns ``None`` if unknown (never defaults to 18).

    Per codebase rules: "NEVER default to 18 decimals -- always raise if
    decimals unknown."
    """
    try:
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        return resolver.get_decimals(chain, symbol)
    except Exception:
        return None


def price_ratio_to_tick(
    token0_price: Decimal,
    token1_price: Decimal,
    token0_decimals: int,
    token1_decimals: int,
) -> int:
    """Derive an approximate V3 tick from USD prices and decimals.

    V3 price = token1_amount / token0_amount (in wei terms);
    tick = log(price) / log(1.0001). Used only when the pool slot0 tick is
    unavailable.
    """
    if token0_price <= 0 or token1_price <= 0:
        return 0

    price_ratio = float(token0_price / token1_price) * (10**token1_decimals / 10**token0_decimals)
    if price_ratio <= 0:
        return 0

    tick = math.log(price_ratio) / math.log(1.0001)
    return int(tick)


# ---------------------------------------------------------------------------
# The shared repricing engine
# ---------------------------------------------------------------------------


def reprice_lp_position(
    lp_reader: Any,
    position: Any,
    chain: str,
    price_fn: Callable[[str], Any],
    decimals_fn: Callable[[str, str], int | None],
) -> tuple[Decimal, dict[str, Any]] | None:
    """Re-price an open LP position from live on-chain state.

    This is the canonical body shared by ``PortfolioValuer`` and
    ``MarketSnapshot.lp_position_value`` — behaviour-identical to the original
    ``PortfolioValuer._reprice_lp_on_chain_enriched`` (VIB-5664 extraction).

    Args:
        lp_reader: An ``LPPositionReader`` (reads ``positions(tokenId)`` and
            pool ``slot0`` through the gateway).
        position: A ``PositionInfo``-shaped object exposing ``position_id``,
            ``protocol``, ``chain``, ``details``.
        chain: Chain identifier for the on-chain reads / decimals resolution.
        price_fn: Callable ``symbol -> price_usd`` (e.g. ``market.price``). May
            raise; a raise is treated as an unmeasured price.
        decimals_fn: Callable ``(symbol, chain) -> int | None``.

    Returns:
        ``(total_usd, enriched_details)`` where ``total_usd`` is LP value plus
        uncollected fees, or ``None`` on any unmeasured input (Empty ≠ Zero).
        A genuinely empty position (zero liquidity and zero fees) returns
        ``(Decimal("0"), {"position_id": ..., "liquidity": "0"})``.
    """
    try:
        token_id = extract_token_id(position)
        if token_id is None:
            return None

        on_chain = lp_reader.read_position(chain=chain, token_id=token_id, protocol=position.protocol)
        if on_chain is None:
            return None

        if on_chain.liquidity == 0 and on_chain.tokens_owed0 == 0 and on_chain.tokens_owed1 == 0:
            return Decimal("0"), {"position_id": str(token_id), "liquidity": "0"}

        token0_symbol = resolve_token_symbol(on_chain.token0, position, "token0")
        token1_symbol = resolve_token_symbol(on_chain.token1, position, "token1")
        if not token0_symbol or not token1_symbol:
            return None

        try:
            token0_price = Decimal(str(price_fn(token0_symbol)))
            token1_price = Decimal(str(price_fn(token1_symbol)))
        except Exception:
            return None

        if token0_price <= 0 or token1_price <= 0:
            return None

        token0_decimals = decimals_fn(token0_symbol, chain)
        token1_decimals = decimals_fn(token1_symbol, chain)
        if token0_decimals is None or token1_decimals is None:
            return None

        # VIB-4274 — prefer a real pool_address (hex-shape guarded); a
        # descriptor-shaped value would trip ``eth_call`` and the price-ratio
        # fallback would silently lie on the ``in_range`` flag.
        pool_address = resolve_lp_pool_address_from_details(position)
        current_tick: int | None = None
        sqrt_price_x96: int | None = None
        if pool_address:
            slot0 = lp_reader.read_pool_slot0(chain, pool_address)
            if slot0:
                current_tick = slot0.tick
                sqrt_price_x96 = slot0.sqrt_price_x96

        if current_tick is None:
            current_tick = price_ratio_to_tick(token0_price, token1_price, token0_decimals, token1_decimals)

        lp_value = value_lp_position(
            liquidity=on_chain.liquidity,
            tick_lower=on_chain.tick_lower,
            tick_upper=on_chain.tick_upper,
            current_tick=current_tick,
            token0_price_usd=token0_price,
            token1_price_usd=token1_price,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            sqrt_price_x96=sqrt_price_x96,
        )

        fees_usd = Decimal("0")
        fees0_human = Decimal("0")
        fees1_human = Decimal("0")
        if on_chain.tokens_owed0 > 0:
            fees0_human = Decimal(on_chain.tokens_owed0) / Decimal(10**token0_decimals)
            fees_usd += fees0_human * token0_price
        if on_chain.tokens_owed1 > 0:
            fees1_human = Decimal(on_chain.tokens_owed1) / Decimal(10**token1_decimals)
            fees_usd += fees1_human * token1_price

        total = lp_value.value_usd + fees_usd

        enriched = {
            "position_id": str(token_id),
            "amount0": str(lp_value.amount0),
            "amount1": str(lp_value.amount1),
            "token0_value_usd": str(lp_value.token0_value_usd),
            "token1_value_usd": str(lp_value.token1_value_usd),
            "in_range": lp_value.in_range,
            "tick_lower": on_chain.tick_lower,
            "tick_upper": on_chain.tick_upper,
            "liquidity": str(on_chain.liquidity),
            "fees0": str(fees0_human),
            "fees1": str(fees1_human),
            "fees_usd": str(fees_usd),
            "token0_symbol": token0_symbol,
            "token1_symbol": token1_symbol,
            "valuation_source": "on_chain",
        }

        return total, enriched

    except Exception:
        logger.debug("LP enriched re-pricing failed for %s", getattr(position, "position_id", "?"), exc_info=True)
        return None
