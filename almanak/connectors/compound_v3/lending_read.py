"""Compound V3 lending-read capability (aggregate account-state).

Publishes this connector's account-state read spec so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query Compound V3 state without the framework
hardcoding Compound's Comet selection, function selectors, or HF math:

* :data:`ACCOUNT_STATE_READ_SPEC` — aggregate account-state read (VIB-4929 PR-3b):
  collateral USD / debt USD / health factor. Compound V3 reads per-market from a
  per-*market* Comet (``balanceOf`` for base-asset supply, otherwise
  ``userCollateral`` + ``borrowBalanceOf``); the shared
  :data:`~almanak.connectors._strategy_base.lending_read_base.COMPOUND_V3_ACCOUNT_STATE_READ`
  spec describes that ABI and decodes it. That single-leg read answers
  "supply/debt for ONE collateral leg" — the valuation / ``amount="all"`` paths.

* :func:`read_compound_v3_market_health` — the **multi-collateral** account-health
  read (VIB-4851 PR-2). The position-health gate keeps the product-owner-chosen
  summed health factor
  ``HF = Σ_over_held_collaterals(value_usd × LCF) / borrow_value_usd``, which the
  single-leg seam cannot express (it reads one collateral). This is a NEW, parallel
  read that iterates every approved collateral of the market's Comet, reading each
  one's price + scale + liquidation factor ON-CHAIN.

Unlike the Aave family, Compound V3 is **not USD-native** and its read target is
per-market (not a single per-chain contract). The spec therefore declares empty
``contract_kinds`` (market-scoped target, bound by the registry from the
``COMPOUND_V3_ACCOUNT_STATE_MARKETS`` table's ``comet_address``),
``normalize_market_id=str.lower`` (Compound market ids are base-asset symbols),
and a ``query_inputs_fn`` for the intent-derived collateral token. It stays pure
(no gateway, no oracle); the framework reader owns price resolution + the gateway
round-trip.

Compound V3 publishes no single-reserve ``LENDING_READ_SPEC``: its account state is
read per-market via ``userCollateral`` / ``balanceOf`` / ``borrowBalanceOf``, not an
Aave-style ``getUserReserveData(asset, user)`` reserve read.

Gateway-boundary note: this module performs **no** network egress. The
``read_compound_v3_market_health`` reader receives an ``eth_call`` closure the
framework supplies (gateway-routed); the connector NEVER imports a gateway client.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.lending_read_base import (
    COMPOUND_V3_ACCOUNT_STATE_READ,
    AccountStateReadSpec,
    LendingAccountState,
    _parse_asset_info_hex,
    build_compound_asset_info_calldata,
    build_compound_borrow_balance_calldata,
    build_compound_collateral_balance_calldata,
    build_compound_get_price_calldata,
    decode_uint_hex,
)

logger = logging.getLogger(__name__)

#: Aggregate account-state read capability the registry dispatches for ``compound_v3``.
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = COMPOUND_V3_ACCOUNT_STATE_READ

# Comet's ``getPrice`` returns USD-denominated values with 8 decimals (priceScale).
_COMPOUND_PRICE_SCALE = Decimal("1e8")
# Compound V3 collateral factors are uint64 1e18-scaled.
_COMPOUND_CF_SCALE = Decimal("1e18")

__all__ = ["ACCOUNT_STATE_READ_SPEC", "read_compound_v3_market_health"]


def _decode_single_uint(hex_data: str | None) -> int | None:
    """Decode word 0 of a single-uint return blob, or ``None`` on a short/None blob.

    Fail-closed (Empty ≠ Zero): a missing or malformed blob for a HELD collateral
    or the borrow read must surface as ``None`` so the caller never silently
    under-counts collateral or debt.
    """
    if not hex_data:
        return None
    raw = hex_data[2:] if hex_data[:2].lower() == "0x" else hex_data
    if len(raw) < 64:
        return None
    try:
        return decode_uint_hex(raw, 0)
    except (ValueError, ArithmeticError):
        return None


def read_compound_v3_market_health(
    *,
    eth_call: Callable[[str, str], str | None],
    chain: str,
    comet_address: str,
    user_address: str,
    collaterals: Mapping[str, Mapping[str, Any]],
    base_token: str,
    base_token_address: str | None,
    resolve_base_price: Callable[[str], Decimal],
    resolve_base_decimals: Callable[[str, str], int],
) -> LendingAccountState | None:
    """Read a wallet's multi-collateral Compound V3 (Comet) account health.

    Reproduces the legacy ``position_health._get_compound_health`` loop EXACTLY,
    routed through a framework-supplied ``eth_call`` closure (gateway-routed) instead
    of an in-strategy ``Web3(HTTPProvider)``. The product-owner choice keeps the
    summed health factor across every HELD collateral:

        ``HF = Σ_over_held_collaterals(value_usd × LCF) / borrow_value_usd``

    Pricing decision (VIB-4851 — do NOT deviate): the per-collateral liquidation
    factor, price, and scale are read ON-CHAIN (``getAssetInfoByAddress`` /
    ``getPrice``), NEVER from the ``collaterals`` catalogue or an injected oracle —
    so injecting collateral prices would break byte-equivalence (no caller supplies
    them). ``collaterals`` provides only the collateral ADDRESSES to iterate. Only
    the base/borrow token price stays on the injected ``resolve_base_price``
    (stablecoin-1:1 / oracle / raise) and its decimals on ``resolve_base_decimals``
    — asymmetric, by design.

    Args:
        eth_call: ``(to, data) -> hex | None`` closure the framework binds to the
            gateway (+ chain). The connector NEVER imports a gateway client.
        chain: Chain identifier (informational; the closure is already chain-bound).
        comet_address: The per-market Comet contract every read targets.
        user_address: Wallet whose position is read.
        collaterals: The per-market ``{SYMBOL: {address, ...}}`` map — the source of
            the collateral ADDRESSES to iterate (LCF/scale/price come from on-chain).
        base_token: Base/borrow token symbol (priced via ``resolve_base_price``).
        base_token_address: Base token address. ``None`` ⇒ no borrow valuation
            (borrow_value stays a measured ``Decimal("0")``), matching the legacy
            guard ``base_token_address is not None and borrow_raw > 0``.
        resolve_base_price: ``symbol -> Decimal`` USD price for the base token.
            Raises (propagated) for a non-stable base with no oracle — fail-closed,
            never silently $1.
        resolve_base_decimals: ``(symbol, address) -> int`` base-token decimals.
            Raises (propagated) rather than guessing.

    Returns:
        A :class:`LendingAccountState` with ``collateral_usd = Σvalue``,
        ``debt_usd = borrow_value`` (a MEASURED ``Decimal("0")`` when no borrow —
        Empty ≠ Zero), ``health_factor`` computed only when ``debt > 0`` (else
        ``None``; the caller's ``_to_position_health`` maps no-debt to Infinity),
        ``lltv = Σ(value×lcf)/Σvalue`` when ``Σvalue > 0`` else ``None``,
        ``liquidation_threshold_bps=None``, ``e_mode_category=None``, ``family=None``.

    Returns ``None`` (never under-counts silently — Empty ≠ Zero) on any failed/short
    blob for a HELD collateral (or the borrow read).
    """
    if not comet_address or not base_token:
        # Fail closed: an unbound Comet target or missing base token means the
        # market inputs were not resolvable -- never read against a placeholder.
        return None

    collateral_value_usd = Decimal("0")
    liquidation_threshold_usd = Decimal("0")

    for _sym, cinfo in collaterals.items():
        addr = cinfo.get("address")
        if not addr:
            continue
        bal_raw = _decode_single_uint(
            eth_call(comet_address, build_compound_collateral_balance_calldata(user_address, addr))
        )
        if bal_raw is None:
            # A held collateral whose balance read failed/short ⇒ fail closed: we
            # cannot tell zero from unmeasured, and under-counting would inflate HF.
            return None
        if bal_raw == 0:
            continue

        asset_info = _parse_asset_info_hex(eth_call(comet_address, build_compound_asset_info_calldata(addr)))
        if asset_info is None:
            return None
        price_feed, scale, liquidate_cf = asset_info
        if scale == 0:
            # A zero scale would divide-by-zero the balance conversion — treat the
            # blob as malformed and fail closed rather than abort the whole read.
            return None

        price_raw = _decode_single_uint(eth_call(comet_address, build_compound_get_price_calldata(price_feed)))
        if price_raw is None:
            return None

        # Convert balance to human units using scale (token decimals) and price to USD.
        bal = Decimal(bal_raw) / Decimal(scale)
        price = Decimal(price_raw) / _COMPOUND_PRICE_SCALE
        value = bal * price
        liq_cf = Decimal(liquidate_cf) / _COMPOUND_CF_SCALE

        collateral_value_usd += value
        liquidation_threshold_usd += value * liq_cf

    borrow_raw = _decode_single_uint(eth_call(comet_address, build_compound_borrow_balance_calldata(user_address)))
    if borrow_raw is None:
        return None

    # Base-token price for borrow value. USD stablecoin bases fall back to 1:1
    # (via resolve_base_price); non-stable bases (WETH, AERO) MUST be priced via the
    # injected oracle — resolve_base_price refuses to silently assume $1 and inflate HF.
    if borrow_raw > 0:
        if base_token_address is None:
            # Active debt but no configured base-token address: fail closed
            # (Empty != Zero). Counting it as zero debt would inflate HF to
            # Infinity and mask an open borrow.
            return None
        base_price = resolve_base_price(base_token)
        base_decimals = resolve_base_decimals(base_token, base_token_address)
        borrow_amount = Decimal(borrow_raw) / (Decimal("10") ** base_decimals)
        borrow_value_usd = borrow_amount * base_price
    else:
        borrow_value_usd = Decimal("0")

    # HF computed only when there is debt; the caller's ``_to_position_health`` maps
    # no-debt (debt == 0) to Infinity, so leave it None here (Empty ≠ Zero).
    health_factor = (liquidation_threshold_usd / borrow_value_usd) if borrow_value_usd > 0 else None

    # Weighted-share lltv (Plan choice (a)): Σ(value×lcf)/Σvalue so the caller
    # reproduces ``lltv`` AND ``max_borrow_usd`` byte-exactly
    # (max_borrow = collateral × lltv − debt = Σ(value×lcf) − debt).
    lltv = (liquidation_threshold_usd / collateral_value_usd) if collateral_value_usd > 0 else None

    return LendingAccountState(
        collateral_usd=collateral_value_usd,
        debt_usd=borrow_value_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=None,
        e_mode_category=None,
        lltv=lltv,
        family=None,
    )
