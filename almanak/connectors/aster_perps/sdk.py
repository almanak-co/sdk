"""Aster Perps SDK (Aster/ApolloX Diamond on BSC).

Low-level struct encoding and contract-interaction helpers for the Aster perpetual
trading platform (formerly ApolloX, rebranded March 2025 after ApolloX + Astherus
merger). The router is a Diamond proxy (EIP-2535). Aster attributes fees and volume
by a ``broker`` id in every order payload: PancakeSwap Perps is broker id = 2,
"raw Aster" use is broker id = 0, and other partner brokers occupy other ids.

Phase 1 scope: BSC only (PRD at docs/internal/discussions/aster-dex-integration-20260418.md).
Multi-chain EVM expansion is gated on DR-V2ABI / DR-CHAINS (Linear VIB-3044 epic).

The trade surface relevant to v1 (crypto perps, market orders, no SL/TP):

    TradingPortalFacet:
      openMarketTrade((address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24))
      openMarketTradeBNB((address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24))
      closeTrade(bytes32)

    TradingReaderFacet (views):
      getPendingTrade(bytes32)           -> pending-trade struct
      getPositionByHashV2(bytes32)       -> open-position struct
      getPositionsV2(address, address)   -> all open positions for trader/market

Execution model is two-phase: the user's call emits a MarketPendingTrade event with
a bytes32 tradeHash (the position ID). An off-chain keeper then calls
PriceFacadeFacet.requestPriceCallback/batchRequestPriceCallback with the oracle
price, which triggers marketTradeCallback -> TradingOpenFacet.OpenMarketTrade. Close
follows the same pattern (closeTrade -> closeTradeCallback -> CloseTradeSuccessful).

This SDK handles the SYNCHRONOUS side only (building calldata for the user-signed
open/close call). Keeper-driven fulfillment is fixture-level testing concern.

References:
  Router:                0x1b6F2d3844C6ae7D56ceb3C3643b9060ba28FEb0
  TradingPortalFacet:    0x5553F3B5E2fAD83edA4031a3894ee59e25ee90bF (open/close)
  TradingOpenFacet:      0xdbe2b7e92f00dBd70478199577393bE5BBe37201 (keeper settle open)
  TradingCloseFacet:     0x8ECa88449B9AFF247F775B96be6e3479bBE72a09 (keeper settle close)
  TradingReaderFacet:    0x28dE81Bc5B6164d8522ad32AD7D139A21fa1E3b4 (views)
  PriceFacadeFacet:      0x646CbAD1B150E5D3a019827a304717950ba6442e (keeper entry)
  PairsManagerFacet:     0xA32b528D70D1d5bA93a17D2697Efe5D17F1A6F8d (markets)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

from eth_abi import encode as abi_encode
from eth_utils import keccak

from almanak.core.contracts import (
    ASTER_PERPS,
    ASTER_PERPS_MARKETS,
    ASTER_PERPS_TOKENS,
    PANCAKESWAP_PERPS_BROKER_ID,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Function Selectors (keccak256 of signature, first 4 bytes)
# =============================================================================
#
# Verified from BSCScan source for each facet (see abis/*.json).

SELECTOR_OPEN_MARKET_TRADE = bytes.fromhex("703085c7")
"""openMarketTrade((address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24))"""

SELECTOR_OPEN_MARKET_TRADE_BNB = bytes.fromhex("b7aeae66")
"""openMarketTradeBNB((address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24))"""

SELECTOR_CLOSE_TRADE = bytes.fromhex("5177fd3b")
"""closeTrade(bytes32)"""

SELECTOR_GET_PENDING_TRADE = bytes.fromhex("429cbec2")
"""getPendingTrade(bytes32) -> (tuple)"""

SELECTOR_GET_POSITION_BY_HASH_V2 = bytes.fromhex("2c5e754a")
"""getPositionByHashV2(bytes32) -> (tuple)"""

SELECTOR_GET_POSITIONS_V2 = bytes.fromhex("ac6f50ec")
"""getPositionsV2(address, address) -> (tuple[])"""


# =============================================================================
# Event topic hashes (keccak256 of event signature)
# =============================================================================

# TradingPortalFacet.MarketPendingTrade(address indexed user, bytes32 indexed tradeHash, tuple trade)
EVENT_MARKET_PENDING_TRADE = "0x7064b82c073f138da0ec7646ebb51d4a0061d647accb4670bc564edf0bfac41d"

# TradingOpenFacet.OpenMarketTrade(address indexed user, bytes32 indexed tradeHash, tuple ot)
EVENT_OPEN_MARKET_TRADE = "0xa858fcdefab65cbd1997932d8ac8aa1a9a8c46c90b20947575525d9a2a437f8c"

# TradingOpenFacet.PendingTradeRefund(address indexed user, bytes32 indexed tradeHash, uint8 refund)
EVENT_PENDING_TRADE_REFUND = "0x2f8b631074b09d8b7f3fbc9184a5ddb3a59bc2bff68c6511e7e6d3d7a26159da"

# TradingCloseFacet.CloseTradeReceived(address indexed user, bytes32 indexed tradeHash, address indexed token, uint256 amount)
EVENT_CLOSE_TRADE_RECEIVED = "0x82982650e85330c501b1cfb68b1ae3e06d6be2048d4378a018d8b84da6d5c959"

# TradingCloseFacet.CloseTradeSuccessful(address indexed user, bytes32 indexed tradeHash, tuple closeInfo)
EVENT_CLOSE_TRADE_SUCCESSFUL = "0x664a65d3a62405ef2c683493b52778675085be44db1631414b5a2918e4f538b2"

# FeeManagerFacet.OpenFee(address indexed token, uint256 totalFee, uint256 daoAmount, uint24 brokerId, uint256 brokerAmount, uint256 alpPoolAmount)
# Not a critical event for v1 but included for completeness in the registry.


# =============================================================================
# Oracle price scale
# =============================================================================

# ApolloX/PancakeSwap Perps oracle prices are 8-decimal fixed-point (like Pyth/Chainlink).
# qty is 10-decimal fixed-point (per Constants.QTY_DECIMALS in the on-chain source,
# and per IBook/ILimitOrder interface comments "uint80 qty; // 1e10"). Margin
# (amountIn) is in the collateral token's native decimals.
PRICE_DECIMALS = 8
QTY_DECIMALS = 10

# Broker ids (attribution only — does not affect routing or fills).
# Callers must explicitly pass a broker id to the adapter; the SDK exposes the
# well-known ids here as named constants so dispatch layers (the compiler, the
# pancakeswap_perps/ shim) can reference them without magic numbers.
PCS_BROKER_ID: int = PANCAKESWAP_PERPS_BROKER_ID  # = 2
ASTER_BROKER_RAW: int = 0  # No broker attribution (default for raw aster_perps use).


# =============================================================================
# Native BNB sentinel — matches what the router expects in the `tokenIn` slot.
# =============================================================================

# When opening a BNB-margined position via openMarketTradeBNB, tokenIn is address(0)
# and the BNB margin is sent as msg.value. The router wraps it to WBNB internally.
NATIVE_BNB_ADDRESS = "0x0000000000000000000000000000000000000000"


# =============================================================================
# Open-trade struct
# =============================================================================


@dataclass(frozen=True)
class OpenTradeStruct:
    """Python mirror of the Aster openMarketTrade input struct.

    All integer fields use the on-wire units the contract expects:
      - amountIn:   collateral-token smallest units (wei-equivalent)
      - qty:        10-decimal fixed-point (e.g. 0.15 BTC = 1500000000)
      - price:      8-decimal fixed-point limit / acceptable price
      - stopLoss:   8-decimal fixed-point (0 = no SL)
      - takeProfit: 8-decimal fixed-point (0 = no TP)
      - broker:     uint24 broker id (PancakeSwap = 2, raw Aster = 0)
    """

    pair_base: str
    is_long: bool
    token_in: str  # ERC-20 margin token, or NATIVE_BNB_ADDRESS for native BNB
    amount_in: int  # uint96 — collateral amount in token's smallest units
    qty: int  # uint80 — position base units, 10-decimal fixed-point
    price: int  # uint64 — acceptable price (limit), 8-decimal fixed-point
    broker: int  # uint24 — REQUIRED (no default; attribution id)
    stop_loss: int = 0  # uint64
    take_profit: int = 0  # uint64


def encode_open_market_trade_calldata(trade: OpenTradeStruct, *, native: bool = False) -> bytes:
    """Encode calldata for openMarketTrade or openMarketTradeBNB.

    Args:
        trade: populated OpenTradeStruct
        native: when True uses openMarketTradeBNB (native BNB margin via msg.value),
                when False uses openMarketTrade (ERC20 margin, requires prior approve).

    Returns:
        4-byte selector + abi-encoded tuple.
    """
    _validate_open_struct(trade, native=native)
    selector = SELECTOR_OPEN_MARKET_TRADE_BNB if native else SELECTOR_OPEN_MARKET_TRADE
    # Both openMarketTrade and openMarketTradeBNB take a single tuple argument with
    # identical component layout.
    encoded = abi_encode(
        ["(address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24)"],
        [
            (
                _check_address(trade.pair_base),
                bool(trade.is_long),
                _check_address(trade.token_in),
                int(trade.amount_in),
                int(trade.qty),
                int(trade.price),
                int(trade.stop_loss),
                int(trade.take_profit),
                int(trade.broker),
            )
        ],
    )
    return selector + encoded


def encode_close_trade_calldata(trade_hash: str | bytes) -> bytes:
    """Encode calldata for closeTrade(bytes32).

    Args:
        trade_hash: 32-byte position identifier (hex string or raw bytes).

    Returns:
        4-byte selector + abi-encoded bytes32.
    """
    raw = _to_bytes32(trade_hash)
    return SELECTOR_CLOSE_TRADE + abi_encode(["bytes32"], [raw])


def encode_get_pending_trade_calldata(trade_hash: str | bytes) -> bytes:
    """Encode calldata for the getPendingTrade(bytes32) view."""
    raw = _to_bytes32(trade_hash)
    return SELECTOR_GET_PENDING_TRADE + abi_encode(["bytes32"], [raw])


def encode_get_position_by_hash_calldata(trade_hash: str | bytes) -> bytes:
    """Encode calldata for the getPositionByHashV2(bytes32) view."""
    raw = _to_bytes32(trade_hash)
    return SELECTOR_GET_POSITION_BY_HASH_V2 + abi_encode(["bytes32"], [raw])


# =============================================================================
# Size / price unit conversion helpers
# =============================================================================


def usd_size_to_qty(size_usd: Decimal, mark_price: Decimal) -> int:
    """Convert a USD notional to on-wire qty (uint80, 10-decimal fixed-point).

    qty = size_usd / mark_price, then scale to 1e10 (see ``QTY_DECIMALS``).

    Raises ValueError on non-positive inputs (keeps the connector fail-fast
    — silently coercing zero/negative sizes into bogus positions is exactly
    the class of bug the 'no quick patches' guardrail forbids).
    """
    if size_usd <= 0:
        raise ValueError(f"size_usd must be positive, got {size_usd}")
    if mark_price <= 0:
        raise ValueError(f"mark_price must be positive, got {mark_price}")
    qty_decimal = (size_usd / mark_price) * (Decimal(10) ** QTY_DECIMALS)
    qty = int(qty_decimal)  # truncate — under-size is safer than over-size
    if qty == 0:
        raise ValueError(
            f"size_usd={size_usd} / mark_price={mark_price} rounds to zero qty at "
            f"{QTY_DECIMALS}-decimal scale — increase size or check price"
        )
    if qty >= 2**80:
        raise ValueError(f"qty {qty} exceeds uint80 range")
    return qty


def slippage_to_limit_price(mark_price: Decimal, slippage: Decimal, *, is_long: bool) -> int:
    """Convert (mark_price, slippage_fraction) into the `price` field the router expects.

    For longs: acceptable price = mark_price * (1 + slippage) — trader willing to pay up to this.
    For shorts: acceptable price = mark_price * (1 - slippage) — trader willing to receive down to this.

    Returned value is uint64 with 8-decimal scaling.
    """
    if mark_price <= 0:
        raise ValueError(f"mark_price must be positive, got {mark_price}")
    if slippage < 0 or slippage >= 1:
        raise ValueError(f"slippage must be in [0, 1), got {slippage}")
    if is_long:
        limit = mark_price * (Decimal(1) + slippage)
    else:
        limit = mark_price * (Decimal(1) - slippage)
    limit_scaled = int(limit * (Decimal(10) ** PRICE_DECIMALS))
    if limit_scaled <= 0:
        raise ValueError(f"scaled limit price is non-positive ({limit_scaled})")
    if limit_scaled >= 2**64:
        raise ValueError(f"limit price {limit_scaled} exceeds uint64 range")
    return limit_scaled


# =============================================================================
# Market / token registry accessors
# =============================================================================


def get_router_address(chain: str = "bsc") -> str:
    """Return the Aster Perps router address for the given chain."""
    entry = ASTER_PERPS.get(chain)
    if not entry:
        raise ValueError(f"Aster Perps not configured for chain '{chain}'")
    return entry["router"]


def get_pair_base(market: str, chain: str = "bsc") -> str:
    """Resolve a market symbol (e.g. 'BTC/USD') to the on-chain pairBase address.

    Accepts either a registered symbol (v1: BTC/USD, ETH/USD, BNB/USD) or a 0x-prefixed
    EVM address (passed through after validation — lets ``PerpOpenIntent.market`` carry
    the pairBase address directly for synthetic / non-registered markets).

    Raises ValueError if the symbol is not registered and is not a valid address.
    Non-crypto markets (NVDA, TSLA, ...) use synthetic ApolloX-issued pairBases and
    are deferred to v2 per the design doc; callers wanting to hit them today must
    pass the pairBase address explicitly.
    """
    if isinstance(market, str) and market.startswith("0x"):
        return _check_address(market)
    markets = ASTER_PERPS_MARKETS.get(chain, {})
    if market not in markets:
        raise ValueError(
            f"Market '{market}' not registered for Aster Perps on '{chain}'. Supported (v1): {sorted(markets.keys())}"
        )
    return markets[market]


def get_margin_token_address(
    symbol: str,
    chain: str = "bsc",
    token_resolver=None,
) -> str:
    """Resolve a margin-token symbol (WBNB/USDT/USDC) to its BSC ERC-20 address.

    Accepts either a symbol or a 0x-prefixed EVM address (passed through after
    validation). For native BNB, pass symbol='BNB' or 'NATIVE' and the
    NATIVE_BNB_ADDRESS sentinel is returned (the caller must use
    encode_open_market_trade_calldata(native=True)).

    Symbol resolution goes through the framework's unified
    :func:`almanak.framework.data.tokens.get_token_resolver` so the connector
    stays in sync with the rest of the token-metadata surface (aliases,
    on-chain fallbacks, disk cache). The local
    :data:`ASTER_PERPS_TOKENS` allowlist is only consulted to reject
    tokens that the Aster router does not accept as margin; the *address*
    for allowed symbols comes from the resolver.

    Args:
        symbol: Margin token symbol (e.g. 'WBNB', 'USDT', 'USDC', 'BNB', 'NATIVE')
            or a 0x-prefixed EVM address.
        chain: Chain key (default 'bsc').
        token_resolver: Optional ``TokenResolver`` override; defaults to the
            singleton returned by ``get_token_resolver()``.

    Raises:
        ValueError: If the symbol is not one of Aster Perps' supported margin tokens.
        TokenNotFoundError: If the resolver cannot resolve an allowed symbol to
            an on-chain address on ``chain`` (propagated unchanged).
    """
    if isinstance(symbol, str) and symbol.startswith("0x"):
        return _check_address(symbol)
    up = symbol.upper()
    if up in ("BNB", "NATIVE"):
        return NATIVE_BNB_ADDRESS
    tokens = ASTER_PERPS_TOKENS.get(chain, {})
    if up not in tokens:
        raise ValueError(
            f"Margin token '{symbol}' not registered for Aster Perps on '{chain}'. "
            f"Supported: BNB (native), {sorted(tokens.keys())}"
        )
    if token_resolver is None:
        # Lazy import: keeps SDK import light and avoids a circular dep on
        # framework.data (which itself imports from almanak.connectors._strategy_base.base).
        from almanak.framework.data.tokens import get_token_resolver

        token_resolver = get_token_resolver()
    # Delegate to the resolver; this preserves aliases (e.g. BUSD quirks) and
    # honours its on-chain fallback if the symbol is ever removed from the
    # static registry. TokenNotFoundError propagates.
    return token_resolver.get_address(chain, up)


# =============================================================================
# Internal helpers
# =============================================================================


def _to_bytes32(value: str | bytes) -> bytes:
    if isinstance(value, bytes):
        if len(value) != 32:
            raise ValueError(f"bytes32 expected, got {len(value)} bytes")
        return value
    s = value[2:] if value.startswith("0x") else value
    if len(s) != 64:
        raise ValueError(f"bytes32 hex expected (64 chars), got {len(s)}")
    return bytes.fromhex(s)


def _check_address(addr: str) -> str:
    if not isinstance(addr, str) or not addr.startswith("0x") or len(addr) != 42:
        raise ValueError(f"Invalid EVM address: {addr!r}")
    # Reject non-hex payloads here so downstream failures (eth_abi encoding,
    # RPC calls) surface as a deterministic ValueError instead of opaque errors.
    try:
        int(addr[2:], 16)
    except ValueError as exc:
        raise ValueError(f"Invalid EVM address: {addr!r}") from exc
    # eth_abi accepts either checksum or lowercase; we accept either and
    # let eth_abi handle casing.
    return addr


def _validate_open_struct(t: OpenTradeStruct, *, native: bool) -> None:
    _check_address(t.pair_base)
    _check_address(t.token_in)
    if native and int(t.token_in, 16) != 0:
        raise ValueError(f"openMarketTradeBNB requires tokenIn=address(0) sentinel; got {t.token_in}")
    if not native and int(t.token_in, 16) == 0:
        raise ValueError(
            "openMarketTrade requires a non-zero ERC20 tokenIn; for native BNB margin use openMarketTradeBNB"
        )
    if t.amount_in <= 0 or t.amount_in >= 2**96:
        raise ValueError(f"amountIn {t.amount_in} out of uint96 range")
    if t.qty <= 0 or t.qty >= 2**80:
        raise ValueError(f"qty {t.qty} out of uint80 range")
    if t.price <= 0 or t.price >= 2**64:
        raise ValueError(f"price {t.price} out of uint64 range")
    if t.stop_loss < 0 or t.stop_loss >= 2**64:
        raise ValueError(f"stopLoss {t.stop_loss} out of uint64 range")
    if t.take_profit < 0 or t.take_profit >= 2**64:
        raise ValueError(f"takeProfit {t.take_profit} out of uint64 range")
    if t.broker < 0 or t.broker >= 2**24:
        raise ValueError(f"broker {t.broker} out of uint24 range")


# =============================================================================
# Selector self-check (belt-and-braces: recomputes selectors from signatures at
# import time and asserts they match the hard-coded constants above. Catches
# copy/paste errors before they reach production.)
# =============================================================================


def _assert_selector(sig: str, expected: bytes) -> None:
    got = keccak(sig.encode())[:4]
    if got != expected:
        raise RuntimeError(f"Selector mismatch for {sig}: expected {expected.hex()} got {got.hex()}")


_assert_selector(
    "openMarketTrade((address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24))",
    SELECTOR_OPEN_MARKET_TRADE,
)
_assert_selector(
    "openMarketTradeBNB((address,bool,address,uint96,uint80,uint64,uint64,uint64,uint24))",
    SELECTOR_OPEN_MARKET_TRADE_BNB,
)
_assert_selector("closeTrade(bytes32)", SELECTOR_CLOSE_TRADE)
_assert_selector("getPendingTrade(bytes32)", SELECTOR_GET_PENDING_TRADE)
_assert_selector("getPositionByHashV2(bytes32)", SELECTOR_GET_POSITION_BY_HASH_V2)
_assert_selector("getPositionsV2(address,address)", SELECTOR_GET_POSITIONS_V2)


# isort-style ordering (alphabetical, uppercase before lowercase) to satisfy
# ruff RUF022 even though the rule is not in our selected set today.
__all__ = [
    "ASTER_BROKER_RAW",
    "EVENT_CLOSE_TRADE_RECEIVED",
    "EVENT_CLOSE_TRADE_SUCCESSFUL",
    "EVENT_MARKET_PENDING_TRADE",
    "EVENT_OPEN_MARKET_TRADE",
    "EVENT_PENDING_TRADE_REFUND",
    "NATIVE_BNB_ADDRESS",
    "OpenTradeStruct",
    "PCS_BROKER_ID",
    "PRICE_DECIMALS",
    "QTY_DECIMALS",
    "SELECTOR_CLOSE_TRADE",
    "SELECTOR_GET_PENDING_TRADE",
    "SELECTOR_GET_POSITION_BY_HASH_V2",
    "SELECTOR_GET_POSITIONS_V2",
    "SELECTOR_OPEN_MARKET_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE_BNB",
    "encode_close_trade_calldata",
    "encode_get_pending_trade_calldata",
    "encode_get_position_by_hash_calldata",
    "encode_open_market_trade_calldata",
    "get_margin_token_address",
    "get_pair_base",
    "get_router_address",
    "slippage_to_limit_price",
    "usd_size_to_qty",
]
