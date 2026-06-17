"""Aster Perps adapter — translates PerpOpenIntent / PerpCloseIntent into TransactionData.

Aster (formerly ApolloX/Astherus, rebranded March 2025) is the on-chain perpetual
trading platform that powers PancakeSwap Perps (PCS = broker id 2 on Aster). The
on-chain contracts use a Diamond proxy (EIP-2535). Aster has announced deployments
on Arbitrum, opBNB, and Base in addition to BSC — Phase 1 is BSC-only; multi-chain
is gated on DR-V2ABI / DR-CHAINS research (PRD: aster-dex-integration-20260418.md).

Keeps the protocol-specific concerns (selectors, struct encoding, native-BNB vs ERC20
margin routing, oracle price unit conversion) localized so the compiler only has to
decide "which adapter" and pass through a straightforward intent.

Scope (Phase 1):
    - BSC only
    - Market orders only, no SL/TP (those are deferred per PRD)
    - Crypto markets only (BTC/USD, ETH/USD, BNB/USD via the token-address pairBase registry)
    - Broker id is REQUIRED — no default. Callers (compiler / shim) supply the id:
      pancakeswap_perps shim → 2 (PCS), raw aster_perps → 0 (no attribution).
    - Native BNB margin (openMarketTradeBNB) or ERC20 margin (openMarketTrade)
    - Min notional ~$200-250 per pair (enforced by TradingCheckerFacet)

Deliberately small surface vs gmx_v2 — position-query / teardown helpers are deferred
to a later iteration that can build the full TradingReaderFacet client. For v1 the
strategy is responsible for persisting its own tradeHash in state.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .sdk import (
    ASTER_BROKER_RAW,
    NATIVE_BNB_ADDRESS,
    OpenTradeStruct,
    encode_close_trade_calldata,
    encode_open_market_trade_calldata,
    get_margin_token_address,
    get_pair_base,
    get_router_address,
    slippage_to_limit_price,
    usd_size_to_qty,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Config + built transaction
# =============================================================================


@dataclass(frozen=True)
class AsterPerpsConfig:
    """Minimal connector config.

    Attributes:
        broker_id: broker attribution id (REQUIRED — no default).
            PancakeSwap Perps = 2 (supplied by the pancakeswap_perps shim),
            raw Aster = 0 (supplied by the compiler for protocol="aster_perps").
            Other partner brokers use their own assigned ids.
        chain: chain key (Phase 1 = 'bsc').
        wallet_address: trader EOA — not used on-chain by the open call (the
            router derives user from msg.sender) but recorded on the adapter so
            the compiler can pass it through to ActionBundle metadata.
    """

    broker_id: int  # REQUIRED — no default.
    chain: str = "bsc"
    wallet_address: str | None = None


@dataclass
class AsterPerpsTx:
    """Built transaction for the compiler to wrap in TransactionData."""

    to: str
    value: int
    data: bytes
    gas_estimate: int
    description: str


@dataclass
class PerpOpenOrderResult:
    """Adapter output for a compiled PERP_OPEN.

    Attributes:
        success: False if validation fails before tx construction.
        error: description when success is False.
        tx: the built transaction.
        pair_base: resolved pairBase address for the market.
        margin_token_address: resolved margin-token address (or NATIVE sentinel).
        qty: computed on-wire qty (uint80, 10-decimal).
        limit_price: computed acceptable limit price (uint64, 8-decimal).
        native: True if the transaction uses openMarketTradeBNB (value-carrying).
        amount_in_wei: margin amount in token-wei (matches OpenTradeStruct.amountIn).
    """

    success: bool
    error: str | None = None
    tx: AsterPerpsTx | None = None
    pair_base: str | None = None
    margin_token_address: str | None = None
    qty: int = 0
    limit_price: int = 0
    native: bool = False
    amount_in_wei: int = 0


# =============================================================================
# Gas estimates (empirical; tuned off the reference TX at 496k actual gas
# used on mainnet for openMarketTradeBNB — we budget ~2x headroom to cover
# warm-slot variance across different pairs).
# =============================================================================

GAS_OPEN_MARKET_TRADE_BNB: int = 900_000
GAS_OPEN_MARKET_TRADE: int = 1_000_000  # ERC20 path may include an extra transfer + wrap
GAS_CLOSE_TRADE: int = 600_000


# =============================================================================
# Adapter
# =============================================================================


class AsterPerpsAdapter:
    """Translate PERP_OPEN / PERP_CLOSE intents into transaction data."""

    def __init__(self, config: AsterPerpsConfig) -> None:
        # Fail-fast on missing broker_id — the dataclass no longer has a default
        # (per PRD §3.2); this belt-and-braces also catches callers that bypass
        # the dataclass constructor (e.g., dict/kwargs-style construction).
        if config.broker_id is None:
            raise ValueError(
                "broker_id is required on AsterPerpsConfig (no default). "
                "Pass PANCAKESWAP_PERPS_BROKER_ID (2) for PancakeSwap Perps, "
                "or ASTER_BROKER_RAW (0) for raw Aster."
            )
        if config.chain != "bsc":
            raise ValueError(f"Aster Perps Phase 1 supports chain='bsc' only, got '{config.chain}'")
        self.config = config
        self.router = get_router_address(config.chain)

    # -----------------------------------------------------------------
    # Open path
    # -----------------------------------------------------------------

    def build_open(
        self,
        *,
        market: str,
        collateral_token: str,
        collateral_amount: Decimal,
        collateral_decimals: int,
        size_usd: Decimal,
        mark_price: Decimal,
        is_long: bool,
        max_slippage: Decimal,
    ) -> PerpOpenOrderResult:
        """Build an openMarketTrade / openMarketTradeBNB transaction.

        Args:
            market: market symbol (e.g. 'BTC/USD'). Must be registered in
                almanak.core.contracts.ASTER_PERPS_MARKETS[chain].
            collateral_token: token symbol (e.g. 'BNB', 'USDT', 'USDC') or 0x address.
            collateral_amount: margin amount in human decimal terms.
            collateral_decimals: decimals of the margin token (resolver-provided).
            size_usd: position notional in USD.
            mark_price: current oracle mark price (in USD, not scaled) — used to
                convert the USD size into an 8-decimal qty and to compute the
                slippage-to-limit-price bound.
            is_long: True for long.
            max_slippage: fractional slippage tolerance, e.g. Decimal('0.01') for 1%.

        Returns:
            PerpOpenOrderResult. On failure, success=False and error set.
        """
        try:
            pair_base = get_pair_base(market, self.config.chain)
        except ValueError as e:
            return PerpOpenOrderResult(success=False, error=str(e))

        # Resolve margin token: accept symbols ('BNB', 'USDT', 'USDC') and 0x addresses.
        native = False
        if collateral_token.upper() in ("BNB", "NATIVE"):
            token_address = NATIVE_BNB_ADDRESS
            native = True
        elif collateral_token.startswith("0x") and len(collateral_token) == 42:
            token_address = collateral_token
        else:
            try:
                token_address = get_margin_token_address(collateral_token, self.config.chain)
                native = int(token_address, 16) == 0
            except ValueError as e:
                return PerpOpenOrderResult(success=False, error=str(e))

        # Compute on-wire units
        try:
            qty = usd_size_to_qty(size_usd, mark_price)
            limit_price = slippage_to_limit_price(mark_price, max_slippage, is_long=is_long)
        except ValueError as e:
            return PerpOpenOrderResult(success=False, error=f"Unit conversion failed: {e}")

        if collateral_amount <= 0:
            return PerpOpenOrderResult(success=False, error="collateral_amount must be positive")
        amount_in_wei = int(collateral_amount * (Decimal(10) ** collateral_decimals))
        if amount_in_wei <= 0:
            return PerpOpenOrderResult(
                success=False,
                error=(f"collateral_amount {collateral_amount} with decimals={collateral_decimals} rounds to zero wei"),
            )

        struct = OpenTradeStruct(
            pair_base=pair_base,
            is_long=is_long,
            token_in=token_address,
            amount_in=amount_in_wei,
            qty=qty,
            price=limit_price,
            broker=self.config.broker_id,
            stop_loss=0,
            take_profit=0,
        )

        try:
            calldata = encode_open_market_trade_calldata(struct, native=native)
        except (ValueError, RuntimeError) as e:
            return PerpOpenOrderResult(success=False, error=f"Encoding failed: {e}")

        # Native-BNB margin goes in msg.value; ERC20 margin needs prior approve (caller handles).
        tx_value = amount_in_wei if native else 0
        gas = GAS_OPEN_MARKET_TRADE_BNB if native else GAS_OPEN_MARKET_TRADE

        tx = AsterPerpsTx(
            to=self.router,
            value=tx_value,
            data=calldata,
            gas_estimate=gas,
            description=(
                f"AsterPerps {'LONG' if is_long else 'SHORT'} {market} "
                f"size=${size_usd} collateral={collateral_amount} {collateral_token} "
                f"(broker={self.config.broker_id}, native={'yes' if native else 'no'})"
            ),
        )
        return PerpOpenOrderResult(
            success=True,
            tx=tx,
            pair_base=pair_base,
            margin_token_address=token_address,
            qty=qty,
            limit_price=limit_price,
            native=native,
            amount_in_wei=amount_in_wei,
        )

    # -----------------------------------------------------------------
    # Close path
    # -----------------------------------------------------------------

    def build_close(self, *, trade_hash: str | bytes) -> AsterPerpsTx:
        """Build a closeTrade(bytes32) transaction for an open position.

        The strategy must supply the tradeHash — PerpCloseIntent's vocabulary keys
        on (market, is_long, collateral_token) which is insufficient for Aster
        where multiple positions per market+side are possible. The compiler is
        responsible for resolving a tradeHash either from intent metadata or from
        a reader-side lookup (out of v1 scope — strategy stores the hash itself).
        """
        calldata = encode_close_trade_calldata(trade_hash)
        # Normalize for description
        th_hex = trade_hash if isinstance(trade_hash, str) else "0x" + trade_hash.hex()
        return AsterPerpsTx(
            to=self.router,
            value=0,
            data=calldata,
            gas_estimate=GAS_CLOSE_TRADE,
            description=f"AsterPerps closeTrade({th_hex})",
        )


# =============================================================================
# Convenience re-exports
# =============================================================================


def build_open_transaction(
    *,
    broker_id: int,
    chain: str = "bsc",
    wallet_address: str | None = None,
    **open_kwargs: Any,
) -> PerpOpenOrderResult:
    """Build an open transaction without constructing the adapter explicitly.

    Args:
        broker_id: REQUIRED — no default. Pass ``ASTER_BROKER_RAW`` (0) for raw
            Aster use or ``PANCAKESWAP_PERPS_BROKER_ID`` (2) for the PancakeSwap
            Perps attribution path.
    """
    adapter = AsterPerpsAdapter(AsterPerpsConfig(broker_id=broker_id, chain=chain, wallet_address=wallet_address))
    return adapter.build_open(**open_kwargs)


def build_close_transaction(
    *,
    trade_hash: str | bytes,
    broker_id: int = ASTER_BROKER_RAW,
    chain: str = "bsc",
    wallet_address: str | None = None,
) -> AsterPerpsTx:
    """Build a close transaction without constructing the adapter explicitly.

    The close path does not emit a broker-attributed fee; the ``broker_id`` is
    plumbed through for consistency and defaulted to ``ASTER_BROKER_RAW`` (0).
    """
    adapter = AsterPerpsAdapter(AsterPerpsConfig(broker_id=broker_id, chain=chain, wallet_address=wallet_address))
    return adapter.build_close(trade_hash=trade_hash)


__all__ = [
    "GAS_CLOSE_TRADE",
    "GAS_OPEN_MARKET_TRADE",
    "GAS_OPEN_MARKET_TRADE_BNB",
    "AsterPerpsAdapter",
    "AsterPerpsConfig",
    "AsterPerpsTx",
    "PerpOpenOrderResult",
    "build_close_transaction",
    "build_open_transaction",
]
