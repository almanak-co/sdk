"""Connector-owned compiler for Hyperliquid perpetual intents (HyperEVM).

Compiles ``PERP_OPEN`` / ``PERP_CLOSE`` into a single ``CoreWriter.sendRawAction``
transaction on HyperEVM (chain 999). The order settles **asynchronously** on
HyperCore — the EVM tx only emits ``RawAction`` and never carries the fill — so
this compiler's job ends at "submit a correctly-encoded, fail-closed order";
settlement is observed later via the perps-read snapshot (see ``perps_read.py``).

Reference price comes from the HyperCore **oracle precompile** (``0x0807``): it
is the venue's own mark, so the slippage band is anchored to exactly the price
HyperCore fills against — no dependency on a cross-chain price oracle carrying
the asset on HyperEVM.

Scope honesty (bounded by the CoreWriter action set AND the perp intent
vocabulary): this compiles **market** open (IOC) and **market** close
(reduce-only IOC, full or partial). CoreWriter has no set-leverage action and
no native trigger orders, and the vocabulary's ``PerpOpenIntent`` carries no
limit-price/TIF/order-type field — so resting limits, TP/SL, and leverage
changes are NOT reachable through this path (they require the L1 EIP-712 API).
``intent.leverage`` is recorded and surfaced as a warning, never silently
ignored and never faked.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import ClassVar

from eth_utils import keccak

from almanak.connectors._strategy_base.base.compiler import BasePerpCompiler, PerpCompilerContext
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType, PerpCloseIntent, PerpOpenIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

from .addresses import (
    CORE_WRITER_ADDRESS,
    HYPEREVM_CHAIN,
    PERP_PX_MAX_DECIMALS,
    PRECOMPILE_ORACLE_PX,
    PRECOMPILE_POSITION,
)
from .markets import PerpMarket, resolve_market
from .sdk import (
    TIF_IOC,
    LimitOrderAction,
    Position,
    decode_position,
    decode_uint64,
    encode_limit_order_action,
    encode_perp_query,
    encode_position_query,
    encode_send_raw_action_calldata,
    market_limit_price,
    size_to_wire,
)

logger = logging.getLogger(__name__)

# CoreWriter open/close is a single ~47k-gas call; budget generously.
_CORE_WRITER_GAS = 150_000


class HyperliquidCompiler(BasePerpCompiler):
    """Compile Hyperliquid perp intents into CoreWriter transactions on HyperEVM."""

    protocols: ClassVar[frozenset[str]] = frozenset({"hyperliquid"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.PERP_OPEN, IntentType.PERP_CLOSE})
    chains: ClassVar[frozenset[str]] = frozenset({HYPEREVM_CHAIN})

    # ------------------------------------------------------------------ open
    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult:
        chain_err = self._require_hyperevm(ctx, intent.intent_id)
        if chain_err is not None:
            return chain_err

        try:
            market = resolve_market(intent.market)
        except ValueError as exc:
            return self._fail(intent.intent_id, str(exc))

        if intent.size_usd is None or intent.size_usd <= 0:
            return self._fail(intent.intent_id, f"PERP_OPEN requires a positive size_usd, got {intent.size_usd}")

        ref_price = self._read_oracle_price(ctx, market)
        if ref_price is None:
            return self._fail(
                intent.intent_id,
                f"HyperCore oracle price unavailable for {market.symbol} (asset {market.asset_index}); "
                "cannot anchor a fail-closed order band",
            )

        slippage_bps = self._slippage_bps(intent.max_slippage)
        try:
            human_size = Decimal(intent.size_usd) / ref_price
            sz_wire = size_to_wire(human_size, market.sz_decimals)
            limit_px = market_limit_price(
                ref_price, slippage_bps, is_buy=intent.is_long, sz_decimals=market.sz_decimals
            )
        except ValueError as exc:
            return self._fail(intent.intent_id, f"order sizing failed: {exc}")

        action = LimitOrderAction(
            asset=market.asset_index,
            is_buy=intent.is_long,
            limit_px=limit_px,
            sz=sz_wire,
            reduce_only=False,
            tif=TIF_IOC,
            cloid=self._cloid(intent.intent_id),
        )
        tx = self._core_writer_tx(action, tx_type="perp_open", description=f"Hyperliquid open {market.symbol}")

        return self._success(
            intent.intent_id,
            IntentType.PERP_OPEN,
            tx,
            metadata={
                "protocol": intent.protocol,
                "market": intent.market,
                "symbol": market.symbol,
                "asset_index": market.asset_index,
                "sz_decimals": market.sz_decimals,
                "is_long": intent.is_long,
                "size_usd": str(intent.size_usd),
                "reference_price": str(ref_price),
                "limit_px_wire": limit_px,
                "sz_wire": sz_wire,
                "tif": "IOC",
                "reduce_only": False,
                "leverage_requested": str(intent.leverage),
                "chain": ctx.chain,
            },
            warnings=self._leverage_warnings(intent.leverage),
        )

    # ----------------------------------------------------------------- close
    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult:
        chain_err = self._require_hyperevm(ctx, intent.intent_id)
        if chain_err is not None:
            return chain_err

        try:
            market = resolve_market(intent.market)
        except ValueError as exc:
            return self._fail(intent.intent_id, str(exc))

        raw = self._eth_call(ctx, PRECOMPILE_POSITION, encode_position_query(ctx.wallet_address, market.asset_index))
        if raw is None:
            return self._fail(
                intent.intent_id,
                f"could not read HyperCore position for {market.symbol}; retry when the read path is available",
            )
        position = decode_position(raw)
        if not position.is_open:
            return self._fail(
                intent.intent_id,
                f"no open Hyperliquid position for {market.symbol} on {ctx.wallet_address} — nothing to close",
            )

        # Close direction is the opposite of the position's sign; reduce_only
        # guarantees the order can only shrink (never flip) the position.
        close_is_buy = not position.is_long
        if intent.is_long != position.is_long:
            logger.warning(
                "PERP_CLOSE intent.is_long=%s disagrees with on-chain position sign (is_long=%s) for %s; "
                "closing the actual on-chain position",
                intent.is_long,
                position.is_long,
                market.symbol,
            )

        ref_price = self._read_oracle_price(ctx, market)
        if ref_price is None:
            return self._fail(
                intent.intent_id,
                f"HyperCore oracle price unavailable for {market.symbol}; cannot anchor a fail-closed close",
            )

        try:
            sz_wire = self._close_size_wire(intent, position, market, ref_price)
            slippage_bps = self._slippage_bps(intent.max_slippage)
            limit_px = market_limit_price(ref_price, slippage_bps, is_buy=close_is_buy, sz_decimals=market.sz_decimals)
        except ValueError as exc:
            return self._fail(intent.intent_id, f"close sizing failed: {exc}")

        action = LimitOrderAction(
            asset=market.asset_index,
            is_buy=close_is_buy,
            limit_px=limit_px,
            sz=sz_wire,
            reduce_only=True,
            tif=TIF_IOC,
            cloid=self._cloid(intent.intent_id),
        )
        tx = self._core_writer_tx(action, tx_type="perp_close", description=f"Hyperliquid close {market.symbol}")

        return self._success(
            intent.intent_id,
            IntentType.PERP_CLOSE,
            tx,
            metadata={
                "protocol": intent.protocol,
                "market": intent.market,
                "symbol": market.symbol,
                "asset_index": market.asset_index,
                "sz_decimals": market.sz_decimals,
                "is_long": position.is_long,
                "size_usd": str(intent.size_usd) if intent.size_usd is not None else "full",
                "reference_price": str(ref_price),
                "limit_px_wire": limit_px,
                "sz_wire": sz_wire,
                "tif": "IOC",
                "reduce_only": True,
                "chain": ctx.chain,
            },
            warnings=[],
        )

    # --------------------------------------------------------------- helpers
    def _require_hyperevm(self, ctx: PerpCompilerContext, intent_id: str) -> CompilationResult | None:
        if ctx.chain != HYPEREVM_CHAIN:
            return self._fail(
                intent_id,
                f"Hyperliquid perps execute on '{HYPEREVM_CHAIN}' via CoreWriter, got chain '{ctx.chain}'",
            )
        return None

    def _read_oracle_price(self, ctx: PerpCompilerContext, market: PerpMarket) -> Decimal | None:
        """HyperCore oracle price (human USD) via the ``0x0807`` precompile.

        Wire → human: ``raw / 10**(PERP_PX_MAX_DECIMALS - szDecimals)`` (verified
        live: BTC szDecimals 5, raw 598970 → 59897). Returns ``None`` when the
        read is unavailable (Empty≠Zero — never a measured 0).
        """
        raw = self._eth_call(ctx, PRECOMPILE_ORACLE_PX, encode_perp_query(market.asset_index))
        if raw is None:
            return None
        wire = decode_uint64(raw)
        if wire is None or wire <= 0:
            return None
        return Decimal(wire) / (Decimal(10) ** (PERP_PX_MAX_DECIMALS - market.sz_decimals))

    def _eth_call(self, ctx: PerpCompilerContext, to: str, data: bytes) -> str | None:
        """Gateway-routed ``eth_call`` to a HyperEVM precompile. ``None`` on failure."""
        try:
            result = ctx.services.eth_call(to, "0x" + data.hex(), chain=ctx.chain)
        except Exception as exc:  # noqa: BLE001 — read failures are non-fatal; fail closed upstream
            logger.warning("eth_call to %s failed: %s", to, exc)
            return None
        if result is None or result in ("0x", ""):
            return None
        return result

    def _close_size_wire(
        self, intent: PerpCloseIntent, position: Position, market: PerpMarket, ref_price: Decimal
    ) -> int:
        """Wire size for a reduce-only close.

        Full close (``size_usd is None``) uses the whole on-chain position size;
        partial close converts the requested USD notional to base size, capped at
        the position. ``reduce_only`` makes any residual over-estimate harmless.

        ``szi`` is the signed position size scaled by ``10**szDecimals`` (asserted
        end-to-end by the testnet round-trip).
        """
        full_human = Decimal(abs(position.szi)) / (Decimal(10) ** market.sz_decimals)
        if intent.size_usd is None:
            human = full_human
        else:
            if intent.size_usd <= 0:
                # Mirror the open-side positive-size validation: a non-positive
                # partial-close size must fail closed here (inside the caller's
                # try/except -> FAILED CompilationResult), never reach the
                # CoreWriter encoder with a zero/negative sz.
                raise ValueError(f"PERP_CLOSE size_usd must be positive when provided, got {intent.size_usd}")
            requested = Decimal(intent.size_usd) / ref_price
            human = min(requested, full_human)
        return size_to_wire(human, market.sz_decimals)

    def _core_writer_tx(self, action: LimitOrderAction, *, tx_type: str, description: str) -> TransactionData:
        blob = encode_limit_order_action(action)
        calldata = encode_send_raw_action_calldata(blob)
        return TransactionData(
            to=CORE_WRITER_ADDRESS,
            value=0,
            data="0x" + calldata.hex(),
            gas_estimate=_CORE_WRITER_GAS,
            description=description,
            tx_type=tx_type,
        )

    @staticmethod
    def _slippage_bps(max_slippage: Decimal) -> int:
        bps = int((Decimal(max_slippage) * Decimal(10_000)).to_integral_value())
        return max(0, min(bps, 10_000))

    @staticmethod
    def _cloid(intent_id: str) -> int:
        """Deterministic non-zero uint128 client-order-id from the intent id."""
        return int.from_bytes(keccak(intent_id.encode())[:16], "big") or 1

    @staticmethod
    def _leverage_warnings(leverage: Decimal) -> list[str]:
        if leverage is not None and Decimal(leverage) != Decimal("1"):
            return [
                "Hyperliquid via CoreWriter cannot set leverage (no CoreWriter action). "
                f"leverage={leverage} is recorded but NOT applied; the position uses the "
                "account's existing per-asset leverage. Set leverage out-of-band."
            ]
        return []

    def _fail(self, intent_id: str, error: str) -> CompilationResult:
        return CompilationResult(status=CompilationStatus.FAILED, intent_id=intent_id, error=error)

    def _success(
        self,
        intent_id: str,
        intent_type: IntentType,
        tx: TransactionData,
        *,
        metadata: dict,
        warnings: list[str],
    ) -> CompilationResult:
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent_id)
        result.action_bundle = ActionBundle(
            intent_type=intent_type.value,
            transactions=[tx.to_dict()],
            metadata=metadata,
        )
        result.transactions = [tx]
        result.total_gas_estimate = tx.gas_estimate
        result.warnings = warnings
        return result


__all__ = ["HyperliquidCompiler"]
