"""Connector-owned compiler for Hyperliquid perpetual intents (HyperEVM).

Compiles ``PERP_OPEN`` / ``PERP_CLOSE`` into a single ``CoreWriter.sendRawAction``
transaction on HyperEVM (chain 999). The order settles **asynchronously** on
HyperCore — the EVM tx only emits ``RawAction`` and never carries the fill — so
this compiler's job ends at "submit a correctly-encoded, fail-closed order";
settlement is observed later via the perps-read snapshot (see ``perps_read.py``).
``PERP_WITHDRAW`` (VIB-5617) is a cash movement, not a trade, and compiles to a
TWO-action CoreWriter bundle: a ``usdClassTransfer`` (action 7) rotating USDC
perp→spot, then a USDC ``spotSend`` (action 6) to the USDC system address that
HyperCore bridges back to the sender's HyperEVM wallet.

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
from decimal import ROUND_DOWN, Decimal
from typing import ClassVar

from eth_utils import keccak

from almanak.connectors._strategy_base.base.compiler import BasePerpCompiler, PerpCompilerContext
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType, PerpCloseIntent, PerpOpenIntent, PerpWithdrawIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

from .addresses import (
    CORE_WRITER_ADDRESS,
    HYPERCORE_MIN_ORDER_USD,
    HYPEREVM_CHAIN,
    PERP_PX_MAX_DECIMALS,
    PRECOMPILE_ORACLE_PX,
    PRECOMPILE_POSITION,
    USDC_SPOT_TOKEN_INDEX,
    USDC_SPOT_WEI_DECIMALS,
)
from .markets import PerpMarket, resolve_market
from .sdk import (
    TIF_IOC,
    LimitOrderAction,
    Position,
    build_usd_class_transfer_calldata,
    build_usdc_withdraw_calldata,
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
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW}
    )
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

        # Fail closed below HyperCore's minimum order value. HyperCore rejects a
        # sub-$10 open asynchronously off-EVM while `sendRawAction` still returns
        # status 1, so the order silently no-ops (never fills) — refuse to emit a
        # tx HyperCore will drop. Reduce-only closes are exempt (no min-order
        # rule), so this guard lives only on the open path.
        # VIB-5596: the insufficient-*margin* preflight (does the account have
        # enough free margin to open this size) is a separate follow-up — it
        # needs an account-margin read and is intentionally NOT implemented here.
        if intent.size_usd < HYPERCORE_MIN_ORDER_USD:
            return self._fail(
                intent.intent_id,
                f"PERP_OPEN size_usd ${intent.size_usd} is below the HyperCore "
                f"~${HYPERCORE_MIN_ORDER_USD} minimum order value; HyperCore would "
                "reject it off-EVM (silent no-op). Increase size_usd.",
            )

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
            [tx],
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
            [tx],
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

    # -------------------------------------------------------------- withdraw
    def compile_perp_withdraw(self, ctx: PerpCompilerContext, intent: PerpWithdrawIntent) -> CompilationResult:
        """Compile a PERP_WITHDRAW into a 2-action CoreWriter HyperCore→L1 bundle.

        Recovering **perp** margin back on-chain is two ordered CoreWriter actions
        (each a ``sendRawAction`` call), mirroring the real-money proof exactly:

        1. ``usdClassTransfer`` (action 7) — rotate USDC perp→spot. ``spotSend``
           reads the SPOT account, so free perp margin must be moved to spot first;
           this is the leg the proof had to do manually before the verb existed.
        2. ``spotSend`` (action 6) — send that USDC (token index 0, weiDecimals 8 —
           NOT the 1e6 perp ntl scale) to the USDC system address, which HyperCore
           detects as a HyperCore→HyperEVM bridge and credits back to the SENDER's
           HyperEVM (L1) wallet.

        This is a cash movement (no position, no PnL); HyperCore deducts a small
        (~$1) withdraw fee from the credited amount off-EVM. The framework submits
        the bundle sequentially for an EOA and as one atomic Zodiac MultiSend for a
        Safe. Fail-closed on a non-USDC asset (the only bridge-linked token today),
        a non-positive amount, a non-sender destination, and an unresolved ``"all"``
        marker. ``"all"`` is supported ONLY as a CHAINED amount (a prior step's
        received amount, resolved to a concrete Decimal by the runner before
        compile); standalone ``"all"`` is NOT yet supported — PERP_WITHDRAW is not a
        wallet-funded type, so there is no live HyperCore free-margin read to resolve
        it against (a follow-up), and a bare ``"all"`` reaching here fails closed.
        """
        chain_err = self._require_hyperevm(ctx, intent.intent_id)
        if chain_err is not None:
            return chain_err

        if intent.asset.upper() != "USDC":
            return self._fail(
                intent.intent_id,
                f"PERP_WITHDRAW on Hyperliquid supports only USDC (the HyperCore bridge-linked "
                f"token), got asset '{intent.asset}'",
            )

        amount = intent.amount
        if not isinstance(amount, Decimal):
            return self._fail(
                intent.intent_id,
                "PERP_WITHDRAW amount must be a resolved positive Decimal at compile time; "
                f"got {amount!r}. Standalone amount='all' is not supported for PERP_WITHDRAW "
                "(no live HyperCore free-margin read yet); use 'all' only as a chained amount "
                "(a prior step's received amount), which the runner resolves before compile.",
            )
        if not amount.is_finite() or amount <= 0:
            return self._fail(
                intent.intent_id,
                f"PERP_WITHDRAW amount must be a finite positive Decimal, got {amount}",
            )

        # The HyperCore bridge ALWAYS credits the sender's own HyperEVM wallet — a
        # spotSend to the USDC system address is bridge-detected only for the
        # originator. An explicit non-sender destination would be a plain spot
        # transfer, not a bridge (and cannot land funds on-chain), so fail closed.
        # NOTE: destination is a fail-closed sender-equality ASSERTION only; it is
        # NEVER threaded into the encoder — build_usdc_withdraw_calldata hardcodes
        # the USDC system bridge address, which credits the originating sender.
        if not ctx.wallet_address:
            return self._fail(intent.intent_id, "PERP_WITHDRAW requires a resolved deployment wallet address")
        destination = intent.destination or ctx.wallet_address
        if destination.lower() != ctx.wallet_address.lower():
            return self._fail(
                intent.intent_id,
                f"PERP_WITHDRAW destination {destination} must be the sender's own wallet "
                f"({ctx.wallet_address}); the HyperCore bridge credits only the spotSend originator.",
            )

        # The two legs use DIFFERENT venue scales (usdClassTransfer: 1e6 ntl;
        # spotSend USDC: weiDecimals=8). Build BOTH from the SAME amount quantized
        # DOWN to the coarser 6-dp scale — otherwise a >6-dp amount would transfer a
        # 6-dp-floored value perp->spot but spotSend the full 8-dp amount, leaving
        # spot short so HyperCore async-rejects the bridge while the EVM txs still
        # report success (VIB-5617 audit / Codex P1). ROUND_DOWN never over-withdraws.
        amount = amount.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
        if amount <= 0:
            return self._fail(
                intent.intent_id,
                f"PERP_WITHDRAW amount {intent.amount} is below the 6-dp minimum after "
                "quantization to the usdClassTransfer scale.",
            )
        try:
            transfer_calldata = build_usd_class_transfer_calldata(amount, to_perp=False)
            withdraw_calldata = build_usdc_withdraw_calldata(amount)
        except ValueError as exc:
            return self._fail(intent.intent_id, f"withdraw sizing failed: {exc}")

        transfer_tx = TransactionData(
            to=CORE_WRITER_ADDRESS,
            value=0,
            data="0x" + transfer_calldata.hex(),
            gas_estimate=_CORE_WRITER_GAS,
            description=f"Hyperliquid usdClassTransfer {amount} USDC perp->spot",
            tx_type="perp_withdraw",
        )
        withdraw_tx = TransactionData(
            to=CORE_WRITER_ADDRESS,
            value=0,
            data="0x" + withdraw_calldata.hex(),
            gas_estimate=_CORE_WRITER_GAS,
            description=f"Hyperliquid withdraw {amount} USDC HyperCore->HyperEVM",
            tx_type="perp_withdraw",
        )

        return self._success(
            intent.intent_id,
            IntentType.PERP_WITHDRAW,
            [transfer_tx, withdraw_tx],
            metadata={
                "protocol": intent.protocol,
                "asset": "USDC",
                "amount": str(amount),
                "spot_token_index": USDC_SPOT_TOKEN_INDEX,
                "wei_decimals": USDC_SPOT_WEI_DECIMALS,
                "destination": destination,
                "bridge": "hypercore->hyperevm",
                "legs": ["usd_class_transfer_perp_to_spot", "spot_send_bridge_to_l1"],
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
        txs: list[TransactionData],
        *,
        metadata: dict,
        warnings: list[str],
    ) -> CompilationResult:
        # ``txs`` is an ordered bundle: one tx for open/close, TWO for a
        # PERP_WITHDRAW (usd_class_transfer perp->spot, then spotSend spot->L1).
        # The framework submits a multi-tx bundle sequentially for an EOA and as
        # one atomic Zodiac MultiSend for a Safe (see orchestrator; MultiSend is
        # authorized globally as framework infrastructure).
        if not txs:
            raise ValueError("_success requires at least one transaction")
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent_id)
        result.action_bundle = ActionBundle(
            intent_type=intent_type.value,
            transactions=[tx.to_dict() for tx in txs],
            metadata=metadata,
        )
        result.transactions = list(txs)
        result.total_gas_estimate = sum(tx.gas_estimate for tx in txs)
        result.warnings = warnings
        return result


__all__ = ["HyperliquidCompiler"]
