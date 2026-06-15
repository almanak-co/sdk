"""Connector-owned compiler for Fluid DEX LP (SmartLending, Phase 4 / VIB-5032).

Fungible ERC-20-share wrappers over Fluid DEX pools. ``LP_OPEN`` →
``deposit(token0Amt, token1Amt, minShares, to)``; ``LP_CLOSE`` →
``withdraw(token0Amt, token1Amt, maxShares, to)`` sized from the live resolver
share→token read so it drains a position of any balance. Direct pool LP is
whitelist-gated (51013) — the wrapper is the whitelisted supplier; the compiler
pre-flights deposit-enabled and refuses a disabled pool at COMPILE.

Verified mechanics: ``docs/internal/qa/fluid-smartlending-validation-2026-06-12.md``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.connectors.fluid.addresses import (
    FLUID_DEX_LP,
    FLUID_DEX_LP_NATIVE_SENTINEL,
    FLUID_SMARTLENDING_MARKETS,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import IntentType, LPCloseIntent, LPOpenIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)

#: Default slippage tolerance (50 bps), matching the Fluid swap default. The
#: strategy author overrides per-intent via ``protocol_params["max_slippage"]``.
DEFAULT_LP_SLIPPAGE = Decimal("0.005")


def _resolve_slippage(intent: Any) -> Decimal:
    params = getattr(intent, "protocol_params", None) or {}
    raw = params.get("max_slippage")
    if raw is None:
        return DEFAULT_LP_SLIPPAGE
    tol = Decimal(str(raw))
    if tol < 0 or tol >= 1:
        raise ValueError(f"max_slippage must be in [0, 1), got {tol}")
    return tol


class FluidDexLpCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Fluid SmartLending fungible-share DEX LP."""

    protocols: ClassVar[frozenset[str]] = frozenset({"fluid_dex_lp"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.LP_OPEN, IntentType.LP_CLOSE})

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.LP_OPEN:
            return self.compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self.compile_lp_close(ctx, intent)
        return self._unsupported(intent)

    # -- shared helpers -----------------------------------------------------

    def _market(
        self, ctx: BaseCompilerContext, wrapper: str, intent_id: str
    ) -> tuple[dict[str, Any] | None, CompilationResult | None]:
        if ctx.chain not in FLUID_DEX_LP:
            return None, CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Fluid DEX LP is not supported on {ctx.chain}. Supported: {list(FLUID_DEX_LP.keys())}",
                intent_id=intent_id,
            )
        rows = FLUID_SMARTLENDING_MARKETS.get(ctx.chain, {})
        entry = rows.get(wrapper.lower())
        if entry is None:
            return None, CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Unknown Fluid SmartLending wrapper {wrapper} on {ctx.chain}. Known wrappers: {list(rows.keys())}"
                ),
                intent_id=intent_id,
            )
        return entry, None

    def _build_sdk(self, ctx: BaseCompilerContext, intent_id: str):
        from almanak.connectors.fluid.smart_lending_sdk import FluidSmartLendingSDK

        gateway_client = ctx.gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            gateway_client = None
        if gateway_client is None and not ctx.rpc_url:
            return None, CompilationResult(
                status=CompilationStatus.FAILED,
                error="Fluid DEX LP requires a connected gateway (no rpc_url fallback configured)",
                intent_id=intent_id,
            )
        resolver = FLUID_DEX_LP[ctx.chain]["smart_lending_resolver"]
        sdk = FluidSmartLendingSDK(
            chain=ctx.chain,
            resolver_address=resolver,
            rpc_url=None if gateway_client is not None else ctx.rpc_url,
            gateway_client=gateway_client,
        )
        return sdk, None

    @staticmethod
    def _to_wei(amount: Decimal, decimals: int) -> int:
        return int(amount * Decimal(10**decimals))

    # -- LP_OPEN ------------------------------------------------------------

    def compile_lp_open(self, ctx: BaseCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        from almanak.connectors.fluid.smart_lending_sdk import FluidDexLpDepositDisabledError, FluidDexLpError

        entry, err = self._market(ctx, intent.pool, intent.intent_id)
        if err is not None:
            return err
        assert entry is not None
        # Native-ETH legs are now SUPPORTED (VIB-5121): the native leg rides as
        # msg.value (built below) and its amount is measured from a wallet
        # native-balance bracket in the runner at ledger-build time (it emits no
        # ERC-20 Transfer for the log parser). The receipt parser leaves the
        # native leg None (Empty ≠ Zero) for that capture to fill.
        sdk, err = self._build_sdk(ctx, intent.intent_id)
        if err is not None:
            return err

        wrapper = intent.pool
        t0_dec = int(entry["token0_decimals"])
        t1_dec = int(entry["token1_decimals"])
        amount0_wei = self._to_wei(intent.amount0, t0_dec)
        amount1_wei = self._to_wei(intent.amount1, t1_dec)
        native_t1 = bool(entry.get("native_token1"))
        native_t0 = entry["token0"].lower() == FLUID_DEX_LP_NATIVE_SENTINEL.lower()

        try:
            # Deposit-enabled pre-flight (the 51013 guard) — refuse a disabled
            # pool at COMPILE rather than letting the user eat an on-chain revert.
            sdk.check_deposit_enabled(wrapper, amount0_wei, amount1_wei, ctx.wallet_address)
            # Slippage floor on minted shares.
            quote_shares = sdk.quote_deposit_shares(entry["dex"], amount0_wei, amount1_wei)
        except FluidDexLpDepositDisabledError as e:
            return CompilationResult(status=CompilationStatus.FAILED, error=str(e), intent_id=intent.intent_id)
        except (FluidDexLpError, Exception) as e:  # noqa: BLE001
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Fluid DEX LP open quote failed: {e}",
                intent_id=intent.intent_id,
            )

        try:
            tol = _resolve_slippage(intent)
        except (ValueError, ArithmeticError) as e:
            # protocol_params["max_slippage"] is caller-supplied; a bad value
            # ("abc", a non-numeric, or out of [0,1)) must surface as a typed
            # compile FAILURE, not an uncaught exception that aborts the pipeline.
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Fluid DEX LP invalid max_slippage: {e}",
                intent_id=intent.intent_id,
            )
        min_shares = int(Decimal(quote_shares) * (Decimal(1) - tol))

        transactions: list[Any] = []
        native_value = 0
        # ERC-20 legs get an exact approve; native legs ride as msg.value.
        if amount0_wei > 0:
            if native_t0:
                native_value += amount0_wei
            else:
                transactions.extend(ctx.services.build_approve_tx(entry["token0"], wrapper, amount0_wei))
        if amount1_wei > 0:
            if native_t1:
                native_value += amount1_wei
            else:
                transactions.extend(ctx.services.build_approve_tx(entry["token1"], wrapper, amount1_wei))

        deposit_tx = sdk.build_deposit_tx(
            wrapper, amount0_wei, amount1_wei, min_shares, ctx.wallet_address, value=native_value
        )
        transactions.append(deposit_tx)

        action_bundle = ActionBundle(
            intent_type=IntentType.LP_OPEN.value,
            transactions=[tx if isinstance(tx, dict) else tx.to_dict() for tx in transactions],
            metadata={
                "protocol": "fluid_dex_lp",
                "pool": wrapper,
                "wrapper": wrapper,
                "dex": entry["dex"],
                "token0": entry["token0"],
                "token1": entry["token1"],
                "amount0_wei": str(amount0_wei),
                "amount1_wei": str(amount1_wei),
                "min_shares": str(min_shares),
                "quote_shares": str(quote_shares),
            },
        )
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        result.action_bundle = action_bundle
        logger.info(
            "Compiled Fluid DEX LP_OPEN: wrapper=%s amount0=%s amount1=%s minShares=%s",
            entry["symbol"],
            amount0_wei,
            amount1_wei,
            min_shares,
        )
        return result

    # -- LP_CLOSE -----------------------------------------------------------

    def compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        from almanak.connectors.fluid.smart_lending_sdk import FluidDexLpError

        wrapper = intent.position_id or (intent.pool or "")
        entry, err = self._market(ctx, wrapper, intent.intent_id)
        if err is not None:
            return err
        assert entry is not None
        # Native-ETH legs supported (VIB-5121) — see compile_lp_open. The native
        # returned leg is measured from a balance bracket in the runner.
        sdk, err = self._build_sdk(ctx, intent.intent_id)
        if err is not None:
            return err

        try:
            shares = sdk.get_share_balance(wrapper, ctx.wallet_address)
            if shares <= 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Fluid DEX LP position to close on {entry['symbol']} ({wrapper}) for {ctx.wallet_address}",
                    intent_id=intent.intent_id,
                )
            # Size the exact-out token legs from the live proportional claim.
            t0_target, t1_target = sdk.position_token_amounts(wrapper, shares)
        except (FluidDexLpError, Exception) as e:  # noqa: BLE001
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Fluid DEX LP close read failed: {e}",
                intent_id=intent.intent_id,
            )

        if t0_target <= 0 and t1_target <= 0:
            # The held shares are non-zero (checked above) but the live
            # proportional claim floors to (0, 0) — dust shares against tiny/empty
            # reserves. An exact-out withdraw(0, 0, ...) would revert or no-op and
            # waste gas; fail closed so teardown surfaces the dust explicitly.
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid DEX LP close on {entry['symbol']} ({wrapper}): {shares} shares claim "
                    "floors to (0, 0) tokens — position is dust against current reserves, nothing to withdraw"
                ),
                intent_id=intent.intent_id,
            )

        # FULL DRAIN (teardown must remove on-chain risk — no deliberate
        # residual). Request the live proportional claim for ALL held shares and
        # cap the burn at the held balance. ``position_token_amounts`` FLOORS
        # each leg (shares*reserve//total_supply), so delivering the claim needs
        # ≤ ``shares`` — the floor is the rounding margin, so this drains cleanly
        # without reverting on rounding. The earlier ``*(1 - tol)`` deliberately
        # under-withdrew, stranding ~tol (default 0.5%) of the position as
        # residual SHARES (an open LP) on every close — defeating teardown.
        #
        # ``max_shares = shares`` (the held balance) is the real slippage cap:
        # exact-output ``withdraw`` delivers exactly (t0_out, t1_out); if reserves
        # drift adversely so the claim would need to burn MORE than the balance,
        # the tx reverts (no bad-rate exit) and teardown retries against a fresh
        # read rather than leaving risk on chain. Any residual is then only the
        # favorable-drift remainder (tiny, and retained by the wallet — not lost).
        t0_out = t0_target
        t1_out = t1_target
        max_shares = shares

        withdraw_tx = sdk.build_withdraw_tx(wrapper, t0_out, t1_out, max_shares, ctx.wallet_address)

        action_bundle = ActionBundle(
            intent_type=IntentType.LP_CLOSE.value,
            transactions=[withdraw_tx],
            metadata={
                "protocol": "fluid_dex_lp",
                "pool": wrapper,
                "wrapper": wrapper,
                "dex": entry["dex"],
                "token0": entry["token0"],
                "token1": entry["token1"],
                "shares_burned_max": str(max_shares),
                "amount0_out_wei": str(t0_out),
                "amount1_out_wei": str(t1_out),
            },
        )
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        result.action_bundle = action_bundle
        logger.info(
            "Compiled Fluid DEX LP_CLOSE: wrapper=%s shares=%s t0_out=%s t1_out=%s",
            entry["symbol"],
            shares,
            t0_out,
            t1_out,
        )
        return result


__all__ = ["FluidDexLpCompiler"]
