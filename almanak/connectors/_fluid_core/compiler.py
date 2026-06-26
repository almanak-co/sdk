"""Connector-owned compiler for Fluid DEX swaps.

Fluid is SWAP-only at this compile boundary (Phase 1, VIB-5029). Fluid DEX
has **no router** — each pool is its own contract and ``swapIn`` executes
directly on it, so the approve target and the swap target are both the
per-pair pool address resolved at compile time via the DexReservesResolver.

LP intents are intentionally NOT supported: direct pool deposits are
whitelist-gated at Fluid's Liquidity layer (``DexT1__UserSupplyInNotOn``,
verified Phase 0 / VIB-5028 §V4) — retail LP access goes through
SmartLending wrappers or smart vaults, which is Phase-4 scope (VIB-5032).

Quoting goes through ``DexReservesResolver.estimateSwapIn`` — Fluid's
official quote surface; quotes match on-chain execution to the wei
(Phase-0 V1.4). The connector was previously disabled (VIB-2822) due to a
broken eth_call state-override quote shim, not a protocol issue.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    SwapCompilerContext,
)
from almanak.framework.intents._compiler_helpers import (
    PriceImpactDecision,
    assemble_action_bundle,
    check_price_impact,
    choose_safer_quote,
    compute_min_amount_out,
    sum_transaction_gas,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from almanak.framework.utils.log_formatters import _emojis_enabled, format_percentage, format_token_amount

logger = logging.getLogger(__name__)


class FluidCompiler(BaseProtocolCompiler[SwapCompilerContext]):
    """Fluid DEX swap compiler. SWAP-only, routerless (per-pool targets).

    Subclasses ``BaseProtocolCompiler[SwapCompilerContext]`` because the
    swap slippage / price-impact guard reads ``max_price_impact_pct`` and
    ``using_placeholders`` — same shape as ``CamelotCompiler``.
    """

    protocols: ClassVar[frozenset[str]] = frozenset({"fluid"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.SWAP, IntentType.SUPPLY, IntentType.WITHDRAW})
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum", "base", "ethereum", "polygon"})
    # fToken lending ships on the chains Phase-0 validated (VIB-5030 scope:
    # arbitrum + base); SWAP stays available on all four. Lending compiles on
    # the other chains fail loudly below rather than silently half-working.
    LENDING_CHAINS: ClassVar[frozenset[str]] = frozenset({"arbitrum", "base"})
    context_type: ClassVar[type[BaseCompilerContext]] = SwapCompilerContext

    def compile(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        if intent_type == IntentType.SUPPLY:
            return self.compile_supply(ctx, intent)
        if intent_type == IntentType.WITHDRAW:
            return self.compile_withdraw(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        """Compile a Fluid DEX exact-input swap.

        Pipeline: resolve tokens (native legs map to Fluid's ``0xEeee…``
        sentinel — Fluid pools pair raw native, not WETH) → resolve the
        per-pair pool + direction on-chain → quote via the reserves
        resolver → price-impact guard → approve (ERC-20 input only) +
        ``swapIn`` on the pool.
        """
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            inputs = self._resolve_swap_inputs(ctx, intent)
            if isinstance(inputs, CompilationResult):
                return inputs
            from_token, to_token, amount_in, expected_output = inputs

            from almanak.connectors._fluid_core.sdk import (
                FLUID_NATIVE_TOKEN,
                FluidMinAmountError,
                FluidSDK,
                FluidSDKError,
            )

            sdk = self._build_sdk(ctx, FluidSDK, intent)
            if isinstance(sdk, CompilationResult):
                return sdk

            # Fluid pools hold the chain's native gas token directly — no
            # WETH wrapping on either leg.
            value = 0
            fluid_from = from_token.address
            if from_token.is_native:
                fluid_from = FLUID_NATIVE_TOKEN
                value = amount_in
                warnings.append("Native-input swap: amount sent as msg.value to the pool (no approve)")
            fluid_to = to_token.address
            if to_token.is_native:
                fluid_to = FLUID_NATIVE_TOKEN
                warnings.append("Native-output swap: pool pays raw native token to the wallet")

            try:
                found = sdk.find_pool_for_pair(fluid_from, fluid_to)
            except FluidSDKError as exc:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Fluid pool not found for {intent.from_token}->{intent.to_token} on "
                        f"{ctx.chain}: pool enumeration failed ({exc}). Pool discovery requires "
                        f"an on-chain lookup (RPC or gateway)."
                    ),
                    intent_id=intent.intent_id,
                )
            if found is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"No Fluid DEX pool exists for {intent.from_token}->{intent.to_token} "
                        f"on {ctx.chain}. Fluid pools are per-pair contracts; this pair is not "
                        f"deployed. Use a routed protocol (uniswap_v3, enso) for arbitrary pairs."
                    ),
                    intent_id=intent.intent_id,
                )
            pool_address, swap0to1 = found

            quoter_amount: int | None = None
            try:
                quoter_amount = sdk.get_swap_quote(pool_address, swap0to1, amount_in)
            except FluidMinAmountError as exc:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Fluid swap size limit-gated on {ctx.chain} pool {pool_address}: {exc} "
                        f"(Fluid's Liquidity-layer limits expand over time — this is retryable, "
                        f"not a permanent failure.)"
                    ),
                    intent_id=intent.intent_id,
                )
            except FluidSDKError as exc:
                logger.warning("Fluid resolver quote failed, price-impact guard decides: %s", exc)

            clamped_expected, used_quoter = choose_safer_quote(expected_output, quoter_amount)
            if used_quoter:
                logger.info(
                    "Fluid resolver quote (%s) is lower than price oracle estimate (%s) — "
                    "using resolver quote as slippage basis for safer execution",
                    quoter_amount,
                    expected_output,
                )

            offline_mode = ctx.using_placeholders or ctx.permission_discovery
            impact = check_price_impact(
                oracle_estimate=expected_output,
                quoter_amount=quoter_amount,
                intent_max_impact=intent.max_price_impact,
                config_max_impact=ctx.max_price_impact_pct,
                offline_mode=offline_mode,
                using_placeholders=ctx.using_placeholders,
            )
            if impact.decision is PriceImpactDecision.IMPACT_TOO_HIGH:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Price impact too high: resolver quote implies "
                        f"{impact.price_impact:.1%} price impact "
                        f"(oracle estimate: {expected_output}, resolver: {quoter_amount}). "
                        f"Maximum allowed: {impact.effective_max_impact:.2%}. "
                        f"Likely cause: Fluid pool {pool_address} has insufficient depth for "
                        f"{intent.from_token}->{intent.to_token} at this size."
                    ),
                    intent_id=intent.intent_id,
                )
            if impact.decision is PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Price impact guard: Fluid reserves resolver returned no quote for "
                        f"{intent.from_token}->{intent.to_token}. Cannot verify pool liquidity. "
                        f"Refusing to compile a Fluid swap backed only by the oracle price."
                    ),
                    intent_id=intent.intent_id,
                )

            min_output = compute_min_amount_out(clamped_expected, intent.max_slippage)
            quoted_for_metrics = quoter_amount if quoter_amount is not None else expected_output
            expected_output_human = Decimal(str(quoted_for_metrics)) / Decimal(10**to_token.decimals)

            if not from_token.is_native:
                transactions.extend(ctx.services.build_approve_tx(from_token.address, pool_address, amount_in))

            swap_tx_dict = sdk.build_swap_tx(
                dex_address=pool_address,
                swap0to1=swap0to1,
                amount_in=amount_in,
                amount_out_min=min_output,
                to=ctx.wallet_address,
                value=value,
            )
            transactions.append(
                TransactionData(
                    to=swap_tx_dict["to"],
                    value=swap_tx_dict["value"],
                    data=swap_tx_dict["data"],
                    gas_estimate=swap_tx_dict["gas"],
                    description=(
                        f"Swap {ctx.services.format_amount(amount_in, from_token.decimals)} "
                        f"{from_token.symbol} -> {to_token.symbol} via Fluid pool {pool_address} "
                        f"(min: {ctx.services.format_amount(min_output, to_token.decimals)})"
                    ),
                    tx_type="swap",
                )
            )

            total_gas = sum_transaction_gas(transactions)
            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "min_amount_out": str(min_output),
                    "expected_output_human": str(expected_output_human),
                    "slippage": str(intent.max_slippage),
                    "protocol": "fluid",
                    "pool": pool_address,
                    "swap0to1": swap0to1,
                    "chain": ctx.chain,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            expected_out_fmt = format_token_amount(clamped_expected, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(min_output, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)
            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info(
                "%s Compiled Fluid SWAP: %s → %s (min: %s, pool: %s)",
                ok,
                amount_in_fmt,
                expected_out_fmt,
                min_out_fmt,
                pool_address,
            )
            logger.info("   Slippage: %s | Txs: %d | Gas: %s", slippage_fmt, len(transactions), f"{total_gas:,}")
        except Exception as e:
            logger.exception("Failed to compile Fluid SWAP intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    @staticmethod
    def _build_sdk(
        ctx: SwapCompilerContext,
        sdk_cls: type,
        intent: Any,
        no_transport_error: str | None = None,
    ) -> Any | CompilationResult:
        """Construct ``FluidSDK`` with gateway-preferred transport.

        Mirrors the connector's historical transport selection: a connected
        gateway client wins; otherwise fall back to the context RPC URL.
        ``no_transport_error`` lets non-swap callers phrase the failure for
        their surface (the default text is the swap pool-discovery wording).
        """
        gateway_client = ctx.gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            gateway_client = None
        if gateway_client is None and not ctx.rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=no_transport_error
                or (
                    "Fluid pool not found: pool discovery and quoting require a connected "
                    "gateway client or RPC URL (Fluid is routerless — the per-pair pool "
                    "address is resolved on-chain at compile time)."
                ),
                intent_id=intent.intent_id,
            )
        return sdk_cls(
            chain=ctx.chain,
            rpc_url=None if gateway_client is not None else ctx.rpc_url,
            gateway_client=gateway_client,
        )

    @staticmethod
    def _resolve_swap_inputs(
        ctx: SwapCompilerContext, intent: SwapIntent
    ) -> tuple[Any, Any, int, int] | CompilationResult:
        """Resolve tokens + amount_in + oracle expected output.

        Returns a 4-tuple ``(from_token, to_token, amount_in, expected_output)``
        on success, or a FAILED ``CompilationResult`` on any setup failure.
        Same shape as ``CamelotCompiler._resolve_swap_inputs``.
        """
        from_token = ctx.services.resolve_token(intent.from_token)
        if from_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.from_token}",
                intent_id=intent.intent_id,
            )
        to_token = ctx.services.resolve_token(intent.to_token)
        if to_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.to_token}",
                intent_id=intent.intent_id,
            )

        if intent.amount_usd is not None:
            amount_in = ctx.services.usd_to_token_amount(intent.amount_usd, from_token)
        elif intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )
            # mypy can't narrow Decimal | Literal["all"] through an `==` check;
            # assignment-narrowing matches CamelotCompiler._resolve_swap_inputs.
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Either amount_usd or amount must be provided",
                intent_id=intent.intent_id,
            )

        # Reject a non-positive base-unit amount BEFORE quote/tx assembly: a
        # tiny ``amount`` / ``amount_usd`` can round to 0 base units (int() floor
        # at L345 / usd_to_token_amount), and the DexReservesResolver would
        # otherwise be quoted for a 0-input swap (revert / nonsense quote). Fail
        # closed with an actionable message instead.
        if amount_in <= 0:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Swap amount resolves to {amount_in} base units of {from_token.symbol} "
                    f"(rounded down to zero). Increase amount/amount_usd above one base unit "
                    f"(1e-{from_token.decimals} {from_token.symbol})."
                ),
                intent_id=intent.intent_id,
            )

        try:
            expected_output = ctx.services.calculate_expected_output(amount_in, from_token, to_token)
        except ValueError as e:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Cannot calculate slippage protection for "
                    f"{from_token.symbol} -> {to_token.symbol}: {e}. "
                    f"The price oracle does not have a price for one of the tokens. "
                    f"Ensure the token price is available via market.price() before swapping."
                ),
                intent_id=intent.intent_id,
            )

        return from_token, to_token, amount_in, expected_output

    # =========================================================================
    # fToken lending (ERC-4626) — VIB-5030
    # =========================================================================

    def _resolve_lending_setup(self, ctx: SwapCompilerContext, intent: Any) -> tuple[Any, Any, Any] | CompilationResult:
        """Shared SUPPLY/WITHDRAW setup: chain gate → token → SDK → fToken.

        Returns ``(token, sdk, ftoken_address)`` or a FAILED result. Every
        failure is typed and actionable; resolution reads fail CLOSED (a
        missing market lookup never falls through to a guessed market).
        """
        from almanak.connectors._fluid_core.sdk import FluidSDK, FluidSDKError

        if ctx.chain not in self.LENDING_CHAINS:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid fToken lending is enabled on {sorted(self.LENDING_CHAINS)} "
                    f"(VIB-5030 scope); chain '{ctx.chain}' has Fluid SWAP support only. "
                    f"Lending on additional Fluid chains ships after on-chain validation."
                ),
                intent_id=intent.intent_id,
            )

        token = ctx.services.resolve_token(intent.token)
        if token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.token}",
                intent_id=intent.intent_id,
            )
        if token.is_native:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid fTokens take ERC-20 underlying only; supply/withdraw the wrapped "
                    f"form (e.g. WETH) instead of native {token.symbol}."
                ),
                intent_id=intent.intent_id,
            )

        sdk = self._build_sdk(
            ctx,
            FluidSDK,
            intent,
            no_transport_error=(
                "Fluid fToken market resolution requires a connected gateway client or "
                "RPC URL (the underlying->fToken mapping is read on-chain from the "
                "LendingResolver at compile time)."
            ),
        )
        if isinstance(sdk, CompilationResult):
            return sdk

        try:
            ftoken = sdk.find_ftoken_for_underlying(token.address)
        except FluidSDKError as exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid fToken market lookup failed for {token.symbol} on {ctx.chain}: {exc}. "
                    f"The on-chain read is unavailable — failing closed rather than guessing a market."
                ),
                intent_id=intent.intent_id,
            )
        if ftoken is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid has no fToken (lending) market for {token.symbol} on {ctx.chain}. "
                    f"Fluid lists one ERC-4626 fToken per supported underlying; this asset is not listed."
                ),
                intent_id=intent.intent_id,
            )

        # Fluid lending intents must NOT carry market_id: position keys are
        # wallet+asset scoped (lending:{chain}:fluid:{wallet}:{asset} — one
        # fToken per underlying per chain), and the accounting key deriver
        # inserts any intent market_id as an extra segment. Accepting even a
        # CORRECT fToken address here would fork one real position into two
        # key shapes depending on caller spelling (CodeRabbit, PR #2723) —
        # reject loudly instead.
        intent_market = getattr(intent, "market_id", None)
        if intent_market:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid lending intents must omit market_id (got {intent_market}): Fluid has "
                    f"exactly one fToken per underlying per chain, and positions are keyed by "
                    f"wallet+asset. The resolved market for {token.symbol} on {ctx.chain} is "
                    f"{ftoken}."
                ),
                intent_id=intent.intent_id,
            )

        # Compile/valuation market-universe sync: accounting + valuation read
        # the pinned ``FLUID_FTOKEN_MARKETS`` table while compilation resolves
        # the fToken on-chain. A resolvable-but-unpinned market would compile
        # supplies the accounting layer cannot value (silent ESTIMATED /
        # unmarked positions); a pinned address that differs from the
        # resolver's means the fToken migrated and the table is stale. Both
        # fail CLOSED, loudly.
        from almanak.connectors._fluid_core.lending_read import FLUID_FTOKEN_MARKETS

        pinned = FLUID_FTOKEN_MARKETS.get(ctx.chain, {}).get(token.symbol.lower())
        if pinned is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid market {token.symbol} on {ctx.chain} is resolvable on-chain but "
                    f"not yet enabled (no valuation coverage — FLUID_FTOKEN_MARKETS); enable "
                    f"it with validation before compiling supplies into it."
                ),
                intent_id=intent.intent_id,
            )
        pinned_address = str(pinned.get("comet_address", ""))
        if pinned_address.lower() != ftoken.lower():
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Fluid fToken mismatch for {token.symbol} on {ctx.chain}: pinned "
                    f"{pinned_address} vs on-chain resolver {ftoken} — possible fToken "
                    f"migration; failing closed."
                ),
                intent_id=intent.intent_id,
            )

        return token, sdk, ftoken

    def compile_supply(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        """Compile a Fluid fToken supply: approve + ERC-4626 ``deposit``."""
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        try:
            setup = self._resolve_lending_setup(ctx, intent)
            if isinstance(setup, CompilationResult):
                return setup
            token, sdk, ftoken = setup

            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' for supply must be resolved to a wallet balance before "
                        "compilation. Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount
            assets = int(amount_decimal * Decimal(10**token.decimals))
            if assets <= 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Supply amount resolves to 0 base units of {token.symbol}",
                    intent_id=intent.intent_id,
                )

            transactions: list[TransactionData] = []
            transactions.extend(ctx.services.build_approve_tx(token.address, ftoken, assets))
            deposit_tx = sdk.build_deposit_tx(ftoken, assets, ctx.wallet_address)
            transactions.append(
                TransactionData(
                    to=deposit_tx["to"],
                    value=deposit_tx["value"],
                    data=deposit_tx["data"],
                    gas_estimate=deposit_tx["gas"],
                    description=(
                        f"Supply {ctx.services.format_amount(assets, token.decimals)} {token.symbol} "
                        f"into Fluid fToken {ftoken}"
                    ),
                    tx_type="lending_supply",
                )
            )

            result.transactions = transactions
            result.total_gas_estimate = sum_transaction_gas(transactions)
            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=transactions,
                metadata={
                    "protocol": "fluid",
                    "chain": ctx.chain,
                    # ``supply_token`` + wei ``supply_amount`` is the shape the
                    # orchestrator's pre-flight balance check and description
                    # formatter read (aave_helpers precedent); the manifest's
                    # ``metadata_amount_encoding=lending:"wei"`` declares the
                    # encoding.
                    "supply_token": token.to_dict(),
                    "ftoken": ftoken,
                    "supply_amount": str(assets),
                },
            )
            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info(
                "%s Compiled Fluid SUPPLY: %s %s -> fToken %s",
                ok,
                format_token_amount(assets, token.symbol, token.decimals),
                token.symbol,
                ftoken,
            )
        except Exception as e:
            logger.exception("Failed to compile Fluid SUPPLY intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def compile_withdraw(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        """Compile a Fluid fToken withdraw.

        Exact amounts use ERC-4626 ``withdraw(assets, ...)`` behind a
        ``maxWithdraw`` pre-flight; full exits (``withdraw_all`` /
        ``amount='all'``) use ``redeem(shares, ...)`` over the exact share
        balance so rounding can never strand dust shares.

        Fluid's withdrawal limits EXPAND OVER TIME: a request beyond the
        currently-withdrawable amount is a distinct, retryable failure —
        never a silent clamp, never a compiled-but-doomed transaction.
        """
        from almanak.connectors._fluid_core.sdk import FluidSDKError

        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        try:
            setup = self._resolve_lending_setup(ctx, intent)
            if isinstance(setup, CompilationResult):
                return setup
            token, sdk, ftoken = setup
            wallet = ctx.wallet_address

            full_exit = bool(getattr(intent, "withdraw_all", False)) or intent.amount == "all"
            # Permission discovery compiles for calldata SHAPE (targets +
            # selectors), not executability: skip balance/limit pre-flights
            # that depend on the synthetic wallet's (empty) position.
            discovery = bool(getattr(ctx, "permission_discovery", False))

            transactions: list[TransactionData] = []
            if full_exit:
                if discovery:
                    shares, max_redeem = 1, 1
                else:
                    try:
                        shares = sdk.get_ftoken_share_balance(ftoken, wallet)
                        max_redeem = sdk.get_max_redeem(ftoken, wallet)
                    except FluidSDKError as exc:
                        return self._lending_read_failed(intent, token, ctx, exc)
                if shares <= 0:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"No Fluid fToken position to withdraw: wallet holds 0 {token.symbol} "
                            f"fToken shares on {ctx.chain}."
                        ),
                        intent_id=intent.intent_id,
                    )
                if max_redeem < shares:
                    withdrawable = sdk.convert_to_assets(ftoken, max_redeem)
                    return self._limit_gated(intent, token, ctx, withdrawable)
                tx = sdk.build_redeem_tx(ftoken, shares, wallet, wallet)
                amount_label = f"all ({ctx.services.format_amount(shares, token.decimals)} shares)"
                metadata_amount = str(shares)
                mode = "redeem_all_shares"
            else:
                amount_decimal: Decimal = intent.amount
                assets = int(amount_decimal * Decimal(10**token.decimals))
                if assets <= 0:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Withdraw amount resolves to 0 base units of {token.symbol}",
                        intent_id=intent.intent_id,
                    )
                if not discovery:
                    try:
                        max_withdraw = sdk.get_max_withdraw(ftoken, wallet)
                    except FluidSDKError as exc:
                        return self._lending_read_failed(intent, token, ctx, exc)
                    if assets > max_withdraw:
                        return self._limit_gated(intent, token, ctx, max_withdraw)
                tx = sdk.build_withdraw_tx(ftoken, assets, wallet, wallet)
                amount_label = f"{ctx.services.format_amount(assets, token.decimals)} {token.symbol}"
                metadata_amount = str(assets)
                mode = "withdraw_assets"

            transactions.append(
                TransactionData(
                    to=tx["to"],
                    value=tx["value"],
                    data=tx["data"],
                    gas_estimate=tx["gas"],
                    description=f"Withdraw {amount_label} from Fluid fToken {ftoken}",
                    tx_type="lending_withdraw",
                )
            )

            result.transactions = transactions
            result.total_gas_estimate = sum_transaction_gas(transactions)
            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=transactions,
                metadata={
                    "protocol": "fluid",
                    "chain": ctx.chain,
                    # ``withdraw_token`` is the key ``_describe_withdraw``
                    # reads (no withdraw pre-flight exists — withdrawing
                    # needs no wallet balance); wei-encoded per the
                    # manifest's ``metadata_amount_encoding``. NOTE: in
                    # ``redeem_all_shares`` mode ``withdraw_amount`` carries
                    # SHARES, not assets — consumers branch on ``mode``.
                    "withdraw_token": token.to_dict(),
                    "ftoken": ftoken,
                    "withdraw_amount": metadata_amount,
                    "mode": mode,
                },
            )
            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info("%s Compiled Fluid WITHDRAW (%s): %s from fToken %s", ok, mode, amount_label, ftoken)
        except Exception as e:
            logger.exception("Failed to compile Fluid WITHDRAW intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    @staticmethod
    def _limit_gated(intent: Any, token: Any, ctx: SwapCompilerContext, withdrawable: int) -> CompilationResult:
        """Distinct, retryable failure for Fluid's time-expanding limits.

        Funds are safe and merely time-gated; strategies can branch on the
        'limit-gated' / 'retry later' markers. The amount is NEVER silently
        clamped — a partial withdrawal the strategy didn't ask for is the
        money-losing failure mode.
        """
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Fluid withdrawal limit-gated (retryable): currently withdrawable "
                f"{ctx.services.format_amount(withdrawable, token.decimals)} {token.symbol} on "
                f"{ctx.chain}. Fluid limits expand over time — retry later or reduce the amount. "
                f"Funds remain in the fToken; nothing was compiled."
            ),
            intent_id=intent.intent_id,
        )

    @staticmethod
    def _lending_read_failed(intent: Any, token: Any, ctx: SwapCompilerContext, exc: Exception) -> CompilationResult:
        """Fail CLOSED when the withdraw pre-flight read is unavailable."""
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Fluid withdraw pre-flight read failed for {token.symbol} on {ctx.chain}: {exc}. "
                f"Refusing to compile without the on-chain limit check (fail closed)."
            ),
            intent_id=intent.intent_id,
        )


__all__ = ["FluidCompiler"]
