"""Connector-owned compiler for GMX V2 perpetual intents."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from eth_utils import to_checksum_address

from almanak.connectors._strategy_base.base.compiler import (
    BasePerpCompiler,
    PerpCompilerContext,
    PreflightOutcome,
    PreflightVerdict,
)
from almanak.connectors._strategy_base.slippage import SlippagePrecisionError, slippage_to_bps
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.min_out_guard import UnprotectedTradeError
from almanak.framework.intents.vocabulary import (
    IntentType,
    PerpCancelIntent,
    PerpCloseIntent,
    PerpOpenIntent,
)
from almanak.framework.models.reproduction_bundle import ActionBundle

from . import market_catalog
from .acceptable_price import bound_is_maximum, derive_acceptable_price_30dec, price_30dec_to_usd
from .adapter import GMXv2Adapter, GMXv2Config
from .market_identity import canonicalise_market
from .market_metadata import (
    GmxMarketDiscoveryUnavailable,
    GmxMarketNotFound,
    ResolvedGmxMarket,
    resolve_market_via_gateway,
)
from .permission_seed import permission_market
from .sdk import GMXV2SDK, GMXV2OrderParams, PositionQueryError

logger = logging.getLogger(__name__)


class GMXV2Compiler(BasePerpCompiler):
    """Compile GMX V2 PERP_OPEN, PERP_CLOSE and PERP_CANCEL_ORDER intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"gmx_v2"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_CANCEL_ORDER}
    )
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum", "avalanche"})

    #: Stable prefix strategies + the retry-classification keyword table match on.
    NATIVE_FEE_ERROR_PREFIX: ClassVar[str] = "GMX_INSUFFICIENT_NATIVE_FEE"

    def _remember_market(self, chain: str, market: ResolvedGmxMarket) -> None:
        market_catalog.remember(chain, market)

    def _market_metadata(
        self,
        chain: str,
        market_address: str,
        *,
        permission_discovery: bool = False,
    ) -> ResolvedGmxMarket | None:
        if permission_discovery:
            return permission_market(chain, market_address)
        return market_catalog.by_address(chain, market_address)

    def _index_token_decimals(
        self,
        chain: str,
        market_address: str,
        *,
        permission_discovery: bool = False,
    ) -> int | None:
        metadata = self._market_metadata(
            chain,
            market_address,
            permission_discovery=permission_discovery,
        )
        return metadata.index_token_decimals if metadata is not None else None

    def preflight(self, ctx: PerpCompilerContext, intent: Any) -> PreflightVerdict:
        """Reject a GMX order the wallet cannot fund the keeper execution fee for (VIB-5374 / 2303).

        GMX V2 orders pay a native keeper execution fee as ``msg.value`` (consumed
        even if the order later fails). Before VIB-5374 the compiler only emitted a
        ``logger.warning`` (adapter.py) — the order compiled and reverted on-chain,
        burning gas. This compiler owns only the REAL keeper fee. The execution
        pipeline checks the final estimated gas limit and network fee cap after
        they are known; reserving a fixed amount here can reject affordable orders.
        """
        if getattr(intent, "intent_type", None) not in (IntentType.PERP_OPEN, IntentType.PERP_CLOSE):
            return PreflightVerdict.feasible()
        if ctx.chain not in self.chains:
            return PreflightVerdict.feasible()
        if getattr(ctx, "permission_discovery", False):
            # Permission calldata is never submitted and the bounded seed makes
            # discovery fully offline. A keeper-fee balance check has no meaning
            # here and would reintroduce a network dependency.
            return PreflightVerdict.feasible()

        sdk_or_error = self._build_sdk(ctx, getattr(intent, "intent_id", ""))
        if isinstance(sdk_or_error, CompilationResult):
            # No read path to price the fee — let the compile path surface the real
            # configuration error rather than fabricate a feasibility verdict.
            return PreflightVerdict.feasible()
        order_type = "increase" if intent.intent_type == IntentType.PERP_OPEN else "decrease"
        try:
            execution_fee_wei = int(sdk_or_error.get_execution_fee(order_type=order_type))
        except Exception as exc:  # noqa: BLE001 - gas-price read gap → fail-open, never a false reject
            logger.warning("GMX exec-fee preflight: could not price keeper fee; deferring: %s", exc)
            return PreflightVerdict.feasible()

        native_balance_wei = ctx.services.query_native_balance_for_chain(ctx.wallet_address, ctx.chain)
        if native_balance_wei is None:
            # A balance read gap must never block: the downstream compile and the
            # orchestrator's own pre-flight balance check still guard execution.
            logger.debug("GMX exec-fee preflight: native balance unavailable on %s; deferring", ctx.chain)
            return PreflightVerdict.feasible()

        required_wei = execution_fee_wei
        if native_balance_wei < required_wei:
            return PreflightVerdict(
                outcome=PreflightOutcome.INFEASIBLE,
                error_prefix=self.NATIVE_FEE_ERROR_PREFIX,
                reason=(
                    f"native {native_balance_wei} wei < required {required_wei} wei "
                    f"(keeper execution fee {execution_fee_wei}) "
                    f"on {ctx.chain}; the GMX keeper fee would not be covered"
                ),
            )
        return PreflightVerdict.feasible()

    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult:  # noqa: C901
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            if ctx.chain not in self.chains:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            canonical_market = canonicalise_market(intent.market)
            if intent.collateral_amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "collateral_amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )

            sdk_or_error = self._build_sdk(ctx, intent.intent_id)
            if isinstance(sdk_or_error, CompilationResult):
                return sdk_or_error
            sdk = sdk_or_error

            # Risk-increasing: the market must be CURRENTLY LISTED — a
            # delisted market's createOrder succeeds on-chain but the keeper
            # cancels it and keeps the execution fee (silent burn).
            market_or_error = self._resolve_market(ctx, sdk, intent.market, intent.intent_id, require_listed=True)
            if isinstance(market_or_error, CompilationResult):
                return market_or_error
            market_address = market_or_error

            # VIB-6219: the bound MUST be derived before any calldata is built.
            # Resolving the market first is what makes that possible — the price
            # scale and the price symbol both key off the resolved address.
            price_or_error = self._derive_acceptable_price(ctx, intent, market_address, is_increase=True)
            if isinstance(price_or_error, CompilationResult):
                return price_or_error
            acceptable_price_30dec, acceptable_price_usd = price_or_error

            trigger_price_30dec = 0
            if intent.trigger_price is not None:
                decimals = self._index_token_decimals(
                    ctx.chain,
                    market_address,
                    permission_discovery=ctx.permission_discovery,
                )
                if decimals is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"GMX V2 PERP_OPEN: no index-token decimals for trigger price on {intent.market}",
                        intent_id=intent.intent_id,
                        is_safety_refusal=True,
                    )
                try:
                    trigger_price_30dec = derive_acceptable_price_30dec(
                        index_price_usd=Decimal(intent.trigger_price),
                        index_token_decimals=decimals,
                        slippage_bps=0,
                        is_long=intent.is_long,
                        is_increase=True,
                        context=f"gmx_v2 PERP_OPEN trigger {intent.market} on {ctx.chain}",
                    )
                except UnprotectedTradeError as exc:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=str(exc),
                        intent_id=intent.intent_id,
                        is_safety_refusal=True,
                    )

            collateral_or_error = self._resolve_collateral(
                ctx,
                intent.collateral_token,
                intent.intent_id,
                market_address=market_address,
            )
            if isinstance(collateral_or_error, CompilationResult):
                return collateral_or_error
            collateral_address = collateral_or_error
            collateral_error = self._validate_market_collateral(ctx, market_address, collateral_address, intent)
            if collateral_error is not None:
                return collateral_error
            collateral_decimals_or_error = self._market_collateral_decimals(
                ctx,
                market_address,
                collateral_address,
                intent.intent_id,
            )
            if isinstance(collateral_decimals_or_error, CompilationResult):
                return collateral_decimals_or_error
            collateral_decimals = collateral_decimals_or_error

            adapter = GMXv2Adapter(GMXv2Config(chain=ctx.chain, wallet_address=ctx.wallet_address))
            order_result = adapter.open_position(
                market=market_address,
                collateral_token=collateral_address,
                # mypy cannot preserve the Decimal narrowing across attribute reads.
                collateral_amount=intent.collateral_amount,  # type: ignore[arg-type]
                size_delta_usd=intent.size_usd,
                is_long=intent.is_long,
                # Plain USD, matching the adapter's documented parameter contract.
                # The GMX-native integer below is what reaches calldata.
                acceptable_price=acceptable_price_usd,
                trigger_price=intent.trigger_price,
                collateral_decimals=collateral_decimals,
            )
            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create position order",
                    intent_id=intent.intent_id,
                )

            collateral_amount_decimal: Decimal = intent.collateral_amount  # type: ignore[assignment]
            collateral_wei = int(collateral_amount_decimal * Decimal(10**collateral_decimals))
            size_delta_usd = int(intent.size_usd * Decimal(10**30))
            execution_fee = sdk.get_execution_fee(order_type="increase")

            order_params = GMXV2OrderParams(
                from_address=ctx.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=collateral_wei,
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price_30dec,
                execution_fee=execution_fee,
                trigger_price=trigger_price_30dec,
            )
            tx_data = sdk.build_increase_order_multicall(order_params)

            # Derive native-wrapper handling from the exact resolved address,
            # not a hand-maintained symbol family. This also handles callers
            # that supply WETH/WAVAX directly as an address.
            is_native_collateral = collateral_address.lower() == sdk.WETH_ADDRESS.lower()
            if not is_native_collateral and collateral_wei > 0:
                transactions.extend(
                    ctx.services.build_approve_tx(
                        token_address=collateral_address,
                        spender=sdk.ROUTER_ADDRESS,
                        amount=collateral_wei,
                    )
                )

            transactions.append(
                TransactionData(
                    to=tx_data.to,
                    value=tx_data.value,
                    data=tx_data.data,
                    gas_estimate=tx_data.gas_estimate,
                    description=(
                        f"Open {'LONG' if intent.is_long else 'SHORT'} {canonical_market} position: "
                        f"${intent.size_usd} size, {intent.collateral_amount} collateral"
                    ),
                    tx_type="perp_open",
                )
            )

            result.action_bundle = ActionBundle(
                intent_type=IntentType.PERP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": canonical_market,
                    "market_address": market_address,
                    "index_token_decimals": self._index_token_decimals(
                        ctx.chain,
                        market_address,
                        permission_discovery=ctx.permission_discovery,
                    ),
                    "collateral_token": intent.collateral_token,
                    "collateral_token_decimals": collateral_decimals,
                    "collateral_amount": str(intent.collateral_amount),
                    "size_usd": str(intent.size_usd),
                    "is_long": intent.is_long,
                    "leverage": str(intent.leverage),
                    "max_slippage": str(intent.max_slippage),
                    # VIB-6219: the protective bound actually encoded into
                    # createOrder, in GMX's 30-decimal convention plus plain USD.
                    "acceptable_price_30dec": str(acceptable_price_30dec),
                    "acceptable_price_usd": str(acceptable_price_usd),
                    "trigger_price_30dec": str(trigger_price_30dec),
                    "trigger_price_usd": str(intent.trigger_price) if intent.trigger_price is not None else None,
                    "order_key": order_result.order_key,
                    "chain": ctx.chain,
                    "native_funding_preflight": {"error_prefix": self.NATIVE_FEE_ERROR_PREFIX},
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            result.warnings = warnings
        except Exception as exc:
            logger.exception("Failed to compile GMX V2 PERP_OPEN intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)

        return result

    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult:  # noqa: C901
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            if ctx.chain not in self.chains:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            canonical_market = canonicalise_market(intent.market)
            sdk_or_error = self._build_sdk(ctx, intent.intent_id)
            if isinstance(sdk_or_error, CompilationResult):
                return sdk_or_error
            sdk = sdk_or_error

            market_or_error = self._resolve_market(ctx, sdk, intent.market, intent.intent_id)
            if isinstance(market_or_error, CompilationResult):
                return market_or_error
            market_address = market_or_error

            collateral_or_error = self._resolve_collateral(
                ctx,
                intent.collateral_token,
                intent.intent_id,
                market_address=market_address,
            )
            if isinstance(collateral_or_error, CompilationResult):
                return collateral_or_error
            collateral_address = collateral_or_error
            collateral_error = self._validate_market_collateral(ctx, market_address, collateral_address, intent)
            if collateral_error is not None:
                return collateral_error
            collateral_decimals_or_error = self._market_collateral_decimals(
                ctx,
                market_address,
                collateral_address,
                intent.intent_id,
            )
            if isinstance(collateral_decimals_or_error, CompilationResult):
                return collateral_decimals_or_error
            collateral_decimals = collateral_decimals_or_error

            resolved_size_usd = intent.size_usd
            if intent.size_usd:
                size_delta_usd = int(intent.size_usd * Decimal(10**30))
            else:
                queried_size = self._get_position_size_onchain(
                    ctx, sdk, market_address, collateral_address, intent.is_long
                )
                if queried_size is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            "Cannot close full GMX V2 position: unable to read position size on-chain. "
                            "Either specify size_usd explicitly or ensure RPC/API connectivity. "
                            "Refusing to guess — incorrect sizes burn keeper execution fees."
                        ),
                        intent_id=intent.intent_id,
                    )
                size_delta_usd = queried_size
                resolved_size_usd = Decimal(size_delta_usd) / Decimal(10**30)

            # VIB-6219: derive the protective bound before any calldata is built.
            # Teardown reaches this path too — a read gap is classified transient
            # so an RPC blip cannot become a permanent teardown failure.
            price_or_error = self._derive_acceptable_price(ctx, intent, market_address, is_increase=False)
            if isinstance(price_or_error, CompilationResult):
                return price_or_error
            acceptable_price_30dec, acceptable_price_usd = price_or_error

            adapter = GMXv2Adapter(GMXv2Config(chain=ctx.chain, wallet_address=ctx.wallet_address))
            order_result = adapter.close_position(
                market=market_address,
                collateral_token=collateral_address,
                is_long=intent.is_long,
                size_delta_usd=resolved_size_usd,
                # Plain USD, matching the adapter's documented parameter contract.
                acceptable_price=acceptable_price_usd,
            )
            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create close order",
                    intent_id=intent.intent_id,
                )

            execution_fee = sdk.get_execution_fee(order_type="decrease")
            order_params = GMXV2OrderParams(
                from_address=ctx.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=0,
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price_30dec,
                execution_fee=execution_fee,
            )
            tx_data = sdk.build_decrease_order_multicall(order_params)

            size_desc = f"${intent.size_usd}" if intent.size_usd else "full position"
            transactions.append(
                TransactionData(
                    to=tx_data.to,
                    value=tx_data.value,
                    data=tx_data.data,
                    gas_estimate=tx_data.gas_estimate,
                    description=f"Close {'LONG' if intent.is_long else 'SHORT'} {canonical_market} position: {size_desc}",
                    tx_type="perp_close",
                )
            )

            result.action_bundle = ActionBundle(
                intent_type=IntentType.PERP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": canonical_market,
                    "market_address": market_address,
                    "index_token_decimals": self._index_token_decimals(
                        ctx.chain,
                        market_address,
                        permission_discovery=ctx.permission_discovery,
                    ),
                    "collateral_token": intent.collateral_token,
                    "collateral_token_decimals": collateral_decimals,
                    "is_long": intent.is_long,
                    "size_usd": str(intent.size_usd) if intent.size_usd else None,
                    "close_full_position": intent.close_full_position,
                    "max_slippage": str(intent.max_slippage),
                    # VIB-6219: the protective bound actually encoded into createOrder.
                    "acceptable_price_30dec": str(acceptable_price_30dec),
                    "acceptable_price_usd": str(acceptable_price_usd),
                    "order_key": order_result.order_key,
                    "chain": ctx.chain,
                    "native_funding_preflight": {"error_prefix": self.NATIVE_FEE_ERROR_PREFIX},
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            result.warnings = warnings
        except Exception as exc:
            logger.exception("Failed to compile GMX V2 PERP_CLOSE intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)

        return result

    def compile_perp_cancel(self, ctx: PerpCompilerContext, intent: PerpCancelIntent) -> CompilationResult:
        """Compile PERP_CANCEL_ORDER → ``ExchangeRouter.cancelOrder(bytes32)`` (VIB-5568).

        Cancels a pending (unfilled) GMX V2 order, refunding its committed collateral
        and unspent execution fee to the wallet (``cancellationReceiver`` defaults to
        the caller). A single call, ``value=0`` — no keeper execution fee, no
        multicall / ``sendWnt`` (unlike open/close). The ``order_key`` is a validated
        bytes32 (``PerpCancelIntent`` rejects a malformed key that would zero-pad into
        a *different* order), so the cancel can never target the wrong order.
        """
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        try:
            if ctx.chain not in self.chains:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            adapter = GMXv2Adapter(GMXv2Config(chain=ctx.chain, wallet_address=ctx.wallet_address))
            # Pure, stateless calldata builder keyed only by the on-chain order key —
            # NOT adapter.cancel_order(), which requires the order to be in the
            # adapter's in-memory tracking (a teardown-discovered stranded order, on a
            # fresh process, is never tracked there).
            tx_data = adapter.build_cancel_order_tx(intent.order_key)
            transactions = [
                TransactionData(
                    to=tx_data.to,
                    value=tx_data.value,  # 0 — a cancel carries no keeper fee
                    data=tx_data.data,
                    gas_estimate=tx_data.gas_estimate,
                    description=f"Cancel GMX v2 pending order {intent.order_key[:10]}... (recover collateral)",
                    tx_type="perp_cancel_order",
                )
            ]

            result.action_bundle = ActionBundle(
                intent_type=IntentType.PERP_CANCEL_ORDER.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "order_key": intent.order_key,
                    "chain": ctx.chain,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
        except Exception as exc:
            logger.exception("Failed to compile GMX V2 PERP_CANCEL_ORDER intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)

        return result

    def _index_symbol_for_market(
        self,
        chain: str,
        market_address: str,
        *,
        permission_discovery: bool = False,
    ) -> str | None:
        """Reverse-resolve a market ADDRESS to its index-token price symbol.

        Keyed off the resolved address rather than ``intent.market`` so that an
        exact venue label and its market-token address yield one symbol, and so
        the symbol and decimals used for price scaling are always read for the
        *same* market. The only runtime source is the venue-verified catalog — a
        symbol this process never verified is an absence, not a lookup to fall
        back on (address-first contract).
        """
        metadata = self._market_metadata(
            chain,
            market_address,
            permission_discovery=permission_discovery,
        )
        return metadata.index_symbol if metadata is not None else None

    def _derive_acceptable_price(
        self,
        ctx: PerpCompilerContext,
        intent: Any,
        market_address: str,
        *,
        is_increase: bool,
    ) -> tuple[int, Decimal] | CompilationResult:
        """Derive the ``acceptablePrice`` bound, or a classified FAILED result.

        Returns ``(price_30dec, price_usd)`` — the first is what gets ABI-encoded
        into ``createOrder``; the second is the same bound in plain USD for the
        adapter's documented parameter contract and for order metadata.

        Fails **closed** (VIB-6219): there is no fallback to the historical
        "accept any price" sentinel. A missing price in the compiler's fixed
        in-memory oracle is a non-transient safety refusal (VIB-6254): retrying
        the same compile with more slippage cannot populate that oracle.
        Teardown maps ``is_transient`` to slippage-ladder retryability, so
        misclassifying this refusal as transient escalates loss tolerance and
        can request human approval for an action that cannot help.
        """
        leg = "PERP_OPEN" if is_increase else "PERP_CLOSE"
        context = f"gmx_v2 {leg} {intent.market} on {ctx.chain}"

        decimals = self._index_token_decimals(
            ctx.chain,
            market_address,
            permission_discovery=ctx.permission_discovery,
        )
        if decimals is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 {leg}: no verified index-token decimals for market {intent.market} "
                    f"({market_address}) on {ctx.chain}; cannot scale a price bound. "
                    "Refusing to submit an order with no acceptable-price protection."
                ),
                intent_id=intent.intent_id,
                is_safety_refusal=True,
            )

        symbol = self._index_symbol_for_market(
            ctx.chain,
            market_address,
            permission_discovery=ctx.permission_discovery,
        )
        if symbol is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 {leg}: market {intent.market} ({market_address}) has no verified "
                    f"index-token price symbol on {ctx.chain}. "
                    "Refusing to submit an order with no acceptable-price protection."
                ),
                intent_id=intent.intent_id,
                is_safety_refusal=True,
            )

        try:
            # MUST come before require_token_price, not after. In placeholder
            # mode (no price oracle at all — `_using_placeholders`, which the
            # runner enters whenever its price pre-fetch yields nothing, and
            # which the gateway's compile path permits unconditionally)
            # `require_token_price` returns a FAKE `Decimal("1")` for any symbol
            # it cannot price. Deriving the bound from that fails OPEN, which is
            # the whole defect this module exists to delete: a short open on
            # BTC/USD would encode a minimum near `0.99 * 10**22` against a real
            # execution price near `10**27` — trivially satisfied, i.e. no
            # protection at all, while the logs and bundle metadata report a
            # real-looking bound. `$1` is finite and positive, so no guard
            # inside `derive_acceptable_price_30dec` can catch it.
            #
            # `assert_prices_available` already encodes exactly this rule
            # (placeholder mode => every token reported missing) and is what the
            # teardown lane uses for the same reason; reusing it keeps one
            # definition of "is this a real price" rather than inventing a
            # second. The fixed oracle cannot populate during this compile, so
            # absence is a non-transient refusal; the caller must warm first.
            #
            # EXEMPT: offline permission discovery. The Zodiac Roles manifest is
            # built by compiling synthetic intents purely to enumerate the
            # (target, selector) pairs a Safe must authorise — that calldata is
            # never signed or submitted, and discovery runs with no oracle by
            # construction. Refusing there yields ZERO permissions, and an empty
            # manifest means every Safe GMX call reverts at
            # `execTransactionWithRole` (AGENTS.md §Connector additions names
            # this exact trap). Caught by
            # tests/unit/permissions/test_protocol_compatibility.py, which is
            # why the guard is scoped rather than absolute.
            strategy_trigger = getattr(intent, "trigger_price", None) if is_increase else None
            if not ctx.permission_discovery:
                ctx.services.assert_prices_available([symbol])
            measured_spot_usd = ctx.services.require_token_price(symbol)
            index_price_usd = strategy_trigger if strategy_trigger is not None else measured_spot_usd
        except Exception as exc:  # noqa: BLE001 - never a licence to skip the bound
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 {leg}: no usable USD price for the {symbol} index token is present "
                    f"in this compile's price oracle ({exc}). Refusing to submit an unprotected "
                    "order. Retrying this compile or increasing slippage will not populate the "
                    "oracle; warm the index price before compiling."
                ),
                intent_id=intent.intent_id,
                is_safety_refusal=True,
            )

        if strategy_trigger is not None and not ctx.permission_discovery:
            trigger_is_marketable = (intent.is_long and strategy_trigger >= measured_spot_usd) or (
                not intent.is_long and strategy_trigger <= measured_spot_usd
            )
            if trigger_is_marketable:
                direction = "long" if intent.is_long else "short"
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"GMX V2 {leg}: {direction} trigger {strategy_trigger} is already marketable "
                        f"against measured spot {measured_spot_usd}; refusing a resting-order request "
                        "whose slippage bound would be anchored away from the current market"
                    ),
                    intent_id=intent.intent_id,
                    is_safety_refusal=True,
                )

        # GMX's tolerance granularity is a basis point. Canonical conversion
        # refuses a positive sub-bp tolerance rather than silently pinning the
        # order to spot; an explicit zero remains a legitimate request.
        # Inside its own classified try: the oracle legitimately holds price
        # values as strings (`inner_runner._fetch_prices_for_intent` stores
        # `str(resp.price)`), so `Decimal(...)` can raise `InvalidOperation`.
        # That malformed read is a transient read gap. Slippage conversion is
        # classified separately below: a non-Decimal intent value is deterministic
        # caller input, so retrying cannot repair it and the compiler safety-refuses.
        try:
            price_decimal = Decimal(index_price_usd)
        except (ArithmeticError, TypeError, ValueError) as exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 {leg}: the {symbol} index price could not be interpreted as a number "
                    f"({exc!r}); refusing to derive a bound from it."
                ),
                intent_id=intent.intent_id,
                is_transient=True,
            )

        try:
            slippage_bps = slippage_to_bps(intent.max_slippage)
        except SlippagePrecisionError:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 {leg}: max_slippage={intent.max_slippage} is finer than GMX's "
                    "one-basis-point granularity and truncates to 0 bps, which pins the "
                    "acceptable price to spot exactly — the order would burn a keeper "
                    "execution fee without being fillable. Use max_slippage >= 0.0001 "
                    "(1 bp), or exactly 0 to pin to spot deliberately."
                ),
                intent_id=intent.intent_id,
                is_safety_refusal=True,
            )
        except ValueError as exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"GMX V2 {leg}: refusing invalid max_slippage={intent.max_slippage} ({exc}).",
                intent_id=intent.intent_id,
                is_safety_refusal=True,
            )
        except (ArithmeticError, TypeError) as exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 {leg}: the slippage tolerance could not be "
                    f"interpreted as a number ({exc!r}); refusing to derive a bound from it."
                ),
                intent_id=intent.intent_id,
                is_safety_refusal=True,
            )

        try:
            price_30dec = derive_acceptable_price_30dec(
                index_price_usd=price_decimal,
                index_token_decimals=decimals,
                slippage_bps=slippage_bps,
                is_long=intent.is_long,
                is_increase=is_increase,
                context=context,
            )
        except UnprotectedTradeError as exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(exc),
                intent_id=intent.intent_id,
                is_safety_refusal=True,
            )

        logger.info(
            "GMX V2 %s %s: index %s=$%s, max_slippage=%s -> acceptablePrice=%d (30-dec, %s bound)",
            leg,
            "LONG" if intent.is_long else "SHORT",
            symbol,
            index_price_usd,
            intent.max_slippage,
            price_30dec,
            "upper" if bound_is_maximum(is_long=intent.is_long, is_increase=is_increase) else "lower",
        )
        return price_30dec, price_30dec_to_usd(price_30dec, decimals)

    def _build_sdk(self, ctx: PerpCompilerContext, intent_id: str) -> GMXV2SDK | CompilationResult:
        if ctx.permission_discovery:
            return GMXV2SDK(chain=ctx.chain, permission_discovery=True)
        gateway_client = ctx.gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            gateway_client = None
        rpc_url = None if gateway_client is not None else ctx.rpc_url or ctx.services.get_chain_rpc_url()
        if gateway_client is None and not rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 requires either a connected gateway_client or an RPC URL. "
                    f"Set ALMANAK_{ctx.chain.upper()}_RPC_URL, RPC_URL, ALCHEMY_API_KEY, "
                    "or use GatewayExecutionOrchestrator."
                ),
                intent_id=intent_id,
            )
        return GMXV2SDK(rpc_url=rpc_url, chain=ctx.chain, gateway_client=gateway_client)

    def _resolve_market(
        self,
        ctx: PerpCompilerContext,
        sdk: GMXV2SDK,
        market: str,
        intent_id: str,
        *,
        require_listed: bool = False,
    ) -> str | CompilationResult:
        """Resolve ``intent.market`` to a verified market-token address.

        Address-first contract: the strategy supplies the market-token ADDRESS
        and this method verifies it — dynamically against the venue catalogue +
        on-chain ``Reader.getMarket`` when a gateway is connected (VIB-6561),
        or against this process's already-verified catalog when the dynamic
        surface is momentarily unreachable (a verified address tuple is
        immutable, so a remembered verification stays valid — this is what
        keeps the close path off the venue API's uptime).

        Labels are venue vocabulary and resolve only through the dynamic
        registry, which pins ambiguous multi-collateral labels by API ``name``
        verification. Offline permission discovery is the sole exception: it
        resolves one generated, audited representative row per chain without
        publishing that row to the runtime verified catalog.
        """
        del sdk  # retained in the private signature for downstream test compatibility
        canonical_market = canonicalise_market(market)
        is_address_input = canonical_market[:2].lower() == "0x"

        if ctx.permission_discovery:
            seeded = permission_market(ctx.chain, canonical_market)
            if seeded is not None:
                return to_checksum_address(seeded.market_token)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 permission discovery has no generated seed for {market} "
                    f"on {ctx.chain}; regenerate permission_seed.json instead of adding "
                    "a runtime market alias."
                ),
                intent_id=intent_id,
                is_safety_refusal=True,
            )

        gateway_client = ctx.gateway_client
        # A missing OR disconnected gateway is the same transient condition: the
        # dynamic surface is momentarily unreachable and a retry can heal it.
        # Classifying a disconnected client as permanent would let teardown's
        # retry ladder give up on a close it could complete after reconnect.
        gateway_usable = gateway_client is not None and getattr(gateway_client, "is_connected", False)
        dynamic_unavailable: Exception | None = None
        if gateway_usable:
            try:
                metadata = resolve_market_via_gateway(
                    gateway_client, chain=ctx.chain, market=canonical_market, require_listed=require_listed
                )
            except GmxMarketDiscoveryUnavailable as exc:
                dynamic_unavailable = exc
                logger.warning(
                    "GMX dynamic market resolution unavailable for %s on %s: %s",
                    market,
                    ctx.chain,
                    exc,
                )
            except GmxMarketNotFound as exc:
                logger.warning(
                    "GMX venue catalogue has no row for %s on %s: %s",
                    market,
                    ctx.chain,
                    exc,
                )
            except Exception as exc:  # noqa: BLE001 - verification/refusal must fail closed
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX dynamic market verification failed for {market} on {ctx.chain}: {exc}",
                    intent_id=intent_id,
                    is_safety_refusal=True,
                )
            else:
                self._remember_market(ctx.chain, metadata)
                return to_checksum_address(metadata.market_token)

        if is_address_input:
            # Already verified in this process (open → close, retry, teardown
            # of a delisted market): the tuple behind an address cannot change,
            # so the remembered verification is still good — for CLOSES. A
            # remembered verification cannot prove CURRENT listing, so a
            # risk-increasing caller gets no catalog grace (review P1: an
            # increase on a delisted market burns its keeper fee).
            if not require_listed and market_catalog.by_address(ctx.chain, canonical_market) is not None:
                return to_checksum_address(canonical_market)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 market address {market} on {ctx.chain} is not verified: dynamic "
                    "resolution is unavailable and this process has not verified it before. "
                    "Refusing to trade an unverified market tuple."
                ),
                intent_id=intent_id,
                # A gateway/API blip heals on retry; an address the venue truly
                # does not know keeps failing here, loudly, on every attempt.
                is_transient=dynamic_unavailable is not None or not gateway_usable,
            )

        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Unknown market: {market}. GMX markets are address-first — supply the "
                "market-token address or an exact venue label for dynamic verification. "
                "No static runtime market aliases are consulted."
            ),
            intent_id=intent_id,
            # Same transiency rule as the address branch: a label the dynamic
            # surface could resolve after reconnect/API recovery must not be
            # classified permanent while that surface is down.
            is_transient=dynamic_unavailable is not None or not gateway_usable,
        )

    def _resolve_collateral(
        self,
        ctx: PerpCompilerContext,
        collateral_token: str,
        intent_id: str,
        *,
        market_address: str | None = None,
    ) -> str | CompilationResult:
        collateral_upper = collateral_token.upper()
        # The verified market tuple is the authoritative collateral vocabulary.
        # Resolve its symbols directly to its exact long/short addresses before
        # consulting the generic token registry. This makes every newly listed
        # long token usable immediately and prevents a generic symbol collision
        # (USDC vs USDC.e) from selecting a token the market does not accept.
        metadata = (
            self._market_metadata(
                ctx.chain,
                market_address,
                permission_discovery=ctx.permission_discovery,
            )
            if market_address is not None
            else None
        )
        if metadata is not None:
            for symbol, address in (
                (metadata.long_token_symbol, metadata.long_token),
                (metadata.short_token_symbol, metadata.short_token),
            ):
                if symbol and symbol.upper() == collateral_upper:
                    return address
        if ctx.token_resolver is not None:
            try:
                resolved_token = ctx.services.resolve_token(collateral_token)
                if resolved_token is not None:
                    return resolved_token.address
            except Exception:  # noqa: BLE001 - connector-owned offline fallback follows
                pass
        # Accept both ``0x`` and ``0X`` prefixes. A lowercase-only check would
        # reject valid ``0X...`` inputs as "unknown collateral".
        if collateral_token[:2].lower() == "0x":
            return collateral_token
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Unknown collateral token: {collateral_token}",
            intent_id=intent_id,
        )

    def _validate_market_collateral(
        self,
        ctx: PerpCompilerContext,
        market_address: str,
        collateral_address: str,
        intent: Any,
    ) -> CompilationResult | None:
        metadata = self._market_metadata(
            ctx.chain,
            market_address,
            permission_discovery=ctx.permission_discovery,
        )
        if metadata is None:
            return None
        if collateral_address.lower() in {metadata.long_token.lower(), metadata.short_token.lower()}:
            return None
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Invalid collateral '{intent.collateral_token}' for GMX V2 market {metadata.label} "
                f"on {ctx.chain}. Verified allowed collateral: "
                f"{metadata.long_token_symbol or metadata.long_token}, "
                f"{metadata.short_token_symbol or metadata.short_token}."
            ),
            intent_id=intent.intent_id,
            is_safety_refusal=True,
        )

    def _market_collateral_decimals(
        self,
        ctx: PerpCompilerContext,
        market_address: str,
        collateral_address: str,
        intent_id: str,
    ) -> int | CompilationResult:
        """Return decimals from the same verified tuple used for collateral identity."""
        metadata = self._market_metadata(
            ctx.chain,
            market_address,
            permission_discovery=ctx.permission_discovery,
        )
        if metadata is not None:
            collateral_lower = collateral_address.lower()
            if collateral_lower == metadata.long_token.lower():
                return metadata.long_token_decimals
            if collateral_lower == metadata.short_token.lower():
                return metadata.short_token_decimals
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"GMX V2 market {market_address} has no verified decimals for collateral "
                f"{collateral_address} on {ctx.chain}; refusing to guess token units."
            ),
            intent_id=intent_id,
            is_safety_refusal=True,
        )

    def _get_position_size_onchain(
        self,
        ctx: PerpCompilerContext,
        sdk: GMXV2SDK,
        market_address: str,
        collateral_address: str,
        is_long: bool,
    ) -> int | None:
        try:
            positions = sdk.get_account_positions(ctx.wallet_address)
        except PositionQueryError as exc:
            logger.warning("GMX V2 position query failed: %s", exc)
            return None
        except Exception as exc:
            logger.warning("Unexpected error querying GMX V2 positions: %s", exc)
            return None

        if not positions:
            logger.warning("No GMX V2 positions found for %s", ctx.wallet_address)
            return None

        market_lower = market_address.lower()
        collateral_lower = collateral_address.lower()
        for pos in positions:
            if (
                pos.get("market", "").lower() == market_lower
                and pos.get("collateral_token", "").lower() == collateral_lower
                and pos.get("is_long") == is_long
                and pos.get("size_in_usd", 0) > 0
            ):
                size_in_usd = pos["size_in_usd"]
                logger.info(
                    "Read on-chain GMX V2 position size: %s (30-decimal) for market=%s is_long=%s",
                    size_in_usd,
                    market_address,
                    is_long,
                )
                return int(size_in_usd)

        logger.warning(
            "No matching GMX V2 position found for market=%s collateral=%s is_long=%s",
            market_address,
            collateral_address,
            is_long,
        )
        return None


__all__ = ["GMXV2Compiler"]
