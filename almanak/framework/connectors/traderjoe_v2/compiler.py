"""Connector-owned compiler for TraderJoe V2 Liquidity Book."""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, cast

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents._compiler_helpers import (
    assemble_action_bundle,
    normalise_gateway_or_rpc,
    probe_traderjoe_bin_step,
    sum_transaction_gas,
)
from almanak.framework.intents.compiler_constants import DEFAULT_GAS_ESTIMATES, LP_POSITION_MANAGERS
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


class TraderJoeV2Compiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for TraderJoe V2 Liquidity Book intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"traderjoe_v2"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
            IntentType.LP_COLLECT_FEES,
        }
    )
    chains: ClassVar[frozenset[str]] = frozenset({"avalanche", "arbitrum", "bnb", "ethereum"})

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        if intent_type == IntentType.LP_OPEN:
            return self.compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self.compile_lp_close(ctx, intent)
        if intent_type == IntentType.LP_COLLECT_FEES:
            return self.compile_collect_fees(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: BaseCompilerContext, intent: SwapIntent) -> CompilationResult:
        return _TraderJoeV2CompileImpl(ctx)._compile_swap_traderjoe_v2(intent)

    def compile_lp_open(self, ctx: BaseCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        return _TraderJoeV2CompileImpl(ctx)._compile_lp_open_traderjoe_v2(intent)

    def compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        return _TraderJoeV2CompileImpl(ctx)._compile_lp_close_traderjoe_v2(intent)

    def compile_collect_fees(self, ctx: BaseCompilerContext, intent: CollectFeesIntent) -> CompilationResult:
        return _TraderJoeV2CompileImpl(ctx)._compile_collect_fees_traderjoe_v2(intent)


class _TraderJoeV2CompileImpl:
    """Per-call adapter that preserves the pre-fold TraderJoe compiler body."""

    def __init__(self, ctx: BaseCompilerContext) -> None:
        self._ctx = ctx
        self.chain = ctx.chain
        self.wallet_address = ctx.wallet_address
        self.rpc_timeout = ctx.rpc_timeout
        self.price_oracle = ctx.price_oracle
        self._gateway_client = ctx.gateway_client
        self._token_resolver = ctx.token_resolver

    def _get_chain_rpc_url(self) -> str | None:
        return self._ctx.rpc_url

    def _resolve_token(self, token: str) -> TokenInfo | None:
        return self._ctx.services.resolve_token(token)

    def _require_token_price(self, symbol: str) -> Decimal:
        return self._ctx.services.require_token_price(symbol)

    def _usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int:
        return self._ctx.services.usd_to_token_amount(usd_amount, token)

    def _build_approve_tx(self, token_address: str, spender: str, amount: int) -> list[TransactionData]:
        return self._ctx.services.build_approve_tx(token_address, spender, amount)

    def _validate_pool(self, result: Any, intent_id: str) -> CompilationResult | None:
        return self._ctx.services.validate_pool(result, intent_id)

    def _format_amount(self, amount: int, decimals: int) -> str:
        return self._ctx.services.format_amount(amount, decimals)

    @staticmethod
    def _parse_traderjoe_v2_pool_spec(
        intent: LPOpenIntent,
    ) -> tuple[str, str, int] | CompilationResult:
        """Parse ``intent.pool`` as ``TOKEN_X/TOKEN_Y[/BIN_STEP]``.

        Defaults ``BIN_STEP`` to 20 (most common for TraderJoe V2) when
        omitted. Preserves the exact "Invalid pool format..." error string
        pinned by the LP characterization tests.
        """
        pool_parts = intent.pool.split("/")
        if len(pool_parts) < 2:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP"
                ),
                intent_id=intent.intent_id,
            )
        token_x_symbol = pool_parts[0]
        token_y_symbol = pool_parts[1]
        bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20
        return token_x_symbol, token_y_symbol, bin_step

    def _resolve_traderjoe_v2_lp_tokens(
        self,
        *,
        intent: LPOpenIntent,
        token_x_symbol: str,
        token_y_symbol: str,
    ) -> tuple[TokenInfo, TokenInfo] | CompilationResult:
        """Resolve both pool tokens or fail with the exact pinned error string.

        Error format matches the pre-refactor compiler:
        ``Unknown token {symbol} for chain {self.chain}``.
        """
        token_x_info = self._resolve_token(token_x_symbol)
        if not token_x_info:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token {token_x_symbol} for chain {self.chain}",
                intent_id=intent.intent_id,
            )
        token_y_info = self._resolve_token(token_y_symbol)
        if not token_y_info:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token {token_y_symbol} for chain {self.chain}",
                intent_id=intent.intent_id,
            )
        return token_x_info, token_y_info

    def _resolve_traderjoe_v2_lp_router(self, intent: LPOpenIntent) -> str | CompilationResult:
        """Return the TraderJoe V2 LP position-manager router for the chain."""
        router_address = LP_POSITION_MANAGERS.get(self.chain, {}).get(
            "traderjoe_v2", "0x0000000000000000000000000000000000000000"
        )
        if router_address == "0x0000000000000000000000000000000000000000":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"TraderJoe V2 not configured for chain {self.chain}",
                intent_id=intent.intent_id,
            )
        return router_address

    @staticmethod
    def _extract_traderjoe_v2_bin_range_params(
        intent: LPOpenIntent,
    ) -> tuple[int, int]:
        """Read ``bin_range`` / ``id_slippage`` from ``intent.protocol_params``.

        Raises ``ValueError`` (caught by the caller's generic try/except,
        surfacing as ``result.error``) when ``bin_range`` is out of range
        ``[1, 100]``. Defaults match pre-refactor behaviour (bin_range=5,
        id_slippage=5).
        """
        params = intent.protocol_params or {}
        bin_range = int(params.get("bin_range", 5))
        if bin_range < 1 or bin_range > 100:
            raise ValueError(f"bin_range must be between 1 and 100, got {bin_range}")
        id_slippage = int(params.get("id_slippage", 5))
        return bin_range, id_slippage

    @staticmethod
    def _build_traderjoe_v2_lp_open_tx_data(
        *,
        lp_tx: Any,
        intent: LPOpenIntent,
        token_x_symbol: str,
        token_y_symbol: str,
        bin_step: int,
    ) -> TransactionData:
        """Convert the adapter's add-liquidity TransactionData into compiler form."""
        return TransactionData(
            to=lp_tx.to,
            value=lp_tx.value,
            data=lp_tx.data if isinstance(lp_tx.data, str) else lp_tx.data,
            gas_estimate=lp_tx.gas or 400000,
            description=(
                f"Add liquidity to TraderJoe V2: {intent.amount0} {token_x_symbol} + "
                f"{intent.amount1} {token_y_symbol} (bin_step={bin_step})"
            ),
            tx_type="traderjoe_v2_add_liquidity",
        )

    def _build_traderjoe_v2_lp_approvals(
        self,
        *,
        token_x_info: TokenInfo,
        token_y_info: TokenInfo,
        amount_x_wei: int,
        amount_y_wei: int,
        router_address: str,
    ) -> list[TransactionData]:
        """Build ERC-20 approval TXs for both LP tokens, in X-then-Y order.

        Native tokens and zero amounts are skipped, matching pre-refactor
        behaviour. The X-before-Y ordering is load-bearing: the approval
        chain ordering is preserved across the compile -> sign -> submit
        pipeline and tests assert it.
        """
        approvals: list[TransactionData] = []
        if amount_x_wei > 0 and not token_x_info.is_native:
            approvals.extend(self._build_approve_tx(token_x_info.address, router_address, amount_x_wei))
        if amount_y_wei > 0 and not token_y_info.is_native:
            approvals.extend(self._build_approve_tx(token_y_info.address, router_address, amount_y_wei))
        return approvals

    def _compile_lp_open_traderjoe_v2(self, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for TraderJoe V2 Liquidity Book.

        TraderJoe V2 uses discrete price bins instead of continuous ticks:
        - Price at bin ID: price = (1 + binStep/10000)^(binId - 8388608)
        - Liquidity is distributed across bins with explicit distributions
        - LP tokens are fungible ERC1155-like tokens per bin (not NFTs)

        Args:
            intent: LPOpenIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 LP ActionBundle
        """
        transactions: list[TransactionData] = []

        try:
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            pool_spec = self._parse_traderjoe_v2_pool_spec(intent)
            if isinstance(pool_spec, CompilationResult):
                return pool_spec
            token_x_symbol, token_y_symbol, bin_step = pool_spec

            tokens = self._resolve_traderjoe_v2_lp_tokens(
                intent=intent,
                token_x_symbol=token_x_symbol,
                token_y_symbol=token_y_symbol,
            )
            if isinstance(tokens, CompilationResult):
                return tokens
            token_x_info, token_y_info = tokens
            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # Resolve transport up front so pool validation AND the adapter
            # use the same gateway/RPC pair. A disconnected ``self._gateway_client``
            # would otherwise make ``validate_traderjoe_pool`` fail against a
            # stale client even though the adapter falls back to RPC.
            gateway_client, rpc_url = self._resolve_traderjoe_v2_gateway_rpc(
                adapter_name="TraderJoe V2 adapter",
            )

            # Validate pool existence (best-effort; LP_OPEN can seed empty pools).
            from almanak.framework.intents.pool_validation import validate_traderjoe_pool

            pool_check = validate_traderjoe_pool(
                self.chain,
                token_x_addr,
                token_y_addr,
                bin_step,
                rpc_url,
                gateway_client=gateway_client,
                allow_empty_reserves=True,
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            amount_x_wei = int(intent.amount0 * Decimal(10**token_x_info.decimals))
            amount_y_wei = int(intent.amount1 * Decimal(10**token_y_info.decimals))

            router_or_err = self._resolve_traderjoe_v2_lp_router(intent)
            if isinstance(router_or_err, CompilationResult):
                return router_or_err
            router_address: str = router_or_err

            # Approval chain — X before Y, native/zero skipped. Ordering is
            # preserved across compile -> sign -> submit; tests assert it.
            transactions.extend(
                self._build_traderjoe_v2_lp_approvals(
                    token_x_info=token_x_info,
                    token_y_info=token_y_info,
                    amount_x_wei=amount_x_wei,
                    amount_y_wei=amount_y_wei,
                    router_address=router_address,
                )
            )
            tj_adapter = TraderJoeV2Adapter(
                TraderJoeV2Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    rpc_url=rpc_url,
                    gateway_client=gateway_client,
                )
            )

            bin_range, id_slippage = self._extract_traderjoe_v2_bin_range_params(intent)

            lp_tx = tj_adapter.build_add_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                amount_x=intent.amount0,
                amount_y=intent.amount1,
                bin_step=bin_step,
                bin_range=bin_range,
                id_slippage=id_slippage,
            )
            transactions.append(
                self._build_traderjoe_v2_lp_open_tx_data(
                    lp_tx=lp_tx,
                    intent=intent,
                    token_x_symbol=token_x_symbol,
                    token_y_symbol=token_y_symbol,
                    bin_step=bin_step,
                )
            )

            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=transactions,
                metadata={
                    "pool": intent.pool,
                    "token_x": token_x_info.to_dict(),
                    "token_y": token_y_info.to_dict(),
                    "bin_step": bin_step,
                    "bin_range": bin_range,
                    "range_lower": str(intent.range_lower),
                    "range_upper": str(intent.range_upper),
                    "amount_x": str(amount_x_wei),
                    "amount_y": str(amount_y_wei),
                    "protocol": "traderjoe_v2",
                    "router": router_address,
                    "chain": self.chain,
                },
            )

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled TraderJoe V2 LP_OPEN intent: {token_x_symbol}/{token_y_symbol}, "
                f"bin_step={bin_step}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
                warnings=[],
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_OPEN intent: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

    # crap-allowlist: VIB-4139 — pre-existing complexity (cc=32) relocated from
    # almanak/framework/intents/compiler.py by the phase-2 connector fold. The
    # original location carried the same allowlist on main; this is the same
    # function body, just relocated. Refactor tracked in VIB-4139.
    def _compile_lp_close_traderjoe_v2(self, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for TraderJoe V2 Liquidity Book.

        TraderJoe V2 LP close differs from Uniswap V3:
        - Need to query LP token balances per bin
        - Call removeLiquidity with bin IDs and amounts
        - No NFT to burn (fungible LP tokens)

        Args:
            intent: LPCloseIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 LP close ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            # Import TraderJoe V2 adapter
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            if intent.pool is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="pool is required for TraderJoe V2 LP close",
                    intent_id=intent.intent_id,
                )
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info or not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown tokens for pool {intent.pool} on {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # TraderJoe V2 adapter accepts either a connected gateway_client
            # (production path) or a direct RPC URL (local/backtest fallback).
            # Treat a disconnected client as unavailable so we don't hand a
            # dead client to the adapter.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                raise ValueError(
                    "Connected gateway_client or RPC URL required for TraderJoe V2 adapter. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
                gateway_client=gateway_client,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            protocol_params = getattr(intent, "protocol_params", None) or {}
            known_bin_ids_raw = protocol_params.get("bin_ids")
            known_bin_ids = [int(bin_id) for bin_id in (known_bin_ids_raw or [])]
            # VIB-3742: Track whether bin_ids were provided by the caller. The
            # heuristic fallback (active_id ± 50 bins) silently misses bins
            # outside that window after price drift — a partial close that the
            # framework otherwise reports as success. We need this flag to
            # decide whether to emit a WARNING when we hit the heuristic path.
            #
            # An explicit ``bin_ids=[]`` counts as "provided" — the strategy is
            # telling us "I already cleared my tracked positions" (e.g. last
            # close just emptied ``self._position_bin_ids``), not "I forgot to
            # tell you what to close." Treating that as the silent-leak
            # scenario would emit a misleading warning on a no-op close.
            bin_ids_were_provided = "bin_ids" in protocol_params and known_bin_ids_raw is not None

            position = None
            if known_bin_ids:
                t0 = time.perf_counter()
                pool_addr = tj_adapter.sdk.get_pool_address(token_x_addr, token_y_addr, bin_step)
                balances = tj_adapter.sdk.get_position_balances_for_ids(
                    pool_addr,
                    self.wallet_address,
                    known_bin_ids,
                )
                logger.debug(
                    "TraderJoe V2 targeted balance lookup (LP_CLOSE): %.2fs",
                    time.perf_counter() - t0,
                )
                if balances:
                    from almanak.framework.connectors.traderjoe_v2 import LiquidityPosition

                    # Compute underlying token X/Y the position would yield so
                    # build_remove_liquidity_transaction can derive proper
                    # slippage-protected amount_x_min/amount_y_min. Without this,
                    # the targeted path would fall back to amount_x=0/amount_y=0
                    # and ship a close with no slippage protection (VIB-3741).
                    # NOTE: get_total_position_value is best-effort — it
                    # tolerates per-bin read errors (returning a partial sum)
                    # so transient RPC blips during compilation don't abort
                    # closing positions on otherwise healthy bins. The
                    # heuristic fallback path below uses the same tolerant
                    # pattern via tj_adapter.get_position(). Tracked as a
                    # follow-up to harden once we understand which fork-only
                    # reverts trigger the skip path.
                    amount_x, amount_y = tj_adapter.sdk.get_total_position_value(
                        pool_addr,
                        self.wallet_address,
                        precomputed_balances=balances,
                    )
                    # active_bin is informational; build_remove_liquidity_transaction
                    # derives slippage minimums from amount_x/amount_y and uses
                    # bin_ids/balances directly. Don't let a get_pool_info revert
                    # block a close when we already have enough data to build it.
                    try:
                        active_bin = tj_adapter.sdk.get_pool_info(pool_addr).active_id
                    except Exception as exc:  # noqa: BLE001
                        logger.debug(
                            "TraderJoe V2 get_pool_info failed in LP_CLOSE compile, proceeding with active_bin=0: %s",
                            exc,
                        )
                        active_bin = 0
                    position = LiquidityPosition(
                        pool_address=pool_addr,
                        token_x=token_x_addr,
                        token_y=token_y_addr,
                        bin_step=bin_step,
                        bin_ids=list(balances.keys()),
                        balances=balances,
                        amount_x=amount_x,
                        amount_y=amount_y,
                        active_bin=active_bin,
                    )

            if position is None:
                # Fall back to full discovery when the strategy did not provide
                # known bin IDs or the targeted lookup no longer finds liquidity.
                # Note: we intentionally let build_remove_liquidity_transaction
                # derive slippage-protected minimums for this path (below).
                #
                # VIB-3742: When bin_ids were absent from the LP_CLOSE intent,
                # this heuristic falls back to TraderJoeV2Adapter.get_position()
                # which scans only ±50 bins around the *current* active_id.
                # After price drift the original bins may sit outside that
                # window — `removeLiquidity` then closes only a subset and the
                # framework otherwise reports success while liquidity remains
                # stranded on-chain (root cause of the $1.16 leak that prompted
                # VIB-3741 / VIB-3742).
                #
                # Fire a WARNING ONLY when the caller did not supply bin_ids.
                # If bin_ids WERE supplied but the targeted lookup returned
                # zero balance, that is a legitimate "already closed" no-op
                # and not the silent-leak scenario — no warning in that case.
                if not bin_ids_were_provided:
                    logger.warning(
                        "TraderJoe V2 LP_CLOSE for pool %s on %s: protocol_params['bin_ids'] "
                        "was not supplied. Falling back to active_id ± 50 bin heuristic, "
                        "which silently MISSES bins outside the current ±50 window after "
                        "price drift and can leave liquidity stranded on-chain. Capture "
                        "bin_ids from the LP_OPEN result and pass them on close: "
                        "Intent.lp_close(..., protocol_params={'bin_ids': captured_bin_ids}). "
                        "See blueprints/05-connectors.md (TraderJoe V2 section).",
                        intent.pool,
                        self.chain,
                    )
                t0 = time.perf_counter()
                position = tj_adapter.get_position(token_x_addr, token_y_addr, bin_step)
                logger.debug(f"TraderJoe V2 get_position (LP_CLOSE): {time.perf_counter() - t0:.2f}s")

            if not position or not position.bin_ids:
                warnings.append("No LP position found to close")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_CLOSE.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Build approval for LB tokens (ERC1155-like, need approveForAll)
            pool_addr = position.pool_address
            router_addr = tj_adapter.sdk.router_address
            approve_tx, approve_gas = tj_adapter.sdk.build_approve_for_all_transaction(
                pool_address=pool_addr,
                spender_address=router_addr,
                from_address=self.wallet_address,
            )
            approve_tx_data = TransactionData(
                to=approve_tx["to"],
                value=approve_tx.get("value", 0),
                data=approve_tx["data"].hex() if isinstance(approve_tx["data"], bytes) else approve_tx["data"],
                gas_estimate=approve_gas,
                description="Approve LB tokens for router",
                tx_type="approve",
            )
            transactions.append(approve_tx_data)

            # Build remove liquidity transaction. Pass pre-fetched position so
            # the adapter skips a redundant get_position() call (saves ~50 serial
            # RPC calls). Both targeted (bin_ids) and discovery paths populate
            # position.amount_x/amount_y, so the adapter computes proper
            # slippage-protected minimums in both cases (VIB-3741).
            lp_tx = tj_adapter.build_remove_liquidity_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                bin_step=bin_step,
                position=position,
            )

            if lp_tx is None:
                warnings.append("No LP position found to close")
                # Return success with empty transactions
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_CLOSE.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Convert to TransactionData format
            lp_tx_data = TransactionData(
                to=lp_tx.to,
                value=lp_tx.value,
                data=lp_tx.data if isinstance(lp_tx.data, str) else lp_tx.data,
                gas_estimate=lp_tx.gas or 300000,
                description=(f"Remove liquidity from TraderJoe V2: {token_x_symbol}/{token_y_symbol}"),
                tx_type="traderjoe_v2_remove_liquidity",
            )
            transactions.append(lp_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "position_id": intent.position_id,
                    "collect_fees": intent.collect_fees,
                    "protocol": "traderjoe_v2",
                    "chain": self.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                f"Compiled TraderJoe V2 LP_CLOSE intent: {token_x_symbol}/{token_y_symbol}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    # crap-allowlist: VIB-4688 — pre-existing logic (cc=18) relocated from compiler.py by phase-2 fold; coverage-driven score. Unit-coverage backfill tracked in VIB-4688.
    def _compile_collect_fees_traderjoe_v2(self, intent: CollectFeesIntent) -> CompilationResult:
        """Compile LP_COLLECT_FEES intent for TraderJoe V2 Liquidity Book.

        Calls LBPair.collectFees(account, binIds) to harvest accumulated fees
        without removing any liquidity from the position.

        Args:
            intent: CollectFeesIntent to compile

        Returns:
            CompilationResult with TraderJoe V2 fee collection ActionBundle
        """
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

            # Parse pool info (format: TOKEN_X/TOKEN_Y/BIN_STEP)
            pool_parts = intent.pool.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid pool format for TraderJoe V2: {intent.pool}. Expected: TOKEN_X/TOKEN_Y/BIN_STEP",
                    intent_id=intent.intent_id,
                )

            token_x_symbol = pool_parts[0]
            token_y_symbol = pool_parts[1]
            bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

            # Resolve token addresses via TokenResolver
            token_x_info = self._resolve_token(token_x_symbol)
            token_y_info = self._resolve_token(token_y_symbol)

            if not token_x_info or not token_y_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown tokens for pool {intent.pool} on {self.chain}",
                    intent_id=intent.intent_id,
                )

            token_x_addr = token_x_info.address
            token_y_addr = token_y_info.address

            # TraderJoe V2 adapter accepts either a connected gateway_client
            # (production path) or a direct RPC URL (local/backtest fallback).
            # Treat a disconnected client as unavailable so we don't hand a
            # dead client to the adapter.
            gateway_client = self._gateway_client
            if gateway_client is not None and not gateway_client.is_connected:
                gateway_client = None

            rpc_url = None if gateway_client is not None else self._get_chain_rpc_url()
            if gateway_client is None and not rpc_url:
                raise ValueError(
                    "Connected gateway_client or RPC URL required for TraderJoe V2 adapter. "
                    "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
                )

            # Create TraderJoe V2 adapter
            config = TraderJoeV2Config(
                chain=self.chain,
                wallet_address=self.wallet_address,
                rpc_url=rpc_url,
                gateway_client=gateway_client,
            )
            tj_adapter = TraderJoeV2Adapter(config)

            # Get position to check if we have liquidity
            position = tj_adapter.get_position(token_x_addr, token_y_addr, bin_step)
            if not position or not position.bin_ids:
                warnings.append("No LP position found for fee collection")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_COLLECT_FEES.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Build collect fees transaction (no approval needed - calling LBPair directly)
            fee_tx = tj_adapter.build_collect_fees_transaction(
                token_x=token_x_addr,
                token_y=token_y_addr,
                bin_step=bin_step,
            )

            if fee_tx is None:
                warnings.append("No LP position found for fee collection")
                action_bundle = ActionBundle(
                    intent_type=IntentType.LP_COLLECT_FEES.value,
                    transactions=[],
                    metadata={
                        "pool": intent.pool,
                        "protocol": "traderjoe_v2",
                        "warning": "No position found",
                    },
                )
                result.action_bundle = action_bundle
                result.warnings = warnings
                return result

            # Convert to TransactionData format
            fee_tx_data = TransactionData(
                to=fee_tx.to,
                value=fee_tx.value,
                data=fee_tx.data if isinstance(fee_tx.data, str) else fee_tx.data,
                gas_estimate=fee_tx.gas or 200000,
                description=f"Collect fees from TraderJoe V2: {token_x_symbol}/{token_y_symbol}",
                tx_type="traderjoe_v2_collect_fees",
            )
            transactions.append(fee_tx_data)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_COLLECT_FEES.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool": intent.pool,
                    "protocol": "traderjoe_v2",
                    "chain": self.chain,
                    "bin_ids": position.bin_ids,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled TraderJoe V2 LP_COLLECT_FEES intent: {token_x_symbol}/{token_y_symbol}, "
                f"{len(position.bin_ids)} bins, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile TraderJoe V2 LP_COLLECT_FEES intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _resolve_traderjoe_v2_swap_tokens(
        self,
        intent: SwapIntent,
    ) -> tuple[TokenInfo, TokenInfo, Any, Any] | CompilationResult:
        """Resolve from/to tokens and their wrapped-for-swap equivalents.

        TraderJoe V2 LB pairs are ERC-20 only; native input/output must probe
        and swap against the wrapped token. Preserves exact error strings
        ("Unknown from_token: ...", "Unknown to_token: ...") pinned by
        ``tests/unit/intents/test_compiler_traderjoe_v2_swap.py``.

        Returns ``(from_token, to_token, swap_from_token, swap_to_token)`` on
        success or a ``CompilationResult`` (FAILED) on unknown-token. The
        swap tokens are either ``TokenInfo`` (non-native path) or
        ``ResolvedToken`` (native path via ``resolve_for_swap``); both expose
        ``.address`` which is all downstream consumers need, so the return
        type is widened to ``Any`` for the last two elements rather than
        importing ``ResolvedToken`` here.
        """
        resolver = self._token_resolver
        from_token = self._resolve_token(intent.from_token)
        to_token = self._resolve_token(intent.to_token)

        if from_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown from_token: {intent.from_token}",
                intent_id=intent.intent_id,
            )
        if to_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown to_token: {intent.to_token}",
                intent_id=intent.intent_id,
            )

        swap_from_token: Any = (
            resolver.resolve_for_swap(intent.from_token, self.chain) if from_token.is_native else from_token
        )
        swap_to_token: Any = resolver.resolve_for_swap(intent.to_token, self.chain) if to_token.is_native else to_token
        return from_token, to_token, swap_from_token, swap_to_token

    def _resolve_traderjoe_v2_swap_amount(
        self,
        intent: SwapIntent,
        from_token: TokenInfo,
    ) -> Decimal | CompilationResult:
        """Resolve a SwapIntent's amount to a Decimal in token units.

        Preserves the exact error strings tested in
        ``tests/unit/intents/test_compiler_traderjoe_v2_swap.py::...::
        test_amount_all_rejected`` and the "Either amount_usd or amount must
        be provided" branch.
        """
        if intent.amount_usd is not None:
            price = self._require_token_price(from_token.symbol)
            return intent.amount_usd / price
        if intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )
            return intent.amount  # type: ignore[return-value]
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Either amount_usd or amount must be provided",
            intent_id=intent.intent_id,
        )

    def _resolve_traderjoe_v2_gateway_rpc(self, adapter_name: str) -> tuple[GatewayClient | None, str | None]:
        """Return ``(gateway_client, rpc_url)`` for a TraderJoe V2 adapter.

        Normalises a disconnected gateway to None and falls back to the
        chain RPC URL. Raises ``ValueError`` (caught by the caller's generic
        try/except, surfacing as ``result.error``) when neither is usable.

        The ``GatewayClient`` cast is sound because the input to
        ``normalise_gateway_or_rpc`` is already ``self._gateway_client:
        GatewayClient | None`` — the helper only narrows via
        ``is_connected``, it does not widen the type.
        """
        client, rpc_url = normalise_gateway_or_rpc(
            gateway_client=self._gateway_client,
            rpc_url_supplier=self._get_chain_rpc_url,
        )
        if client is None and not rpc_url:
            raise ValueError(
                f"Connected gateway_client or RPC URL required for {adapter_name}. "
                "Either provide rpc_url to IntentCompiler or use GatewayExecutionOrchestrator."
            )
        # Cast: the helper's `object | None` return is really the same
        # `GatewayClient | None` the caller passed in.
        return cast("GatewayClient | None", client), rpc_url

    def _autodetect_traderjoe_v2_bin_step(
        self,
        *,
        intent: SwapIntent,
        tj_adapter: Any,
        swap_from_token: TokenInfo,
        swap_to_token: TokenInfo,
        from_token_symbol: str,
        to_token_symbol: str,
        pool_not_found_exc: type[BaseException],
    ) -> int | CompilationResult:
        """Auto-detect a TraderJoe V2 bin step by probing the SDK.

        Iterates common bin steps (20, 25, 15, 10, 50, 5, 100, 1) and returns
        the first one with a pool that is not fully empty (at least one
        reserve > 0). The liquidity gate (VIB-4374) reflects
        ``blueprints/05-connectors.md``'s Pool Selection Policy: "do not
        assume a single fee tier has viable liquidity in both directions."
        On arbitrum, several common pairs (e.g. WETH/USDC) have a
        ``(0, 0)`` bin_step=25 pool ahead of a liquid bin_step=15 pool,
        so a pool-existence-only probe would build a swap guaranteed to
        revert at execution. The gate matches ``validate_traderjoe_pool``'s
        definition of "empty" (both reserves zero) so pools with usable
        one-sided liquidity remain selectable — the quote path will still
        fail closed on zero output for the requested direction.

        Preserves the exact error strings pinned by
        ``test_compiler_traderjoe_v2_swap``:
            - "Failed to probe TraderJoe V2 pool for bin_step={bs}: {exc}"
            - "No TraderJoe V2 pool found for {X}/{Y} on {chain}. Tried bin
              steps: [...]. The pair may not have a Liquidity Book pool."
        """
        bin_step_order = [20, 25, 15, 10, 50, 5, 100, 1]

        def _pool_has_liquidity(pool_address: str) -> bool:
            # Fail-open: if reserve probing fails or returns non-numeric
            # values (e.g. unit-test MagicMocks, RPC flakes, ABI drift), we
            # cannot prove the pool is empty. Accept the candidate and let
            # downstream ``validate_traderjoe_pool`` surface zero-liquidity
            # cases without regressing call sites that genuinely have a
            # live pool. Only reject when *both* reserves are zero —
            # matches ``validate_traderjoe_pool``'s empty-pool definition
            # (``reserve_x == 0 and reserve_y == 0``) so we don't skip
            # pools with usable one-sided liquidity, where the quote path
            # can still ask the router whether the requested direction
            # has output liquidity and fail closed if it doesn't.
            try:
                info = tj_adapter.sdk.get_pool_info(pool_address)
                return int(info.reserve_x or 0) > 0 or int(info.reserve_y or 0) > 0
            except Exception:  # noqa: BLE001 — fail-open: keep iterating
                return True

        found_bin_step, broken_bs, unexpected_exc = probe_traderjoe_bin_step(
            probe=tj_adapter.sdk.get_pool_address,
            token_a=swap_from_token.address,
            token_b=swap_to_token.address,
            not_found_exception=pool_not_found_exc,
            candidates=tuple(bin_step_order),
            is_liquid=_pool_has_liquidity,
        )
        if unexpected_exc is not None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to probe TraderJoe V2 pool for bin_step={broken_bs}: {unexpected_exc}",
                intent_id=intent.intent_id,
            )
        if found_bin_step is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"No TraderJoe V2 pool found for {from_token_symbol}/{to_token_symbol} on {self.chain}. "
                    f"Tried bin steps: {bin_step_order}. "
                    f"The pair may not have a Liquidity Book pool."
                ),
                intent_id=intent.intent_id,
            )
        return found_bin_step

    @staticmethod
    def _fetch_traderjoe_v2_swap_quote(
        *,
        intent: SwapIntent,
        tj_adapter: Any,
        from_token_symbol: str,
        to_token_symbol: str,
        amount_decimal: Decimal,
        bin_step: int,
        pool_not_found_exc: type[BaseException],
        sdk_error_exc: type[BaseException],
    ) -> Any:
        """Fetch the Phase-B quote once (reused for both min-out and metadata).

        Returns the raw quote on success or a ``CompilationResult`` (FAILED)
        when the quote call fails or returns zero amount_out. See VIB-3203
        Phase B for the "anchor both reads to the same on-chain quote"
        rationale.
        """
        try:
            quote = tj_adapter.get_swap_quote(
                token_in=from_token_symbol,
                token_out=to_token_symbol,
                amount_in=amount_decimal,
                bin_step=bin_step,
            )
        except (pool_not_found_exc, sdk_error_exc) as quote_exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"TraderJoe V2 quote failed for {from_token_symbol} -> {to_token_symbol} "
                    f"(bin_step={bin_step}): {quote_exc}"
                ),
                intent_id=intent.intent_id,
            )

        if quote.amount_out <= 0:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"TraderJoe V2 quote returned zero amount_out for {from_token_symbol} -> "
                    f"{to_token_symbol} (bin_step={bin_step}); refusing to build swap with no "
                    f"slippage floor"
                ),
                intent_id=intent.intent_id,
            )
        return quote

    @staticmethod
    def _build_traderjoe_v2_swap_tx_data(
        *,
        swap_tx: Any,
        amount_decimal: Decimal,
        from_token_symbol: str,
        to_token_symbol: str,
        bin_step: int,
    ) -> TransactionData:
        """Convert the adapter's TransactionData into compiler TransactionData.

        Extracted so the main compile method stays small. Gas default matches
        pre-refactor behaviour (``DEFAULT_GAS_ESTIMATES["traderjoe_v2_swap"]``
        falling back to 200_000).
        """
        return TransactionData(
            to=swap_tx.to,
            value=swap_tx.value,
            data=swap_tx.data if isinstance(swap_tx.data, str) else f"0x{swap_tx.data.hex()}",
            gas_estimate=swap_tx.gas or DEFAULT_GAS_ESTIMATES.get("traderjoe_v2_swap", 200_000),
            description=(
                f"TraderJoe V2 swap: {amount_decimal} {from_token_symbol} -> {to_token_symbol} (bin_step={bin_step})"
            ),
            tx_type="traderjoe_v2_swap",
        )

    def _compile_swap_traderjoe_v2(self, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for TraderJoe V2 Liquidity Book (VIB-1928).

        TraderJoe V2 uses LBRouter2 with a bin-based AMM interface:
        - swapExactTokensForTokens(amountIn, amountOutMin, Path, to, deadline)
        - Path struct: {pairBinSteps, versions, tokenPath}

        This is incompatible with DefaultSwapAdapter (Uniswap V3 exactInputSingle),
        hence the dedicated compilation path.

        Bin step is auto-detected across common bin steps (20, 25, 15, 10, 50, 5, 100, 1).

        Args:
            intent: SwapIntent with from_token, to_token, and amount

        Returns:
            CompilationResult with TraderJoe V2 swap ActionBundle
        """
        transactions: list[TransactionData] = []

        try:
            from almanak.core.contracts import TRADERJOE_V2 as TJ_ADDRESSES
            from almanak.framework.connectors.traderjoe_v2 import (
                TraderJoeV2Adapter,
                TraderJoeV2Config,
            )
            from almanak.framework.connectors.traderjoe_v2.sdk import (
                PoolNotFoundError as _TJPoolNotFoundError,
            )
            from almanak.framework.connectors.traderjoe_v2.sdk import (
                TraderJoeV2SDKError as _TJSDKError,
            )

            if self.chain not in TJ_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"TraderJoe V2 is not supported on {self.chain}. Supported: {list(TJ_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            tokens = self._resolve_traderjoe_v2_swap_tokens(intent)
            if isinstance(tokens, CompilationResult):
                return tokens
            from_token, to_token, swap_from_token, swap_to_token = tokens

            amount_resolution = self._resolve_traderjoe_v2_swap_amount(intent, from_token)
            if isinstance(amount_resolution, CompilationResult):
                return amount_resolution
            amount_decimal: Decimal = amount_resolution
            amount_in_wei = int(amount_decimal * Decimal(10**from_token.decimals))

            gateway_client, rpc_url = self._resolve_traderjoe_v2_gateway_rpc(
                adapter_name="TraderJoe V2 swap compilation",
            )

            router_address = TJ_ADDRESSES[self.chain]["router"]
            slippage_bps = int(intent.max_slippage * Decimal("10000"))
            tj_adapter = TraderJoeV2Adapter(
                TraderJoeV2Config(
                    chain=self.chain,
                    wallet_address=self.wallet_address,
                    rpc_url=rpc_url,
                    default_slippage_bps=slippage_bps,
                    gateway_client=gateway_client,
                )
            )

            bin_step_or_err = self._autodetect_traderjoe_v2_bin_step(
                intent=intent,
                tj_adapter=tj_adapter,
                swap_from_token=swap_from_token,
                swap_to_token=swap_to_token,
                from_token_symbol=from_token.symbol,
                to_token_symbol=to_token.symbol,
                pool_not_found_exc=_TJPoolNotFoundError,
            )
            if isinstance(bin_step_or_err, CompilationResult):
                return bin_step_or_err
            bin_step: int = bin_step_or_err

            logger.info(
                "Compiling TraderJoe V2 SWAP: %s -> %s, amount=%s, bin_step=%d",
                from_token.symbol,
                to_token.symbol,
                amount_decimal,
                bin_step,
            )

            from almanak.framework.intents.pool_validation import validate_traderjoe_pool

            # Use the same normalised gateway as the adapter: a disconnected
            # ``self._gateway_client`` would otherwise make validation fail
            # against a stale client while the adapter succeeds via RPC.
            pool_check = validate_traderjoe_pool(
                self.chain,
                swap_from_token.address,
                swap_to_token.address,
                bin_step,
                rpc_url,
                gateway_client=gateway_client,
            )
            failed = self._validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            if not from_token.is_native:
                transactions.extend(self._build_approve_tx(from_token.address, router_address, amount_in_wei))

            # VIB-3203 Phase B: quote once; re-use for both amount_out_min and metadata.
            quote_or_err = self._fetch_traderjoe_v2_swap_quote(
                intent=intent,
                tj_adapter=tj_adapter,
                from_token_symbol=from_token.symbol,
                to_token_symbol=to_token.symbol,
                amount_decimal=amount_decimal,
                bin_step=bin_step,
                pool_not_found_exc=_TJPoolNotFoundError,
                sdk_error_exc=_TJSDKError,
            )
            if isinstance(quote_or_err, CompilationResult):
                return quote_or_err
            quote = quote_or_err
            expected_output_human: Decimal = quote.amount_out

            swap_tx = tj_adapter.build_swap_transaction(
                token_in=from_token.symbol,
                token_out=to_token.symbol,
                amount_in=amount_decimal,
                bin_step=bin_step,
                slippage_bps=slippage_bps,
                quote=quote,
            )
            transactions.append(
                self._build_traderjoe_v2_swap_tx_data(
                    swap_tx=swap_tx,
                    amount_decimal=amount_decimal,
                    from_token_symbol=from_token.symbol,
                    to_token_symbol=to_token.symbol,
                    bin_step=bin_step,
                )
            )

            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in_wei),
                    "bin_step": bin_step,
                    "protocol": "traderjoe_v2",
                    "router": router_address,
                    "chain": self.chain,
                    # Anchored to the same on-chain read as ``amount_out_min``.
                    # Consumed by ResultEnricher -> extract_swap_amounts for
                    # realized slippage_bps (VIB-3203).
                    "expected_output_human": str(expected_output_human),
                },
            )

            logger.info(
                "Compiled TraderJoe V2 SWAP: %s -> %s, bin_step=%d, %d txs, %d gas",
                from_token.symbol,
                to_token.symbol,
                bin_step,
                len(transactions),
                total_gas,
            )

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
                warnings=[],
            )

        except Exception as e:
            logger.exception("Failed to compile TraderJoe V2 SWAP intent: %s", e)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )
