"""Teardown execution methods for StrategyRunner.

Extracted from strategy_runner.py for maintainability. Each function takes
``runner`` (a StrategyRunner instance) as its first argument and is called
via a thin delegation stub in StrategyRunner.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..intents.compiler import IntentCompiler, IntentCompilerConfig
from ..intents.vocabulary import Intent

if TYPE_CHECKING:
    from ..teardown import TeardownMode
    from .runner_models import IterationResult, StrategyProtocol

# Use the original strategy_runner logger so existing log-capture tests and
# log-filtering rules continue to work after the extraction.
logger = logging.getLogger("almanak.framework.runner.strategy_runner")


# -------------------------------------------------------------------------
# Main teardown entry point
# -------------------------------------------------------------------------


async def execute_teardown(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_mode: TeardownMode,
    start_time: datetime,
) -> IterationResult:
    """Execute teardown, routing through TeardownManager when possible.

    For single-chain strategies, delegates to TeardownManager which provides:
    - Position-aware loss caps (1-3% based on position size)
    - Escalating slippage tolerance (tight -> loose with approval gates)
    - Cancel window (configurable, default 10 seconds)
    - Post-execution verification (checks positions are actually closed)
    - State persistence for resumability

    For multi-chain strategies, uses the inline execution path (TeardownManager
    does not yet support multi-chain orchestration).

    Args:
        runner: StrategyRunner instance
        strategy: The strategy to teardown
        teardown_mode: SOFT (graceful) or HARD (emergency)
        start_time: When the iteration started

    Returns:
        IterationResult with teardown status
    """
    from ..teardown import get_teardown_state_manager
    from .runner_models import IterationResult, IterationStatus

    strategy_id = strategy.strategy_id
    manager = get_teardown_state_manager()
    request = manager.get_active_request(strategy_id)

    # Step T1: Create market snapshot (SAME as normal decide() path)
    teardown_market = None
    try:
        teardown_market = strategy.create_market_snapshot()
        if hasattr(teardown_market, "get_price_oracle_dict"):
            logger.debug(
                f"Created market snapshot for teardown with prices: "
                f"{list(teardown_market.get_price_oracle_dict().keys())}"
            )
        else:
            logger.debug("Created multi-chain market snapshot for teardown")
    except Exception as e:
        logger.warning(f"Failed to create market snapshot for teardown: {e}. Continuing without market data.")

    # Step T2: Generate teardown intents WITH market (symmetric with decide(market))
    try:
        try:
            teardown_intents = strategy.generate_teardown_intents(teardown_mode, market=teardown_market)
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            # Backward compat: old-style signature def generate_teardown_intents(self, mode)
            logger.debug(f"Strategy {strategy_id} uses old teardown signature (no market param), falling back")
            teardown_intents = strategy.generate_teardown_intents(teardown_mode)
    except Exception as e:
        logger.error(f"Failed to generate teardown intents for {strategy_id}: {e}")
        if request:
            try:
                manager.mark_failed(strategy_id, error=str(e))
            except Exception:
                logger.warning("Failed to update teardown state for %s", strategy_id, exc_info=True)
        runner._request_teardown_failure_shutdown(str(e))
        return runner._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, str(e), start_time)

    if not teardown_intents:
        logger.info(f"🛑 {strategy_id} teardown complete (no positions to close)")
        if request:
            manager.mark_completed(strategy_id, result={"reason": "no_positions"})
        runner.request_shutdown()
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    logger.info(f"🛑 {strategy_id} entering TEARDOWN mode ({len(teardown_intents)} intents to execute)")
    if request:
        manager.mark_started(strategy_id, total_positions=len(teardown_intents))

    # Step T2.5: Pre-fetch prices for tokens in teardown intents
    if teardown_market is not None and hasattr(teardown_market, "price"):
        try:
            prefetch_teardown_prices(teardown_market, teardown_intents)
        except Exception as e:
            logger.warning(f"Failed to pre-fetch teardown prices: {e}")

    # Note: amount="all" resolution is handled lazily inside _execute_intents
    # (per-intent, just before execution) so staged exits work correctly
    # (e.g., withdraw then swap uses tokens produced by the earlier step).

    # Step T2.7: If all intents were resolved away, teardown is complete
    if not teardown_intents:
        logger.info(f"🛑 {strategy_id} teardown complete (all positions already closed)")
        if request:
            manager.mark_completed(strategy_id, result={"reason": "all_balances_zero"})
        runner.request_shutdown()
        runner._lifecycle_write_state(strategy_id, "TERMINATED")
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )

    # Step T3: Execute teardown intents
    if runner._is_multi_chain:
        # Multi-chain: use inline path (TeardownManager doesn't support multi-chain yet)
        result = await runner._execute_multi_chain(
            strategy=strategy,
            intents=teardown_intents,
            start_time=start_time,
            market=teardown_market,
        )
        if result.success:
            result.status = IterationStatus.TEARDOWN
            logger.info(f"🛑 {strategy_id} teardown complete - shutting down strategy runner")
            runner.request_shutdown()
            if request:
                manager.mark_completed(strategy_id, result={"intents": len(teardown_intents)})
        else:
            if request:
                try:
                    manager.mark_failed(strategy_id, error=result.error or "execution failed")
                except Exception:
                    logger.warning("Failed to update teardown state for %s", strategy_id, exc_info=True)
            runner._request_teardown_failure_shutdown(result.error or "multi-chain teardown execution failed")
        return result
    else:
        # Single-chain: route through TeardownManager for safety guarantees
        # Call through runner method (not standalone function) so instance-level
        # mock patching in tests continues to work.
        return await runner._execute_teardown_via_manager(
            strategy=strategy,
            teardown_intents=teardown_intents,
            teardown_mode=teardown_mode,
            teardown_market=teardown_market,
            start_time=start_time,
            request=request,
            state_manager=manager,
        )


# -------------------------------------------------------------------------
# TeardownManager path (single-chain)
# -------------------------------------------------------------------------


async def execute_teardown_via_manager(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_mode: TeardownMode,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> IterationResult:
    """Execute single-chain teardown through TeardownManager for full safety.

    TeardownManager provides safety features that the inline path lacks:
    - Position-aware loss caps (1-3% based on portfolio size)
    - Escalating slippage tolerance with operator approval gates
    - Cancel window for operator intervention
    - Post-execution verification (checks positions are closed on-chain)
    - Resumable state persistence

    Falls back to inline sequential execution if TeardownManager cannot
    be initialized (e.g., incompatible orchestrator type).

    Args:
        runner: StrategyRunner instance
        strategy: The strategy to teardown
        teardown_intents: Pre-resolved teardown intents
        teardown_mode: SOFT (graceful) or HARD (emergency)
        teardown_market: Market snapshot (may be None)
        start_time: When the iteration started
        request: Active teardown request from state manager
        state_manager: Teardown state manager for lifecycle tracking
    """
    import uuid

    from ..teardown import TeardownMode
    from ..teardown.teardown_manager import TeardownManager
    from .runner_models import IterationResult, IterationStatus

    strategy_id = strategy.strategy_id
    mode_str = "graceful" if teardown_mode == TeardownMode.SOFT else "emergency"

    # Build compiler for TeardownManager
    # Call through runner method so instance-level mock patching in tests works.
    compiler = runner._build_teardown_compiler(strategy, teardown_market)
    if compiler is None:
        logger.warning(f"Cannot build compiler for TeardownManager — falling back to inline teardown for {strategy_id}")
        return await runner._execute_teardown_inline(
            strategy, teardown_intents, teardown_market, start_time, request, state_manager
        )

    # Create TeardownManager with safety features
    teardown_mgr = TeardownManager(
        orchestrator=runner.execution_orchestrator,
        compiler=compiler,
        alert_manager=runner.alert_manager,
    )

    # Execute with TeardownManager safety: loss caps, escalating slippage,
    # cancel window, post-execution verification
    logger.info(
        f"🛑 Routing {strategy_id} teardown through TeardownManager (mode={mode_str}, intents={len(teardown_intents)})"
    )

    try:
        # Get positions for safety validation (loss caps).
        # If positions can't be fetched, fall back to inline execution —
        # we must NOT pass an empty portfolio through safety validation
        # as it would trivially pass loss cap checks (3% of $0 = $0).
        try:
            positions = strategy.get_open_positions()
        except Exception as pos_err:
            logger.warning(
                f"Cannot fetch positions for safety validation — "
                f"falling back to inline teardown for {strategy_id}: {pos_err}"
            )
            return await runner._execute_teardown_inline(
                strategy, teardown_intents, teardown_market, start_time, request, state_manager
            )

        # Safety validation: check loss caps before execution
        validation = teardown_mgr.safety_guard.validate_teardown_request(positions, teardown_mode)
        if not validation.all_passed:
            logger.error(f"🛑 Teardown safety validation failed: {validation.blocked_reason}")
            if request:
                try:
                    state_manager.mark_failed(
                        strategy_id, error=f"Safety validation failed: {validation.blocked_reason}"
                    )
                except Exception:
                    logger.warning("Failed to update teardown state for %s", strategy_id, exc_info=True)
            runner._request_teardown_failure_shutdown(f"Teardown safety validation failed: {validation.blocked_reason}")
            return runner._create_error_result(
                strategy_id,
                IterationStatus.STRATEGY_ERROR,
                f"Teardown safety validation failed: {validation.blocked_reason}",
                start_time,
            )

        # Persist state for resumability
        teardown_id = f"td_{uuid.uuid4().hex[:12]}"
        teardown_state = await teardown_mgr._persist_state(
            teardown_id=teardown_id,
            strategy=strategy,  # type: ignore[arg-type]
            mode=teardown_mode,
            intents=teardown_intents,
        )

        # Run cancel window — gives operator time to abort
        cancel_result = await teardown_mgr.cancel_window.run_cancel_window(
            teardown_id=teardown_id,
            is_auto_mode=True,
        )
        if cancel_result.was_cancelled:
            logger.info(f"🛑 Teardown {teardown_id} cancelled during window")
            runner._record_success()
            return IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                strategy_id=strategy_id,
                duration_ms=runner._calculate_duration_ms(start_time),
            )

        # Update state to EXECUTING after cancel window
        from ..teardown.models import TeardownStatus

        teardown_state.status = TeardownStatus.EXECUTING
        if teardown_mgr.state_manager:
            await teardown_mgr.state_manager.save_teardown_state(teardown_state)

        # Extract price oracle for accurate compilation during execution.
        # Do NOT use `or None` — an empty dict {} should stay as-is,
        # not collapse to None (which triggers placeholder prices).
        price_oracle = None
        if teardown_market is not None and hasattr(teardown_market, "get_price_oracle_dict"):
            fetched = teardown_market.get_price_oracle_dict()
            price_oracle = fetched if fetched is not None else None
        if not price_oracle:
            price_oracle = get_fallback_teardown_prices(teardown_market)

        # Execute intents with escalating slippage
        teardown_result = await teardown_mgr._execute_intents(
            teardown_id=teardown_state.teardown_id,
            strategy=strategy,  # type: ignore[arg-type]
            intents=teardown_intents,
            positions=positions,
            mode=teardown_mode,
            teardown_state=teardown_state,
            is_auto_mode=True,
            price_oracle=price_oracle,
            market=teardown_market,
        )

        # Post-execution verification: check positions are actually closed.
        # Fail closed: if verification raises, treat teardown as failed to
        # avoid reporting success while positions may still be open.
        try:
            positions_closed = await teardown_mgr._verify_closure(strategy)  # type: ignore[arg-type]
            if not positions_closed:
                # Log warning but don't fail — some strategies have advisory
                # get_open_positions() that doesn't reflect on-chain state.
                # Matches TeardownManager.execute() which also doesn't fail on this.
                logger.warning(
                    f"Post-teardown verification: {strategy_id} still reports open positions "
                    f"(may be advisory — check strategy's get_open_positions())"
                )
        except Exception as verify_err:
            logger.error(f"Post-teardown verification failed: {verify_err}")
            if request:
                try:
                    state_manager.mark_failed(strategy_id, error=f"Post-teardown verification failed: {verify_err}")
                except Exception:
                    logger.warning("Failed to update teardown state for %s", strategy_id, exc_info=True)
            runner._request_teardown_failure_shutdown(f"Post-teardown verification failed: {verify_err}")
            return runner._create_error_result(
                strategy_id,
                IterationStatus.STRATEGY_ERROR,
                f"Post-teardown verification failed: {verify_err}",
                start_time,
            )

        # Send completion alert
        if teardown_mgr.alert_manager and teardown_result.success:
            try:
                await teardown_mgr.alert_manager.send_teardown_complete(teardown_result)
            except Exception as alert_err:
                logger.warning(f"Failed to send teardown completion alert: {alert_err}")

        # Clean up persisted state on success
        if teardown_mgr.state_manager and teardown_result.success:
            try:
                await teardown_mgr.state_manager.delete_teardown_state(teardown_id)
            except Exception as cleanup_err:
                logger.warning(f"Failed to clean up teardown state: {cleanup_err}")

    except Exception as e:
        logger.error(f"🛑 TeardownManager execution failed for {strategy_id}: {e}")
        if request:
            try:
                state_manager.mark_failed(strategy_id, error=str(e))
            except Exception:
                logger.warning("Failed to update teardown state for %s", strategy_id, exc_info=True)
        runner._request_teardown_failure_shutdown(str(e))
        return runner._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, str(e), start_time)

    # Map TeardownResult -> IterationResult
    if teardown_result.success:
        logger.info(
            f"🛑 {strategy_id} teardown complete via TeardownManager "
            f"({teardown_result.intents_succeeded}/{teardown_result.intents_total} intents, "
            f"{teardown_result.duration_seconds:.1f}s)"
        )
        runner.request_shutdown()
        runner._lifecycle_write_state(strategy_id, "TERMINATED")
        if request:
            state_manager.mark_completed(
                strategy_id,
                result={
                    "intents": teardown_result.intents_succeeded,
                    "mode": mode_str,
                    "duration_s": teardown_result.duration_seconds,
                },
            )
        runner._record_success()
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )
    else:
        logger.warning(f"🛑 {strategy_id} teardown incomplete via TeardownManager: {teardown_result.error}")
        if request:
            try:
                state_manager.mark_failed(strategy_id, error=teardown_result.error or "teardown failed")
            except Exception:
                logger.warning("Failed to update teardown state for %s", strategy_id, exc_info=True)
        runner._request_teardown_failure_shutdown(teardown_result.error or "teardown failed")
        return IterationResult(
            status=IterationStatus.STRATEGY_ERROR,
            error=teardown_result.error,
            strategy_id=strategy_id,
            duration_ms=runner._calculate_duration_ms(start_time),
        )


# -------------------------------------------------------------------------
# Inline teardown fallback
# -------------------------------------------------------------------------


async def execute_teardown_inline(
    runner: Any,
    strategy: StrategyProtocol,
    teardown_intents: list,
    teardown_market: Any | None,
    start_time: datetime,
    request: Any | None,
    state_manager: Any,
) -> IterationResult:
    """Fallback inline teardown execution (no TeardownManager safety features).

    Used when TeardownManager cannot be initialized (e.g., incompatible
    orchestrator type or missing compiler dependencies).

    Executes teardown intents sequentially via _execute_single_chain.
    """
    from .runner_models import IterationResult, IterationStatus

    strategy_id = strategy.strategy_id

    all_success = True
    last_result = None
    for i, intent in enumerate(teardown_intents):
        logger.info(f"🛑 Executing teardown intent {i + 1}/{len(teardown_intents)}: {intent.intent_type.value}")

        # Resolve amount="all" to actual wallet balance before execution.
        # Only resolve for intents with a token balance field (e.g., SwapIntent.from_token).
        # Intents like vault_redeem(shares="all") are handled natively by the compiler.
        intent_to_execute = intent
        if Intent.has_chained_amount(intent):
            balance_token = (
                getattr(intent, "from_token", None)
                or getattr(intent, "token", None)
                or getattr(intent, "token_in", None)
            )
            if balance_token and teardown_market is not None:
                # Resolve balance — pass chain for multi-chain market snapshots
                intent_chain = getattr(intent, "chain", None)
                try:
                    if intent_chain:
                        bal = teardown_market.balance(balance_token, intent_chain)
                    else:
                        bal = teardown_market.balance(balance_token)
                except TypeError:
                    # Single-chain MarketSnapshot doesn't accept chain param
                    bal = teardown_market.balance(balance_token)
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        f"🛑 Teardown intent {i + 1}: failed to resolve balance for {balance_token}: {e}. "
                        f"Token may be missing from the registry. Position may remain open."
                    )
                    all_success = False
                    last_result = IterationResult(
                        status=IterationStatus.COMPILATION_FAILED,
                        intent=intent,
                        error=f"Cannot resolve amount='all' for {balance_token}: {e}",
                        strategy_id=strategy_id,
                        duration_ms=runner._calculate_duration_ms(start_time),
                    )
                    break
                # MarketSnapshot.balance() returns Decimal; IntentStrategy.balance() returns TokenBalance
                balance_value = bal.balance if hasattr(bal, "balance") else bal
                if balance_value <= 0:
                    logger.info(f"🛑 Teardown intent {i + 1}: {balance_token} balance is 0, skipping (already closed)")
                    continue
                intent_to_execute = Intent.set_resolved_amount(intent, balance_value)
                logger.info(f"🛑 Resolved amount='all' for {balance_token}: {balance_value}")
            elif balance_token and teardown_market is None:
                # Have a token to resolve but no market — log warning, let compiler try
                logger.warning(
                    f"🛑 Teardown intent {i + 1}: amount='all' for {balance_token} but no market context. "
                    f"Passing to compiler as-is — compilation may fail."
                )
            else:
                # No token field — let compiler handle natively (e.g., shares="all")
                logger.debug(f"🛑 Teardown intent {i + 1}: no token field, passing to compiler as-is")

        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent_to_execute,
            start_time=start_time,
            total_intents=1,
            market=teardown_market,
        )
        last_result = result
        if not result.success:
            all_success = False
            logger.error(f"🛑 Teardown intent {i + 1} failed: {result.error}")
            break  # Stop on first failure

    if last_result:
        if all_success:
            last_result.status = IterationStatus.TEARDOWN
            logger.info(f"🛑 {strategy_id} teardown complete - shutting down strategy runner")
            runner.request_shutdown()
            runner._lifecycle_write_state(strategy_id, "TERMINATED")
            runner._record_success()
            if request:
                state_manager.mark_completed(strategy_id, result={"intents": len(teardown_intents)})
        else:
            logger.warning(f"🛑 {strategy_id} teardown incomplete - manual intervention may be required")
            if request:
                try:
                    state_manager.mark_failed(strategy_id, error=last_result.error or "execution failed")
                except Exception:
                    logger.warning("Failed to update teardown state for %s", strategy_id, exc_info=True)
            runner._request_teardown_failure_shutdown(last_result.error or "inline teardown execution failed")
        return last_result

    # Edge case: no intents executed (all positions already closed)
    logger.info(f"🛑 {strategy_id} teardown: all positions already closed, shutting down")
    runner.request_shutdown()
    runner._lifecycle_write_state(strategy_id, "TERMINATED")
    runner._record_success()
    if request:
        state_manager.mark_completed(strategy_id, result={"reason": "all_positions_already_closed"})
    return IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        strategy_id=strategy_id,
        duration_ms=runner._calculate_duration_ms(start_time),
    )


# -------------------------------------------------------------------------
# Compiler / price helpers
# -------------------------------------------------------------------------


def build_teardown_compiler(
    runner: Any,
    strategy: StrategyProtocol,
    market: Any | None,
) -> IntentCompiler | None:
    """Build an IntentCompiler for TeardownManager teardown execution.

    Returns None if compiler cannot be built (e.g., missing RPC access).
    """
    from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

    gateway_client = None
    rpc_url = None

    if isinstance(runner.execution_orchestrator, GatewayExecutionOrchestrator):
        gateway_client = runner.execution_orchestrator._client
    else:
        rpc_url = getattr(runner.execution_orchestrator, "rpc_url", None)

    # Extract prices from market snapshot.
    # IMPORTANT: do NOT convert {} to None via `or None` — an empty dict
    # is distinct from None.  With None the compiler falls back to $1
    # placeholder prices, producing wildly wrong slippage calculations
    # and silent None action bundles on mainnet (VIB-1386..1391).
    fetched: dict[str, Decimal] | None = None
    if market is not None and hasattr(market, "get_price_oracle_dict"):
        fetched = market.get_price_oracle_dict()
    # Merge fallback prices (stablecoins + major tokens) into the fetched
    # oracle.  This ensures partially-populated caches (e.g. only USDC)
    # still get WETH/WBTC fallback prices instead of $1 placeholders.
    fallback = get_fallback_teardown_prices(market)
    merged = {**(fallback or {}), **(fetched if fetched is not None else {})}
    price_oracle = merged if merged else None

    has_prices = bool(price_oracle)
    if not has_prices:
        logger.warning(
            "No token prices available for teardown compiler — "
            "compilation will use placeholder prices ($1 for all tokens). "
            "This is likely a gateway connectivity issue."
        )

    try:
        compiler_config = IntentCompilerConfig(
            allow_placeholder_prices=not has_prices,
        )
        return IntentCompiler(
            chain=strategy.chain,
            wallet_address=strategy.wallet_address,
            rpc_url=rpc_url,
            price_oracle=price_oracle,
            config=compiler_config,
            gateway_client=gateway_client,
            chain_wallets=getattr(strategy, "_chain_wallets", None),
        )
    except Exception as e:
        logger.warning(f"Failed to build teardown compiler: {e}")
        return None


def prefetch_teardown_prices(market: Any, intents: list) -> None:
    """Eagerly fetch prices for tokens referenced in teardown intents.

    MarketSnapshot uses lazy loading — prices only populate when market.price()
    is called. During teardown, generate_teardown_intents() typically doesn't call
    market.price(), so get_price_oracle_dict() returns {} until this method
    pre-populates the cache with real prices for the teardown tokens.

    Teardown intents often reference tokens by address (e.g. 0xdefa1d...) rather
    than symbol. market.price() expects a symbol, so we resolve addresses to
    symbols first using the token resolver. Without this, tokens like ALMANAK
    (not in CoinGecko/Chainlink) fail price resolution during teardown.
    """
    token_attrs = ("from_token", "to_token", "token", "collateral_token", "borrow_token", "token_in")
    tokens: set[str] = set()
    for intent in intents:
        for attr in token_attrs:
            val = getattr(intent, attr, None)
            if val and isinstance(val, str):
                tokens.add(val)

    if not tokens:
        return

    # Resolve addresses to symbols so market.price() can look them up.
    # market.price() expects symbols (e.g. "ALMANAK"), not addresses.
    chain = getattr(market, "_chain", None) or getattr(market, "chain", None)
    address_to_symbol: dict[str, str] = {}
    if chain:
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            for token in tokens:
                if token.startswith("0x") and len(token) == 42:
                    try:
                        resolved = resolver.resolve(token, chain, log_errors=False, skip_gateway=True)
                        address_to_symbol[token] = resolved.symbol
                    except Exception as e:
                        logger.debug(f"Could not resolve teardown token address {token} to symbol: {e}")
        except Exception as e:
            logger.debug(f"Token resolver unavailable for teardown prefetch: {e}")

    fetched = []
    for token in sorted(tokens):
        # Try the symbol if we resolved the address, otherwise try the raw value
        symbol = address_to_symbol.get(token, token)
        try:
            market.price(symbol)
            fetched.append(symbol)
        except Exception:
            # If symbol lookup failed and we have the original address, try that too
            if symbol != token:
                try:
                    market.price(token)
                    fetched.append(token)
                except Exception:
                    logger.debug(f"Could not pre-fetch price for teardown token {token} (symbol={symbol})")
            else:
                logger.debug(f"Could not pre-fetch price for teardown token {token}")

    if fetched:
        logger.info(f"Pre-fetched {len(fetched)} teardown prices: {fetched}")


def get_fallback_teardown_prices(market: Any) -> dict[str, Decimal] | None:
    """Build a minimal fallback price oracle when the market snapshot has no cached prices.

    This prevents the compiler from using $1 placeholder prices for ALL tokens
    on mainnet, which causes wildly wrong slippage calculations and silent
    compilation failures (None action bundles).

    Returns a dict with at least stablecoin prices, or None if nothing can be
    determined.
    """
    # Start with stablecoin fallbacks (always ~$1, safe to assume)
    fallback: dict[str, Decimal] = {
        "USDC": Decimal("1"),
        "USDT": Decimal("1"),
        "DAI": Decimal("1"),
        "USDC.e": Decimal("1"),
        "USDbC": Decimal("1"),
    }

    # Derive native + wrapped token symbols from existing registry maps
    # so new chains are picked up automatically without code changes here.
    from almanak.framework.data.models import _NATIVE_TO_WRAPPED
    from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS

    chain = getattr(market, "_chain", None) or getattr(market, "chain", None)
    native = NATIVE_TOKEN_SYMBOLS.get(str(chain).lower(), "ETH") if chain else "ETH"
    wrapped = _NATIVE_TO_WRAPPED.get(native, f"W{native}")
    tokens_to_fetch = (native, wrapped, "WBTC")

    # Try to get real prices from the market one more time — the gateway
    # may have recovered since the prefetch attempt.
    if market is not None and hasattr(market, "price"):
        for symbol in tokens_to_fetch:
            try:
                price = market.price(symbol)
                if price and price > 0:
                    fallback[symbol] = price
            except Exception as exc:
                logger.warning("Could not fetch fallback teardown price for %s: %s", symbol, exc)

    # If we only have stablecoins, still return — it's better than $1 for everything
    return fallback if fallback else None


def inject_simulated_balances(runner: Any, market: Any, strategy: Any) -> None:
    """Inject simulated_balances from strategy config into the market snapshot.

    Called in dry-run mode (VIB-2329). When --dry-run --no-gateway is active,
    balance providers return 0 or error for chains where the wallet has no
    on-chain positions. simulated_balances in config.json lets strategy authors
    test logic without needing real funds on every chain.

    Injection is skipped when the market snapshot already has a real balance
    provider (gateway is active). This prevents simulated balances from
    silently overriding real on-chain data in normal dry-run simulations.

    Config format (config.json):
        {
            "simulated_balances": {
                "USDC": "10000",
                "WETH": "5"
            }
        }

    For MultiChainMarketSnapshot, balances are injected into every configured chain.

    balance_usd is computed by attempting market.price() lookup.  For tokens
    where the price is unavailable, balance_usd defaults to 0 (safe fallback —
    the strategy still sees a non-zero balance and can pass balance gates).
    """
    from decimal import InvalidOperation

    from almanak.framework.strategies.intent_strategy import MultiChainMarketSnapshot, TokenBalance

    # Skip injection when a real balance provider is active. MarketSnapshot.balance()
    # prefers pre-populated balances over the provider, so injecting with a live
    # gateway would silently override real on-chain data.
    if getattr(market, "_balance_provider", None) is not None:
        return

    simulated: dict | None = None
    try:
        simulated = strategy.get_config("simulated_balances")
    except AttributeError:
        # Strategy does not implement get_config — skip silently.
        return

    if not simulated or not isinstance(simulated, dict):
        if simulated is not None and not isinstance(simulated, dict):
            logger.warning("[dry-run] simulated_balances must be a dict, got %s — skipping", type(simulated).__name__)
        return

    is_multi_chain = isinstance(market, MultiChainMarketSnapshot)

    injected: list[str] = []
    for token, raw_amount in simulated.items():
        try:
            amount = Decimal(str(raw_amount))
        except InvalidOperation:
            logger.warning(f"[dry-run] simulated_balances: invalid amount for {token}: {raw_amount!r}")
            continue

        if not amount.is_finite() or amount <= 0:
            logger.warning(
                f"[dry-run] simulated_balances: amount must be a positive finite number for {token}: {raw_amount!r}"
            )
            continue

        tb = TokenBalance(symbol=token, balance=amount, balance_usd=Decimal("0"))
        try:
            if is_multi_chain:
                # MultiChainMarketSnapshot.set_balance and .price() both require an
                # explicit chain argument — inject and price each chain separately.
                for chain in market.chains:
                    balance_usd = Decimal("0")
                    try:
                        price = market.price(token, chain=chain)
                        balance_usd = amount * Decimal(str(price))
                    except Exception:
                        pass
                    chain_tb = TokenBalance(symbol=token, balance=amount, balance_usd=balance_usd)
                    market.set_balance(token, chain, chain_tb)
            else:
                # Best-effort USD valuation using the live price oracle.
                # Silently falls back to 0 if price is unavailable (strategy still
                # sees a non-zero balance, which is all that matters for gate checks).
                try:
                    price = market.price(token)
                    tb = TokenBalance(symbol=token, balance=amount, balance_usd=amount * Decimal(str(price)))
                except Exception:
                    pass
                market.set_balance(token, tb)
            injected.append(f"{token}={amount}")
        except Exception as e:
            logger.warning(f"[dry-run] simulated_balances: could not set {token}: {e}")

    if injected:
        logger.info(f"[dry-run] Injected simulated balances: {', '.join(injected)}")


def bridge_token_resolution_candidates(
    token_symbol: str | None,
    bridge_status: dict[str, Any],
) -> list[str]:
    """Collect token identifiers for bridge amount normalization."""
    candidates: list[str] = []
    keys = (
        "destination_token_address",
        "destinationTokenAddress",
        "token_address",
        "tokenAddress",
        "destination_token",
        "destinationToken",
        "token",
        "token_symbol",
    )

    def _append_candidate(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())

    for key in keys:
        _append_candidate(bridge_status.get(key))

    route_data = bridge_status.get("route_data")
    if isinstance(route_data, dict):
        for key in keys:
            _append_candidate(route_data.get(key))

    if token_symbol:
        candidates.append(token_symbol)

    # Preserve first-seen ordering while de-duplicating
    seen: set[str] = set()
    deduped: list[str] = []
    for candidate in candidates:
        candidate_key = candidate.lower()
        if candidate_key not in seen:
            seen.add(candidate_key)
            deduped.append(candidate)
    return deduped


def normalize_bridge_balance_increase(
    balance_increase_wei: int | str,
    destination_chain: str,
    token_symbol: str | None,
    bridge_status: dict[str, Any],
) -> tuple[Decimal | None, dict[str, Any]]:
    """Normalize bridge completion balance increase from wei to token units.

    Returns:
        (normalized_amount, metadata). If normalization fails, returns
        (None, metadata) with raw wei preserved for diagnostics.
    """
    try:
        raw_wei = int(balance_increase_wei)
    except (TypeError, ValueError):
        return None, {
            "raw_wei": balance_increase_wei,
            "destination_chain": destination_chain,
            "token_symbol": token_symbol,
            "error": "invalid_balance_increase_wei",
        }

    from ..data.tokens import get_token_resolver
    from ..data.tokens.exceptions import TokenNotFoundError

    resolver = get_token_resolver()
    candidates = bridge_token_resolution_candidates(token_symbol, bridge_status)
    for candidate in candidates:
        try:
            resolved = resolver.resolve(candidate, destination_chain)
            decimals = resolved.decimals
            normalized = Decimal(raw_wei) / Decimal(10**decimals)
            return normalized, {
                "raw_wei": raw_wei,
                "destination_chain": destination_chain,
                "token_symbol": token_symbol,
                "resolved_from": candidate,
                "resolved_address": resolved.address,
                "decimals": decimals,
            }
        except Exception:
            continue

    unresolved = token_symbol or (candidates[0] if candidates else "<unknown-token>")
    raise TokenNotFoundError(
        token=unresolved,
        chain=destination_chain,
        reason=(f"Unable to resolve token decimals for bridge balance normalization (candidates={candidates})"),
    )
