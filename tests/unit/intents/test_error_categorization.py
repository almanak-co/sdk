"""Tests for VIB-327 + VIB-2096: Fail-fast for non-retriable compilation errors.

Validates that the IntentStateMachine correctly categorizes errors
and that the StrategyRunner aborts retries for permanent errors
(like 'not supported' compilation failures) instead of retrying 3 times.

VIB-2096: Extended to cover 'unknown router' and similar deterministic
compilation failures that previously retried 3 times with exponential backoff.
"""

from unittest.mock import MagicMock

from almanak.framework.intents.state_machine import (
    IntentStateMachine,
    RetryConfig,
    SadflowActionType,
    SadflowContext,
    StateMachineConfig,
)
from almanak.framework.intents.vocabulary import IntentType


# ---------------------------------------------------------------------------
# Error categorization tests
# ---------------------------------------------------------------------------


class TestErrorCategorization:
    """Test _categorize_error classifies error messages correctly."""

    def _make_state_machine(self) -> IntentStateMachine:
        """Create a minimal IntentStateMachine for testing error categorization."""
        intent = MagicMock()
        intent.intent_type = IntentType.SWAP
        intent.intent_id = "test-intent"
        compiler = MagicMock()
        return IntentStateMachine(intent=intent, compiler=compiler)

    # Permanent errors (COMPILATION_PERMANENT)

    def test_not_supported_on_chain(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Aerodrome not supported on optimism") == "COMPILATION_PERMANENT"

    def test_not_supported_generic(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Protocol not supported") == "COMPILATION_PERMANENT"

    def test_unsupported_chain(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Unsupported chain: polygon") == "COMPILATION_PERMANENT"

    def test_unsupported_action_type(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Unsupported action type LP_CLOSE for protocol") == "COMPILATION_PERMANENT"

    def test_not_available(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Feature not available on this chain") == "COMPILATION_PERMANENT"

    def test_compilation_error_not_supported(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Compilation error: Aerodrome not supported on optimism") == "COMPILATION_PERMANENT"

    # Position/perp-related permanent errors

    def test_no_existing_position(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("No size specified and no existing position found") == "COMPILATION_PERMANENT"

    def test_no_position_found(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("No position found for user in market") == "COMPILATION_PERMANENT"

    def test_no_size_specified(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("No size specified for close order") == "COMPILATION_PERMANENT"

    # Transient errors (should NOT be COMPILATION_PERMANENT)

    def test_timeout_is_transient(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Connection timed out") == "TIMEOUT"

    def test_rate_limit_is_transient(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Rate limit exceeded") == "RATE_LIMIT"

    def test_network_error_is_transient(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Network connection refused") == "NETWORK_ERROR"

    def test_revert_is_not_permanent(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Transaction reverted") == "REVERT"

    def test_nonce_error(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Nonce too low") == "NONCE_ERROR"

    def test_insufficient_funds(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Insufficient funds for transfer") == "INSUFFICIENT_FUNDS"

    def test_gas_error(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Gas price too high") == "GAS_ERROR"

    def test_slippage_error(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Slippage tolerance exceeded") == "SLIPPAGE"

    # VIB-2096: Additional deterministic compilation failures

    def test_unknown_router(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Unknown router for protocol pancakeswap_v3 on optimism") == "COMPILATION_PERMANENT"

    def test_unknown_protocol(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Unknown protocol: sushiswap_v4") == "COMPILATION_PERMANENT"

    def test_no_router_configured(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("No router configured for chain polygon") == "COMPILATION_PERMANENT"

    def test_no_adapter_found(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("No adapter found for protocol xyz") == "COMPILATION_PERMANENT"

    def test_protocol_not_available(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Protocol not available on this chain") == "COMPILATION_PERMANENT"

    def test_unknown_market(self):
        # Morpho Blue on a chain where the contract isn't deployed reports
        # "Unknown market" — deterministic, should fail fast (no retries).
        sm = self._make_state_machine()
        assert sm._categorize_error("Unknown market: 0xabc") == "COMPILATION_PERMANENT"

    def test_not_deployed(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("PancakeSwap V3 not deployed on optimism") == "COMPILATION_PERMANENT"

    # VIB-3141: CLOB 4xx fatal rejections (Polymarket and similar order books)

    def test_clob_breaks_minimum_tick_size(self):
        # Polymarket CLOB rejects orders with off-tick prices. Deterministic.
        sm = self._make_state_machine()
        assert sm._categorize_error(
            "CLOB 400: order 0xabc breaks minimum tick size rule: 0.001"
        ) == "COMPILATION_PERMANENT"

    def test_clob_minimum_order_value(self):
        sm = self._make_state_machine()
        assert sm._categorize_error(
            "CLOB 400: order below minimum order value"
        ) == "COMPILATION_PERMANENT"

    def test_clob_invalid_order(self):
        sm = self._make_state_machine()
        assert sm._categorize_error(
            "CLOB 400 INVALID_ORDER: validation failed"
        ) == "COMPILATION_PERMANENT"

    def test_clob_invalid_tick(self):
        sm = self._make_state_machine()
        assert sm._categorize_error(
            "CLOB 400 INVALID_TICK: price not on tick grid"
        ) == "COMPILATION_PERMANENT"

    def test_clob_order_below_minimum(self):
        sm = self._make_state_machine()
        assert sm._categorize_error(
            "CLOB 400 ORDER_BELOW_MINIMUM: size too small"
        ) == "COMPILATION_PERMANENT"

    # VIB-2866: deterministic market/pool/Drift validation strings

    def test_market_not_found_is_permanent(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Market not found: weth-perp") == "COMPILATION_PERMANENT"

    def test_invalid_market_is_permanent(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Invalid market index: 999") == "COMPILATION_PERMANENT"

    def test_market_does_not_exist_is_permanent(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Market does not exist on this chain") == "COMPILATION_PERMANENT"

    def test_pool_not_found_is_permanent(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Pool not found for token pair USDC/WETH") == "COMPILATION_PERMANENT"

    def test_invalid_pool_is_permanent(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Invalid pool address 0x0000") == "COMPILATION_PERMANENT"

    def test_drift_no_user_account_is_permanent(self):
        # DriftAdapter._get_position_size raises this when PERP_CLOSE is
        # attempted before the wallet has an initialized Drift user PDA.
        sm = self._make_state_machine()
        assert sm._categorize_error(
            "No Drift user account found. Cannot close position."
        ) == "COMPILATION_PERMANENT"

    def test_drift_no_active_position_for_market_is_permanent(self):
        # DriftAdapter._get_position_size raises this when the user PDA
        # exists but the requested market has no open position.
        sm = self._make_state_machine()
        assert sm._categorize_error(
            "No active position found for market index 0"
        ) == "COMPILATION_PERMANENT"

    def test_no_market_alone_does_not_trigger_permanent(self):
        # Regression guard: the bare token ``no market`` was deliberately
        # excluded from the permanent list because transient market-data
        # messages can contain it. Only the unambiguous longer phrases
        # match.
        sm = self._make_state_machine()
        # No matching permanent keyword — falls through to None.
        assert sm._categorize_error("Temporarily no market data available") is None

    def test_clob_5xx_stays_retryable(self):
        """Regression guard: transient CLOB 5xx / timeout must remain retryable (VIB-3141)."""
        sm = self._make_state_machine()
        # 5xx with no fatal substring -> not a permanent error
        # "internal server error" has no matching keyword so returns None
        assert sm._categorize_error("CLOB 500: internal server error") is None
        # Explicit timeout should still categorize as TIMEOUT (retryable)
        assert sm._categorize_error("CLOB request timed out") == "TIMEOUT"
        # Generic network blip
        assert sm._categorize_error("CLOB connection reset") == "NETWORK_ERROR"

    def test_unknown_error_returns_none(self):
        sm = self._make_state_machine()
        assert sm._categorize_error("Something went wrong") is None

    # Edge cases: errors with overlapping keywords

    def test_revert_with_not_supported_is_revert(self):
        """'revert' check comes before 'not supported' — revert takes priority."""
        sm = self._make_state_machine()
        result = sm._categorize_error("Transaction reverted: token not supported")
        assert result == "REVERT"

    def test_timeout_with_not_supported_is_timeout(self):
        """'timeout' check comes before 'not supported' — timeout takes priority."""
        sm = self._make_state_machine()
        result = sm._categorize_error("Connection timed out: feature not supported")
        assert result == "TIMEOUT"


# ---------------------------------------------------------------------------
# Sadflow abort tests (via _on_sadflow_enter in strategy runner)
# ---------------------------------------------------------------------------


class TestNonRetryableAbort:
    """Test that non-retryable errors abort retries via the sadflow hook."""

    def test_compilation_permanent_aborts(self):
        """COMPILATION_PERMANENT errors should abort immediately."""
        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)
        context = MagicMock(spec=SadflowContext)
        context.error_message = "Aerodrome not supported on optimism"

        result = runner._on_sadflow_enter("COMPILATION_PERMANENT", 1, context)

        assert result is not None
        assert result.action_type == SadflowActionType.ABORT
        assert result.reason == "Aerodrome not supported on optimism"

    def test_insufficient_funds_aborts(self):
        """INSUFFICIENT_FUNDS errors should abort immediately (existing behavior)."""
        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)
        context = MagicMock(spec=SadflowContext)
        context.error_message = "Insufficient ETH for gas"

        result = runner._on_sadflow_enter("INSUFFICIENT_FUNDS", 1, context)

        assert result is not None
        assert result.action_type == SadflowActionType.ABORT

    def test_nonce_error_aborts(self):
        """NONCE_ERROR errors should abort immediately (existing behavior)."""
        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)
        context = MagicMock(spec=SadflowContext)
        context.error_message = "Nonce too low"

        result = runner._on_sadflow_enter("NONCE_ERROR", 1, context)

        assert result is not None
        assert result.action_type == SadflowActionType.ABORT

    def test_clob_fatal_aborts(self):
        """VIB-3141: CLOB fatal 4xx errors (categorized as COMPILATION_PERMANENT) abort."""
        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)
        context = MagicMock(spec=SadflowContext)
        context.error_message = "CLOB 400: order 0xabc breaks minimum tick size rule: 0.001"

        result = runner._on_sadflow_enter("COMPILATION_PERMANENT", 1, context)

        assert result is not None
        assert result.action_type == SadflowActionType.ABORT

    def test_transient_errors_do_not_abort(self):
        """Transient errors should return None (allow normal retry)."""
        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)
        context = MagicMock(spec=SadflowContext)
        context.error_message = "Connection timed out"

        result = runner._on_sadflow_enter("TIMEOUT", 1, context)
        assert result is None

    def test_unknown_errors_do_not_abort(self):
        """Unknown error types should return None (allow normal retry)."""
        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)
        context = MagicMock(spec=SadflowContext)
        context.error_message = "Something unexpected"

        result = runner._on_sadflow_enter(None, 1, context)
        assert result is None


# ---------------------------------------------------------------------------
# Integration test: state machine skips retries for permanent errors
# ---------------------------------------------------------------------------


class TestStateMachinePermanentErrorFlow:
    """Test that the state machine + hook integration actually skips retries."""

    def _make_swap_intent(self) -> MagicMock:
        """Create a mock intent with proper IntentType enum."""
        from almanak.framework.intents.vocabulary import IntentType

        intent = MagicMock()
        intent.intent_type = IntentType.SWAP
        intent.intent_id = "test-intent"
        return intent

    def test_not_supported_error_fails_immediately(self):
        """A 'not supported' compilation error should fail without retry."""
        intent = self._make_swap_intent()

        compiler = MagicMock()
        from almanak.framework.intents.compiler import CompilationResult, CompilationStatus

        compiler.compile.return_value = CompilationResult(
            status=CompilationStatus.FAILED,
            error="Aerodrome not supported on optimism",
        )

        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)

        config = StateMachineConfig(
            retry_config=RetryConfig(max_retries=3),
        )
        sm = IntentStateMachine(
            intent=intent,
            compiler=compiler,
            config=config,
            on_sadflow_enter=runner._on_sadflow_enter,
        )

        # Step 1: PREPARING -> compilation fails -> SADFLOW
        result = sm.step()
        assert result.error is not None
        assert "not supported" in result.error

        # Step 2: SADFLOW -> hook aborts -> FAILED (no retry)
        result = sm.step()
        assert result.is_complete is True
        assert result.success is False
        assert sm.retry_count == 0  # No retries attempted

    def test_unknown_router_fails_immediately_without_hook(self):
        """VIB-2096: 'Unknown router' should fail without retry even without sadflow hook."""
        intent = self._make_swap_intent()

        compiler = MagicMock()
        from almanak.framework.intents.compiler import CompilationResult, CompilationStatus

        compiler.compile.return_value = CompilationResult(
            status=CompilationStatus.FAILED,
            error="Unknown router for protocol pancakeswap_v3 on optimism",
        )

        config = StateMachineConfig(
            retry_config=RetryConfig(max_retries=3),
        )
        sm = IntentStateMachine(
            intent=intent,
            compiler=compiler,
            config=config,
            # No on_sadflow_enter hook — the state machine itself should abort
        )

        # Step 1: PREPARING -> compilation fails -> SADFLOW
        result = sm.step()
        assert result.error is not None
        assert "Unknown router" in result.error

        # Step 2: SADFLOW -> built-in fail-fast -> FAILED (no retry)
        result = sm.step()
        assert result.is_complete is True
        assert result.success is False
        assert sm.retry_count == 0  # No retries attempted

    def test_clob_breaks_minimum_tick_fails_immediately(self):
        """VIB-3141: CLOB 'breaks minimum tick size' must fail with 0 retries."""
        intent = self._make_swap_intent()

        compiler = MagicMock()
        from almanak.framework.intents.compiler import CompilationResult, CompilationStatus

        compiler.compile.return_value = CompilationResult(
            status=CompilationStatus.FAILED,
            error="CLOB 400: order 0xabc breaks minimum tick size rule: 0.001",
        )

        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)

        config = StateMachineConfig(
            retry_config=RetryConfig(max_retries=3),
        )
        sm = IntentStateMachine(
            intent=intent,
            compiler=compiler,
            config=config,
            on_sadflow_enter=runner._on_sadflow_enter,
        )

        # Step 1: PREPARING -> compilation fails -> SADFLOW
        result = sm.step()
        assert result.error is not None
        assert "tick size" in result.error

        # Step 2: SADFLOW -> hook aborts -> FAILED (no retry)
        result = sm.step()
        assert result.is_complete is True
        assert result.success is False
        assert sm.retry_count == 0  # CRITICAL: 0 retries for fatal CLOB rejection

    def test_clob_minimum_order_value_fails_immediately(self):
        """VIB-3141: CLOB 'minimum order value' must fail with 0 retries."""
        intent = self._make_swap_intent()

        compiler = MagicMock()
        from almanak.framework.intents.compiler import CompilationResult, CompilationStatus

        compiler.compile.return_value = CompilationResult(
            status=CompilationStatus.FAILED,
            error="CLOB 400: order below minimum order value",
        )

        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)

        config = StateMachineConfig(
            retry_config=RetryConfig(max_retries=3),
        )
        sm = IntentStateMachine(
            intent=intent,
            compiler=compiler,
            config=config,
            on_sadflow_enter=runner._on_sadflow_enter,
        )

        result = sm.step()
        assert result.error is not None
        result = sm.step()
        assert result.is_complete is True
        assert result.success is False
        assert sm.retry_count == 0

    def test_clob_500_still_retries(self):
        """VIB-3141 regression guard: transient CLOB 5xx must stay retryable."""
        intent = self._make_swap_intent()

        compiler = MagicMock()
        from almanak.framework.intents.compiler import CompilationResult, CompilationStatus

        # "timed out" -> TIMEOUT (transient, retryable)
        compiler.compile.side_effect = [
            CompilationResult(status=CompilationStatus.FAILED, error="CLOB request timed out"),
            CompilationResult(status=CompilationStatus.FAILED, error="CLOB request timed out"),
        ]

        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)

        config = StateMachineConfig(
            retry_config=RetryConfig(max_retries=3, initial_delay_seconds=0.0),
        )
        sm = IntentStateMachine(
            intent=intent,
            compiler=compiler,
            config=config,
            on_sadflow_enter=runner._on_sadflow_enter,
        )

        # Step 1: PREPARING -> fails -> SADFLOW
        result = sm.step()
        assert result.error is not None

        # Step 2: SADFLOW -> retry allowed -> back to PREPARING
        result = sm.step()
        assert result.retry_delay is not None
        assert sm.retry_count == 1  # Transient: retry counted
        assert not result.is_complete

    def test_transient_error_does_retry(self):
        """A transient error should trigger retry (existing behavior preserved)."""
        intent = self._make_swap_intent()

        compiler = MagicMock()
        from almanak.framework.intents.compiler import CompilationResult, CompilationStatus

        # First call fails with transient error, second succeeds
        compiler.compile.side_effect = [
            CompilationResult(
                status=CompilationStatus.FAILED,
                error="Connection timed out",
            ),
            CompilationResult(
                status=CompilationStatus.FAILED,
                error="Connection timed out",
            ),
        ]

        from almanak.framework.runner.strategy_runner import StrategyRunner

        runner = StrategyRunner.__new__(StrategyRunner)

        config = StateMachineConfig(
            retry_config=RetryConfig(max_retries=3, initial_delay_seconds=0.0),
        )
        sm = IntentStateMachine(
            intent=intent,
            compiler=compiler,
            config=config,
            on_sadflow_enter=runner._on_sadflow_enter,
        )

        # Step 1: PREPARING -> fails -> SADFLOW
        result = sm.step()
        assert result.error is not None

        # Step 2: SADFLOW -> retry allowed -> back to PREPARING
        result = sm.step()
        assert result.retry_delay is not None  # Has retry delay
        assert sm.retry_count == 1  # First retry counted
        assert not result.is_complete  # Not done yet


# ---------------------------------------------------------------------------
# VIB-493: Improved error message for missing state machine wiring
# ---------------------------------------------------------------------------


class TestMissingStateWiringError:
    """Test that missing state machine wiring produces actionable error messages."""

    def test_unsupported_intent_type_raises_with_name(self):
        """An IntentType without state machine wiring should raise ValueError naming the type."""
        import pytest

        # Create a mock IntentType that won't be in the state machine maps
        mock_intent_type = MagicMock()
        mock_intent_type.name = "FICTIONAL_INTENT"

        intent = MagicMock()
        intent.intent_type = mock_intent_type
        intent.intent_id = "test-intent"
        compiler = MagicMock()

        with pytest.raises(ValueError, match="FICTIONAL_INTENT"):
            IntentStateMachine(intent=intent, compiler=compiler)

    def test_error_message_includes_fix_guidance(self):
        """Error message should tell the developer what to add."""
        import pytest

        mock_intent_type = MagicMock()
        mock_intent_type.name = "MY_NEW_INTENT"

        intent = MagicMock()
        intent.intent_type = mock_intent_type
        intent.intent_id = "test-intent"
        compiler = MagicMock()

        with pytest.raises(ValueError, match="PREPARING_MY_NEW_INTENT.*VALIDATING_MY_NEW_INTENT.*SADFLOW_MY_NEW_INTENT"):
            IntentStateMachine(intent=intent, compiler=compiler)
