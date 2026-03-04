"""Tests for VIB-327: Fail-fast for non-retriable compilation errors.

Validates that the IntentStateMachine correctly categorizes errors
and that the StrategyRunner aborts retries for permanent errors
(like 'not supported' compilation failures) instead of retrying 3 times.
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
