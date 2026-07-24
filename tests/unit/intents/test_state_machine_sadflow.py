"""Branch coverage for ``IntentStateMachine._handle_sadflow``.

Exercises the sadflow lifecycle hooks (``on_sadflow_enter`` / ``on_retry`` /
``on_sadflow_exit``) and the retry bookkeeping that the fail-fast tests in
``test_error_categorization.py`` do not reach:

- on_sadflow_enter returning ABORT / SKIP / MODIFY / RETRY / None / raising
- on_retry returning ABORT / SKIP / MODIFY / custom-delay RETRY / raising
- retry exhaustion (with and without a recorded error message)
- receipt clearing + transition back to PREPARING on retry

These tests document actual behavior; they do not modify production code.
"""

from unittest.mock import MagicMock

from almanak.framework.intents.compiler import CompilationResult, CompilationStatus
from almanak.framework.intents.state_machine import (
    IntentState,
    IntentStateMachine,
    RetryConfig,
    SadflowAction,
    StateMachineConfig,
    TransactionReceipt,
)
from almanak.framework.intents.vocabulary import IntentType

# Categorizes as TIMEOUT — transient, so the retry path is taken.
TRANSIENT_ERROR = "Connection timed out"


def _make_intent() -> MagicMock:
    intent = MagicMock()
    intent.intent_type = IntentType.SWAP
    intent.intent_id = "sadflow-test-intent"
    return intent


def _failing_compiler(error: str | None = TRANSIENT_ERROR) -> MagicMock:
    compiler = MagicMock()
    compiler.compile.return_value = CompilationResult(
        status=CompilationStatus.FAILED,
        error=error,
    )
    return compiler


def _config(max_retries: int = 3) -> StateMachineConfig:
    return StateMachineConfig(
        retry_config=RetryConfig(
            max_retries=max_retries,
            initial_delay_seconds=0.0,
            jitter_factor=0.0,
        ),
    )


def _sm_in_sadflow(
    error: str | None = TRANSIENT_ERROR,
    max_retries: int = 3,
    **hooks,
) -> IntentStateMachine:
    """Build a state machine and step it once so it sits in SADFLOW_SWAP."""
    sm = IntentStateMachine(
        intent=_make_intent(),
        compiler=_failing_compiler(error),
        config=_config(max_retries),
        **hooks,
    )
    sm.step()  # PREPARING -> compile FAILED -> SADFLOW
    assert sm.state == IntentState.SADFLOW_SWAP
    return sm


# ---------------------------------------------------------------------------
# Non-retryable fail-fast (INSUFFICIENT_FUNDS member of _NON_RETRYABLE_TYPES)
# ---------------------------------------------------------------------------


class TestNonRetryableFailFast:
    def test_insufficient_funds_fails_without_retry(self):
        exit_calls = []
        sm = _sm_in_sadflow(
            error="Insufficient funds for transfer",
            on_sadflow_exit=lambda success, attempts: exit_calls.append((success, attempts)),
        )

        result = sm.step()

        assert result.is_complete is True
        assert result.success is False
        assert result.error == "Insufficient funds for transfer"
        assert sm.state == IntentState.FAILED
        assert sm.retry_count == 0
        # Fail-fast marks _in_sadflow before calling the exit hook.
        assert exit_calls == [(False, 1)]


# ---------------------------------------------------------------------------
# on_sadflow_enter hook branches
# ---------------------------------------------------------------------------


class TestSadflowEnterHook:
    def test_enter_hook_abort_uses_hook_reason(self):
        sm = _sm_in_sadflow(
            on_sadflow_enter=lambda error_type, attempt, ctx: SadflowAction.abort(reason="operator said no"),
        )

        result = sm.step()

        assert result.is_complete is True
        assert result.success is False
        assert result.error == "operator said no"
        assert sm.state == IntentState.FAILED
        assert sm.retry_count == 0

    def test_enter_hook_abort_without_reason_falls_back_to_last_error(self):
        sm = _sm_in_sadflow(
            on_sadflow_enter=lambda error_type, attempt, ctx: SadflowAction.abort(),
        )

        result = sm.step()

        assert result.is_complete is True
        assert result.error == TRANSIENT_ERROR

    def test_enter_hook_skip_completes_successfully(self):
        exit_calls = []
        sm = _sm_in_sadflow(
            on_sadflow_enter=lambda error_type, attempt, ctx: SadflowAction.skip(reason="not worth retrying"),
            on_sadflow_exit=lambda success, attempts: exit_calls.append((success, attempts)),
        )

        result = sm.step()

        assert result.is_complete is True
        assert result.success is True
        assert sm.state == IntentState.COMPLETED
        assert sm.retry_count == 0
        assert exit_calls == [(True, 1)]

    def test_enter_hook_modify_swaps_action_bundle_then_retries(self):
        modified = MagicMock()
        sm = _sm_in_sadflow(
            on_sadflow_enter=lambda error_type, attempt, ctx: SadflowAction.modify(modified),
        )

        result = sm.step()

        # MODIFY does not terminate: the machine records the bundle and retries.
        assert result.is_complete is False
        assert result.retry_delay is not None
        assert sm.action_bundle is modified
        assert sm.retry_count == 1
        assert sm.state == IntentState.PREPARING_SWAP

    def test_enter_hook_retry_action_falls_through_to_default_retry(self):
        # A truthy hook action that is neither ABORT/SKIP/MODIFY falls through;
        # its custom_delay is NOT honored (only on_retry controls the delay).
        sm = _sm_in_sadflow(
            on_sadflow_enter=lambda error_type, attempt, ctx: SadflowAction.retry(custom_delay=99.0),
        )

        result = sm.step()

        assert result.is_complete is False
        assert result.retry_delay == 0.0
        assert sm.retry_count == 1

    def test_enter_hook_returning_none_uses_default_retry(self):
        sm = _sm_in_sadflow(on_sadflow_enter=lambda error_type, attempt, ctx: None)

        result = sm.step()

        assert result.is_complete is False
        assert result.retry_delay is not None
        assert sm.retry_count == 1

    def test_enter_hook_exception_is_swallowed_and_retry_proceeds(self):
        def _boom(error_type, attempt, ctx):
            raise RuntimeError("hook exploded")

        sm = _sm_in_sadflow(on_sadflow_enter=_boom)

        result = sm.step()

        assert result.is_complete is False
        assert result.retry_delay is not None
        assert sm.retry_count == 1

    def test_enter_hook_called_once_with_error_type_and_attempt(self):
        enter_calls = []

        def _record(error_type, attempt, ctx):
            enter_calls.append((error_type, attempt, ctx))
            return None

        sm = _sm_in_sadflow(max_retries=3, on_sadflow_enter=_record)

        sm.step()  # SADFLOW -> retry 1 -> PREPARING
        sm.step()  # PREPARING -> compile FAILED again -> SADFLOW
        sm.step()  # SADFLOW (second visit) -> retry 2

        # Hook fires only on the FIRST sadflow entry (_in_sadflow latches).
        assert len(enter_calls) == 1
        error_type, attempt, ctx = enter_calls[0]
        assert error_type == "TIMEOUT"
        assert attempt == 1
        assert ctx.error_message == TRANSIENT_ERROR
        assert ctx.attempt_number == 1
        assert sm.retry_count == 2


# ---------------------------------------------------------------------------
# Retry exhaustion
# ---------------------------------------------------------------------------


class TestRetryExhaustion:
    def test_exhausted_retries_fail_with_last_error(self):
        exit_calls = []
        sm = _sm_in_sadflow(
            max_retries=1,
            on_sadflow_exit=lambda success, attempts: exit_calls.append((success, attempts)),
        )

        result = sm.step()  # SADFLOW -> retry 1 -> PREPARING
        assert result.retry_delay is not None
        sm.step()  # PREPARING -> compile FAILED -> SADFLOW
        result = sm.step()  # SADFLOW -> retries exhausted -> FAILED

        assert result.is_complete is True
        assert result.success is False
        assert result.error == TRANSIENT_ERROR
        assert sm.state == IntentState.FAILED
        assert sm.retry_count == 1
        assert exit_calls == [(False, 2)]

    def test_exhausted_with_no_error_message_uses_fallback(self):
        # Compilation FAILED with error=None: _last_error stays None, so the
        # categorization guard is skipped and the exhaustion fallback fires.
        sm = _sm_in_sadflow(error=None, max_retries=0)

        result = sm.step()

        assert result.is_complete is True
        assert result.success is False
        assert result.error == "Max retries exceeded"
        assert sm.state == IntentState.FAILED


# ---------------------------------------------------------------------------
# on_retry hook branches
# ---------------------------------------------------------------------------


class TestOnRetryHook:
    def test_on_retry_abort(self):
        exit_calls = []
        sm = _sm_in_sadflow(
            on_retry=lambda ctx, default: SadflowAction.abort(reason="give up now"),
            on_sadflow_exit=lambda success, attempts: exit_calls.append((success, attempts)),
        )

        result = sm.step()

        assert result.is_complete is True
        assert result.success is False
        assert result.error == "give up now"
        assert sm.state == IntentState.FAILED
        assert sm.retry_count == 0
        assert exit_calls == [(False, 1)]

    def test_on_retry_abort_without_reason_falls_back_to_last_error(self):
        sm = _sm_in_sadflow(on_retry=lambda ctx, default: SadflowAction.abort())

        result = sm.step()

        assert result.is_complete is True
        assert result.error == TRANSIENT_ERROR

    def test_on_retry_skip_completes_successfully(self):
        exit_calls = []
        sm = _sm_in_sadflow(
            on_retry=lambda ctx, default: SadflowAction.skip(reason="skip it"),
            on_sadflow_exit=lambda success, attempts: exit_calls.append((success, attempts)),
        )

        result = sm.step()

        assert result.is_complete is True
        assert result.success is True
        assert sm.state == IntentState.COMPLETED
        assert exit_calls == [(True, 1)]

    def test_on_retry_modify_without_custom_delay_keeps_computed_delay(self):
        modified = MagicMock()
        sm = _sm_in_sadflow(
            on_retry=lambda ctx, default: SadflowAction.modify(modified),
        )

        result = sm.step()

        # MODIFY carries custom_delay=None, so the backoff-computed delay stands.
        assert result.is_complete is False
        assert result.retry_delay == 0.0
        assert sm.action_bundle is modified
        assert sm.retry_count == 1
        assert sm.state == IntentState.PREPARING_SWAP

    def test_on_retry_custom_delay_overrides_backoff(self):
        sm = _sm_in_sadflow(
            on_retry=lambda ctx, default: SadflowAction.retry(custom_delay=42.5),
        )

        result = sm.step()

        assert result.is_complete is False
        assert result.retry_delay == 42.5
        assert sm.retry_count == 1

    def test_on_retry_receives_context_and_default_action(self):
        seen = []

        def _capture(ctx, default):
            seen.append((ctx, default))
            return default

        sm = _sm_in_sadflow(on_retry=_capture)

        sm.step()

        assert len(seen) == 1
        ctx, default = seen[0]
        assert ctx.error_message == TRANSIENT_ERROR
        assert ctx.error_type == "TIMEOUT"
        assert ctx.attempt_number == 1
        # Default action is a RETRY carrying the backoff-computed delay.
        assert default.custom_delay is not None

    def test_on_retry_exception_falls_back_to_default_retry(self):
        def _boom(ctx, default):
            raise RuntimeError("retry hook exploded")

        sm = _sm_in_sadflow(on_retry=_boom)

        result = sm.step()

        assert result.is_complete is False
        assert result.retry_delay == 0.0
        assert sm.retry_count == 1
        assert sm.state == IntentState.PREPARING_SWAP


# ---------------------------------------------------------------------------
# Retry bookkeeping via the receipt-failure (VALIDATING) route
# ---------------------------------------------------------------------------


class TestRetryBookkeeping:
    def test_receipt_failure_retry_clears_receipt_and_returns_to_preparing(self):
        bundle = MagicMock()
        compiler = MagicMock()
        compiler.compile.return_value = CompilationResult(
            status=CompilationStatus.SUCCESS,
            action_bundle=bundle,
        )
        sm = IntentStateMachine(
            intent=_make_intent(),
            compiler=compiler,
            config=_config(max_retries=3),
        )

        result = sm.step()  # PREPARING -> VALIDATING
        assert result.needs_execution is True
        assert sm.state == IntentState.VALIDATING_SWAP

        sm.set_receipt(TransactionReceipt(success=False, error="Transaction reverted"))
        result = sm.step()  # VALIDATING -> SADFLOW
        assert sm.state == IntentState.SADFLOW_SWAP
        assert result.error == "Transaction reverted"

        result = sm.step()  # SADFLOW -> retry -> PREPARING

        assert result.is_complete is False
        assert result.retry_delay is not None
        assert sm.retry_count == 1
        assert sm.state == IntentState.PREPARING_SWAP
        # The stale receipt must be cleared so the retry re-validates fresh.
        assert sm._receipt is None
