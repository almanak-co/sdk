"""Regression tests for ``_bridge_wait_verify_source_tx`` pre-check + narrow retry.

See issue #1666 (follow-up to PR #1653 / issue #1647).

Background
==========
PR #1653 added a fail-loud guard when ``state.gateway_client`` is ``None``
(enforces the gateway-only architecture described in
``docs/internal/blueprints/20-gateway-security-architecture.md``). That closed one hole.
This test module closes a second hole identified in #1666: if the gateway
client is non-``None`` but *miswired* (wrong stub bound, ``execution``
attribute missing, ``GetTransactionStatus`` signature wrong), the previous
bare ``except Exception`` inside the 30-attempt retry loop would swallow
the resulting ``AttributeError`` / ``TypeError`` on every attempt and
surface the defect as a 60-second timeout instead of an immediate loud
failure.

The fix has four parts:

1. Pre-validate the gateway client *shape* BEFORE entering the retry loop
   (``execution`` attribute exists; ``GetTransactionStatus`` is callable).
   On mismatch, raise ``RuntimeError`` with a descriptive message instead
   of log-and-continue.
2. Narrow the per-attempt ``except`` inside the retry loop from
   ``Exception`` to ``grpc.RpcError`` so only transient transport errors
   are retried - config defects propagate immediately.
3. PR #1676 review feedback (Gemini): narrow the retry further to only
   the TRANSIENT gRPC status codes (UNAVAILABLE, DEADLINE_EXCEEDED,
   RESOURCE_EXHAUSTED, ABORTED, INTERNAL, UNKNOWN). Permanent codes
   (UNAUTHENTICATED, PERMISSION_DENIED, INVALID_ARGUMENT, UNIMPLEMENTED,
   ...) propagate on the FIRST attempt instead of waiting 60 seconds.
4. PR #1676 review feedback (Codex P1): after narrowing, any config-defect
   exception that escapes ``_bridge_wait_cross_chain`` is POST-SUBMISSION
   (``orchestrator.execute`` has already broadcast the source TX). Letting
   it escape means ``_bridge_wait_finalize`` never runs and
   ``progress.failed_at_step_index`` is never persisted, so the next
   iteration would have no failure marker and could re-execute the same
   cross-chain step. ``_bridge_wait_process_intent`` now catches such
   exceptions and materialises them into bridge failure state.

These tests exercise all regression vectors from (1)-(4).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.strategy_runner import (
    BridgeWaitState,
    RunnerConfig,
    StrategyRunner,
)

# =============================================================================
# Helpers
# =============================================================================


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _make_state(*, gateway_client: object) -> BridgeWaitState:
    strategy = MagicMock()
    strategy.deployment_id = "test-strategy"
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
    state = BridgeWaitState(
        strategy=strategy,
        intents=[intent],
        orchestrator=MagicMock(),
        start_time=datetime.now(UTC),
        deployment_id="test-strategy",
        first_intent=intent,
    )
    state.gateway_client = gateway_client
    return state


# =============================================================================
# 1. Miswired client: precheck must raise RuntimeError on the FIRST call
# =============================================================================


class TestMiswiredGatewayClient:
    """Precheck must fail loud BEFORE the 30-attempt retry loop starts."""

    @pytest.mark.asyncio
    async def test_missing_execution_attribute_raises_immediately(self) -> None:
        """If ``gateway_client.execution`` is missing, raise at once."""
        runner = _make_runner()

        # Plain object - has no ``execution`` attribute at all.
        class NoExecution:
            pass

        state = _make_state(gateway_client=NoExecution())

        start = time.monotonic()
        with pytest.raises(RuntimeError, match="miswired"):
            await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        elapsed = time.monotonic() - start

        # Must fail immediately (not after 30 retries * 2s sleep = 60s).
        # Allow generous headroom for slow CI while still asserting the
        # retry loop never ran (a single iteration would already exceed 2s).
        assert elapsed < 1.0, f"Precheck should fail immediately, but took {elapsed:.2f}s - retry loop likely ran."

    @pytest.mark.asyncio
    async def test_missing_get_transaction_status_raises_immediately(self) -> None:
        """If ``execution.GetTransactionStatus`` is missing, raise at once."""
        runner = _make_runner()

        # Mock with spec so it has no ``GetTransactionStatus`` attribute.
        class EmptyExecution:
            pass

        gw = MagicMock()
        gw.execution = EmptyExecution()
        state = _make_state(gateway_client=gw)

        start = time.monotonic()
        with pytest.raises(RuntimeError, match="GetTransactionStatus"):
            await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        elapsed = time.monotonic() - start

        assert elapsed < 1.0, f"Precheck should fail immediately, but took {elapsed:.2f}s - retry loop likely ran."

    @pytest.mark.asyncio
    async def test_get_transaction_status_not_callable_raises_immediately(self) -> None:
        """If ``GetTransactionStatus`` is not callable, raise at once."""
        runner = _make_runner()

        # Attribute exists but is a non-callable value.
        class BrokenExecution:
            GetTransactionStatus = "not a function"

        gw = MagicMock()
        gw.execution = BrokenExecution()
        state = _make_state(gateway_client=gw)

        start = time.monotonic()
        with pytest.raises(RuntimeError, match="GetTransactionStatus"):
            await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        elapsed = time.monotonic() - start

        assert elapsed < 1.0, f"Precheck should fail immediately, but took {elapsed:.2f}s - retry loop likely ran."


# =============================================================================
# 2. Transient gRPC errors: retry loop must tolerate them
# =============================================================================


class TestTransientRpcRetry:
    """``grpc.RpcError`` is the ONE exception class that should be swallowed."""

    @pytest.mark.asyncio
    async def test_retries_on_grpc_rpc_error_then_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Transient RpcError should be caught; loop retries and eventually succeeds."""
        runner = _make_runner()

        # Patch asyncio.sleep so the test doesn't actually wait 2s per attempt.
        async def _fast_sleep(_seconds: float) -> None:
            return None

        monkeypatch.setattr("almanak.framework.runner.strategy_runner.asyncio.sleep", _fast_sleep)

        call_count = {"n": 0}

        class FlakyRpcError(grpc.RpcError):
            pass

        def get_transaction_status(_request, timeout=None):  # noqa: ARG001
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise FlakyRpcError("transient gRPC failure")
            return MagicMock(status="confirmed", block_number=123456)

        gw = MagicMock()
        gw.execution.GetTransactionStatus = get_transaction_status
        state = _make_state(gateway_client=gw)

        result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)

        assert result is True
        assert call_count["n"] == 3, "Should have retried 2 transient failures and succeeded on the 3rd"
        assert state.failed_step is None
        assert state.error_message is None


# =============================================================================
# 3. Non-transient errors: must propagate, NOT be swallowed 30 times
# =============================================================================


class TestNonTransientErrorsPropagate:
    """Non-RpcError exceptions (AttributeError, TypeError) must escape the loop."""

    @pytest.mark.asyncio
    async def test_attribute_error_propagates_on_first_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An ``AttributeError`` from the stub must not be swallowed."""
        runner = _make_runner()

        # Patch asyncio.sleep so a regression (silent retry) doesn't hang the
        # test for 60s: instead it hits 30 fast iterations.
        async def _fast_sleep(_seconds: float) -> None:
            return None

        monkeypatch.setattr("almanak.framework.runner.strategy_runner.asyncio.sleep", _fast_sleep)

        call_count = {"n": 0}

        def broken_stub(_request, timeout=None):  # noqa: ARG001
            call_count["n"] += 1
            raise AttributeError("no attribute 'status' on response")

        gw = MagicMock()
        gw.execution.GetTransactionStatus = broken_stub
        state = _make_state(gateway_client=gw)

        with pytest.raises(AttributeError, match="no attribute 'status'"):
            await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)

        # Must NOT have been retried 30 times - a config defect should
        # surface on the very first attempt.
        assert call_count["n"] == 1, (
            f"AttributeError should propagate on first attempt, "
            f"but stub was called {call_count['n']} times (silent retry regression)."
        )

    @pytest.mark.asyncio
    async def test_type_error_propagates_on_first_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A ``TypeError`` (wrong signature) must not be swallowed either."""
        runner = _make_runner()

        async def _fast_sleep(_seconds: float) -> None:
            return None

        monkeypatch.setattr("almanak.framework.runner.strategy_runner.asyncio.sleep", _fast_sleep)

        call_count = {"n": 0}

        def wrong_signature_stub(_request, timeout=None):  # noqa: ARG001
            call_count["n"] += 1
            raise TypeError("GetTransactionStatus() got unexpected keyword 'timeout'")

        gw = MagicMock()
        gw.execution.GetTransactionStatus = wrong_signature_stub
        state = _make_state(gateway_client=gw)

        with pytest.raises(TypeError, match="unexpected keyword"):
            await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)

        assert call_count["n"] == 1, (
            f"TypeError should propagate on first attempt, "
            f"but stub was called {call_count['n']} times (silent retry regression)."
        )


# =============================================================================
# 4. Permanent gRPC status codes: must propagate, NOT be retried
# =============================================================================


class TestPermanentGrpcCodesPropagate:
    """Permanent gRPC codes (auth/config defects) must propagate immediately.

    Added in response to PR #1676 review feedback: the original narrow-to-
    ``grpc.RpcError`` catch still retried permanent codes like
    ``UNAUTHENTICATED`` or ``PERMISSION_DENIED`` for the full 60-second budget.
    Those are config / auth defects and should surface on the first attempt.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code",
        [
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.UNIMPLEMENTED,
            grpc.StatusCode.FAILED_PRECONDITION,
            grpc.StatusCode.NOT_FOUND,
        ],
    )
    async def test_permanent_code_fails_on_first_attempt(
        self, status_code: grpc.StatusCode, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Permanent gRPC codes must NOT retry 30 times.

        The inner retry loop re-raises them; the outer ``except grpc.RpcError``
        in ``_bridge_wait_verify_source_tx`` materialises them into
        ``failed_step`` / ``error_message`` and returns False. Either way, the
        stub must only be called ONCE -- never 30 times.
        """
        runner = _make_runner()

        async def _fast_sleep(_seconds: float) -> None:
            return None

        monkeypatch.setattr("almanak.framework.runner.strategy_runner.asyncio.sleep", _fast_sleep)

        call_count = {"n": 0}

        class CodedRpcError(grpc.RpcError):
            def __init__(self, code: grpc.StatusCode) -> None:
                super().__init__(f"permanent: {code}")
                self._code = code

            def code(self) -> grpc.StatusCode:
                return self._code

        def permanent_stub(_request, timeout=None):  # noqa: ARG001
            call_count["n"] += 1
            raise CodedRpcError(status_code)

        gw = MagicMock()
        gw.execution.GetTransactionStatus = permanent_stub
        state = _make_state(gateway_client=gw)

        result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)

        assert result is False
        assert state.failed_step == "step-1"
        assert state.error_message is not None
        assert call_count["n"] == 1, (
            f"Permanent gRPC code {status_code} must fail on first attempt, "
            f"but stub was called {call_count['n']} times (silent retry regression)."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code",
        [
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.ABORTED,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.UNKNOWN,
        ],
    )
    async def test_transient_coded_error_is_retried(
        self, status_code: grpc.StatusCode, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Transient codes must still be retried until success or the 30-attempt budget."""
        runner = _make_runner()

        async def _fast_sleep(_seconds: float) -> None:
            return None

        monkeypatch.setattr("almanak.framework.runner.strategy_runner.asyncio.sleep", _fast_sleep)

        call_count = {"n": 0}

        class CodedRpcError(grpc.RpcError):
            def __init__(self, code: grpc.StatusCode) -> None:
                super().__init__(f"transient: {code}")
                self._code = code

            def code(self) -> grpc.StatusCode:
                return self._code

        def flaky_stub(_request, timeout=None):  # noqa: ARG001
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise CodedRpcError(status_code)
            return MagicMock(status="confirmed", block_number=1)

        gw = MagicMock()
        gw.execution.GetTransactionStatus = flaky_stub
        state = _make_state(gateway_client=gw)

        result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        assert result is True
        assert call_count["n"] == 3, f"Transient code {status_code} should have been retried."


# =============================================================================
# 5. Proto symbol validation: missing TxStatusRequest -> RuntimeError
# =============================================================================


class TestProtoSymbolValidation:
    """Precheck must fail loud if gateway_pb2.TxStatusRequest is missing.

    Hardens the fail-fast path beyond "module imports": if the proto module
    loads but the expected message class was renamed or removed, we want a
    RuntimeError at the precheck, not a raw AttributeError on first poll.
    """

    @pytest.mark.asyncio
    async def test_missing_tx_status_request_raises_runtime_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runner = _make_runner()
        gw = MagicMock()
        # Execution stub looks well-formed (attribute exists, callable) so we
        # reach the proto-symbol check.
        gw.execution.GetTransactionStatus = MagicMock()
        state = _make_state(gateway_client=gw)

        # Stub out gateway_pb2 so that ``TxStatusRequest`` is missing.
        import almanak.gateway.proto.gateway_pb2 as real_pb2

        class _StubPb2:
            pass

        monkeypatch.setattr("almanak.gateway.proto.gateway_pb2", _StubPb2())
        try:
            start = time.monotonic()
            with pytest.raises(RuntimeError, match="TxStatusRequest"):
                await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
            elapsed = time.monotonic() - start
            assert elapsed < 1.0, f"Precheck should fail immediately, but took {elapsed:.2f}s - retry loop likely ran."
        finally:
            # Restore to avoid cross-test pollution.
            monkeypatch.setattr("almanak.gateway.proto.gateway_pb2", real_pb2)


# =============================================================================
# 6. Outer loop materializes unexpected exceptions into bridge failure state
# =============================================================================


class TestBridgeWaitProcessIntentMaterializesPostSubmissionDefects:
    """Post-submission config-defect exceptions must be materialized.

    If ``_bridge_wait_cross_chain`` raises a config-defect exception AFTER
    the source TX has been submitted (e.g. RuntimeError from the gateway
    precheck, permanent gRPC code re-raised from the verify loop), the
    exception must be caught in ``_bridge_wait_process_intent`` and
    converted into bridge failure state so ``_bridge_wait_finalize`` runs
    and ``progress.failed_at_step_index`` is persisted.

    Without this, the next iteration would have no persisted failure marker
    and could re-decide / re-execute the same cross-chain step, risking
    duplicate source-TX submissions. See PR #1676 review feedback.

    Pre-submission failures (e.g. the ``state.gateway_client is None`` guard
    BEFORE ``orchestrator.execute`` is called) still propagate as
    ``RuntimeError`` because nothing has been broadcast on-chain yet --
    that behaviour is covered by
    ``tests/unit/runner/test_bridge_intent_destination_fields.py``.
    """

    @pytest.mark.asyncio
    async def test_runtime_error_after_submission_materializes_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unittest.mock import AsyncMock, patch

        runner = _make_runner()
        # ``_bridge_wait_process_intent`` is the caller that wraps
        # ``_bridge_wait_cross_chain`` -- assemble a minimal state that
        # makes it run through the cross-chain branch.
        strategy = MagicMock()
        strategy.deployment_id = "test-strategy"
        strategy.on_intent_executed = MagicMock()
        strategy.save_state = MagicMock()

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))

        from types import SimpleNamespace

        orchestrator = MagicMock()
        orchestrator.primary_chain = "arbitrum"
        success_tx_result = SimpleNamespace(
            tx_hash="0xdeadbeef",
            actual_amount_received=Decimal("1"),
        )
        exec_result = SimpleNamespace(success=True, error=None, tx_result=success_tx_result)
        orchestrator.execute = AsyncMock(return_value=exec_result)

        state = _make_state(gateway_client=MagicMock())
        state.strategy = strategy
        state.intents = [intent]
        state.orchestrator = orchestrator
        state.price_map = None
        state.price_oracle = None
        state.start_step_index = 0
        state.successful_count = 0
        state.progress = MagicMock()
        state.progress.completed_step_index = -1

        async def _raise_runtime_error(*_args, **_kwargs):
            raise RuntimeError("Gateway client is miswired: missing ``execution`` attribute.")

        with (
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_bridge_wait_cross_chain", side_effect=_raise_runtime_error),
            patch(
                "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
                return_value=True,
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_chain",
                return_value="optimism",
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_token",
                return_value="USDC",
            ),
        ):
            should_break = await runner._bridge_wait_process_intent(state, 0)

        # Exception was caught -> helper returns True to break outer loop
        assert should_break is True
        # Failure state is materialized for _bridge_wait_finalize. The
        # ``-bridge`` suffix signals to _bridge_wait_build_failed_result that
        # the source tx broadcast succeeded and only the wait/verify failed,
        # so revert diagnostics are skipped and the BRIDGE FAILURE banner
        # is logged. See PR #1676 review feedback.
        assert state.failed_step == "step-1-bridge"
        assert state.error_message is not None
        assert "miswired" in state.error_message
        assert state.failed_result is exec_result
        # The post-submission error is also propagated onto the result so
        # downstream consumers see the real failure instead of an empty
        # ``result.error`` (the exec_result was originally success=True).
        assert exec_result.error is not None
        assert "miswired" in exec_result.error


# =============================================================================
# 7. Proto import failure: ImportError must be converted to RuntimeError
# =============================================================================


class TestProtoImportFailure:
    """Precheck must wrap the ``gateway_pb2`` import in the RuntimeError contract.

    If the proto module is missing/renamed, the bare ``from almanak.gateway.proto
    import gateway_pb2`` would otherwise raise a raw ``ImportError`` and bypass
    the fail-fast ``RuntimeError`` contract this precheck enforces. See PR #1676
    follow-up CodeRabbit review: comment id 3116784083.
    """

    @pytest.mark.asyncio
    async def test_proto_import_error_is_converted_to_runtime_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A missing proto module must surface as RuntimeError (not ImportError)."""
        import builtins

        runner = _make_runner()
        gw = MagicMock()
        # Execution stub looks well-formed so we reach the proto import.
        gw.execution.GetTransactionStatus = MagicMock()
        state = _make_state(gateway_client=gw)

        # Force the ``from almanak.gateway.proto import gateway_pb2`` line to
        # raise ImportError by intercepting the import machinery. We only
        # intercept the exact target module - every other import must behave
        # normally (otherwise we'd break unrelated imports triggered by the
        # call path).
        real_import = builtins.__import__

        def _raising_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "almanak.gateway.proto" and fromlist and "gateway_pb2" in fromlist:
                raise ImportError("simulated missing gateway_pb2 module")
            return real_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", _raising_import)

        start = time.monotonic()
        with pytest.raises(RuntimeError, match="gateway_pb2") as exc_info:
            await runner._bridge_wait_verify_source_tx(
                state, tx_hash="0xabc", chain="arbitrum", step_num=1
            )
        elapsed = time.monotonic() - start

        # Original ImportError is preserved via ``raise ... from exc``.
        assert isinstance(exc_info.value.__cause__, ImportError)
        assert "simulated missing gateway_pb2 module" in str(exc_info.value.__cause__)

        # Must fail immediately (not after 30 retries * 2s sleep = 60s).
        assert elapsed < 1.0, (
            f"Precheck should fail immediately, but took {elapsed:.2f}s - retry loop likely ran."
        )
