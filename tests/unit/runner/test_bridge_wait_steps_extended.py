"""Extended tests for Phase 3c ``_execute_with_bridge_waiting`` helpers.

Complements ``test_bridge_wait_steps.py`` with additional coverage:
- ``_init_bridge_wait_state`` edge cases (no _config, gateway client wiring)
- ``_bridge_wait_process_intent`` success/same-chain paths
- ``_bridge_wait_cross_chain`` verified tx + poll delegation
- ``_bridge_wait_verify_source_tx`` fallback (no RPC URL, timeout, reverted)
- ``_bridge_wait_poll_completion`` inner branches and callback paths
- ``_bridge_wait_apply_completion`` amount chaining + partial metadata
- ``_bridge_wait_finalize`` / ``_bridge_wait_build_failed_result`` variants
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.runner_models import ExecutionProgress
from almanak.framework.runner.strategy_runner import (
    BridgeWaitState,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)

# =============================================================================
# Helpers
# =============================================================================


def _make_runner(
    *,
    state_manager: MagicMock | None = None,
    balance_provider: MagicMock | None = None,
) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    if state_manager is None:
        state_manager = MagicMock()
    if balance_provider is None:
        balance_provider = MagicMock()
        balance_provider.invalidate_cache = MagicMock()
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        config=config,
    )


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    return strategy


def _make_orchestrator(*, with_config: bool = True) -> MagicMock:
    orch = MagicMock()
    orch.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    orch.primary_chain = "arbitrum"
    if with_config:
        orch._config = SimpleNamespace(rpc_urls={"arbitrum": "https://arb"})
    else:
        orch._config = None
    orch.execute = AsyncMock()
    return orch


def _make_state(
    *,
    intents: list,
    strategy: MagicMock | None = None,
    orchestrator: MagicMock | None = None,
    resume_progress: ExecutionProgress | None = None,
) -> BridgeWaitState:
    strategy = strategy or _make_strategy()
    orchestrator = orchestrator or _make_orchestrator()
    return BridgeWaitState(
        strategy=strategy,
        intents=intents,
        orchestrator=orchestrator,
        start_time=datetime.now(UTC),
        resume_progress=resume_progress,
        strategy_id=strategy.strategy_id,
        first_intent=intents[0] if intents else None,
    )


def _make_progress(
    strategy_id: str = "test-strategy",
    *,
    total_steps: int = 1,
) -> ExecutionProgress:
    return ExecutionProgress(
        execution_id="abcd",
        strategy_id=strategy_id,
        intents_hash="hash",
        total_steps=total_steps,
    )


# =============================================================================
# _init_bridge_wait_state - extended
# =============================================================================


class TestInitBridgeWaitStateExtended:
    @pytest.mark.asyncio
    async def test_orchestrator_without_config_uses_empty_rpc_urls(self) -> None:
        """Gateway-mode orchestrators lack `_config`; rpc_urls resolves to {}."""
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="h1")
        runner._load_execution_progress = AsyncMock(return_value=None)
        runner._save_execution_progress = AsyncMock()
        runner._clear_execution_progress = AsyncMock()
        runner._get_gateway_client = MagicMock(return_value="fake-gw")

        orchestrator = _make_orchestrator(with_config=False)
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], orchestrator=orchestrator)

        with patch(
            "almanak.framework.runner.strategy_runner.EnsoStateProvider",
            return_value=MagicMock(),
        ) as mock_provider:
            await runner._init_bridge_wait_state(state)

        assert state.rpc_urls == {}
        # EnsoStateProvider got the gateway client from _get_gateway_client
        _, kwargs = mock_provider.call_args
        assert kwargs.get("gateway_client") == "fake-gw"

    @pytest.mark.asyncio
    async def test_resume_progress_sets_successful_count_from_start_index(self) -> None:
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="x")
        runner._get_gateway_client = MagicMock(return_value=None)

        resume = _make_progress(total_steps=3)
        resume.completed_step_index = 1  # next = 2
        resume.previous_amount_received = Decimal("0")

        state = _make_state(
            intents=[
                SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1")),
                SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1")),
                SwapIntent(from_token="DAI", to_token="USDC", amount=Decimal("1")),
            ],
            resume_progress=resume,
        )

        with patch(
            "almanak.framework.runner.strategy_runner.EnsoStateProvider",
            return_value=MagicMock(),
        ):
            await runner._init_bridge_wait_state(state)

        assert state.start_step_index == 2
        # successful_count counts already-completed steps
        assert state.successful_count == 2


# =============================================================================
# _bridge_wait_process_intent - extended (success paths)
# =============================================================================


class TestBridgeWaitProcessIntentSuccess:
    @pytest.mark.asyncio
    async def test_same_chain_success_increments_counter_and_saves_progress(self) -> None:
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress(total_steps=1)

        success_result = SimpleNamespace(
            success=True,
            error=None,
            tx_result=SimpleNamespace(tx_hash="0xabc"),
        )
        state.orchestrator.execute.return_value = success_result

        with patch(
            "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
            return_value=False,
        ):
            should_break = await runner._bridge_wait_process_intent(state, 0)

        assert should_break is False
        assert state.successful_count == 1
        # Progress was persisted after the successful step
        runner._save_execution_progress.assert_awaited()
        assert state.progress.completed_step_index == 0
        # Callback fired with success=True
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is True

    @pytest.mark.asyncio
    async def test_resolves_amount_all_from_previous_received(self) -> None:
        """When the intent uses amount='all' and previous_amount_received is set, it's resolved."""
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.previous_amount_received = Decimal("42")

        state.orchestrator.execute.return_value = SimpleNamespace(
            success=True, error=None, tx_result=SimpleNamespace(tx_hash="0xabc")
        )

        with patch(
            "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
            return_value=False,
        ):
            await runner._bridge_wait_process_intent(state, 0)

        # Orchestrator was called with a resolved-amount intent (not 'all')
        called_intent = state.orchestrator.execute.call_args.args[0]
        assert called_intent.amount == Decimal("42")

    @pytest.mark.asyncio
    async def test_strategy_save_state_exception_does_not_abort(self) -> None:
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        strategy = _make_strategy()
        strategy.save_state.side_effect = RuntimeError("state save boom")
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()

        state.orchestrator.execute.return_value = SimpleNamespace(
            success=True, error=None, tx_result=SimpleNamespace(tx_hash="0xabc")
        )

        with patch(
            "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
            return_value=False,
        ):
            should_break = await runner._bridge_wait_process_intent(state, 0)

        assert should_break is False
        # Still counted as success, state.failed_step stays None
        assert state.failed_step is None


# =============================================================================
# _bridge_wait_verify_source_tx - extended
# =============================================================================


class TestBridgeWaitVerifySourceTx:
    @pytest.mark.asyncio
    async def test_no_gateway_client_raises_runtime_error(self) -> None:
        """Gateway-only boundary: a missing gateway client must fail loud.

        Direct Web3 fallback is forbidden by the gateway-only architecture
        (see ``blueprints/20-gateway-security-architecture.md``). The helper
        must raise rather than silently fall back to an unmediated egress
        path.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.gateway_client = None
        state.rpc_urls = {}  # no RPC configured

        with pytest.raises(RuntimeError, match="Gateway client required"):
            await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        # State remains untouched when we fail loud before any I/O
        assert state.failed_step is None
        assert state.error_message is None

    @pytest.mark.asyncio
    async def test_gateway_reverted_status_fails(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        # Gateway client returns a "reverted" status
        gw = MagicMock()
        gw.execution.GetTransactionStatus.return_value = SimpleNamespace(status="reverted", block_number=0)
        state.gateway_client = gw

        result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xdead", chain="arbitrum", step_num=1)
        assert result is False
        assert state.failed_step == "step-1"
        assert "reverted" in (state.error_message or "")

    @pytest.mark.asyncio
    async def test_gateway_confirmed_status_returns_true(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        gw = MagicMock()
        gw.execution.GetTransactionStatus.return_value = SimpleNamespace(status="confirmed", block_number=100)
        state.gateway_client = gw

        result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xok", chain="arbitrum", step_num=1)
        assert result is True
        assert state.failed_step is None

    @pytest.mark.asyncio
    async def test_grpc_rpc_error_fails_cleanly_after_retries(self) -> None:
        """Transient gRPC errors across all 30 attempts -> timeout, clean failure.

        Narrowed from the prior ``test_outer_exception_fails_cleanly`` as part
        of #1666. ``grpc.RpcError`` is the only exception class retried inside
        the loop; config defects (``AttributeError`` / ``TypeError``) now
        propagate immediately and are exercised in
        ``tests/unit/runner/test_bridge_verify_precheck.py``.
        """
        import grpc

        runner = _make_runner()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        # Gateway client where the RPC call raises a transient gRPC error on
        # every attempt. All 30 attempts fail -> tx_verified=False ->
        # failed_step / error_message set, helper returns False.
        class _FakeRpcError(grpc.RpcError):
            pass

        gw = MagicMock()
        gw.execution.GetTransactionStatus = MagicMock(side_effect=_FakeRpcError("transient"))
        state.gateway_client = gw

        # Use a very small sleep for the delay to keep test fast.
        with patch("almanak.framework.runner.strategy_runner.asyncio.sleep", new=AsyncMock()):
            result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        assert result is False
        assert state.failed_step == "step-1"
        assert "Timeout" in (state.error_message or "") or "receipt" in (state.error_message or "")

    @pytest.mark.asyncio
    async def test_permanent_grpc_code_fast_fails_without_retry(self) -> None:
        """A permanent gRPC status code propagates immediately on the first attempt.

        Permanent codes (PERMISSION_DENIED, UNAUTHENTICATED, INVALID_ARGUMENT, …)
        indicate a config/auth defect that will not resolve with more attempts.
        The loop must re-raise rather than silently consuming the full 60-second
        retry budget. See PR #1676 review feedback.
        """
        import grpc

        runner = _make_runner()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        class _PermanentRpcError(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.PERMISSION_DENIED

        gw = MagicMock()
        gw.execution.GetTransactionStatus = MagicMock(side_effect=_PermanentRpcError())
        state.gateway_client = gw

        with patch("almanak.framework.runner.strategy_runner.asyncio.sleep", new=AsyncMock()):
            result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        # The permanent error escapes the inner retry loop and is caught by the outer
        # except-Exception guard, which sets failed_step and returns False immediately.
        assert result is False
        assert gw.execution.GetTransactionStatus.call_count == 1
        assert state.failed_step == "step-1"

    @pytest.mark.asyncio
    async def test_unknown_grpc_code_retries_all_attempts(self) -> None:
        """A bare grpc.RpcError with no .code() is treated as transient → all retries used.

        Per PR #1676: when the status code cannot be determined, retry rather
        than crash to preserve pre-change behaviour for unknown-code edge cases.
        """
        import grpc

        runner = _make_runner()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        class _NoCodeRpcError(grpc.RpcError):
            pass  # no .code() method

        gw = MagicMock()
        gw.execution.GetTransactionStatus = MagicMock(side_effect=_NoCodeRpcError("no code"))
        state.gateway_client = gw

        with patch("almanak.framework.runner.strategy_runner.asyncio.sleep", new=AsyncMock()):
            result = await runner._bridge_wait_verify_source_tx(state, tx_hash="0xabc", chain="arbitrum", step_num=1)
        # All 30 attempts retried (not fast-failed)
        assert gw.execution.GetTransactionStatus.call_count == 30
        assert result is False


# =============================================================================
# _bridge_wait_poll_completion - extended
# =============================================================================


class TestBridgeWaitPollCompletionExtended:
    @pytest.mark.asyncio
    async def test_completed_status_delegates_to_apply_completion(self) -> None:
        runner = _make_runner()
        runner._bridge_wait_apply_completion = AsyncMock(return_value=False)
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent
        state.state_provider = MagicMock()
        state.state_provider.register_bridge_transfer = MagicMock(return_value="d1")
        state.state_provider.wait_for_bridge_completion = AsyncMock(
            return_value={"status": "completed", "balance_increase": 1000}
        )

        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))
        should_break = await runner._bridge_wait_poll_completion(
            state,
            result=result,
            tx_hash="0xabc",
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is False
        runner._bridge_wait_apply_completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_intent_executed_callback_exception_swallowed_on_bridge_failure(
        self,
    ) -> None:
        """Strategy callback errors during failure paths do not prevent break."""
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.on_intent_executed.side_effect = RuntimeError("strat bug")
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent
        state.state_provider = MagicMock()
        state.state_provider.register_bridge_transfer = MagicMock(return_value="d1")
        state.state_provider.wait_for_bridge_completion = AsyncMock(
            return_value={"status": "failed", "error": "revert"}
        )

        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))
        should_break = await runner._bridge_wait_poll_completion(
            state,
            result=result,
            tx_hash="0xabc",
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is True
        assert state.callback_fired is True
        assert state.failed_step == "step-1-bridge"


# =============================================================================
# _bridge_wait_apply_completion - extended
# =============================================================================


class TestBridgeWaitApplyCompletionExtended:
    @pytest.mark.asyncio
    async def test_normalization_populates_metadata_fields_in_log(self) -> None:
        """The success path uses all three metadata fields (decimals, resolved_from, raw_wei)."""
        runner = _make_runner()
        runner._normalize_bridge_balance_increase = MagicMock(
            return_value=(
                Decimal("9.999"),
                {"raw_wei": 9999000000, "decimals": 9, "resolved_from": "gateway"},
            )
        )

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))
        should_break = await runner._bridge_wait_apply_completion(
            state,
            result=result,
            bridge_status={"status": "completed", "balance_increase": 9999000000},
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is False
        assert state.previous_amount_received == Decimal("9.999")

    @pytest.mark.asyncio
    async def test_normalization_returning_none_preserves_prior_previous_amount(
        self,
    ) -> None:
        """If decimals can't be resolved, previous_amount_received is not overwritten."""
        runner = _make_runner()
        runner._normalize_bridge_balance_increase = MagicMock(return_value=(None, {"raw_wei": 123, "decimals": None}))

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent])
        state.current_intent = intent
        state.previous_amount_received = Decimal("99")

        should_break = await runner._bridge_wait_apply_completion(
            state,
            result=SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0x")),
            bridge_status={"status": "completed", "balance_increase": 123},
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is False
        assert state.previous_amount_received == Decimal("99")


# =============================================================================
# _bridge_wait_cross_chain - extended
# =============================================================================


class TestBridgeWaitCrossChainExtended:
    @pytest.mark.asyncio
    async def test_verify_true_then_poll_completion_invoked(self) -> None:
        runner = _make_runner()
        runner._bridge_wait_verify_source_tx = AsyncMock(return_value=True)
        runner._bridge_wait_poll_completion = AsyncMock(return_value=False)

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))
        should_break = await runner._bridge_wait_cross_chain(
            state,
            result=result,
            step_num=1,
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
        )
        assert should_break is False
        runner._bridge_wait_verify_source_tx.assert_awaited_once()
        runner._bridge_wait_poll_completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bare_hex_tx_hash_is_normalized(self) -> None:
        """A tx_hash without 0x prefix gets normalized before verification."""
        runner = _make_runner()
        runner._bridge_wait_verify_source_tx = AsyncMock(return_value=True)
        runner._bridge_wait_poll_completion = AsyncMock(return_value=False)

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="deadbeef"))
        await runner._bridge_wait_cross_chain(
            state,
            result=result,
            step_num=1,
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
        )
        # Verify was called with 0x-prefixed
        kwargs = runner._bridge_wait_verify_source_tx.await_args.kwargs
        assert kwargs["tx_hash"] == "0xdeadbeef"
        # Poll completion also gets the prefixed hash
        poll_kwargs = runner._bridge_wait_poll_completion.await_args.kwargs
        assert poll_kwargs["tx_hash"] == "0xdeadbeef"


# =============================================================================
# _bridge_wait_finalize - extended
# =============================================================================


class TestBridgeWaitFinalizeExtended:
    @pytest.mark.asyncio
    async def test_success_does_not_invoke_failed_builder(self) -> None:
        runner = _make_runner()
        runner._clear_execution_progress = AsyncMock()
        runner._bridge_wait_build_failed_result = AsyncMock()

        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.successful_count = 1  # success

        result = await runner._bridge_wait_finalize(state)
        assert result.status == IterationStatus.SUCCESS
        runner._bridge_wait_build_failed_result.assert_not_called()

    @pytest.mark.asyncio
    async def test_failure_callback_still_fires_when_callback_not_fired_and_no_handler(
        self,
    ) -> None:
        """Strategy without on_intent_executed must not break finalization path."""
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        strategy = MagicMock(spec=[])  # no on_intent_executed, save_state etc.
        strategy.strategy_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1111"
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.failed_step = "step-1"
        state.error_message = "boom"
        state.callback_fired = False
        state.current_intent = intent
        state.rpc_urls = {}

        result = await runner._bridge_wait_finalize(state)
        assert result.status == IterationStatus.EXECUTION_FAILED


class TestBridgeWaitBuildFailedResult:
    @pytest.mark.asyncio
    async def test_malformed_failed_step_name_defaults_to_index_zero(self) -> None:
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.failed_step = "bogus"  # no hyphen, no step number
        state.error_message = "oops"
        state.current_intent = intent
        state.rpc_urls = {}

        result = await runner._bridge_wait_build_failed_result(state)
        assert result.status == IterationStatus.EXECUTION_FAILED
        # Progress records defaulted to index 0
        assert state.progress.failed_at_step_index == 0

    @pytest.mark.asyncio
    async def test_bridge_failure_logs_bridge_message_not_revert_diagnostics(self) -> None:
        """A failed_step name containing '-bridge' goes through the bridge-failure branch."""
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        # Use a valid checksum wallet so Web3BalanceProvider construction succeeds
        strategy = MagicMock()
        strategy.strategy_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890AbcdEF1234567890aBcdef12345678"
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="arbitrum")
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.failed_step = "step-1-bridge"
        state.error_message = "bridge timed out"
        state.current_intent = intent
        # Provide an RPC url so the diagnostic branch enters but chooses bridge path
        state.rpc_urls = {"arbitrum": "https://rpc"}

        with patch(
            "almanak.framework.runner.strategy_runner.diagnose_revert",
            new=AsyncMock(),
        ) as mock_diag:
            result = await runner._bridge_wait_build_failed_result(state)

        assert result.status == IterationStatus.EXECUTION_FAILED
        # Bridge failures skip revert diagnostics
        mock_diag.assert_not_called()

    @pytest.mark.asyncio
    async def test_execution_failure_with_failed_result_runs_diagnose_revert(
        self,
    ) -> None:
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        # Use a checksummed wallet address so Web3BalanceProvider construction does not
        # raise during the diagnostic path.
        strategy = MagicMock()
        strategy.strategy_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890AbcdEF1234567890aBcdef12345678"

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="arbitrum")
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.failed_step = "step-1"
        state.error_message = "reverted"
        state.current_intent = intent
        state.rpc_urls = {"arbitrum": "https://rpc"}
        state.failed_result = SimpleNamespace(gas_warnings=None)

        diag_report = MagicMock()
        diag_report.format.return_value = "diag text"
        with patch(
            "almanak.framework.runner.strategy_runner.diagnose_revert",
            new=AsyncMock(return_value=diag_report),
        ) as mock_diag:
            result = await runner._bridge_wait_build_failed_result(state)

        assert result.status == IterationStatus.EXECUTION_FAILED
        mock_diag.assert_awaited_once()
