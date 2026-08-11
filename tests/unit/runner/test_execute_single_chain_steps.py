"""Tests for the ``_execute_single_chain`` step helpers extracted in Phase 3c.

Phase 3c split ``StrategyRunner._execute_single_chain`` (CC=118) into a thin
driver plus per-phase step helpers:

* ``_init_single_chain_state`` (runtime-handle setup)
* ``_single_chain_state_machine_loop`` (state-machine drive)
* ``_single_chain_execute_step`` (per-bundle execution with dry-run short-circuit)
* ``_single_chain_pre_retry_confirmed`` (post-timeout retry short-circuit)
* ``_single_chain_slippage_guard`` (realized-slippage circuit-breaker)
* ``_single_chain_handle_recon_incident`` (reconciliation-failure finalizer)
* ``_single_chain_handle_success`` / ``_single_chain_handle_failure``
* static helper ``_build_single_chain_price_oracle``

These tests exercise the small, deterministic pieces of each helper so
regressions in the early-exit / mutation contract surface at unit level.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult
from almanak.framework.execution.orchestrator import (
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.execution.submission import (
    ReplayPolicy,
    SubmissionProvenance,
    SubmissionTransactionEvidence,
    TransactionRole,
    execution_plan_hash,
)
from almanak.framework.intents.compiler import CompilationResult, CompilationStatus
from almanak.framework.intents.state_machine import IntentStateMachine, RetryConfig, StateMachineConfig
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.runner_models import ExecutionBarrierPhase, ExecutionProgress
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    SingleChainExecutionState,
    StrategyRunner,
)

# =============================================================================
# Helpers
# =============================================================================


def _make_runner(
    *,
    dry_run: bool = False,
    state_manager: MagicMock | None = None,
    balance_provider: MagicMock | None = None,
    execution_orchestrator: MagicMock | None = None,
) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
        max_retries=2,
    )
    if state_manager is None:
        state_manager = MagicMock()
    if balance_provider is None:
        balance_provider = MagicMock()
        balance_provider.invalidate_cache = MagicMock()
    if execution_orchestrator is None:
        execution_orchestrator = MagicMock()
        # Ensure ``getattr(orch, "tx_risk_config", None)`` returns None so tests
        # exercising the slippage path use intent.max_slippage (not a Mock).
        execution_orchestrator.tx_risk_config = None
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=execution_orchestrator,
        state_manager=state_manager,
        config=config,
    )
    runner._load_execution_progress = AsyncMock(
        return_value=ExecutionProgress(
            execution_id="pending",
            deployment_id="test-strategy",
            intents_hash="landed-accounting-pending",
            total_steps=1,
            reconciliation_required_step_index=0,
        )
    )
    runner._save_execution_progress = AsyncMock()
    return runner


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    return strategy


def _make_state(strategy: MagicMock, *, intent=None) -> SingleChainExecutionState:
    if intent is None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
    return SingleChainExecutionState(
        strategy=strategy,
        intent=intent,
        start_time=datetime.now(UTC),
        deployment_id=strategy.deployment_id,
    )


# =============================================================================
# _build_single_chain_price_oracle (static helper)
# =============================================================================


class TestBuildSingleChainPriceOracle:
    def test_no_market_returns_none(self) -> None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        assert StrategyRunner._build_single_chain_price_oracle(None, intent) is None

    def test_market_without_price_oracle_dict_returns_none(self) -> None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = object()  # no get_price_oracle_dict attr
        assert StrategyRunner._build_single_chain_price_oracle(market, intent) is None

    def test_populated_oracle_returned(self) -> None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        # First call returns populated dict containing both intent tokens
        market.get_price_oracle_dict.return_value = {"USDC": Decimal("1"), "ETH": Decimal("2000")}
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result == {"USDC": Decimal("1"), "ETH": Decimal("2000")}

    def test_empty_oracle_after_prefetch_returns_empty_dict_not_none(self) -> None:
        """ALM-3183 negative control. An oracle that EXISTS and priced nothing is
        returned as ``{}``, never ``None``.

        This assertion is the whole fix in one line. Before ALM-3183 the helper
        collapsed empty to ``None``, and ``_init_single_chain_state`` read that
        ``None`` as "placeholder prices are fine" — so a price-service outage
        (which is what empties an oracle) silently bought every swap a slippage
        floor computed from ETH=$2000/WBTC=$45000 and disabled the price-impact
        guard. Revert the fix and this test fails: ``{} != None``.
        """
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result == {}
        assert result is not None

    def test_prefetch_failure_is_logged_at_error_not_swallowed(self, caplog) -> None:
        """ALM-3183 (b) negative control: a failing ``market.price()`` is loud.

        The pre-fetch loop was ``except Exception: pass``. A price-service outage
        and its cover-up were therefore the same line: the outage emptied the
        oracle, and the empty oracle was itself the signal that turned on
        placeholder pricing. Revert to ``pass`` and this test fails — no ERROR
        record is emitted and the token never appears in the log.
        """
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        market.chain = "arbitrum"
        market.get_price_oracle_dict.return_value = {}
        market.price.side_effect = RuntimeError("price service unreachable")

        with caplog.at_level(logging.ERROR, logger="almanak.framework.runner.strategy_runner"):
            result = StrategyRunner._build_single_chain_price_oracle(market, intent)

        assert result == {}
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors, "a failing price pre-fetch must be logged at ERROR"
        joined = "\n".join(r.getMessage() for r in errors)
        assert "price service unreachable" in joined, "the underlying exception must be surfaced"
        assert "ETH" in joined, "the token that could not be priced must be named"

    def test_prefetches_missing_tokens(self) -> None:
        """Tokens missing from the oracle trigger ``market.price(token)`` pre-fetch calls."""
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        # First call returns oracle missing ETH; second call (post pre-fetch) adds ETH
        market.get_price_oracle_dict.side_effect = [
            {"USDC": Decimal("1")},
            {"USDC": Decimal("1"), "ETH": Decimal("2000")},
        ]
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result == {"USDC": Decimal("1"), "ETH": Decimal("2000")}
        # ETH must have been pre-fetched; USDC was already present
        called = {call.args[0] for call in market.price.call_args_list}
        assert "ETH" in called

    def test_prefetches_native_gas_token_on_non_native_swap(self) -> None:
        """VIB-3804: a polygon USDC->WETH swap never references MATIC, but the
        chain's native gas token must still be in the oracle so
        ``accounting.gas_pricing.compute_gas_usd`` can populate
        ``transaction_ledger.gas_usd``. Without this pre-fetch, every
        polygon/avalanche/bsc swap silently writes ``gas_usd=""``.
        """
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("1"))
        market = MagicMock()
        market.chain = "polygon"
        market.get_price_oracle_dict.side_effect = [
            {"USDC": Decimal("1"), "WETH": Decimal("3000")},
            {
                "USDC": Decimal("1"),
                "WETH": Decimal("3000"),
                "MATIC": Decimal("0.5"),
            },
        ]
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result is not None
        assert "MATIC" in result
        called = {call.args[0] for call in market.price.call_args_list}
        assert "MATIC" in called

    def test_native_token_prefetch_skipped_when_chain_unknown(self) -> None:
        """When neither the market nor the intent expose a chain, the native
        pre-fetch is silently skipped — we don't want to fabricate an "ETH"
        request on a market with no chain context.
        """
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("1"))
        market = MagicMock(spec=["get_price_oracle_dict", "price"])
        # ``spec`` strips ``.chain`` so getattr falls through; intent.chain is None.
        market.get_price_oracle_dict.return_value = {
            "USDC": Decimal("1"),
            "WETH": Decimal("3000"),
        }
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result == {"USDC": Decimal("1"), "WETH": Decimal("3000")}
        # No price() calls — both swap legs already in oracle and no native added.
        market.price.assert_not_called()

    def test_native_prefetch_skipped_on_empty_oracle(
        self,
    ) -> None:
        """The native-gas pre-fetch is not attempted on an all-failed oracle.

        Originally this guard existed to stop the native pre-fetch from
        flipping the ``allow_placeholder_prices`` signal by turning an empty
        oracle into a native-only one (Codex audit P2 on the VIB-3804 patch).
        ALM-3183 removed that signal — emptiness no longer selects placeholder
        mode — so the guard now only short-circuits a pointless price call when
        every intent leg already failed to price. The behaviour is asserted
        UNCHANGED here on purpose: lifting the guard would start writing
        ``gas_usd`` on runs that previously wrote it empty, which is an
        accounting-surface change and belongs in its own PR.
        """
        intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("1"))
        market = MagicMock()
        market.chain = "polygon"
        # Oracle stays empty after the intent-token pre-fetch loop —
        # this simulates an indicator-only strategy with no upstream price feed.
        market.get_price_oracle_dict.return_value = {}
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        # Real-but-empty (ALM-3183), not None: the oracle existed and priced nothing.
        assert result == {}
        # Crucial: MATIC pre-fetch was NOT attempted -- an oracle carrying only
        # MATIC is not more useful to a USDC->WETH swap than an empty one, and
        # it would change what gets written to gas_usd.
        called = {call.args[0] for call in market.price.call_args_list}
        assert "MATIC" not in called


# =============================================================================
# _single_chain_pre_retry_confirmed
# =============================================================================


class TestSingleChainPreRetryConfirmed:
    @pytest.mark.asyncio
    async def test_no_prior_timeout_returns_false(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0  # first attempt -- not a retry

        single_chain_orch = MagicMock()
        assert await runner._single_chain_pre_retry_confirmed(state, single_chain_orch) is False

    @pytest.mark.asyncio
    async def test_non_timeout_error_returns_false(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SIGNING,
            transaction_results=[TransactionResult(tx_hash="0xabc", success=False, gas_used=0, gas_cost_wei=0)],
            error="reverted",
        )

        single_chain_orch = MagicMock()
        assert await runner._single_chain_pre_retry_confirmed(state, single_chain_orch) is False

    @pytest.mark.asyncio
    async def test_all_prior_confirmed_short_circuits_to_success(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.state_machine.set_receipt = MagicMock()
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            transaction_results=[TransactionResult(tx_hash="0xdead", success=False, gas_used=0, gas_cost_wei=0)],
            error="timeout waiting for receipt",
        )

        submitted_receipt = SimpleNamespace(tx_hash="0xdead", success=True, gas_used=21000, gas_cost_wei=100, logs=[])
        single_chain_orch = MagicMock()
        single_chain_orch.submitter = MagicMock()
        single_chain_orch.submitter.get_receipt = AsyncMock(return_value=submitted_receipt)

        assert await runner._single_chain_pre_retry_confirmed(state, single_chain_orch) is True
        # Success ExecutionResult synthesised
        assert state.last_execution_result.success is True
        assert state.last_execution_result.total_gas_used == 21000
        state.state_machine.set_receipt.assert_called_once()

    @pytest.mark.asyncio
    async def test_prior_reverted_tx_does_not_short_circuit(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.state_machine.set_receipt = MagicMock()
        original_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            transaction_results=[TransactionResult(tx_hash="0xdead", success=False, gas_used=0, gas_cost_wei=0)],
            error="timeout waiting for receipt",
        )
        state.last_execution_result = original_result

        reverted_receipt = SimpleNamespace(tx_hash="0xdead", success=False, gas_used=21000, gas_cost_wei=100, logs=[])
        single_chain_orch = MagicMock()
        single_chain_orch.submitter = MagicMock()
        single_chain_orch.submitter.get_receipt = AsyncMock(return_value=reverted_receipt)

        # Reverted TX -> not all confirmed -> do not short-circuit
        assert await runner._single_chain_pre_retry_confirmed(state, single_chain_orch) is False
        # Does not overwrite last_execution_result
        assert state.last_execution_result is original_result
        state.state_machine.set_receipt.assert_not_called()


@pytest.mark.asyncio
async def test_incomplete_gateway_receipt_set_with_known_hash_is_never_redispatched() -> None:
    """Live state-machine control: a mined-but-unmeasured bundle executes once.

    The gateway deliberately returns no partial receipts because positional
    association is no longer trustworthy. The retained hash must survive into
    the receipt state machine and force a terminal reconciliation verdict; it
    must not enter the automatic retry loop and broadcast the bundle again.
    """
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = None
    orchestrator.reset_nonce_cache = MagicMock()
    orchestrator.execute = AsyncMock(
        return_value=GatewayExecutionResult(
            success=False,
            tx_hashes=["0x" + "a" * 64],
            total_gas_used=21_000,
            receipts=[],
            execution_id="exec-incomplete",
            error="receipt 1 could not be serialized",
            error_code="RECEIPT_SET_INCOMPLETE",
        )
    )
    runner = _make_runner(execution_orchestrator=orchestrator)
    runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
    strategy = _make_strategy()
    state = _make_state(strategy)

    bundle = MagicMock()
    bundle.transactions = [MagicMock(), MagicMock()]
    bundle.intent_type = "SWAP"
    bundle.metadata = {}
    compiler = MagicMock()
    compiler.default_protocol = None
    compiler.compile.return_value = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=state.intent.intent_id,
        action_bundle=bundle,
    )
    state.compiler = compiler
    state.state_machine = IntentStateMachine(
        intent=state.intent,
        compiler=compiler,
        config=StateMachineConfig(
            retry_config=RetryConfig(max_retries=2, initial_delay_seconds=0.0, jitter_factor=0.0)
        ),
        on_sadflow_enter=runner._on_sadflow_enter,
    )

    with patch("almanak.framework.observability.emitter.emit_phase_event") as emit_phase_event:
        assert await runner._single_chain_state_machine_loop(state) is None

    assert state.state_machine.is_complete
    assert not state.state_machine.success
    assert state.state_machine.retry_count == 0
    assert "BROADCAST_RECONCILIATION_REQUIRED" in (state.state_machine.error or "")
    orchestrator.execute.assert_awaited_once()
    progress = runner._save_execution_progress.await_args.args[1]
    assert progress.reconciliation_required_step_index == 0
    assert progress.serialized_intents == [state.intent.serialize()]
    [evidence] = progress.submission_evidence
    assert evidence.step_index == 0
    assert evidence.chain == "arbitrum"
    assert evidence.submitted_transaction_ids == ["0x" + "a" * 64]
    timeline = emit_phase_event.call_args.kwargs
    assert timeline["tx_hash"] == "0x" + "a" * 64
    assert timeline["details"]["submitted_tx_hashes"] == ["0x" + "a" * 64]

    # The same retained hash reaches the failure-ledger extractor even though
    # transaction_results is necessarily empty without trustworthy receipts.
    from almanak.framework.observability.ledger import _extract_tx_and_gas

    tx_hash, _gas_used, _gas_usd = _extract_tx_and_gas(state.last_execution_result)
    assert tx_hash == "0x" + "a" * 64

    # Next-cycle negative control: the pre-decide gate reads the durable marker
    # and returns before strategy.decide() or another orchestrator broadcast.
    runner._load_execution_progress = AsyncMock(return_value=progress)  # type: ignore[method-assign]
    resumed = await runner._check_and_resume_stuck_execution(strategy, datetime.now(UTC))
    assert resumed is not None
    assert resumed.status == IterationStatus.EXECUTION_FAILED
    orchestrator.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_landed_approval_reverted_action_recompiles_fresh_bundle() -> None:
    """A mixed receipt set may recompile, but the original bundle is never replayed."""

    def _receipt(tx_hash: str, status: int) -> dict[str, object]:
        return {
            "tx_hash": tx_hash,
            "block_number": 42,
            "block_hash": "0xblock",
            "gas_used": 21_000,
            "effective_gas_price": "1",
            "status": status,
            "logs": [],
        }

    approval_tx = {
        "tx_type": "approve",
        "data": "0x095ea7b3" + "00" * 64,
        "value": "0x0",
        "to": "0xtoken",
    }
    action_tx = {"tx_type": "swap", "data": "0x12345678", "value": "0x0", "to": "0xrouter"}
    first_bundle = MagicMock(transactions=[approval_tx, action_tx], metadata={})
    first_bundle.to_dict.return_value = {"intent_type": "SWAP", "transactions": [approval_tx, action_tx]}
    fresh_bundle = MagicMock(transactions=[action_tx], metadata={})
    fresh_bundle.to_dict.return_value = {"intent_type": "SWAP", "transactions": [action_tx]}

    first_result = GatewayExecutionResult(
        success=False,
        tx_hashes=["0xapprove", "0xaction"],
        total_gas_used=42_000,
        receipts=[_receipt("0xapprove", 1), _receipt("0xaction", 0)],
        execution_id="mixed",
        error="action reverted",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        execution_plan_hash=execution_plan_hash(first_bundle),
        submission_transactions=[
            SubmissionTransactionEvidence("0xapprove", TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
            SubmissionTransactionEvidence("0xaction", TransactionRole.ACTION, ReplayPolicy.NEVER),
        ],
    )
    second_result = GatewayExecutionResult(
        success=True,
        tx_hashes=["0xfreshaction"],
        total_gas_used=21_000,
        receipts=[_receipt("0xfreshaction", 1)],
        execution_id="fresh",
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        execution_plan_hash=execution_plan_hash(fresh_bundle),
        submission_transactions=[
            SubmissionTransactionEvidence("0xfreshaction", TransactionRole.ACTION, ReplayPolicy.NEVER)
        ],
    )
    orchestrator = MagicMock(tx_risk_config=None)
    orchestrator.reset_nonce_cache = MagicMock()
    orchestrator.execute = AsyncMock(side_effect=[first_result, second_result])
    runner = _make_runner(execution_orchestrator=orchestrator)
    runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
    state = _make_state(_make_strategy())
    compiler = MagicMock(default_protocol=None)
    compiler.compile.side_effect = [
        CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=state.intent.intent_id,
            action_bundle=first_bundle,
        ),
        CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=state.intent.intent_id,
            action_bundle=fresh_bundle,
        ),
    ]
    state.compiler = compiler
    state.state_machine = IntentStateMachine(
        intent=state.intent,
        compiler=compiler,
        config=StateMachineConfig(retry_config=RetryConfig(max_retries=1, initial_delay_seconds=0, jitter_factor=0)),
    )

    assert await runner._single_chain_state_machine_loop(state) is None

    assert state.state_machine.success
    assert compiler.compile.call_count == 2
    assert [call.kwargs["action_bundle"] for call in orchestrator.execute.await_args_list] == [
        first_bundle,
        fresh_bundle,
    ]
    saved_phases = [call.args[1].effective_barrier_phase for call in runner._save_execution_progress.await_args_list]
    assert ExecutionBarrierPhase.RECOMPILE_REQUIRED in saved_phases


@pytest.mark.asyncio
async def test_pre_broadcast_marker_write_failure_prevents_submission() -> None:
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = None
    orchestrator.execute = AsyncMock()
    runner = _make_runner(execution_orchestrator=orchestrator)
    runner._save_execution_progress = AsyncMock(side_effect=RuntimeError("state database unavailable"))  # type: ignore[method-assign]
    state = _make_state(_make_strategy())
    state.state_machine = MagicMock(retry_count=0)
    state.compiler = MagicMock(default_protocol=None)
    step_result = MagicMock()
    step_result.action_bundle = MagicMock(transactions=[MagicMock()], metadata={})

    assert await runner._single_chain_execute_step(state, step_result) is None

    orchestrator.execute.assert_not_awaited()
    failed_receipt = state.state_machine.set_receipt.call_args.args[0]
    assert failed_receipt.success is False
    assert "BROADCAST_RECONCILIATION_REQUIRED" in failed_receipt.error
    assert "marker write failed" in failed_receipt.error


@pytest.mark.asyncio
async def test_explicit_not_attempted_failure_clears_single_chain_barrier_for_retry() -> None:
    """A gateway refusal before submission must not become a permanent incident."""
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = None
    orchestrator.reset_nonce_cache = MagicMock()
    orchestrator.execute = AsyncMock(
        return_value=GatewayExecutionResult(
            success=False,
            tx_hashes=[],
            total_gas_used=0,
            receipts=[],
            execution_id="exec-not-submitted",
            error="policy refused transaction before submission",
            submission_provenance=SubmissionProvenance.NOT_ATTEMPTED,
        )
    )
    runner = _make_runner(execution_orchestrator=orchestrator)
    runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
    runner._clear_execution_progress = AsyncMock()  # type: ignore[method-assign]
    state = _make_state(_make_strategy())
    state.state_machine = MagicMock(retry_count=0)
    state.compiler = MagicMock(default_protocol=None)
    step_result = MagicMock()
    step_result.action_bundle = MagicMock(transactions=[MagicMock()], metadata={})

    assert await runner._single_chain_execute_step(state, step_result) is None

    orchestrator.execute.assert_awaited_once()
    runner._clear_execution_progress.assert_awaited_once_with(state.deployment_id)
    failed_receipt = state.state_machine.set_receipt.call_args.args[0]
    assert failed_receipt.success is False
    assert failed_receipt.error == "policy refused transaction before submission"
    assert "BROADCAST_RECONCILIATION_REQUIRED" not in failed_receipt.error


@pytest.mark.asyncio
async def test_landed_result_retains_marker_until_downstream_completion() -> None:
    orchestrator = MagicMock()
    orchestrator.tx_risk_config = None
    orchestrator.execute = AsyncMock(
        return_value=ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
    )
    runner = _make_runner(execution_orchestrator=orchestrator)
    runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
    runner._clear_execution_progress = AsyncMock(side_effect=AssertionError("must not clear before accounting"))  # type: ignore[method-assign]
    state = _make_state(_make_strategy())
    state.state_machine = MagicMock(retry_count=0)
    state.compiler = MagicMock(default_protocol=None)
    step_result = MagicMock()
    step_result.action_bundle = MagicMock(transactions=[MagicMock()], metadata={})

    assert await runner._single_chain_execute_step(state, step_result) is None

    orchestrator.execute.assert_awaited_once()
    assert runner._save_execution_progress.await_count == 2
    runner._clear_execution_progress.assert_not_awaited()
    landed_receipt = state.state_machine.set_receipt.call_args.args[0]
    assert landed_receipt.success is True
    retained_marker = runner._save_execution_progress.await_args.args[1]
    assert retained_marker.is_accounting_pending
    assert retained_marker.intents_hash == "landed-accounting-pending"

    # Crash-gap control: a restart between receipt validation and accounting
    # observes the retained marker and refuses a second broadcast.
    runner._load_execution_progress = AsyncMock(return_value=retained_marker)  # type: ignore[method-assign]
    resumed = await runner._check_and_resume_stuck_execution(state.strategy, datetime.now(UTC))
    assert resumed is not None
    assert resumed.status == IterationStatus.ACCOUNTING_FAILED
    orchestrator.execute.assert_awaited_once()


# =============================================================================
# _single_chain_execute_step (dry-run only -- full path is integration-tested)
# =============================================================================


class TestSingleChainExecuteStepDryRun:
    @pytest.mark.asyncio
    async def test_dry_run_returns_dry_run_iteration_result(self) -> None:
        """In dry-run mode, the step should short-circuit before calling the orchestrator."""
        runner = _make_runner(dry_run=True)
        # Spy on the orchestrator so we can prove dry-run does not call execute().
        runner.execution_orchestrator.execute = AsyncMock()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()

        fake_bundle = MagicMock()
        fake_bundle.transactions = [object(), object()]
        fake_bundle.metadata = {"expected_output_human": "1.0"}
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        result = await runner._single_chain_execute_step(state, step_result)
        assert result is not None
        assert result.status == IterationStatus.DRY_RUN
        assert result.intent is state.intent
        # Metadata should have been captured for enrichment (matches pre-refactor behaviour)
        assert state.last_bundle_metadata == {"expected_output_human": "1.0"}
        # Dry-run must short-circuit before reaching the orchestrator.
        runner.execution_orchestrator.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_dry_run_closes_clob_client(self) -> None:
        """Dry-run must still release the ClobClient httpx pool (if one was wired up)."""
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.clob_client = MagicMock()

        fake_bundle = MagicMock()
        fake_bundle.transactions = []
        fake_bundle.metadata = None
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        result = await runner._single_chain_execute_step(state, step_result)
        assert result is not None
        state.clob_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_dry_run_records_success_when_flag_enabled(self) -> None:
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.record_metrics = True

        fake_bundle = MagicMock()
        fake_bundle.transactions = []
        fake_bundle.metadata = None
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        pre_total = runner._total_iterations
        pre_success = runner._successful_iterations
        await runner._single_chain_execute_step(state, step_result)
        assert runner._total_iterations == pre_total + 1
        assert runner._successful_iterations == pre_success + 1

    @pytest.mark.asyncio
    async def test_dry_run_skips_metrics_when_flag_disabled(self) -> None:
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.record_metrics = False

        fake_bundle = MagicMock()
        fake_bundle.transactions = []
        fake_bundle.metadata = None
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        pre_total = runner._total_iterations
        await runner._single_chain_execute_step(state, step_result)
        assert runner._total_iterations == pre_total  # no metric recorded


# =============================================================================
# _single_chain_slippage_guard
# =============================================================================


class TestSingleChainSlippageGuard:
    @pytest.mark.asyncio
    async def test_no_execution_result_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.last_execution_result = None

        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_no_swap_amounts_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        # swap_amounts is None by default -- no slippage guard applies
        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_slippage_within_limit_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.05"),  # 500 bps
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=100,  # within 500 bps limit
            token_in="USDC",
            token_out="ETH",
        )

        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_slippage_breach_returns_execution_failed(self) -> None:
        runner = _make_runner()
        # Track write_ledger / save_state side effects
        runner._write_ledger_entry = AsyncMock()
        runner._emit_execution_timeline_event = MagicMock()

        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),  # 100 bps
        )
        state = _make_state(strategy, intent=intent)
        # Issue #1780: make the metric gate explicit. The single-intent
        # flow enters the guard with record_metrics=True; spell it out so
        # the assertion on _total_iterations below does not depend on a
        # default that could drift.
        state.record_metrics = True
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=200,  # exceeds 100 bps limit
            token_in="USDC",
            token_out="ETH",
        )

        pre_total = runner._total_iterations
        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        assert result.status == IterationStatus.EXECUTION_FAILED
        assert "Slippage circuit breaker" in result.error
        assert "200" in result.error and "100" in result.error
        # Timeline event and ledger entry must be written with success=False
        runner._emit_execution_timeline_event.assert_called_once()
        assert runner._emit_execution_timeline_event.call_args.kwargs.get("success") is False
        runner._write_ledger_entry.assert_awaited_once()
        assert runner._write_ledger_entry.await_args.kwargs.get("success") is False
        # Strategy callback fired with success=False
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False
        # Strategy state must be persisted on slippage breach (on-chain state already changed).
        strategy.save_state.assert_called_once()
        # Issue #1780: the slippage-breach iteration counts in the
        # lifetime total when record_metrics=True (single-intent flow),
        # mirroring the ``_record_success`` tick on the success branch.
        assert runner._total_iterations == pre_total + 1

    @pytest.mark.asyncio
    async def test_slippage_breach_multi_intent_defers_metrics(self) -> None:
        """Issue #1780: when state.record_metrics is False (multi-intent
        sequence), the slippage guard must NOT bump ``_total_iterations``.
        The caller (``_run_single_chain_intents``) records once per
        sequence to avoid double-counting.
        """
        runner = _make_runner()
        runner._write_ledger_entry = AsyncMock()
        runner._emit_execution_timeline_event = MagicMock()

        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
        )
        state = _make_state(strategy, intent=intent)
        state.record_metrics = False  # multi-intent sequence
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=200,
            token_in="USDC",
            token_out="ETH",
        )

        pre_total = runner._total_iterations
        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        assert result.status == IterationStatus.EXECUTION_FAILED
        # Multi-intent: the caller will record, so this helper must not.
        assert runner._total_iterations == pre_total

    @pytest.mark.asyncio
    async def test_slippage_breach_sets_error_before_emitting_timeline(self) -> None:
        """Regression for issue #1649.

        The timeline event description is built from ``result.error`` at the
        moment of emission. If the slippage-breach error is assigned AFTER
        the event fires, consumers (UI, operator cards, Slack alerts) see
        "Unknown error" instead of the real reason. Assert that at the time
        ``_emit_execution_timeline_event`` is called the ``last_execution_result.error``
        already contains the slippage-circuit-breaker message.
        """
        runner = _make_runner()
        runner._write_ledger_entry = AsyncMock()

        # Capture the error value on the result at call time (not at assertion
        # time, which is after the error is set either way).
        captured: dict[str, str | None] = {}

        def _capture(strategy, intent, *, success, result, related_ledger_entry_id=""):  # noqa: ANN001
            captured["error"] = getattr(result, "error", None)
            captured["success"] = success
            captured["related_ledger_entry_id"] = related_ledger_entry_id

        runner._emit_execution_timeline_event = MagicMock(side_effect=_capture)

        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),  # 100 bps
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=200,  # exceeds 100 bps limit
            token_in="USDC",
            token_out="ETH",
        )

        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        # Timeline event fired with success=False and error already populated
        assert captured["success"] is False
        assert captured["error"] is not None
        assert "Slippage circuit breaker" in captured["error"]
        assert "200" in captured["error"] and "100" in captured["error"]
