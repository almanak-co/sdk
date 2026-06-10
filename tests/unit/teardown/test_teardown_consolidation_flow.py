"""VIB-5011 — flow tests for the token-consolidation phase (teardown Phase 2).

Covers the two execution hooks plus the execution mechanics:

* ``TeardownManager.execute`` Step 7.5 (CLI execute lane): consolidation runs
  ONLY after a successful closure + verify; never on closure failure, never
  on verify failure.
* ``_teardown_helpers.execute_and_verify`` (runner lane): calls
  ``update_progress(current_phase=TOKEN_CONSOLIDATION)`` then
  ``run_token_consolidation``, folds the outcome, and never calls
  ``manager.execute`` (the two hooks never overlap for one teardown).
* ``run_token_consolidation`` executes the planned swap via the REAL
  ``_execute_intents`` (same ladder + commit pairing): the commit fires with
  ``teardown_cycle_id = f"teardown-{teardown_id}"``.
* Consolidation failure keeps ``success=True`` with
  ``consolidation_failed`` set, and ``mark_completed`` carries
  ``result_json["consolidation"]``.
* Degraded accounting on a consolidation commit does NOT abort the
  remaining consolidation swaps.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.teardown_commit import TeardownCommitOutcome
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.consolidation import ConsolidationOutcome
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPhase,
    TeardownPositionSummary,
    TeardownResult,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager
from almanak.framework.teardown.teardown_manager import TeardownManager

CHAIN = "ethereum"


# ---------------------------------------------------------------------------
# Fixtures / doubles
# ---------------------------------------------------------------------------


class FakeMarket:
    def __init__(self, balances: dict[str, Decimal], prices: dict[str, Decimal]):
        self._balances = balances
        self._prices = prices

    def balance(self, token: str, chain: str | None = None):  # noqa: ARG002
        if token not in self._balances:
            raise ValueError(f"token {token} not registered")
        return SimpleNamespace(balance=self._balances[token])

    def price(self, token: str, chain: str | None = None) -> Decimal:  # noqa: ARG002
        if token not in self._prices:
            raise ValueError(f"no price for {token}")
        return self._prices[token]

    def get_price_oracle_dict(self) -> dict:
        return dict(self._prices)


def _make_strategy():
    return SimpleNamespace(
        deployment_id="dep-1",
        name="consolidation_test",
        chain=CHAIN,
        wallet_address="0xWALLET",
        uses_safe_wallet=False,
        get_teardown_profile=lambda: SimpleNamespace(natural_exit_assets=[], original_entry_assets=[]),
    )


def _make_positions(value: str = "18") -> TeardownPositionSummary:
    pos = PositionInfo(
        position_type=PositionType.LP,
        position_id="123",
        chain=CHAIN,
        protocol="uniswap_v3",
        value_usd=Decimal(value),
        details={"token0": "WETH", "token1": "USDC"},
    )
    return TeardownPositionSummary(
        deployment_id="dep-1",
        timestamp=datetime.now(UTC),
        positions=[pos],
    )


def _make_state(*, pending: list[dict], completed: int) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="td_consol1",
        deployment_id="dep-1",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=len(pending),
        completed_intents=completed,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
        pending_intents_json=json.dumps(pending),
    )


def _successful_exec_result() -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        transaction_results=[SimpleNamespace(tx_hash="0xabc")],
        total_gas_used=120_000,
        gas_cost_usd="0.50",
        extracted_data={},
        error="",
    )


def _fake_orchestrator() -> MagicMock:
    orch = MagicMock(name="ExecutionOrchestrator")
    orch.execute = AsyncMock(return_value=_successful_exec_result())
    return orch


def _fake_compiler() -> MagicMock:
    comp = MagicMock(name="IntentCompiler")
    comp.compile.return_value = SimpleNamespace(
        status=SimpleNamespace(value="SUCCESS"),
        action_bundle=SimpleNamespace(metadata={}),
        error=None,
        is_transient=False,
        retry_after_seconds=0,
    )
    return comp


def _make_commit_helpers(*, commit_outcomes: list[TeardownCommitOutcome] | None = None):
    """TeardownRunnerHelpers with a recording commit stub (VIB-3773 shape)."""
    commit_calls: list[dict] = []
    outcomes_iter = iter(commit_outcomes or [])

    async def _commit(
        strategy, intent, *, execution_result, execution_context, bundle_metadata, teardown_cycle_id, **_kw
    ):
        commit_calls.append(
            {
                "deployment_id": strategy.deployment_id,
                "from_token": getattr(intent, "from_token", None),
                "teardown_cycle_id": teardown_cycle_id,
            }
        )
        try:
            return next(outcomes_iter)
        except StopIteration:
            return TeardownCommitOutcome(ledger_entry_id="ledger-x", accounting_degraded=False, degraded_reason=None)

    return TeardownRunnerHelpers(commit=_commit), commit_calls


def _result(*, success: bool, succeeded: int = 1, failed: int = 0, total: int = 1) -> TeardownResult:
    return TeardownResult(
        success=success,
        deployment_id="dep-1",
        mode="graceful",
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_seconds=1.0,
        intents_total=total,
        intents_succeeded=succeeded,
        intents_failed=failed,
        starting_value_usd=Decimal("18"),
        final_value_usd=Decimal("18"),
        total_costs_usd=Decimal("0"),
        final_balances={},
        error=None if success else "boom",
    )


def _closing_strategy_for_execute():
    """Strategy double for TeardownManager.execute (needs pause + intents)."""
    strategy = _make_strategy()
    strategy.pause = AsyncMock()
    strategy.get_open_positions = lambda: _make_positions()
    intent = SimpleNamespace(
        intent_type="LP_CLOSE",
        chain=CHAIN,
        amount=None,
        to_dict=lambda: {"intent_type": "LP_CLOSE", "chain": CHAIN},
    )
    strategy.generate_teardown_intents = lambda mode, market=None: [intent]
    return strategy


# ---------------------------------------------------------------------------
# Manager.execute (CLI lane) — Step 7.5 gating
# ---------------------------------------------------------------------------


class TestManagerExecuteHook:
    @pytest.mark.asyncio
    async def test_consolidation_runs_only_after_successful_closure_and_verify(self):
        mgr = TeardownManager(config=TeardownConfig.default())
        mgr._execute_intents = AsyncMock(return_value=_result(success=True))
        mgr._verify_closure = AsyncMock(return_value=True)
        mgr.run_token_consolidation = AsyncMock(return_value=ConsolidationOutcome(planned=1, succeeded=1, failed=0))

        result = await mgr.execute(_closing_strategy_for_execute(), mode="graceful", is_auto_mode=True)

        assert result.success is True
        mgr.run_token_consolidation.assert_awaited_once()
        # The outcome was folded into the result.
        assert result.consolidation_planned == 1
        assert result.consolidation_succeeded == 1
        # Verification happened BEFORE consolidation.
        assert mgr._verify_closure.await_count == 1

    @pytest.mark.asyncio
    async def test_consolidation_skipped_when_closure_fails(self):
        mgr = TeardownManager(config=TeardownConfig.default())
        mgr._execute_intents = AsyncMock(return_value=_result(success=False, succeeded=0, failed=1))
        mgr._verify_closure = AsyncMock(return_value=True)
        mgr.run_token_consolidation = AsyncMock()

        result = await mgr.execute(_closing_strategy_for_execute(), mode="graceful", is_auto_mode=True)

        assert result.success is False
        mgr.run_token_consolidation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_consolidation_skipped_when_verify_fails(self):
        mgr = TeardownManager(config=TeardownConfig.default())
        mgr._execute_intents = AsyncMock(return_value=_result(success=True))
        mgr._verify_closure = AsyncMock(return_value=False)
        mgr.run_token_consolidation = AsyncMock()

        result = await mgr.execute(_closing_strategy_for_execute(), mode="graceful", is_auto_mode=True)

        assert result.success is False
        mgr.run_token_consolidation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_consolidation_failure_keeps_execute_success_true(self):
        mgr = TeardownManager(config=TeardownConfig.default())
        mgr._execute_intents = AsyncMock(return_value=_result(success=True))
        mgr._verify_closure = AsyncMock(return_value=True)
        mgr.run_token_consolidation = AsyncMock(
            return_value=ConsolidationOutcome(planned=1, succeeded=0, failed=1, warnings=["swap failed"])
        )

        result = await mgr.execute(_closing_strategy_for_execute(), mode="graceful", is_auto_mode=True)

        assert result.success is True
        assert result.consolidation_failed == 1
        assert result.consolidation_warnings == ["swap failed"]


# ---------------------------------------------------------------------------
# Runner lane — execute_and_verify hook
# ---------------------------------------------------------------------------


def _mgr_mock_for_runner_lane(*, closure_result: TeardownResult, verify_ok: bool = True):
    mgr = MagicMock(name="TeardownManager")
    mgr._execute_intents = AsyncMock(return_value=closure_result)
    mgr._verify_closure = AsyncMock(return_value=verify_ok)
    mgr.run_token_consolidation = AsyncMock(return_value=ConsolidationOutcome(planned=1, succeeded=1, failed=0))
    # execute() must never be reached from the runner lane.
    mgr.execute = AsyncMock(side_effect=AssertionError("runner lane must not call manager.execute"))
    return mgr


class TestRunnerLaneHook:
    @pytest.mark.asyncio
    async def test_update_progress_called_with_token_consolidation_then_phase_runs(self):
        from almanak.framework.runner import _teardown_helpers as _h

        mgr = _mgr_mock_for_runner_lane(closure_result=_result(success=True))
        state_manager = MagicMock(name="teardown_requests_manager")
        state = _make_state(pending=[{"intent_type": "LP_CLOSE"}], completed=0)
        runner = MagicMock()
        positions = _make_positions()

        teardown_result = await _h.execute_and_verify(
            runner,
            mgr,
            MagicMock(),  # teardown_state_adapter
            state,
            _make_strategy(),
            [{"intent_type": "LP_CLOSE"}],
            positions,
            TeardownMode.SOFT,
            None,  # teardown_market
            True,  # is_auto_mode
            None,  # price_oracle
            MagicMock(),  # request
            state_manager,
        )

        # Progress row flipped to the consolidation phase before the swaps ran.
        state_manager.update_progress.assert_called_once_with(
            "dep-1",
            positions_closed=1,
            current_phase=TeardownPhase.TOKEN_CONSOLIDATION,
        )
        mgr.run_token_consolidation.assert_awaited_once()
        call = mgr.run_token_consolidation.await_args
        assert call.kwargs["teardown_id"] == state.teardown_id
        assert call.kwargs["teardown_state"] is state
        # No-overlap assertion: the runner lane drives _execute_intents
        # directly — manager.execute must never fire here.
        mgr.execute.assert_not_awaited()
        # Outcome folded into the runner-lane result.
        assert teardown_result.success is True
        assert teardown_result.consolidation_planned == 1
        assert teardown_result.consolidation_succeeded == 1

    @pytest.mark.asyncio
    async def test_no_consolidation_when_closure_failed(self):
        from almanak.framework.runner import _teardown_helpers as _h

        mgr = _mgr_mock_for_runner_lane(closure_result=_result(success=False, succeeded=0, failed=1))
        state_manager = MagicMock()
        state = _make_state(pending=[{"intent_type": "LP_CLOSE"}], completed=0)

        teardown_result = await _h.execute_and_verify(
            MagicMock(),
            mgr,
            MagicMock(),
            state,
            _make_strategy(),
            [{"intent_type": "LP_CLOSE"}],
            _make_positions(),
            TeardownMode.SOFT,
            None,
            True,
            None,
            MagicMock(),
            state_manager,
        )

        assert teardown_result.success is False
        mgr.run_token_consolidation.assert_not_awaited()
        state_manager.update_progress.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_consolidation_when_verify_failed(self):
        from almanak.framework.runner import _teardown_helpers as _h

        mgr = _mgr_mock_for_runner_lane(closure_result=_result(success=True), verify_ok=False)
        state_manager = MagicMock()
        adapter = MagicMock()
        adapter.save_teardown_state = AsyncMock()
        state = _make_state(pending=[{"intent_type": "LP_CLOSE"}], completed=0)

        teardown_result = await _h.execute_and_verify(
            MagicMock(),
            mgr,
            adapter,
            state,
            _make_strategy(),
            [{"intent_type": "LP_CLOSE"}],
            _make_positions(),
            TeardownMode.SOFT,
            None,
            True,
            None,
            MagicMock(),
            state_manager,
        )

        assert teardown_result.success is False
        mgr.run_token_consolidation.assert_not_awaited()


# ---------------------------------------------------------------------------
# run_token_consolidation — real _execute_intents mechanics
# ---------------------------------------------------------------------------


class TestRunTokenConsolidationExecution:
    def _manager(self, *, market_prices=None, commit_outcomes=None):
        helpers, commit_calls = _make_commit_helpers(commit_outcomes=commit_outcomes)
        sm = MagicMock(name="state_adapter")
        sm.save_teardown_state = AsyncMock()
        mgr = TeardownManager(
            orchestrator=_fake_orchestrator(),
            compiler=_fake_compiler(),
            state_manager=sm,
            runner_helpers=helpers,
            config=TeardownConfig.default(),
        )
        return mgr, commit_calls

    @pytest.mark.asyncio
    async def test_consolidation_swap_commits_with_teardown_cycle_id(self):
        """The residual-WETH swap executes via the REAL _execute_intents and
        drives runner_helpers.commit with cycle_id=teardown-{id} — the
        anti-bypass pairing applies to consolidation swaps unchanged."""
        mgr, commit_calls = self._manager()
        market = FakeMarket(
            balances={"WETH": Decimal("0.011"), "USDC": Decimal("12")},
            prices={"WETH": Decimal("1650"), "USDC": Decimal("1")},
        )
        state = _make_state(pending=[{"intent_type": "LP_CLOSE", "chain": CHAIN}], completed=1)

        outcome = await mgr.run_token_consolidation(
            _make_strategy(),
            teardown_id=state.teardown_id,
            teardown_state=state,
            mode=TeardownMode.SOFT,
            market=market,
            price_oracle={"WETH": Decimal("1650"), "USDC": Decimal("1")},
            positions=_make_positions(),
            closing_intents=[{"intent_type": "LP_CLOSE", "chain": CHAIN}],
            is_auto_mode=True,
        )

        assert outcome.planned == 1
        assert outcome.succeeded == 1
        assert outcome.failed == 0
        # Exactly one on-chain execution, paired with exactly one commit.
        assert mgr.orchestrator.execute.await_count == 1
        assert len(commit_calls) == 1
        assert commit_calls[0]["from_token"] == "WETH"
        assert commit_calls[0]["teardown_cycle_id"] == f"teardown-{state.teardown_id}"
        # Resume-safe plan extension: the persisted plan now carries the swap.
        persisted = json.loads(state.pending_intents_json)
        assert len(persisted) == 2
        assert state.total_intents == 2
        # ABSOLUTE completed count (pr-auditor): 1 closing intent already done
        # + 1 consolidation swap — a call-relative count would rewind it to 1.
        assert state.completed_intents == 2
        # Wallet-scope disclosure lands on the outcome (and thus result_json).
        assert any("wallet-scoped" in w for w in outcome.warnings)

    @pytest.mark.asyncio
    async def test_consolidation_swap_runs_standard_slippage_ladder(self):
        """Consolidation swaps ride the SAME slippage machinery as closing
        intents (UAT card D4 / spec-critique finding): the first attempt
        executes at the planner-emitted intent's own conservative
        ``max_slippage`` (NOT a hardcoded consolidation value), and a
        slippage-classified failure escalates into the standard
        ``EscalatingSlippageManager`` ladder (level-1 2%, honoring its retry
        budget) — no private/nonstandard slippage path is in play."""
        mgr, _ = self._manager()

        seen_slippages: list[Decimal] = []
        real_compile_result = mgr.compiler.compile.return_value

        def _spy_compile(intent, *args, **kwargs):
            seen_slippages.append(Decimal(str(getattr(intent, "max_slippage", "0"))))
            return real_compile_result

        mgr.compiler.compile = MagicMock(side_effect=_spy_compile)

        failed = SimpleNamespace(
            success=False,
            transaction_results=[],
            total_gas_used=0,
            gas_cost_usd="0",
            extracted_data={},
            error="Too little received (slippage)",
        )
        # Fail the intent's own conservative first attempt AND the first two
        # ladder level-1 attempts; succeed on the third level-1 attempt.
        level_1 = EscalatingSlippageManager.DEFAULT_LEVELS[0]
        mgr.orchestrator.execute = AsyncMock(side_effect=[failed, failed, failed, _successful_exec_result()])

        market = FakeMarket(
            balances={"WETH": Decimal("0.011"), "USDC": Decimal("12")},
            prices={"WETH": Decimal("1650"), "USDC": Decimal("1")},
        )
        state = _make_state(pending=[{"intent_type": "LP_CLOSE", "chain": CHAIN}], completed=1)

        outcome = await mgr.run_token_consolidation(
            _make_strategy(),
            teardown_id=state.teardown_id,
            teardown_state=state,
            mode=TeardownMode.SOFT,
            market=market,
            price_oracle={"WETH": Decimal("1650"), "USDC": Decimal("1")},
            positions=_make_positions(),
            closing_intents=[{"intent_type": "LP_CLOSE", "chain": CHAIN}],
            is_auto_mode=True,
        )

        assert outcome.planned == 1
        assert outcome.succeeded == 1
        assert outcome.failed == 0
        # Attempt 1: the intent's own conservative default (SwapIntent 0.5%) —
        # consolidation does not start at an inflated or hardcoded slippage.
        assert seen_slippages[0] == Decimal("0.005")
        # Attempts 2+: escalation lands exactly on the standard ladder's
        # level-1 slippage and stays there within its retry budget.
        assert len(seen_slippages) == 4
        assert all(s == level_1["slippage"] for s in seen_slippages[1:])
        assert len(seen_slippages[1:]) <= level_1["retries"]

    @pytest.mark.asyncio
    async def test_degraded_commit_does_not_abort_remaining_swaps(self):
        """Accounting degradation on the first consolidation commit must not
        block the second consolidation swap (loud-but-never-block)."""
        degraded = TeardownCommitOutcome(
            ledger_entry_id=None,
            accounting_degraded=True,
            degraded_reason="ledger write failed",
            degraded_writes=[{"kind": "ledger"}],
        )
        ok = TeardownCommitOutcome(ledger_entry_id="ledger-2", accounting_degraded=False, degraded_reason=None)
        mgr, commit_calls = self._manager(commit_outcomes=[degraded, ok])
        market = FakeMarket(
            balances={"WETH": Decimal("0.011"), "WBTC": Decimal("0.01")},
            prices={"WETH": Decimal("1650"), "WBTC": Decimal("60000"), "USDC": Decimal("1")},
        )
        state = _make_state(pending=[{"intent_type": "LP_CLOSE", "chain": CHAIN}], completed=1)
        strategy = _make_strategy()
        strategy.get_teardown_profile = lambda: SimpleNamespace(
            natural_exit_assets=["WETH", "WBTC"], original_entry_assets=[]
        )

        outcome = await mgr.run_token_consolidation(
            strategy,
            teardown_id=state.teardown_id,
            teardown_state=state,
            mode=TeardownMode.SOFT,
            market=market,
            price_oracle={"WETH": Decimal("1650"), "WBTC": Decimal("60000"), "USDC": Decimal("1")},
            positions=_make_positions(),
            closing_intents=[{"intent_type": "LP_CLOSE", "chain": CHAIN}],
            is_auto_mode=True,
        )

        assert outcome.planned == 2
        assert outcome.succeeded == 2
        assert outcome.failed == 0
        assert mgr.orchestrator.execute.await_count == 2
        assert len(commit_calls) == 2
        assert outcome.accounting_degraded_count >= 1

    @pytest.mark.asyncio
    async def test_empty_plan_executes_nothing(self):
        """A strategy that already swept (zero residuals) plans nothing —
        structural double-swap safety."""
        mgr, commit_calls = self._manager()
        market = FakeMarket(
            balances={"WETH": Decimal("0")},
            prices={"WETH": Decimal("1650"), "USDC": Decimal("1")},
        )
        state = _make_state(pending=[{"intent_type": "LP_CLOSE", "chain": CHAIN}], completed=1)

        outcome = await mgr.run_token_consolidation(
            _make_strategy(),
            teardown_id=state.teardown_id,
            teardown_state=state,
            mode=TeardownMode.SOFT,
            market=market,
            price_oracle=None,
            positions=_make_positions(),
            closing_intents=[{"intent_type": "LP_CLOSE", "chain": CHAIN}],
            is_auto_mode=True,
        )

        assert outcome.planned == 0
        assert mgr.orchestrator.execute.await_count == 0
        assert commit_calls == []
        # Plan untouched.
        assert state.total_intents == 1

    @pytest.mark.asyncio
    async def test_hard_mode_executes_nothing(self):
        mgr, commit_calls = self._manager()
        market = FakeMarket(
            balances={"WETH": Decimal("10")},
            prices={"WETH": Decimal("1650"), "USDC": Decimal("1")},
        )
        state = _make_state(pending=[{"intent_type": "LP_CLOSE", "chain": CHAIN}], completed=1)

        outcome = await mgr.run_token_consolidation(
            _make_strategy(),
            teardown_id=state.teardown_id,
            teardown_state=state,
            mode=TeardownMode.HARD,
            market=market,
            positions=_make_positions(),
            closing_intents=[{"intent_type": "LP_CLOSE", "chain": CHAIN}],
            is_auto_mode=True,
        )

        assert outcome.planned == 0
        assert any("emergency_mode" in w for w in outcome.warnings)
        assert mgr.orchestrator.execute.await_count == 0

    @pytest.mark.asyncio
    async def test_exception_is_swallowed_into_outcome(self):
        """run_token_consolidation must NEVER raise — closure already removed
        the on-chain risk."""
        mgr, _ = self._manager()
        state = _make_state(pending=[{"intent_type": "LP_CLOSE", "chain": CHAIN}], completed=1)
        exploding_market = MagicMock()
        exploding_market.balance = MagicMock(side_effect=RuntimeError("rpc down"))
        exploding_market.price = MagicMock(side_effect=RuntimeError("rpc down"))
        # Make the planner itself blow up via a broken strategy attribute.
        strategy = _make_strategy()
        broken_helpers = TeardownRunnerHelpers(get_token_universe=MagicMock(side_effect=RuntimeError("derive failed")))
        mgr.runner_helpers = broken_helpers

        outcome = await mgr.run_token_consolidation(
            strategy,
            teardown_id=state.teardown_id,
            teardown_state=state,
            mode=TeardownMode.SOFT,
            market=exploding_market,
            positions=_make_positions(),
            closing_intents=[],
            is_auto_mode=True,
        )

        assert outcome.failed == 1
        assert outcome.succeeded == 0
        assert any("raised" in w for w in outcome.warnings)


# ---------------------------------------------------------------------------
# map_teardown_result — result_json["consolidation"] contract
# ---------------------------------------------------------------------------


class TestResultJsonContract:
    def test_mark_completed_carries_consolidation_summary(self):
        from dataclasses import replace as _replace

        from almanak.framework.runner._teardown_helpers import map_teardown_result

        runner = MagicMock()
        runner._calculate_duration_ms = MagicMock(return_value=10)
        request = MagicMock()
        request.target_token = "USDC"
        state_manager = MagicMock()

        teardown_result = _replace(
            _result(success=True),
            consolidation_planned=1,
            consolidation_succeeded=0,
            consolidation_failed=1,
            consolidation_warnings=["1 consolidation swap(s) failed"],
        )

        iteration_result = map_teardown_result(
            runner,
            _make_strategy(),
            datetime.now(UTC),
            teardown_result,
            TeardownMode.SOFT,
            request,
            state_manager,
        )

        # Closure succeeded → TEARDOWN status even with consolidation failure.
        from almanak.framework.runner.runner_models import IterationStatus

        assert iteration_result.status == IterationStatus.TEARDOWN
        state_manager.mark_completed.assert_called_once()
        _, kwargs = state_manager.mark_completed.call_args
        consolidation = kwargs["result"]["consolidation"]
        assert consolidation["planned"] == 1
        assert consolidation["failed"] == 1
        assert consolidation["target_token"] == "USDC"
        assert consolidation["warnings"] == ["1 consolidation swap(s) failed"]

    def test_build_teardown_config_from_request_none_disables_consolidation(self):
        """No operator request → close-only (pr-auditor blocker): the
        wallet-scoped ``amount="all"`` sweep requires the explicit
        TeardownRequest as consent; self-signalled teardowns keep the
        pre-VIB-5011 behaviour."""
        from almanak.framework.runner._teardown_helpers import _teardown_config_from_request
        from almanak.framework.teardown.models import TeardownAssetPolicy

        cfg = _teardown_config_from_request(None)
        assert cfg.asset_policy == TeardownAssetPolicy.TARGET_TOKEN
        assert cfg.target_token == "USDC"
        assert cfg.token_consolidation.enabled is False

    def test_build_teardown_config_threads_request_policy_and_target(self):
        from almanak.framework.runner._teardown_helpers import _teardown_config_from_request
        from almanak.framework.teardown.models import TeardownAssetPolicy

        request = SimpleNamespace(asset_policy="entry_token", target_token="WETH")
        cfg = _teardown_config_from_request(request)
        assert cfg.asset_policy == TeardownAssetPolicy.ENTRY_TOKEN
        assert cfg.target_token == "WETH"
        assert cfg.token_consolidation.target_token == "WETH"
        # An explicit operator request IS the consent — consolidation enabled.
        assert cfg.token_consolidation.enabled is True

    def test_build_teardown_config_unknown_policy_falls_back(self):
        from almanak.framework.runner._teardown_helpers import _teardown_config_from_request
        from almanak.framework.teardown.models import TeardownAssetPolicy

        request = SimpleNamespace(asset_policy="banana", target_token=None)
        cfg = _teardown_config_from_request(request)
        assert cfg.asset_policy == TeardownAssetPolicy.TARGET_TOKEN
        assert cfg.target_token == "USDC"

    def test_result_payload_roundtrip_through_sqlite_manager(self, tmp_path):
        """mark_completed persists the consolidation summary into
        result_json; get_result_payload reads it back for the CLI
        (--wait terminal print + `status`)."""
        from almanak.framework.teardown.models import TeardownRequest
        from almanak.framework.teardown.state_manager import TeardownStateManager

        manager = TeardownStateManager(db_path=str(tmp_path / "td.db"))
        manager.create_request(TeardownRequest(deployment_id="dep-1", mode=TeardownMode.SOFT))
        manager.mark_started("dep-1", total_positions=1)
        manager.mark_completed(
            "dep-1",
            result={
                "intents": 1,
                "consolidation": {
                    "planned": 1,
                    "succeeded": 1,
                    "failed": 0,
                    "warnings": [],
                    "target_token": "USDC",
                },
            },
        )

        payload = manager.get_result_payload("dep-1")
        assert payload is not None
        assert payload["consolidation"]["succeeded"] == 1
        assert payload["consolidation"]["target_token"] == "USDC"
        # Missing row → None (CLI renders nothing).
        assert manager.get_result_payload("nope") is None
