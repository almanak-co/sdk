"""Branch coverage for MultiChainOrchestrator.execute_sequence.

Covers ordering, amount='all' chaining (tx-result and intent-amount
fallbacks), the first-step chained-amount guard, stop_on_failure vs
skip semantics, and the defensive no-previous-amount branch. The
per-intent ``execute`` call is faked; no chain, no gateway.
"""

import asyncio
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.multichain import (
    ExecutionStatus,
    IntentExecutionResult,
    MultiChainOrchestrator,
)
from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import InvalidAmountError


@pytest.fixture
def orchestrator():
    return MultiChainOrchestrator(config=MagicMock())


def _swap(amount, chain="base"):
    return Intent.swap("USDC", "WETH", amount=amount, chain=chain)


def _wire_execute(orchestrator, monkeypatch, *, fail_ids=frozenset(), tx_results=None):
    """Fake per-intent execution; scripted failures and tx_results by intent id."""
    executed = []

    async def _execute(intent, build_tx_func=None, *, price_map=None, price_oracle=None):
        executed.append(intent)
        failed = intent.intent_id in fail_ids
        return IntentExecutionResult(
            intent=intent,
            chain=intent.chain,
            status=ExecutionStatus.FAILED if failed else ExecutionStatus.SUCCESS,
            tx_result=(tx_results or {}).get(intent.intent_id),
            error="simulated failure" if failed else None,
        )

    monkeypatch.setattr(orchestrator, "execute", _execute)
    return executed


class TestExecuteSequence:
    def test_empty_sequence(self, orchestrator):
        result = asyncio.run(orchestrator.execute_sequence([]))
        assert result.results == []
        assert result.success
        assert result.total_execution_time_ms == 0.0

    def test_chained_amount_on_first_step_raises(self, orchestrator):
        with pytest.raises(InvalidAmountError, match="first step"):
            asyncio.run(orchestrator.execute_sequence([_swap("all")]))

    def test_executes_in_order(self, orchestrator, monkeypatch):
        first, second = _swap(Decimal("100")), _swap(Decimal("50"))
        executed = _wire_execute(orchestrator, monkeypatch)
        result = asyncio.run(orchestrator.execute_sequence([first, second]))
        assert executed == [first, second]
        assert result.success
        assert result.successful_count == 2

    def test_all_resolves_from_tx_result(self, orchestrator, monkeypatch):
        first, chained = _swap(Decimal("100")), _swap("all")
        tx = MagicMock()
        tx.actual_amount_received = Decimal("97.5")
        executed = _wire_execute(
            orchestrator, monkeypatch, tx_results={first.intent_id: tx}
        )
        result = asyncio.run(orchestrator.execute_sequence([first, chained]))
        assert result.success
        # The second executed intent is a resolved copy carrying the actual
        # received amount, not the "all" sentinel.
        assert executed[1].amount == Decimal("97.5")
        assert not executed[1].is_chained_amount

    def test_all_falls_back_to_previous_intent_amount(self, orchestrator, monkeypatch):
        first, chained = _swap(Decimal("100")), _swap("all")
        executed = _wire_execute(orchestrator, monkeypatch)
        result = asyncio.run(orchestrator.execute_sequence([first, chained]))
        assert result.success
        assert executed[1].amount == Decimal("100")

    def test_all_without_tracking_fails_step(self, orchestrator, monkeypatch):
        # A hold step succeeds but exposes no amount, so the following
        # amount="all" step cannot resolve and fails defensively.
        hold = Intent.hold(reason="wait", chain="base")
        chained = _swap("all")
        trailing = _swap(Decimal("10"))
        executed = _wire_execute(orchestrator, monkeypatch)
        result = asyncio.run(
            orchestrator.execute_sequence([hold, chained, trailing], stop_on_failure=True)
        )
        assert not result.success
        assert executed == [hold]
        assert result.results[1].status == ExecutionStatus.FAILED
        assert "no previous step amount" in result.results[1].error
        # stop_on_failure: the trailing intent is never reached.
        assert len(result.results) == 2

    def test_stop_on_failure_halts_sequence(self, orchestrator, monkeypatch):
        first, second, third = _swap(Decimal("1")), _swap(Decimal("2")), _swap(Decimal("3"))
        executed = _wire_execute(orchestrator, monkeypatch, fail_ids={second.intent_id})
        result = asyncio.run(
            orchestrator.execute_sequence([first, second, third], stop_on_failure=True)
        )
        assert executed == [first, second]
        assert len(result.results) == 2
        assert not result.success

    def test_continue_on_failure_skips_remaining(self, orchestrator, monkeypatch):
        first, second, third = _swap(Decimal("1")), _swap(Decimal("2")), _swap(Decimal("3"))
        executed = _wire_execute(orchestrator, monkeypatch, fail_ids={first.intent_id})
        result = asyncio.run(
            orchestrator.execute_sequence([first, second, third], stop_on_failure=False)
        )
        assert executed == [first]
        assert [r.status for r in result.results] == [
            ExecutionStatus.FAILED,
            ExecutionStatus.SKIPPED,
            ExecutionStatus.SKIPPED,
        ]
        assert result.results[1].error == "Skipped due to previous failure in sequence"
        assert result.results[1].chain == "base"
