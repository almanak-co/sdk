"""Behavioral tests for ``PlanExecutor.refresh_stale_quotes``.

Covers every branch of the stale-quote refresh loop:

- no quote provider configured -> empty result, no refresh attempted
- no stale quotes -> empty result
- stale steps refreshed through the bridge quote provider (UNCHANGED vs
  CHANGED results, re-pinned quotes)
- stale step ids that no longer resolve to a step, or resolve to a step
  without a pinned quote, are skipped
- custom threshold_seconds forwarded to staleness checking

No network — the bridge quote provider is an AsyncMock.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from almanak.framework.execution.plan import PlanBundle, PlanStep
from almanak.framework.execution.plan_executor import (
    PlanExecutor,
    PlanExecutorConfig,
    QuoteRefreshResult,
)


def _quote(output_amount: str = "995") -> dict:
    return {
        "bridge_name": "across",
        "token": "USDC",
        "input_amount": "1000",
        "output_amount": output_amount,
        "from_chain": "arbitrum",
        "to_chain": "base",
        "slippage_tolerance": "0.005",
    }


def _stale_step(step_id: str = "step-1", age_seconds: int = 3600) -> PlanStep:
    step = PlanStep(step_id=step_id, chain="arbitrum", intent={"type": "BRIDGE"})
    step.artifacts.pin_quote(_quote())
    step.artifacts.pinned_at = datetime.now(UTC) - timedelta(seconds=age_seconds)
    return step


def _executor(quote_provider=None) -> PlanExecutor:
    return PlanExecutor(config=PlanExecutorConfig(), quote_provider=quote_provider)


def _run(coro):
    return asyncio.run(coro)


class TestRefreshStaleQuotes:
    def test_no_quote_provider_returns_empty(self) -> None:
        executor = _executor(quote_provider=None)
        plan = PlanBundle(plan_id="p1", steps=[_stale_step()])

        assert _run(executor.refresh_stale_quotes(plan)) == []

    def test_no_stale_quotes_returns_empty(self) -> None:
        provider = AsyncMock()
        executor = _executor(quote_provider=provider)
        fresh = PlanStep(step_id="fresh", chain="arbitrum", intent={"type": "BRIDGE"})
        fresh.artifacts.pin_quote(_quote())  # pinned just now -> not stale
        plan = PlanBundle(plan_id="p1", steps=[fresh])

        assert _run(executor.refresh_stale_quotes(plan)) == []
        provider.get_quote.assert_not_awaited()

    def test_unchanged_quote_refreshed(self) -> None:
        provider = AsyncMock()
        provider.get_quote.return_value = _quote()  # identical parameters
        executor = _executor(quote_provider=provider)
        step = _stale_step()
        plan = PlanBundle(plan_id="p1", steps=[step])

        results = _run(executor.refresh_stale_quotes(plan))

        assert len(results) == 1
        assert results[0].step_id == "step-1"
        assert results[0].result == QuoteRefreshResult.UNCHANGED
        assert results[0].change_details is None
        # Quote was re-pinned at refresh time (no longer stale).
        assert plan.get_stale_quote_steps(3000) == []
        provider.get_quote.assert_awaited_once_with(
            token="USDC",
            amount=Decimal("1000"),
            from_chain="arbitrum",
            to_chain="base",
            max_slippage=Decimal("0.005"),
        )

    def test_changed_quote_reports_change_details(self) -> None:
        provider = AsyncMock()
        # A different bridge is a hash-relevant parameter change (the quote
        # hash covers bridge/token/amount/chains/slippage, not output_amount).
        new_quote = {**_quote(output_amount="950"), "bridge_name": "stargate"}
        provider.get_quote.return_value = new_quote
        executor = _executor(quote_provider=provider)
        step = _stale_step()
        original = dict(step.artifacts.pinned_quote)
        plan = PlanBundle(plan_id="p1", steps=[step])

        results = _run(executor.refresh_stale_quotes(plan))

        assert len(results) == 1
        assert results[0].result == QuoteRefreshResult.CHANGED
        assert results[0].original_quote == original
        assert results[0].new_quote["output_amount"] == "950"
        assert results[0].change_details is not None
        # New quote is pinned on the step.
        assert step.artifacts.pinned_quote["bridge_name"] == "stargate"

    def test_missing_step_and_unpinned_step_are_skipped(self) -> None:
        provider = AsyncMock()
        provider.get_quote.return_value = _quote()
        executor = _executor(quote_provider=provider)

        no_quote_step = PlanStep(step_id="no-quote", chain="arbitrum", intent={"type": "BRIDGE"})
        stale = _stale_step(step_id="stale-real")
        plan = PlanBundle(plan_id="p1", steps=[no_quote_step, stale])

        # Force the staleness check to also report a ghost id and a step
        # without a pinned quote — both must be skipped without refreshing.
        with patch.object(
            executor,
            "check_quote_staleness",
            return_value=["ghost-step", "no-quote", "stale-real"],
        ):
            results = _run(executor.refresh_stale_quotes(plan))

        assert [r.step_id for r in results] == ["stale-real"]
        provider.get_quote.assert_awaited_once()

    def test_custom_threshold_forwarded(self) -> None:
        provider = AsyncMock()
        provider.get_quote.return_value = _quote()
        executor = _executor(quote_provider=provider)
        # 100s old: stale under a 50s threshold, fresh under the 300s default.
        step = _stale_step(age_seconds=100)
        plan = PlanBundle(plan_id="p1", steps=[step])

        assert _run(executor.refresh_stale_quotes(plan)) == []

        results = _run(executor.refresh_stale_quotes(plan, threshold_seconds=50))
        assert [r.step_id for r in results] == ["step-1"]
