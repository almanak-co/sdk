"""Every refusal the run-validity classifier depends on leaves a ledger entry (ALM-3496).

The classifier sees only what the ledgers record: a decision-input refusal that
returns empty or raises without a ``decision_input_failures`` entry, a pending
intent that vanishes from both the trade and failure ledgers, or a coverage
ratio that reads perfect when nothing was looked up all let a hollow run
classify VALID.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.exceptions import NoAcceptableDataSourceError
from almanak.framework.backtesting.models import DataQualityReport
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl._engine_helpers import (
    _drain_pending_intents_at_end,
    enforce_data_quality_gate,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_quality import DataQualityTracker
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.error_handling import BacktestErrorHandler, HandleErrorResult
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.market import MarketSnapshot
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from tests.unit.backtesting.pnl.test_pending_intent_execution import (
    MockDataProviderWithTicks,
    MockStrategy,
    MockSwapIntent,
    RecordingStrategy,
    _make_backtester,
    _market_state,
)

POOL = "0x" + "1" * 40


def _snapshot() -> MarketSnapshot:
    from almanak.framework.market.builders import MarketSnapshotBuilder

    return MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address="0x" + "0" * 40)


def _recorded_sources(snapshot: MarketSnapshot) -> set[str]:
    return {source for source, _key in snapshot._critical_data_failures}


class _RaisingRateHistoryReader:
    def get_lending_rate_history(self, **_: Any) -> Any:
        raise RuntimeError("rate history lane down")

    def get_funding_rate_history(self, **_: Any) -> Any:
        raise RuntimeError("funding history lane down")


class _EmptyPoolRegistry:
    def protocols_for_chain(self, _chain: str) -> list[str]:
        return []


class _RaisingPoolReader:
    async def get_pool_reserves(self, _pool: str, _chain: str) -> Any:
        raise RuntimeError("reserves lane down")


class _RaisingFundingProvider:
    async def get_funding_rate(self, *_: Any) -> Any:
        raise RuntimeError("funding lane down")


class TestSnapshotRefusalsAreRecorded:
    def test_perp_market_without_discovery_records(self):
        from almanak.framework.market.errors import MarketSnapshotError

        snapshot = _snapshot()
        with pytest.raises(MarketSnapshotError):
            snapshot.perp_market("unregistered_venue", "ETH-USD")
        assert ("perp_market", "unregistered_venue:ETH-USD@arbitrum") in snapshot._critical_data_failures

    def test_reference_price_unavailable_response_records(self):
        from unittest.mock import MagicMock

        from almanak.gateway.proto import gateway_pb2

        snapshot = _snapshot()
        snapshot._reference_price_refusal_detail = None
        client = MagicMock()
        client.is_connected = True
        client.market.GetReferencePrice.return_value = SimpleNamespace(
            availability=gateway_pb2.REFERENCE_PRICE_AVAILABILITY_UNMEASURED,
            reason="market closed",
            instrument="AAPL",
            quote="USD",
            chain="arbitrum",
            source="feed",
            observed_at=0,
            market_status=0,
            market_status_as_of=0,
            market_status_source="",
        )
        snapshot._gateway_client = client

        result = snapshot.reference_price("AAPL")

        assert result.price is None
        assert snapshot._critical_data_failures[("reference_price", "AAPL@arbitrum")] == "market closed"

    def test_perp_positions_gateway_fall_through_records(self):
        snapshot = _snapshot()
        read = snapshot.perp_positions("gmx_v2")
        assert read.ok is False
        assert ("perp_positions", "gmx_v2@arbitrum") in snapshot._critical_data_failures

    def test_lending_position_balances_unmeasured_records(self):
        snapshot = _snapshot()
        assert snapshot.lending_position_balances("aave_v3", "USDC") == (None, None)
        assert ("lending_position_balances", "no_gateway") in snapshot._critical_data_failures

    def test_funding_rate_unknown_venue_and_provider_failure_record(self):
        snapshot = _snapshot()
        snapshot._funding_rate_provider = _RaisingFundingProvider()
        with pytest.raises(ValueError):
            snapshot.funding_rate("not_a_venue", "ETH-USD")
        assert "unsupported venue" in snapshot._critical_data_failures[("funding_rate", "not_a_venue:ETH-USD")]

        with pytest.raises(RuntimeError):
            snapshot.funding_rate("hyperliquid", "ETH-USD")
        assert "funding lane down" in snapshot._critical_data_failures[("funding_rate", "hyperliquid:ETH-USD")]

    def test_pool_price_lanes_record(self):
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        snapshot = _snapshot()
        snapshot._pool_reader_registry = _EmptyPoolRegistry()
        with pytest.raises(PoolPriceUnavailableError):
            snapshot.pool_price(POOL)
        with pytest.raises(PoolPriceUnavailableError):
            snapshot.pool_price_by_pair("WETH", "USDC")
        assert ("pool_price", POOL) in snapshot._critical_data_failures
        assert ("pool_price_by_pair", "WETH/USDC") in snapshot._critical_data_failures

    def test_pool_reserves_failure_records(self):
        from almanak.framework.data.market_snapshot import PoolReservesUnavailableError

        snapshot = _snapshot()
        snapshot._pool_reader = _RaisingPoolReader()
        with pytest.raises(PoolReservesUnavailableError):
            snapshot.pool_reserves(POOL)
        assert "reserves lane down" in snapshot._critical_data_failures[("pool_reserves", POOL)]

    def test_rate_history_failures_record(self):
        from almanak.framework.data.market_snapshot import (
            FundingRateHistoryUnavailableError,
            LendingRateHistoryUnavailableError,
        )

        snapshot = _snapshot()
        snapshot._rate_history_reader = _RaisingRateHistoryReader()
        with pytest.raises(LendingRateHistoryUnavailableError):
            snapshot.lending_rate_history("aave_v3", "USDC")
        with pytest.raises(FundingRateHistoryUnavailableError):
            snapshot.funding_rate_history("hyperliquid", "ETH-USD")
        assert ("lending_rate_history", "aave_v3:USDC@arbitrum") in snapshot._critical_data_failures
        assert ("funding_rate_history", "hyperliquid:ETH-USD") in snapshot._critical_data_failures
        assert {"lending_rate_history", "funding_rate_history"} <= _recorded_sources(snapshot)


def _config(start: datetime, **overrides: Any) -> PnLBacktestConfig:
    params: dict[str, Any] = {
        "start_time": start,
        "end_time": start + timedelta(hours=2),
        "token_funding": _pnl_token_funding(Decimal("10000")),
        "tokens": ["WETH", "USDC"],
        "include_gas_costs": False,
    }
    params.update(overrides)
    return PnLBacktestConfig(**params)


def _missing_price() -> NoAcceptableDataSourceError:
    return NoAcceptableDataSourceError(
        data_type="price",
        identifier="WETH",
        remediation="provide a measured WETH price",
    )


class TestPendingIntentMissingData:
    @pytest.mark.asyncio
    async def test_non_fatal_missing_data_is_a_typed_rejection_with_a_ledger_entry(self, monkeypatch):
        """A tolerated missing-data refusal must not vanish from both ledgers."""
        start = datetime.now(UTC)
        backtester = PnLBacktester(
            data_provider=MockDataProviderWithTicks(num_ticks=2, start_time=start),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )
        intent = MockSwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"))
        strategy = MockStrategy(intents_to_return=[intent])

        async def raise_missing_data(*_: Any, **__: Any) -> Any:
            raise _missing_price()

        monkeypatch.setattr(backtester, "_execute_intent", raise_missing_data)
        # The default policy stops on missing data; a tolerant policy is the
        # branch under test.
        monkeypatch.setattr(
            BacktestErrorHandler,
            "handle_error",
            lambda self, error, context="": HandleErrorResult(should_continue=True),
        )

        result = await backtester.backtest(strategy, _config(start, inclusion_delay_blocks=0))

        assert len(result.trades) == 1
        assert result.trades[0].success is False
        assert result.trades[0].metadata["rejection_code"] == "missing_data"
        assert result.decision_summary is not None
        assert result.decision_summary["executions"] == {"fills": 0, "rejected": 1}
        assert result.run_validity is not None
        assert result.run_validity.validity.value == "INVALID"
        assert result.run_validity.reason_codes == ("FAMILY_ALL_REJECTED",)
        assert result.error is not None and result.error.startswith("BACKTEST_EXECUTION_REJECTED:")
        lanes = {(entry["source"], entry["key"]) for entry in result.decision_input_failures or []}
        assert ("execution:price", "WETH") in lanes

    @pytest.mark.asyncio
    async def test_drain_at_end_non_fatal_missing_data_is_rejected(self, monkeypatch):
        now = datetime.now(UTC)
        backtester = _make_backtester()
        backtester._error_handler = SimpleNamespace(
            handle_error=lambda error, context="": HandleErrorResult(should_continue=True)
        )
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        intent = MockSwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100"))
        strategy = RecordingStrategy()
        state = SimpleNamespace(
            pending_intents=[(intent, now, 1)],
            last_market_state=_market_state(now),
            portfolio=portfolio,
            data_quality_tracker=None,
            execution_delayed_at_end=0,
        )

        async def raise_missing_data(*_: Any, **__: Any) -> Any:
            raise _missing_price()

        monkeypatch.setattr(backtester, "_execute_intent", raise_missing_data)

        await _drain_pending_intents_at_end(
            backtester=backtester,
            strategy=strategy,
            config=_config(now),
            bt_logger=BacktestLogger(backtest_id="missing-data-drain"),
            state=state,
        )

        assert len(portfolio.trades) == 1
        assert portfolio.trades[0].success is False
        assert portfolio.trades[0].delayed_at_end is True
        assert portfolio.trades[0].metadata["rejection_code"] == "missing_data"
        assert state.execution_delayed_at_end == 1
        assert backtester._execution_input_failures == {
            ("execution:price", "WETH"): f"MockSwapIntent not executed: {_missing_price()}"
        }
        assert strategy.executed_callbacks[0][1] is False

    @pytest.mark.asyncio
    async def test_drain_without_market_state_rejects_every_pending_intent(self):
        """The no-market-state bail is a rejection per intent, never a silent drop."""
        now = datetime.now(UTC)
        backtester = _make_backtester()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        intents = [
            MockSwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100")),
            MockSwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("200")),
        ]
        strategy = RecordingStrategy()
        state = SimpleNamespace(
            pending_intents=[(intent, now, 1) for intent in intents],
            last_market_state=None,
            portfolio=portfolio,
            data_quality_tracker=None,
            execution_delayed_at_end=0,
        )

        await _drain_pending_intents_at_end(
            backtester=backtester,
            strategy=strategy,
            config=_config(now),
            bt_logger=BacktestLogger(backtest_id="no-market-state-drain"),
            state=state,
        )

        assert [trade.success for trade in portfolio.trades] == [False, False]
        assert {trade.metadata["rejection_code"] for trade in portfolio.trades} == {"no_market_state"}
        assert all(trade.delayed_at_end for trade in portfolio.trades)
        assert state.execution_delayed_at_end == 2
        assert [success for _, success, _ in strategy.executed_callbacks] == [False, False]


class TestCoverageIsUnmeasuredOnZeroLookups:
    def test_tracker_and_report_carry_none(self):
        tracker = DataQualityTracker()
        assert tracker.coverage_ratio is None
        report = tracker.to_data_quality_report()
        assert report.coverage_ratio is None
        assert report.to_dict()["coverage_ratio"] is None
        assert DataQualityReport.from_dict(report.to_dict()).coverage_ratio is None
        assert DataQualityReport.from_dict({}).coverage_ratio is None
        assert DataQualityReport.from_dict({"coverage_ratio": "0.5"}).coverage_ratio == Decimal("0.5")

        tracker.record_lookup(success=True, source="x")
        assert tracker.coverage_ratio == Decimal("1")

    def _state(self, ticks: int) -> SimpleNamespace:
        return SimpleNamespace(data_quality_tracker=DataQualityTracker(), tick_count=ticks, compliance_violations=[])

    def test_gate_fails_a_ticked_run_that_measured_nothing(self):
        now = datetime.now(UTC)
        state = self._state(ticks=12)
        enforce_data_quality_gate(config=_config(now), bt_logger=BacktestLogger(backtest_id="gate"), state=state)
        assert state.compliance_violations == [
            "Data coverage unmeasured: no price lookups were recorded over 12 tick(s)"
        ]

        strict = self._state(ticks=12)
        with pytest.raises(ValueError, match="unmeasured"):
            enforce_data_quality_gate(
                config=_config(now, institutional_mode=True),
                bt_logger=BacktestLogger(backtest_id="gate"),
                state=strict,
            )
        assert len(strict.compliance_violations) == 1

    def test_gate_leaves_an_empty_run_to_the_no_ticks_verdict(self):
        now = datetime.now(UTC)
        state = self._state(ticks=0)
        enforce_data_quality_gate(
            config=_config(now, institutional_mode=True),
            bt_logger=BacktestLogger(backtest_id="gate"),
            state=state,
        )
        assert state.compliance_violations == []

    @pytest.mark.asyncio
    async def test_institutional_gate_failure_is_an_error_result_not_an_escape(self, monkeypatch):
        start = datetime.now(UTC)
        backtester = PnLBacktester(
            data_provider=MockDataProviderWithTicks(num_ticks=2, start_time=start),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        def gate_boom(**_: Any) -> None:
            raise ValueError("Data quality gate failed in institutional mode: synthetic")

        monkeypatch.setattr(_engine_helpers, "enforce_data_quality_gate", gate_boom)

        result = await backtester.backtest(MockStrategy(intents_to_return=[None]), _config(start))

        assert result.success is False
        assert "synthetic" in (result.error or "")
        assert result.run_validity is not None
        assert result.run_validity.reason_codes == ("ENGINE_ERROR",)
