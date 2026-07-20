"""ALM-2943 decision-input serve batch, part C: pure-math calculators wired,
``prices`` dict-style alias resolution, il_exposure default on the typed
data-level failures only, and once-per-run soft-empty ledger notes.

Covers the audit gaps from .logs/2026-07-13/DECISION-INPUT-AUDIT.md rows:
``projected_il`` / ``il_exposure`` / ``portfolio_risk`` / ``rolling_sharpe``
(REFUSED "no calculator" though pure math), ``prices`` dict-style (silent
symbol miss on address-keyed backtest snapshots), and ``wallet_activity`` /
``prediction_price`` (documented-soft empties with no ledger).
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.pnl.engine import (
    create_market_snapshot_from_state,
    sync_il_calculator_positions,
)

WETH_ADDR = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ADDR = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
TOKEN_ADDRESSES = {"WETH": ("ethereum", WETH_ADDR), "USDC": ("ethereum", USDC_ADDR)}


def _address_keyed_state(weth_price: str = "1650", timestamp: datetime | None = None):
    from almanak.framework.backtesting.pnl.data_provider import MarketState

    return MarketState(
        timestamp=timestamp or datetime(2026, 6, 20, tzinfo=UTC),
        chain="ethereum",
        prices={
            ("ethereum", WETH_ADDR): Decimal(weth_price),
            ("ethereum", USDC_ADDR): Decimal("1"),
        },
    )


def _backtest_snapshot(**kwargs):
    return create_market_snapshot_from_state(
        _address_keyed_state(),
        chain="ethereum",
        token_addresses=TOKEN_ADDRESSES,
        **kwargs,
    )


class TestPureMathCalculatorsWired:
    """projected_il / portfolio_risk / rolling_sharpe serve without any wiring."""

    def test_projected_il_serves_and_matches_hand_check(self):
        snapshot = _backtest_snapshot()
        # Constant-product IL for +50%: 2*sqrt(1.5)/2.5 - 1 = -0.020204...
        result = snapshot.projected_il("WETH", "USDC", Decimal("50"))
        assert result.il_ratio == Decimal("-0.020204")
        assert result.il_bps == -202

    def test_projected_il_symmetric_range_hand_check(self):
        # IL depends only on the price RATIO: +100% (r=2) and -50% (r=0.5)
        # are reciprocal ratios and constant-product IL is symmetric under
        # r -> 1/r, so both must give exactly the same IL.
        snapshot = _backtest_snapshot()
        up = snapshot.projected_il("WETH", "USDC", Decimal("100"))
        down = snapshot.projected_il("WETH", "USDC", Decimal("-50"))
        assert up.il_ratio == down.il_ratio == Decimal("-0.057191")
        assert up.il_ratio < 0 and down.il_ratio < 0

    def test_projected_il_invalid_args_still_refuse(self):
        snapshot = _backtest_snapshot()
        with pytest.raises(ValueError):
            snapshot.projected_il("WETH", "USDC", Decimal("-100"))

    def test_portfolio_risk_serves_on_caller_series(self):
        snapshot = _backtest_snapshot()
        series = [0.01, -0.02, 0.005] * 10  # >= MIN_OBSERVATIONS (30)
        envelope = snapshot.portfolio_risk(series, total_value_usd=Decimal("1000"))
        risk = envelope.value
        assert risk.conventions.sample_count == 30
        assert risk.max_drawdown > 0
        assert risk.var_95 != 0

    def test_rolling_sharpe_constant_return_series_is_zero(self):
        # Constant returns -> zero variance -> Sharpe pinned to 0.0 for
        # every window (the calculator's documented zero-std behavior).
        snapshot = _backtest_snapshot()
        envelope = snapshot.rolling_sharpe([0.01] * 40, window_days=30, return_interval="1d")
        result = envelope.value
        assert len(result.entries) == 11  # 40 obs, 30-per-window -> 11 windows
        assert all(entry.sharpe == 0.0 for entry in result.entries)

    def test_run_scoped_calculators_are_used_when_passed(self):
        from almanak.framework.data.lp import ILCalculator, LPPosition

        il_calculator = ILCalculator()
        il_calculator.add_position(
            LPPosition(
                position_id="pos-1",
                pool_address="0xpool",
                token_a="WETH",
                token_b="USDC",
                entry_price_a=Decimal("1500"),
                entry_price_b=Decimal("1"),
                amount_a=Decimal("1"),
                amount_b=Decimal("1500"),
            )
        )
        snapshot = _backtest_snapshot(il_calculator=il_calculator)
        exposure = snapshot.il_exposure("pos-1")
        assert exposure.position_id == "pos-1"
        # Price moved 1500 -> 1650 (+10%): constant-product IL is negative.
        assert exposure.current_il.il_ratio < 0


class TestILExposureDefaultContract:
    """default=... covers ONLY the typed data-level failures (PositionNotFound +
    typed CalcILExposureError). A missing calculator and unexpected errors stay
    loud regardless of ``default`` — backtest now WIRES the calculator
    (ALM-2943), so the missing-calculator branch is unreachable in backtest and
    the strict raise protects live wiring errors (contract pinned in
    tests/framework/market/test_builder_calculator_wiring.py)."""

    SENTINEL = object()

    def _snapshot_without_calculator(self):
        from almanak.framework.market.builders import MarketSnapshotBuilder

        return MarketSnapshotBuilder.seeded(chain="ethereum")

    def test_missing_calculator_raises_even_with_default_and_records_ledger(self):
        snapshot = self._snapshot_without_calculator()
        with pytest.raises(ValueError, match="No IL calculator"):
            snapshot.il_exposure("pid", default=None)
        # The wiring gap is still recorded on the run report before raising.
        assert ("il_exposure", "unconfigured") in snapshot._critical_data_failures

    def test_missing_calculator_without_default_still_raises(self):
        snapshot = self._snapshot_without_calculator()
        with pytest.raises(ValueError, match="No IL calculator"):
            snapshot.il_exposure("pid")

    def test_position_not_found_returns_default(self):
        snapshot = _backtest_snapshot()
        assert snapshot.il_exposure("nonexistent", default=None) is None

    def test_position_not_found_without_default_raises(self):
        from almanak.framework.data.market_snapshot import ILExposureUnavailableError

        snapshot = _backtest_snapshot()
        with pytest.raises(ILExposureUnavailableError):
            snapshot.il_exposure("nonexistent")

    def test_typed_calculator_failure_returns_default(self):
        from almanak.framework.data.lp import ILExposureUnavailableError as CalcError

        calculator = MagicMock()
        calculator.get_position.side_effect = CalcError("pid", "not computable yet")
        snapshot = _backtest_snapshot(il_calculator=calculator)
        assert snapshot.il_exposure("pid", default=None) is None

    def test_unexpected_error_raises_even_with_default(self):
        from almanak.framework.data.market_snapshot import ILExposureUnavailableError

        calculator = MagicMock()
        calculator.get_position.side_effect = RuntimeError("upstream bug")
        snapshot = _backtest_snapshot(il_calculator=calculator)
        with pytest.raises(ILExposureUnavailableError, match="Unexpected error"):
            snapshot.il_exposure("pid", default=self.SENTINEL)

    def test_unexpected_error_without_default_stays_loud(self):
        from almanak.framework.data.market_snapshot import ILExposureUnavailableError

        calculator = MagicMock()
        calculator.get_position.side_effect = RuntimeError("upstream bug")
        snapshot = _backtest_snapshot(il_calculator=calculator)
        with pytest.raises(ILExposureUnavailableError, match="Unexpected error"):
            snapshot.il_exposure("pid")


class TestPricesDictAliasResolution:
    """prices.get()/[] resolve plain symbols through the price() alias bridge."""

    def test_get_resolves_symbol_on_address_keyed_snapshot(self):
        snapshot = _backtest_snapshot()
        assert snapshot.prices.get("WETH") == Decimal("1650")
        assert snapshot.prices.get("weth") == Decimal("1650")  # alias map is case-insensitive

    def test_getitem_resolves_symbol_on_address_keyed_snapshot(self):
        snapshot = _backtest_snapshot()
        assert snapshot.prices["WETH"] == Decimal("1650")
        assert snapshot.prices["USDC"] == Decimal("1")

    def test_contains_resolves_symbol(self):
        snapshot = _backtest_snapshot()
        assert "WETH" in snapshot.prices

    def test_address_keyed_access_still_works(self):
        snapshot = _backtest_snapshot()
        assert snapshot.prices[f"ethereum:{WETH_ADDR}"] == Decimal("1650")
        assert snapshot.prices.get(f"ethereum:{WETH_ADDR}") == Decimal("1650")
        assert snapshot.prices.get(WETH_ADDR) == Decimal("1650")  # bare address normalizes

    def test_dict_semantics_preserved_on_true_miss(self):
        snapshot = _backtest_snapshot()
        assert snapshot.prices.get("PEPE") is None
        assert snapshot.prices.get("PEPE", Decimal("0")) == Decimal("0")
        with pytest.raises(KeyError):
            snapshot.prices["PEPE"]
        assert "PEPE" not in snapshot.prices

    def test_live_symbol_keyed_snapshot_unchanged(self):
        from almanak.framework.market.builders import MarketSnapshotBuilder

        snapshot = MarketSnapshotBuilder.seeded(chain="ethereum", prices={"ETH": Decimal("1700")})
        assert snapshot.prices.get("ETH") == Decimal("1700")
        assert snapshot.prices["ETH"] == Decimal("1700")
        assert snapshot.prices.get("DOGE") is None


class TestSoftEmptyLedger:
    """wallet_activity / prediction_price keep their soft shape but explain
    themselves on the decision-input ledger, once per run."""

    def test_wallet_activity_soft_empty_records_ledger(self):
        snapshot = _backtest_snapshot()
        assert snapshot.wallet_activity() == []
        assert ("wallet_activity", "not_simulated") in snapshot._critical_data_failures
        detail = snapshot._critical_data_failures[("wallet_activity", "not_simulated")]
        assert "no historical copy-trade plane" in detail

    def test_prediction_price_soft_empty_records_ledger(self):
        snapshot = _backtest_snapshot()
        assert snapshot.prediction_price("btc-100k", "YES") is None
        assert ("prediction_price", "not_simulated") in snapshot._critical_data_failures

    def test_ledger_note_recorded_once_per_run(self):
        # Two snapshots sharing the run-scoped set = two ticks of one run:
        # only the FIRST soft-empty per source records a ledger entry.
        noted: set[str] = set()
        first = _backtest_snapshot(soft_empty_noted=noted)
        assert first.wallet_activity() == []
        assert first.wallet_activity() == []  # same-tick repeat: no double note
        assert ("wallet_activity", "not_simulated") in first._critical_data_failures

        second = _backtest_snapshot(soft_empty_noted=noted)
        assert second.wallet_activity() == []
        assert ("wallet_activity", "not_simulated") not in second._critical_data_failures

        # Sources are tracked independently.
        assert second.prediction_price("btc-100k", "YES") is None
        assert ("prediction_price", "not_simulated") in second._critical_data_failures

    def test_live_snapshot_soft_contract_untouched(self):
        from almanak.framework.market.builders import MarketSnapshotBuilder

        snapshot = MarketSnapshotBuilder.seeded(chain="ethereum")
        assert snapshot.wallet_activity() == []
        assert snapshot.prediction_price("btc-100k", "YES") is None
        assert snapshot._critical_data_failures == {}

    def test_wired_provider_still_served_without_note(self):
        provider = MagicMock()
        provider.get_signals.return_value = ["signal"]
        snapshot = _backtest_snapshot()
        snapshot._wallet_activity_provider = provider
        assert snapshot.wallet_activity() == ["signal"]
        assert ("wallet_activity", "not_simulated") not in snapshot._critical_data_failures


class TestSyncILCalculatorPositions:
    """Engine LP opens are mirrored into the run's IL calculator."""

    def _lp_position(self, position_id: str = "LP_uniswap_v3_WETH_USDC_1750377600"):
        from almanak.framework.backtesting.pnl.position_models import PositionType, SimulatedPosition

        return SimulatedPosition(
            position_type=PositionType.LP,
            protocol="uniswap_v3",
            tokens=["WETH", "USDC"],
            amounts={"WETH": Decimal("0.01"), "USDC": Decimal("16.5")},
            entry_price=Decimal("1650"),
            entry_time=datetime(2026, 6, 20, tzinfo=UTC),
            position_id=position_id,
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=Decimal("1"),
        )

    def _symbol_state(self, weth_price: str = "1650"):
        from almanak.framework.backtesting.pnl.data_provider import MarketState

        return MarketState(
            timestamp=datetime(2026, 6, 20, tzinfo=UTC),
            chain="ethereum",
            prices={"WETH": Decimal(weth_price), "USDC": Decimal("1")},
        )

    def test_open_position_registered_with_fill_tick_prices(self):
        from almanak.framework.data.lp import ILCalculator

        calculator = ILCalculator()
        portfolio = MagicMock()
        position = self._lp_position()
        portfolio.positions = [position]

        sync_il_calculator_positions(calculator, portfolio, self._symbol_state(), "ethereum")

        tracked = calculator.get_position(position.position_id)
        assert tracked.entry_price_a == Decimal("1650")
        assert tracked.entry_price_b == Decimal("1")
        assert tracked.amount_a == Decimal("0.01")
        assert tracked.tick_lower == -887220

    def test_sync_is_idempotent_and_keeps_entry_prices(self):
        from almanak.framework.data.lp import ILCalculator

        calculator = ILCalculator()
        portfolio = MagicMock()
        portfolio.positions = [self._lp_position()]

        sync_il_calculator_positions(calculator, portfolio, self._symbol_state("1650"), "ethereum")
        # Later tick at a different price: entry prices must NOT be rewritten.
        sync_il_calculator_positions(calculator, portfolio, self._symbol_state("1800"), "ethereum")

        tracked = calculator.get_all_positions()
        assert len(tracked) == 1
        assert tracked[0].entry_price_a == Decimal("1650")

    def test_closed_position_removed_restoring_typed_refusal(self):
        from almanak.framework.data.lp import ILCalculator

        calculator = ILCalculator()
        portfolio = MagicMock()
        position = self._lp_position()
        portfolio.positions = [position]
        sync_il_calculator_positions(calculator, portfolio, self._symbol_state(), "ethereum")
        assert calculator.get_all_positions()

        portfolio.positions = []  # LP_CLOSE drained it
        sync_il_calculator_positions(calculator, portfolio, self._symbol_state(), "ethereum")
        assert calculator.get_all_positions() == []

    def test_missing_price_defers_registration(self):
        from almanak.framework.backtesting.pnl.data_provider import MarketState
        from almanak.framework.data.lp import ILCalculator

        calculator = ILCalculator()
        portfolio = MagicMock()
        portfolio.positions = [self._lp_position()]
        empty_state = MarketState(timestamp=datetime(2026, 6, 20, tzinfo=UTC), chain="ethereum", prices={})

        sync_il_calculator_positions(calculator, portfolio, empty_state, "ethereum")
        assert calculator.get_all_positions() == []

        # Prices show up later: the position registers then.
        sync_il_calculator_positions(calculator, portfolio, self._symbol_state(), "ethereum")
        assert len(calculator.get_all_positions()) == 1

    def test_il_exposure_serves_end_to_end_through_snapshot(self):
        from almanak.framework.data.lp import ILCalculator

        calculator = ILCalculator()
        portfolio = MagicMock()
        position = self._lp_position()
        portfolio.positions = [position]

        # Fill tick: register at entry prices.
        sync_il_calculator_positions(calculator, portfolio, self._symbol_state("1500"), "ethereum")

        # Later tick: WETH moved +10%; the snapshot serves IL for the sim's own position.
        snapshot = create_market_snapshot_from_state(
            _address_keyed_state("1650"),
            chain="ethereum",
            token_addresses=TOKEN_ADDRESSES,
            il_calculator=calculator,
        )
        exposure = snapshot.il_exposure(position.position_id)
        assert exposure.position_id == position.position_id
        assert exposure.current_il.il_ratio < 0
        assert exposure.current_il.current_price_a == Decimal("1650")
