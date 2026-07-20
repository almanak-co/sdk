"""Serve-from-sim-state position accessors in the pnl backtest (ALM-2943).

Pins the four accessor fixes:
- ``position_health`` served from the sim's own lending state (the
  liquidation guard's collateral/debt/threshold plane), no-position contract,
  refuse+ledger when uncomputable.
- ``aave_health_factor`` piggybacking the same view; ledger entry instead of
  a bare silent ``None`` when no provider is wired.
- ``lp_position_value`` served from the portfolio marker's repricing plane
  for known engine ids; unknown ids refuse + ledger (never silent ``None``).
- ``pt_position_health`` refusals now carry a decision-input ledger entry.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import SimulatedPositionView
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.position_models import PositionType, SimulatedPosition
from almanak.framework.market import HealthUnavailableError, MarketSnapshot

D = Decimal
TS = datetime(2026, 6, 20, 12, tzinfo=UTC)


def _snapshot() -> MarketSnapshot:
    from almanak.framework.market.builders import MarketSnapshotBuilder

    return MarketSnapshotBuilder.seeded(chain="ethereum", wallet_address="0x" + "0" * 40)


def _market_state(weth: str = "2000", usdc: str = "1") -> MarketState:
    return MarketState(timestamp=TS, prices={"WETH": D(weth), "USDC": D(usdc)}, chain="ethereum")


def _supply(amount: str = "2", interest: str = "0") -> SimulatedPosition:
    return SimulatedPosition(
        position_type=PositionType.SUPPLY,
        protocol="aave_v3",
        tokens=["WETH"],
        amounts={"WETH": D(amount)},
        entry_price=D("2000"),
        entry_time=TS,
        interest_accrued=D(interest),
    )


def _borrow(amount: str = "1000", interest: str = "0") -> SimulatedPosition:
    return SimulatedPosition(
        position_type=PositionType.BORROW,
        protocol="aave_v3",
        tokens=["USDC"],
        amounts={"USDC": D(amount)},
        entry_price=D("1"),
        entry_time=TS,
        interest_accrued=D(interest),
    )


def _lp(liquidity: str = "1000", fees: str = "7") -> SimulatedPosition:
    return SimulatedPosition(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        tokens=["WETH", "USDC"],
        amounts={"WETH": D("0"), "USDC": D("0")},
        entry_price=D("2000"),
        entry_time=TS,
        position_id="LP_uniswap_v3_WETH_USDC_1",
        liquidity=D(liquidity),
        fees_earned=D(fees),
    )


def _bound_view(portfolio: SimulatedPortfolio, market_state: MarketState | None = None) -> SimulatedPositionView:
    view = SimulatedPositionView(portfolio)
    view.bind(market_state or _market_state(), TS)
    return view


def _view_snapshot(portfolio: SimulatedPortfolio, bind: bool = True) -> MarketSnapshot:
    snapshot = _snapshot()
    view = SimulatedPositionView(portfolio)
    if bind:
        view.bind(_market_state(), TS)
    snapshot._simulated_position_view = view
    snapshot._aave_health_factor_provider = view.aave_health_factor
    return snapshot


class TestPositionHealthServe:
    def test_supply_and_borrow_hand_computed(self):
        # $4000 collateral (2 WETH @ $2000), $1000 debt, aave_v3 LT 0.825:
        # HF = 4000 * 0.825 / 1000 = 3.3 — the liquidation guard's own formula.
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        portfolio.positions.extend([_supply(), _borrow()])
        snapshot = _view_snapshot(portfolio)

        health = snapshot.position_health("aave_v3", "")

        assert health.health_factor == D("3.3")
        assert health.collateral_value_usd == D("4000")
        assert health.debt_value_usd == D("1000")
        assert health.lltv == D("0.825")
        assert health.max_borrow_usd == D("2300")  # 4000*0.825 - 1000
        assert health.price_source == "backtest_simulation"
        assert not snapshot._critical_data_failures

    def test_interest_included_in_both_legs(self):
        # Same accrual plane as the guard: value = amount*price + interest.
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        portfolio.positions.extend([_supply(interest="100"), _borrow(interest="100")])
        snapshot = _view_snapshot(portfolio)

        health = snapshot.position_health("aave_v3", "")

        assert health.collateral_value_usd == D("4100")
        assert health.debt_value_usd == D("1100")
        assert health.health_factor == D("4100") * D("0.825") / D("1100")

    def test_supply_only_is_infinite_health(self):
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        portfolio.positions.append(_supply())
        snapshot = _view_snapshot(portfolio)

        health = snapshot.position_health("aave_v3", "WETH")

        assert health.health_factor == D("Infinity")
        assert health.is_healthy
        assert health.collateral_value_usd == D("4000")
        assert health.debt_value_usd == D("0")
        assert health.market_id == "WETH"

    def test_no_position_keeps_documented_contract(self):
        # Empty account == what a live getUserAccountData read yields: no
        # debt => Infinity, zero collateral/debt. Served, NOT a refusal.
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        snapshot = _view_snapshot(portfolio)

        health = snapshot.position_health("aave_v3", "")

        assert health.health_factor == D("Infinity")
        assert health.collateral_value_usd == D("0")
        assert health.debt_value_usd == D("0")
        assert not snapshot._critical_data_failures

    def test_uncomputable_refuses_with_ledger(self):
        # An unbound view (no tick) is genuinely uncomputable: honest refusal
        # plus a decision-input ledger entry, exactly like the live path.
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        portfolio.positions.append(_supply())
        snapshot = _view_snapshot(portfolio, bind=False)

        with pytest.raises(HealthUnavailableError, match="not bound"):
            snapshot.position_health("aave_v3", "")
        assert ("position_health", "simulation") in snapshot._critical_data_failures


class TestAaveHealthFactorServe:
    def test_served_from_sim_position(self):
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        portfolio.positions.extend([_supply(), _borrow()])
        snapshot = _view_snapshot(portfolio)

        assert snapshot.aave_health_factor() == D("3.3")
        assert not snapshot._critical_data_failures

    def test_none_means_truly_no_position(self):
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        snapshot = _view_snapshot(portfolio)

        assert snapshot.aave_health_factor() is None
        # Provider IS wired and answered truthfully — no ledger noise.
        assert not snapshot._critical_data_failures

    def test_no_provider_records_ledger_entry(self):
        snapshot = _snapshot()

        assert snapshot.aave_health_factor() is None
        assert ("aave_health_factor", "unconfigured") in snapshot._critical_data_failures


class TestLpPositionValueServe:
    def test_served_on_the_marker_plane(self):
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        position = _lp()
        portfolio.positions.append(position)
        market_state = _market_state()
        snapshot = _view_snapshot(portfolio)

        result = snapshot.lp_position_value(position.position_id, "uniswap_v3")

        assert result is not None
        p0, p1, a0, a1 = portfolio.reprice_lp_position_amounts(position, market_state)
        assert result.amount0 == a0
        assert result.amount1 == a1
        assert result.value_usd == a0 * p0 + a1 * p1
        assert result.fees_usd == D("7")
        assert result.total_usd == result.value_usd + result.fees_usd
        assert result.in_range is True  # full-range position holds both legs
        assert result.position_id == position.position_id
        # One plane: the per-tick marker returns the same total (zero elapsed
        # time here, so the marker accrues no additional fees).
        marked = portfolio._mark_lp_position(position, market_state, TS)
        assert marked == result.total_usd
        assert not snapshot._critical_data_failures

    def test_unknown_id_refuses_with_ledger_never_silent_none(self):
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        snapshot = _view_snapshot(portfolio)

        with pytest.raises(ValueError, match="unknown LP position id"):
            snapshot.lp_position_value("no-such-position", "uniswap_v3")
        assert ("lp_position_value", "unknown_position") in snapshot._critical_data_failures

    def test_non_lp_id_refuses_with_ledger(self):
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        supply = _supply()
        portfolio.positions.append(supply)
        snapshot = _view_snapshot(portfolio)

        with pytest.raises(ValueError, match="not an LP position"):
            snapshot.lp_position_value(supply.position_id, "aave_v3")
        assert ("lp_position_value", "unknown_position") in snapshot._critical_data_failures


class TestPtPositionHealthLedger:
    def test_no_transport_refusal_carries_ledger_entry(self):
        snapshot = _snapshot()  # no gateway client, no rpc_url

        with pytest.raises(HealthUnavailableError, match="pt_position_health requires"):
            snapshot.pt_position_health("0x" + "ab" * 32)
        assert ("pt_position_health", "unconfigured") in snapshot._critical_data_failures


class TestViewDirect:
    def test_market_id_echoed_and_protocol_normalized(self):
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        portfolio.positions.extend([_supply(), _borrow()])
        view = _bound_view(portfolio)

        health = view.position_health("AAVE_V3", "market-x")

        assert health.protocol == "aave_v3"
        assert health.market_id == "market-x"
        assert health.health_factor == D("3.3")

    def test_matches_liquidation_guard_number(self):
        # The guard stamps position.health_factor at mark time; the served
        # number must be the same one (single borrow => identical formula).
        portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"))
        borrow = _borrow()
        portfolio.positions.extend([_supply(), borrow])
        market_state = _market_state()

        from almanak.framework.backtesting.pnl.liquidation_simulator import update_health_factors

        update_health_factors(portfolio, market_state)
        view = _bound_view(portfolio, market_state)

        assert view.position_health("aave_v3").health_factor == borrow.health_factor
