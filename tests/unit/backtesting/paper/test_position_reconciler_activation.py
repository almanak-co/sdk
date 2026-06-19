"""Tests for the activated PositionReconciler divergence detector (VIB-2634).

Covers the V1 observe-only contract:

* per-tick invocation on persistent forks, DEBUG-log skip when the fork
  resets every tick, and no-op when disabled
* configurable tolerance plumbed from PaperTraderConfig into reconcile()
* divergence beyond threshold -> WARNING + folded into the session summary
  aggregates; below threshold -> silent
* first-sight MISSING_IN_TRACKER positions are adopted as baseline, not
  reported as divergence
* baseline refresh after a reported divergence (warn once, not every tick)
* balance lane: portfolio tracker expected balances vs on-chain wallet
  balances (native ETH excluded)
* graceful degradation: a reconciler crash never kills the tick loop
* PaperTradingSummary surface: reconciliation block, serialization round-trip
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper.config import ForkLifecycle, PaperTraderConfig
from almanak.framework.backtesting.paper.engine import PaperTrader, _safe_divergence_pct
from almanak.framework.backtesting.paper.models import (
    DivergenceRecord,
    PaperTradingSummary,
    PnLBreakdown,
    ReconciliationSummary,
)
from almanak.framework.backtesting.paper.position_queries import (
    AaveV3LendingPosition,
    GMXv2Position,
    UniswapV3Position,
)
from almanak.framework.backtesting.paper.position_reconciler import (
    DiscrepancyType,
    PositionDiscrepancy,
    PositionReconciler,
    PositionType,
)

ENGINE_LOGGER = "almanak.framework.backtesting.paper.engine"
WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


# ---------------------------------------------------------------------------
# Harness (follows test_paper_trader_characterization.py)
# ---------------------------------------------------------------------------


@dataclass
class _MockPortfolioTracker:
    initial_balances: dict[str, Decimal] = field(default_factory=dict)
    current_balances: dict[str, Decimal] = field(default_factory=dict)

    def start_session(self, **kwargs: Any) -> None:
        pass

    def record_trade(self, trade: Any) -> None:
        pass


@dataclass
class _MockForkManager:
    rpc_url: str = "http://127.0.0.1:0"
    is_running: bool = True
    current_block: int | None = 12345
    chain_id: int = 42161

    async def start(self) -> None:
        self.is_running = True

    async def stop(self) -> None:
        self.is_running = False

    async def reset_to_latest(self) -> bool:
        return True

    def get_rpc_url(self) -> str:
        return self.rpc_url


class _MockStrategy:
    deployment_id = "vib2634_strategy"

    def decide(self, snapshot: Any) -> None:
        return None


def _make_config(**overrides: Any) -> PaperTraderConfig:
    kwargs: dict[str, Any] = {
        "chain": "arbitrum",
        "rpc_url": "https://arb.example/rpc",
        "deployment_id": "vib2634_strategy",
        "tick_interval_seconds": 0.001,
        "price_source": "coingecko",
        "strict_price_mode": False,
    }
    kwargs.update(overrides)
    return PaperTraderConfig(**kwargs)


def _make_trader(config: PaperTraderConfig | None = None) -> PaperTrader:
    cfg = config or _make_config()
    with patch(
        "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
    ), patch(
        "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
    ), patch(
        "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
    ):
        trader = PaperTrader(
            fork_manager=_MockForkManager(),
            portfolio_tracker=_MockPortfolioTracker(),
            config=cfg,
        )
    trader._price_aggregator = MagicMock()
    trader._chainlink_provider = None
    trader._twap_provider = None
    trader._rsi_calculator = None
    trader._backtest_id = "vib2634-test"
    return trader


def _supply_discrepancy(
    discrepancy_type: DiscrepancyType,
    *,
    position_id: str = f"aave_v3_{USDC_ARB.lower()}_supply",
    expected: Any = 1_000_000_000,
    actual: Any = 1_100_000_000,
) -> PositionDiscrepancy:
    return PositionDiscrepancy(
        discrepancy_type=discrepancy_type,
        position_type=PositionType.SUPPLY,
        position_id=position_id,
        expected=expected,
        actual=actual,
        message=f"test {discrepancy_type.value} for {position_id}",
    )


def _install_reconciler(
    trader: PaperTrader,
    discrepancies: list[PositionDiscrepancy],
) -> AsyncMock:
    """Give the trader a real PositionReconciler with a mocked reconcile()."""
    reconciler = PositionReconciler(chain=trader.config.chain)
    reconcile_mock = AsyncMock(return_value=discrepancies)
    reconciler.reconcile = reconcile_mock  # type: ignore[method-assign]
    trader._position_reconciler = reconciler
    return reconcile_mock


# ---------------------------------------------------------------------------
# Config surface
# ---------------------------------------------------------------------------


class TestReconcilerConfig:
    def test_defaults(self) -> None:
        cfg = _make_config()
        assert cfg.position_reconciler_enabled is True
        assert cfg.position_reconciler_tolerance_pct == Decimal("0.01")

    def test_tolerance_bounds_validated(self) -> None:
        with pytest.raises(ValueError, match="position_reconciler_tolerance_pct"):
            _make_config(position_reconciler_tolerance_pct=Decimal("1.5"))
        with pytest.raises(ValueError, match="position_reconciler_tolerance_pct"):
            _make_config(position_reconciler_tolerance_pct=Decimal("-0.01"))

    def test_serialization_round_trip(self) -> None:
        cfg = _make_config(
            position_reconciler_enabled=False,
            position_reconciler_tolerance_pct=Decimal("0.05"),
        )
        data = cfg.to_dict()
        assert data["position_reconciler_enabled"] is False
        assert data["position_reconciler_tolerance_pct"] == "0.05"
        data["rpc_url"] = cfg.rpc_url  # to_dict masks the URL
        restored = PaperTraderConfig.from_dict(data)
        assert restored.position_reconciler_enabled is False
        assert restored.position_reconciler_tolerance_pct == Decimal("0.05")

    def test_from_dict_defaults_on(self) -> None:
        restored = PaperTraderConfig.from_dict(
            {
                "chain": "arbitrum",
                "rpc_url": "https://arb.example/rpc",
                "deployment_id": "x",
            }
        )
        assert restored.position_reconciler_enabled is True
        assert restored.position_reconciler_tolerance_pct == Decimal("0.01")


# ---------------------------------------------------------------------------
# tick() gating
# ---------------------------------------------------------------------------


class TestTickGating:
    def _prep(self, trader: PaperTrader) -> dict[str, int]:
        calls = {"reconciler": 0}

        async def _execute_tick(strategy: Any) -> None:
            return None

        async def _should_refresh_fork() -> bool:
            return False

        async def _advance_persistent_fork() -> None:
            return None

        async def _run_position_reconciler() -> None:
            calls["reconciler"] += 1

        trader._execute_tick = _execute_tick  # type: ignore[method-assign]
        trader._should_refresh_fork = _should_refresh_fork  # type: ignore[method-assign]
        trader._advance_persistent_fork = _advance_persistent_fork  # type: ignore[method-assign]
        trader._run_position_reconciler = _run_position_reconciler  # type: ignore[method-assign]
        trader._running = True
        trader._current_strategy = _MockStrategy()
        return calls

    @pytest.mark.asyncio
    async def test_persistent_runs_reconciler_each_tick(self) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        calls = self._prep(trader)
        await trader.tick()
        await trader.tick()
        assert calls["reconciler"] == 2

    @pytest.mark.asyncio
    async def test_rolling_reset_skips_with_debug_log(self, caplog: pytest.LogCaptureFixture) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.ROLLING_RESET))
        calls = self._prep(trader)
        with caplog.at_level(logging.DEBUG, logger=ENGINE_LOGGER):
            await trader.tick()
        assert calls["reconciler"] == 0
        assert any(
            "Skipping PositionReconciler" in rec.message and rec.levelno == logging.DEBUG
            for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_disabled_skips_silently(self, caplog: pytest.LogCaptureFixture) -> None:
        trader = _make_trader(
            _make_config(
                fork_lifecycle=ForkLifecycle.PERSISTENT,
                position_reconciler_enabled=False,
            )
        )
        calls = self._prep(trader)
        with caplog.at_level(logging.DEBUG, logger=ENGINE_LOGGER):
            await trader.tick()
        assert calls["reconciler"] == 0
        assert not any("Skipping PositionReconciler" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# _run_position_reconciler behaviour
# ---------------------------------------------------------------------------


class TestRunPositionReconciler:
    @pytest.mark.asyncio
    async def test_tolerance_from_config_passed_to_reconcile(self) -> None:
        trader = _make_trader(
            _make_config(
                fork_lifecycle=ForkLifecycle.PERSISTENT,
                position_reconciler_tolerance_pct=Decimal("0.03"),
            )
        )
        reconcile_mock = _install_reconciler(trader, [])
        await trader._run_position_reconciler()
        assert reconcile_mock.await_count == 1
        assert reconcile_mock.await_args.kwargs["tolerance_percent"] == Decimal("0.03")
        assert trader._reconciler_checks == 1
        # Regression (VIB-2634 smoke finding): reconcile()'s position readers
        # are async — a sync Web3 silently reports zero positions.
        from web3 import AsyncWeb3

        assert isinstance(reconcile_mock.await_args.args[0], AsyncWeb3)

    @pytest.mark.asyncio
    async def test_divergence_warns_and_lands_in_aggregates(self, caplog: pytest.LogCaptureFixture) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        discrepancy = _supply_discrepancy(DiscrepancyType.AMOUNT_MISMATCH)
        _install_reconciler(trader, [discrepancy])
        # Pre-track the supply so it counts as drift on an adopted baseline.
        trader._position_reconciler.track_supply(
            asset="USDC", asset_address=USDC_ARB.lower(), amount=1_000_000_000
        )

        with caplog.at_level(logging.WARNING, logger=ENGINE_LOGGER):
            await trader._run_position_reconciler()

        assert any("divergence" in rec.message for rec in caplog.records)
        assert trader._reconciler_discrepancies == [discrepancy]
        assert len(trader._divergence_records) == 1
        record = next(iter(trader._divergence_records.values()))
        assert record.kind == "position"
        assert record.divergence_type == "amount_mismatch"
        assert record.count == 1
        # |1.0e9 - 1.1e9| / 1.0e9 = 10%
        assert record.max_divergence_pct == Decimal("0.1")
        assert record.last_seen is not None
        # Baseline refreshed so the same drift does not warn again next tick.
        tracked = trader._position_reconciler.positions[discrepancy.position_id]
        assert tracked.atoken_balance == 1_100_000_000

    @pytest.mark.asyncio
    async def test_repeat_divergence_folds_into_one_record(self) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        d1 = _supply_discrepancy(DiscrepancyType.AMOUNT_MISMATCH)
        d2 = _supply_discrepancy(
            DiscrepancyType.AMOUNT_MISMATCH,
            expected=1_100_000_000,
            actual=1_375_000_000,
        )
        reconcile_mock = _install_reconciler(trader, [d1])
        trader._position_reconciler.track_supply(
            asset="USDC", asset_address=USDC_ARB.lower(), amount=1_000_000_000
        )

        await trader._run_position_reconciler()
        reconcile_mock.return_value = [d2]
        await trader._run_position_reconciler()

        assert trader._reconciler_checks == 2
        assert len(trader._divergence_records) == 1
        record = next(iter(trader._divergence_records.values()))
        assert record.count == 2
        # max(10%, 25%) = 25%
        assert record.max_divergence_pct == Decimal("0.25")
        assert record.last_actual == "1375000000"

    @pytest.mark.asyncio
    async def test_below_threshold_is_silent(self, caplog: pytest.LogCaptureFixture) -> None:
        """reconcile() returning no discrepancies (all within tolerance) stays quiet."""
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        _install_reconciler(trader, [])
        with caplog.at_level(logging.WARNING, logger=ENGINE_LOGGER):
            await trader._run_position_reconciler()
        assert not [r for r in caplog.records if r.name == ENGINE_LOGGER]
        assert trader._divergence_records == {}
        assert trader._reconciler_discrepancies == []
        assert trader._reconciler_checks == 1

    @pytest.mark.asyncio
    async def test_first_sight_lending_position_is_adopted_not_divergence(self) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        discrepancy = _supply_discrepancy(
            DiscrepancyType.MISSING_IN_TRACKER, expected=None, actual=1_000_000_000
        )
        _install_reconciler(trader, [discrepancy])

        on_chain = AaveV3LendingPosition(
            asset="USDC",
            asset_address=USDC_ARB.lower(),
            current_atoken_balance=1_000_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            principal_stable_debt=0,
            scaled_variable_debt=0,
            stable_borrow_rate=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
            decimals=6,
        )
        with patch(
            "almanak.framework.backtesting.paper.position_queries.query_aave_positions",
            new=AsyncMock(return_value=[on_chain]),
        ):
            await trader._run_position_reconciler()

        # Adopted into the detector baseline...
        supply_id = f"aave_v3_{USDC_ARB.lower()}_supply"
        assert supply_id in trader._position_reconciler.positions
        assert trader._position_reconciler.positions[supply_id].atoken_balance == 1_000_000_000
        # ...and NOT counted as divergence.
        assert trader._divergence_records == {}
        assert trader._reconciler_discrepancies == []

    @pytest.mark.asyncio
    async def test_first_sight_lp_position_is_adopted_with_full_fields(self) -> None:
        """LP adoption re-queries the reader so tick bounds land in the baseline."""
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        discrepancy = PositionDiscrepancy(
            discrepancy_type=DiscrepancyType.MISSING_IN_TRACKER,
            position_type=PositionType.LP,
            position_id="12345",
            expected=None,
            actual=999_000,
            message="LP #12345 found on-chain but not tracked",
        )
        _install_reconciler(trader, [discrepancy])

        def _lp(token_id: int, liquidity: int) -> UniswapV3Position:
            return UniswapV3Position(
                token_id=token_id,
                nonce=0,
                operator="0x" + "0" * 40,
                token0="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                token1=USDC_ARB,
                fee=3000,
                tick_lower=-100,
                tick_upper=100,
                liquidity=liquidity,
                fee_growth_inside0_last_x128=0,
                fee_growth_inside1_last_x128=0,
                tokens_owed0=0,
                tokens_owed1=0,
            )

        with patch(
            "almanak.framework.backtesting.paper.position_queries.query_uniswap_v3_positions",
            new=AsyncMock(return_value=[_lp(12345, 999_000), _lp(777, 0)]),  # 777 inactive
        ):
            await trader._run_position_reconciler()

        positions = trader._position_reconciler.positions
        assert "12345" in positions
        assert positions["12345"].liquidity == 999_000
        assert positions["12345"].tick_lower == -100
        assert positions["12345"].tick_upper == 100
        assert "777" not in positions  # zero-liquidity positions are not adopted
        assert trader._divergence_records == {}

    @pytest.mark.asyncio
    async def test_first_sight_perp_position_is_adopted(self) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        key = "0x" + "ab" * 32
        discrepancy = PositionDiscrepancy(
            discrepancy_type=DiscrepancyType.MISSING_IN_TRACKER,
            position_type=PositionType.PERP_LONG,
            position_id=key,
            expected=None,
            actual=5_000 * 10**30,
            message="perp found on-chain but not tracked",
        )
        _install_reconciler(trader, [discrepancy])

        perp = GMXv2Position(
            position_key=key,
            account=WALLET,
            market="0x" + "11" * 20,
            collateral_token=USDC_ARB,
            size_in_usd=5_000 * 10**30,
            size_in_tokens=2 * 10**18,
            collateral_amount=1_000 * 10**6,
            entry_price=2_500 * 10**30,
            is_long=True,
        )
        with patch(
            "almanak.framework.backtesting.paper.position_queries.query_gmx_positions",
            new=AsyncMock(return_value=[perp]),
        ):
            await trader._run_position_reconciler()

        positions = trader._position_reconciler.positions
        assert key in positions
        assert positions[key].size_in_usd == 5_000 * 10**30
        assert positions[key].is_long is True
        assert trader._divergence_records == {}

    @pytest.mark.asyncio
    async def test_missing_on_chain_warns_once_then_drops_baseline(self) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        discrepancy = _supply_discrepancy(
            DiscrepancyType.MISSING_ON_CHAIN, expected=1_000_000_000, actual=None
        )
        _install_reconciler(trader, [discrepancy])
        trader._position_reconciler.track_supply(
            asset="USDC", asset_address=USDC_ARB.lower(), amount=1_000_000_000
        )

        await trader._run_position_reconciler()

        assert discrepancy.position_id not in trader._position_reconciler.positions
        assert discrepancy.position_id in trader._position_reconciler.closed_positions
        record = next(iter(trader._divergence_records.values()))
        assert record.divergence_type == "missing_on_chain"
        assert record.max_divergence_pct == Decimal("1")

    @pytest.mark.asyncio
    async def test_reconciler_crash_does_not_kill_tick(self, caplog: pytest.LogCaptureFixture) -> None:
        """Graceful degradation: any reconciler failure is caught and logged."""
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        reconciler = PositionReconciler(chain="arbitrum")
        reconciler.reconcile = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        trader._position_reconciler = reconciler

        async def _execute_tick(strategy: Any) -> None:
            return None

        async def _should_refresh_fork() -> bool:
            return False

        async def _advance_persistent_fork() -> None:
            return None

        trader._execute_tick = _execute_tick  # type: ignore[method-assign]
        trader._should_refresh_fork = _should_refresh_fork  # type: ignore[method-assign]
        trader._advance_persistent_fork = _advance_persistent_fork  # type: ignore[method-assign]
        trader._running = True
        trader._current_strategy = _MockStrategy()

        with caplog.at_level(logging.WARNING, logger=ENGINE_LOGGER):
            # Must not raise.
            await trader.tick()
            await trader.tick()

        assert trader._tick_count == 2
        assert any("non-fatal" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_no_fork_rpc_skips_quietly(self) -> None:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        trader.fork_manager.rpc_url = ""  # type: ignore[attr-defined]
        await trader._run_position_reconciler()
        assert trader._reconciler_checks == 0
        assert trader._divergence_records == {}


# ---------------------------------------------------------------------------
# Balance lane (portfolio tracker vs on-chain wallet balances)
# ---------------------------------------------------------------------------


class TestBalanceDivergence:
    def _trader_with_balances(self, balances: dict[str, Decimal]) -> PaperTrader:
        trader = _make_trader(_make_config(fork_lifecycle=ForkLifecycle.PERSISTENT))
        trader.portfolio_tracker.current_balances = balances
        trader._resolve_token_address = lambda symbol: USDC_ARB  # type: ignore[method-assign]
        return trader

    @pytest.mark.asyncio
    async def test_divergence_above_threshold_warns_and_records(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        trader = self._trader_with_balances({"USDC": Decimal("1000")})
        # On-chain: 900 USDC (6 decimals) -> 10% divergence vs tracked 1000.
        trader._snapshot_balances = AsyncMock(return_value={"USDC": 900_000_000})  # type: ignore[method-assign]
        with patch(
            "almanak.framework.backtesting.paper.engine.get_token_decimals_with_fallback",
            new=AsyncMock(return_value=6),
        ), caplog.at_level(logging.WARNING, logger=ENGINE_LOGGER):
            await trader._check_balance_divergence(WALLET)

        assert any("Wallet balance divergence for USDC" in rec.message for rec in caplog.records)
        record = trader._divergence_records["balance:balance_mismatch:USDC"]
        assert record.kind == "balance"
        assert record.count == 1
        assert record.max_divergence_pct == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_divergence_below_threshold_is_silent(self, caplog: pytest.LogCaptureFixture) -> None:
        trader = self._trader_with_balances({"USDC": Decimal("1000")})
        # On-chain: 999.5 USDC -> 0.05% divergence, below the 1% default.
        trader._snapshot_balances = AsyncMock(return_value={"USDC": 999_500_000})  # type: ignore[method-assign]
        with patch(
            "almanak.framework.backtesting.paper.engine.get_token_decimals_with_fallback",
            new=AsyncMock(return_value=6),
        ), caplog.at_level(logging.WARNING, logger=ENGINE_LOGGER):
            await trader._check_balance_divergence(WALLET)

        assert not [r for r in caplog.records if r.name == ENGINE_LOGGER]
        assert trader._divergence_records == {}

    @pytest.mark.asyncio
    async def test_native_eth_excluded_from_balance_lane(self, caplog: pytest.LogCaptureFixture) -> None:
        """Gas spent by poke/maintenance txs must not produce ETH divergence noise."""
        trader = self._trader_with_balances({"ETH": Decimal("10")})
        # On-chain ETH wildly different -- still no divergence recorded.
        trader._snapshot_balances = AsyncMock(return_value={"ETH": 10**18})  # type: ignore[method-assign]
        with caplog.at_level(logging.WARNING, logger=ENGINE_LOGGER):
            await trader._check_balance_divergence(WALLET)
        assert not [r for r in caplog.records if r.name == ENGINE_LOGGER]
        assert trader._divergence_records == {}

    @pytest.mark.asyncio
    async def test_unresolvable_decimals_skipped_not_assumed(self) -> None:
        """Empty != Zero: unmeasurable tokens are skipped, never flagged."""
        trader = self._trader_with_balances({"USDC": Decimal("1000")})
        trader._snapshot_balances = AsyncMock(return_value={"USDC": 900_000_000})  # type: ignore[method-assign]
        with patch(
            "almanak.framework.backtesting.paper.engine.get_token_decimals_with_fallback",
            new=AsyncMock(return_value=None),
        ):
            await trader._check_balance_divergence(WALLET)
        assert trader._divergence_records == {}


# ---------------------------------------------------------------------------
# Summary surface
# ---------------------------------------------------------------------------


class TestReconciliationSummarySurface:
    def test_no_checks_means_no_summary(self) -> None:
        trader = _make_trader()
        assert trader._build_reconciliation_summary() is None

    def test_summary_aggregates_and_sorts_records(self) -> None:
        trader = _make_trader()
        trader._reconciler_checks = 5
        trader._record_divergence(
            kind="balance",
            key="USDC",
            divergence_type="balance_mismatch",
            expected=Decimal("1000"),
            actual=Decimal("900"),
            message="usdc drift",
            divergence_pct=Decimal("0.1"),
        )
        trader._record_divergence(
            kind="position",
            key="aave_v3_0xabc_supply",
            divergence_type="amount_mismatch",
            expected=100,
            actual=125,
            message="supply drift",
        )

        summary = trader._build_reconciliation_summary()
        assert summary is not None
        assert summary.checks_run == 5
        assert summary.total_divergences == 2
        assert summary.max_divergence_pct == Decimal("0.25")
        # Worst divergence first.
        assert summary.records[0].key == "aave_v3_0xabc_supply"
        assert summary.records[1].key == "USDC"

    def test_clean_session_summary_has_zero_divergences(self) -> None:
        trader = _make_trader()
        trader._reconciler_checks = 3
        summary = trader._build_reconciliation_summary()
        assert summary is not None
        assert summary.checks_run == 3
        assert summary.total_divergences == 0
        assert summary.max_divergence_pct is None
        assert summary.records == []

    def test_paper_trading_summary_round_trip_and_text(self) -> None:
        reconciliation = ReconciliationSummary(
            checks_run=10,
            total_divergences=3,
            max_divergence_pct=Decimal("0.25"),
            records=[
                DivergenceRecord(
                    key="USDC",
                    kind="balance",
                    divergence_type="balance_mismatch",
                    count=3,
                    max_divergence_pct=Decimal("0.25"),
                    last_seen=datetime(2026, 6, 13, 12, 0, tzinfo=UTC),
                    last_expected="1000",
                    last_actual="750",
                    last_message="usdc drift",
                )
            ],
        )
        summary = PaperTradingSummary(
            deployment_id="vib2634_strategy",
            start_time=datetime(2026, 6, 13, 10, 0, tzinfo=UTC),
            duration=timedelta(minutes=30),
            total_trades=1,
            successful_trades=1,
            failed_trades=0,
            reconciliation=reconciliation,
        )

        text = summary.summary()
        assert "POSITION RECONCILIATION (observe-only)" in text
        assert "Checks Run:         10" in text
        assert "Max Divergence:     25.00%" in text
        assert "balance_mismatch" in text

        restored = PaperTradingSummary.from_dict(summary.to_dict())
        assert restored.reconciliation is not None
        assert restored.reconciliation.checks_run == 10
        assert restored.reconciliation.max_divergence_pct == Decimal("0.25")
        assert restored.reconciliation.records[0].key == "USDC"
        assert restored.reconciliation.records[0].last_seen == datetime(2026, 6, 13, 12, 0, tzinfo=UTC)

    def test_summary_without_reconciliation_omits_section(self) -> None:
        summary = PaperTradingSummary(
            deployment_id="vib2634_strategy",
            start_time=datetime(2026, 6, 13, 10, 0, tzinfo=UTC),
            duration=timedelta(minutes=30),
            total_trades=0,
            successful_trades=0,
            failed_trades=0,
        )
        assert "POSITION RECONCILIATION" not in summary.summary()
        assert summary.to_dict()["reconciliation"] is None
        assert PaperTradingSummary.from_dict(summary.to_dict()).reconciliation is None

    def test_summary_includes_all_optional_sections(self) -> None:
        reconciliation = ReconciliationSummary(
            checks_run=2,
            total_divergences=1,
            max_divergence_pct=None,
            records=[
                DivergenceRecord(
                    key="lp-1",
                    kind="position",
                    divergence_type="range_mismatch",
                    count=1,
                    max_divergence_pct=None,
                    last_seen=None,
                )
            ],
        )
        summary = PaperTradingSummary(
            deployment_id="vib2634_strategy",
            start_time=datetime(2026, 6, 13, 10, 0, tzinfo=UTC),
            duration=timedelta(minutes=30),
            total_trades=2,
            successful_trades=1,
            failed_trades=1,
            chain="arbitrum",
            initial_balances={"USDC": Decimal("1000")},
            final_balances={"USDC": Decimal("990")},
            total_gas_used=21_000,
            total_gas_cost_usd=Decimal("1.23"),
            pnl_usd=Decimal("-10"),
            pnl_breakdown=PnLBreakdown(
                interest_earned=Decimal("2.5"),
                interest_paid=Decimal("1.0"),
                trading_pnl=Decimal("-8.0"),
                gas_costs=Decimal("1.23"),
                fees_included=False,
            ),
            error_summary={"revert": 1},
            reconciliation=reconciliation,
        )

        text = summary.summary()

        assert "PERFORMANCE" in text
        assert "Estimated PnL:      $-10.00" in text
        assert "PnL BREAKDOWN (ex-LP-fees)" in text
        assert "  Interest Earned:  $2.5000" in text
        assert "INITIAL BALANCES" in text
        assert "  USDC: 1,000.000000" in text
        assert "FINAL BALANCES" in text
        assert "  USDC: 990.000000" in text
        assert "ERROR SUMMARY" in text
        assert "  revert: 1" in text
        assert "Divergences:        1" in text
        assert "  [position] lp-1 range_mismatch: count=1, max=n/a, last_seen=n/a" in text


# ---------------------------------------------------------------------------
# _safe_divergence_pct
# ---------------------------------------------------------------------------


class TestSafeDivergencePct:
    def test_missing_side_is_total_divergence(self) -> None:
        assert _safe_divergence_pct(None, 100) == Decimal("1")
        assert _safe_divergence_pct(100, None) == Decimal("1")

    def test_numeric_relative_divergence(self) -> None:
        assert _safe_divergence_pct(100, 110) == Decimal("0.1")
        assert _safe_divergence_pct(Decimal("200"), Decimal("100")) == Decimal("0.5")

    def test_zero_expected(self) -> None:
        assert _safe_divergence_pct(0, 0) == Decimal("0")
        assert _safe_divergence_pct(0, 5) == Decimal("1")

    def test_non_numeric_returns_none(self) -> None:
        assert _safe_divergence_pct("[100, 200]", "[150, 250]") is None
