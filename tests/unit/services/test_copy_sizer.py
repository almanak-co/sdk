"""Tests for CopySizer sizing modes and cap enforcement."""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.services.copy_sizer import CopySizer, CopySizingConfig
from almanak.framework.services.copy_trading_models import CopySignal, SizingMode


_DEFAULT_AMOUNTS_USD = {"USDC": Decimal("1000")}


def _make_signal(amounts_usd: dict[str, Decimal] | None = _DEFAULT_AMOUNTS_USD) -> CopySignal:
    """Create a minimal CopySignal for testing."""
    return CopySignal(
        event_id="arbitrum:0xabc:0",
        action_type="SWAP",
        protocol="uniswap_v3",
        chain="arbitrum",
        tokens=["USDC", "WETH"],
        amounts={"USDC": Decimal("1000"), "WETH": Decimal("0.5")},
        amounts_usd=amounts_usd if amounts_usd is not None else _DEFAULT_AMOUNTS_USD,
        metadata={},
        leader_address="0xleader",
        block_number=100,
        timestamp=1700000000,
    )


class TestCopySizingConfig:
    def test_from_config_defaults(self):
        config = CopySizingConfig.from_config({}, {})
        assert config.mode == SizingMode.FIXED_USD
        assert config.fixed_usd == Decimal("100")
        assert config.max_trade_usd == Decimal("1000")
        assert config.max_open_positions == 10

    def test_from_config_custom(self):
        sizing = {"mode": "fixed_usd", "fixed_usd": 200, "percentage_of_leader": 0.25}
        risk = {"max_trade_usd": 500, "max_daily_notional_usd": 3000, "max_open_positions": 3}
        config = CopySizingConfig.from_config(sizing, risk)
        assert config.mode == SizingMode.FIXED_USD
        assert config.fixed_usd == Decimal("200")
        assert config.percentage_of_leader == Decimal("0.25")
        assert config.max_trade_usd == Decimal("500")
        assert config.max_daily_notional_usd == Decimal("3000")

    def test_from_config_proportion_of_leader_accepted(self):
        """proportion_of_leader is accepted by from_config."""
        sizing = {"mode": "proportion_of_leader", "fixed_usd": 200}
        risk = {"max_trade_usd": 500}
        config = CopySizingConfig.from_config(sizing, risk)
        assert config.mode == SizingMode.PROPORTION_OF_LEADER
        assert config.fixed_usd == Decimal("200")


class TestCopySizerFixedUSD:
    def test_fixed_usd_returns_configured_amount(self):
        config = CopySizingConfig(mode=SizingMode.FIXED_USD, fixed_usd=Decimal("250"))
        sizer = CopySizer(config=config)
        signal = _make_signal()
        assert sizer.compute_size(signal) == Decimal("250")

    def test_fixed_usd_capped_by_max_trade(self):
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("2000"),
            max_trade_usd=Decimal("500"),
        )
        sizer = CopySizer(config=config)
        assert sizer.compute_size(_make_signal()) == Decimal("500")


class TestCopySizerProportionOfLeader:
    def test_proportion_scales_leader_usd(self):
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_LEADER,
            percentage_of_leader=Decimal("0.1"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={"USDC": Decimal("5000")})
        assert sizer.compute_size(signal) == Decimal("500.0")

    def test_proportion_capped_by_max_trade(self):
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_LEADER,
            percentage_of_leader=Decimal("0.5"),
            max_trade_usd=Decimal("200"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={"USDC": Decimal("5000")})
        assert sizer.compute_size(signal) == Decimal("200")


class TestMinTradeFilter:
    def test_below_min_returns_none(self):
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("5"),
            min_trade_usd=Decimal("10"),
        )
        sizer = CopySizer(config=config)
        assert sizer.compute_size(_make_signal()) is None

    def test_at_min_returns_value(self):
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("10"),
            min_trade_usd=Decimal("10"),
        )
        sizer = CopySizer(config=config)
        assert sizer.compute_size(_make_signal()) == Decimal("10")


class TestDailyCap:
    def test_daily_cap_allows_within_limit(self):
        config = CopySizingConfig(max_daily_notional_usd=Decimal("1000"))
        sizer = CopySizer(config=config)
        assert sizer.check_daily_cap(Decimal("500")) is True

    def test_daily_cap_blocks_at_limit(self):
        config = CopySizingConfig(max_daily_notional_usd=Decimal("1000"))
        sizer = CopySizer(config=config)
        sizer.record_execution(Decimal("800"))
        assert sizer.check_daily_cap(Decimal("201")) is False

    def test_daily_cap_allows_exact_limit(self):
        config = CopySizingConfig(max_daily_notional_usd=Decimal("1000"))
        sizer = CopySizer(config=config)
        sizer.record_execution(Decimal("800"))
        assert sizer.check_daily_cap(Decimal("200")) is True

    def test_daily_reset_on_new_day(self):
        config = CopySizingConfig(max_daily_notional_usd=Decimal("100"))
        sizer = CopySizer(config=config)
        sizer.record_execution(Decimal("100"))
        assert sizer.check_daily_cap(Decimal("1")) is False

        # Simulate date change
        with patch(
            "almanak.framework.services.copy_sizer.datetime"
        ) as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2099-12-31"
            assert sizer.check_daily_cap(Decimal("50")) is True
            assert sizer._daily_notional == Decimal("0")


class TestPositionCap:
    def test_position_cap_allows_below_max(self):
        config = CopySizingConfig(max_open_positions=3)
        sizer = CopySizer(config=config)
        assert sizer.check_position_cap() is True

    def test_position_cap_blocks_at_max(self):
        config = CopySizingConfig(max_open_positions=2)
        sizer = CopySizer(config=config)
        sizer.record_execution(Decimal("100"))
        sizer.record_execution(Decimal("100"))
        assert sizer.check_position_cap() is False

    def test_position_cap_opens_after_close(self):
        config = CopySizingConfig(max_open_positions=1)
        sizer = CopySizer(config=config)
        sizer.record_execution(Decimal("100"))
        assert sizer.check_position_cap() is False
        sizer.record_close()
        assert sizer.check_position_cap() is True

    def test_record_close_does_not_go_negative(self):
        config = CopySizingConfig(max_open_positions=1)
        sizer = CopySizer(config=config)
        sizer.record_close()
        assert sizer._open_positions == 0


class TestGetSkipReason:
    def test_no_skip_reason_when_ok(self):
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("100"),
            max_daily_notional_usd=Decimal("10000"),
            max_open_positions=10,
        )
        sizer = CopySizer(config=config)
        assert sizer.get_skip_reason(_make_signal()) is None

    def test_skip_reason_below_min(self):
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("1"),
            min_trade_usd=Decimal("10"),
        )
        sizer = CopySizer(config=config)
        assert sizer.get_skip_reason(_make_signal()) == "below_min_usd"

    def test_skip_reason_daily_cap(self):
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("100"),
            max_daily_notional_usd=Decimal("50"),
        )
        sizer = CopySizer(config=config)
        assert sizer.get_skip_reason(_make_signal()) == "daily_cap_reached"

    def test_skip_reason_position_cap(self):
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("100"),
            max_daily_notional_usd=Decimal("10000"),
            max_open_positions=0,
        )
        sizer = CopySizer(config=config)
        assert sizer.get_skip_reason(_make_signal()) == "position_cap_reached"


class TestProportionalWithoutUsdAmounts:
    """Proportional sizing with empty amounts_usd fails explicitly."""

    def test_compute_size_returns_none_when_amounts_usd_empty(self):
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_LEADER,
            percentage_of_leader=Decimal("0.1"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={})
        assert sizer.compute_size(signal) is None

    def test_skip_reason_is_explicit_for_missing_usd(self):
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_LEADER,
            percentage_of_leader=Decimal("0.1"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={})
        assert sizer.get_skip_reason(signal) == "no_usd_amounts_for_proportional_sizing"

    def test_fixed_usd_works_with_empty_amounts_usd(self):
        """Fixed USD mode doesn't need amounts_usd at all."""
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("100"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={})
        assert sizer.compute_size(signal) == Decimal("100")


class TestProportionOfEquity:
    """Tests for PROPORTION_OF_EQUITY sizing mode."""

    def test_equity_sizing_scales_portfolio_value(self):
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_EQUITY,
            percentage_of_equity=Decimal("0.02"),
            max_trade_usd=Decimal("10000"),
        )
        sizer = CopySizer(config=config, portfolio_value_fn=lambda: Decimal("50000"))
        signal = _make_signal()
        assert sizer.compute_size(signal) == Decimal("1000.00")  # 50000 * 0.02

    def test_equity_sizing_capped_by_max_trade(self):
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_EQUITY,
            percentage_of_equity=Decimal("0.1"),
            max_trade_usd=Decimal("200"),
        )
        sizer = CopySizer(config=config, portfolio_value_fn=lambda: Decimal("50000"))
        assert sizer.compute_size(_make_signal()) == Decimal("200")

    def test_equity_sizing_returns_none_without_fn(self):
        config = CopySizingConfig(mode=SizingMode.PROPORTION_OF_EQUITY)
        sizer = CopySizer(config=config, portfolio_value_fn=None)
        assert sizer.compute_size(_make_signal()) is None

    def test_equity_sizing_returns_none_for_zero_portfolio(self):
        config = CopySizingConfig(mode=SizingMode.PROPORTION_OF_EQUITY)
        sizer = CopySizer(config=config, portfolio_value_fn=lambda: Decimal("0"))
        assert sizer.compute_size(_make_signal()) is None

    def test_equity_sizing_skip_reason_without_fn(self):
        config = CopySizingConfig(mode=SizingMode.PROPORTION_OF_EQUITY)
        sizer = CopySizer(config=config, portfolio_value_fn=None)
        assert sizer.get_skip_reason(_make_signal()) == "no_portfolio_value_fn_for_equity_sizing"

    def test_equity_sizing_from_config(self):
        sizing = {"mode": "proportion_of_equity", "percentage_of_equity": 0.05}
        risk = {"max_trade_usd": 500}
        config = CopySizingConfig.from_config(sizing, risk)
        assert config.mode == SizingMode.PROPORTION_OF_EQUITY
        assert config.percentage_of_equity == Decimal("0.05")


class TestLeaderWeight:
    """Tests for leader weight multiplier."""

    def test_weight_multiplies_fixed_usd(self):
        config = CopySizingConfig(mode=SizingMode.FIXED_USD, fixed_usd=Decimal("100"))
        sizer = CopySizer(config=config)
        signal = _make_signal()
        assert sizer.compute_size(signal, leader_weight=Decimal("0.5")) == Decimal("50.0")

    def test_weight_multiplies_proportion_of_leader(self):
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_LEADER,
            percentage_of_leader=Decimal("0.1"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={"USDC": Decimal("5000")})
        # 5000 * 0.1 * 0.5 = 250
        assert sizer.compute_size(signal, leader_weight=Decimal("0.5")) == Decimal("250.00")

    def test_weight_none_has_no_effect(self):
        config = CopySizingConfig(mode=SizingMode.FIXED_USD, fixed_usd=Decimal("100"))
        sizer = CopySizer(config=config)
        signal = _make_signal()
        assert sizer.compute_size(signal, leader_weight=None) == Decimal("100")


class TestSwapAutoClose:
    def test_swap_auto_close_keeps_cap_open(self):
        """Swaps that call record_execution + record_close never exhaust position cap."""
        config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("100"),
            max_daily_notional_usd=Decimal("50000"),
            max_open_positions=3,
        )
        sizer = CopySizer(config=config)

        # Execute 10 swaps with immediate close (atomic swap pattern)
        for _ in range(10):
            sizer.record_execution(Decimal("100"))
            sizer.record_close()

        # Position cap should still allow new trades
        assert sizer.check_position_cap() is True
        assert sizer._open_positions == 0

        # Daily notional should reflect all 10 executions
        assert sizer._daily_notional == Decimal("1000")


class TestProportionalMultiLegNotDoubleCount:
    """Verify proportional sizing uses max(), not sum(), for multi-leg swaps."""

    def test_two_leg_swap_uses_max_not_sum(self):
        """A swap with USDC:$5000 + WETH:$5000 should size as $5000, not $10000."""
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_LEADER,
            percentage_of_leader=Decimal("0.1"),
            max_trade_usd=Decimal("10000"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={"USDC": Decimal("5000"), "WETH": Decimal("5000")})
        result = sizer.compute_size(signal)
        # max(5000, 5000) * 0.1 = 500, NOT sum(5000+5000) * 0.1 = 1000
        assert result == Decimal("500.0")

    def test_asymmetric_legs_uses_max(self):
        """Asymmetric USD values should still use max() for sizing."""
        config = CopySizingConfig(
            mode=SizingMode.PROPORTION_OF_LEADER,
            percentage_of_leader=Decimal("0.1"),
            max_trade_usd=Decimal("10000"),
        )
        sizer = CopySizer(config=config)
        signal = _make_signal(amounts_usd={"USDC": Decimal("4800"), "WETH": Decimal("5200")})
        result = sizer.compute_size(signal)
        # max(4800, 5200) * 0.1 = 520
        assert result == Decimal("520.0")
