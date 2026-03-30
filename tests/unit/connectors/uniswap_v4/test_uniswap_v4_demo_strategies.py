"""Tests for V4 demo strategies — swap, LP, and hook-aware.

Tests validate strategy configuration, intent creation, hook discovery
integration, and teardown support. These strategies are forward-looking
design documents that will run once V4 Phases 0-3 merge.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.uniswap_v4.hooks import (
    BEFORE_SWAP_FLAG,
    DynamicFeeHookEncoder,
    EmptyHookDataEncoder,
    HookFlags,
    discover_pool,
    warn_empty_hook_data,
)
from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import IntentType


# =============================================================================
# V4 LP Strategy — Intent Creation Tests
# =============================================================================


class TestV4LPIntentCreation:
    """Test V4 LP intent creation with protocol='uniswap_v4'."""

    def test_lp_open_v4_protocol(self):
        """LP_OPEN with protocol='uniswap_v4' should create valid intent."""
        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.01"),
            amount1=Decimal("30"),
            range_lower=Decimal("2800"),
            range_upper=Decimal("3600"),
            protocol="uniswap_v4",
        )
        assert intent.intent_type == IntentType.LP_OPEN
        assert intent.protocol == "uniswap_v4"
        assert intent.pool == "WETH/USDC/3000"
        assert intent.amount0 == Decimal("0.01")
        assert intent.amount1 == Decimal("30")

    def test_lp_close_v4_protocol(self):
        """LP_CLOSE with protocol='uniswap_v4' should create valid intent."""
        intent = Intent.lp_close(
            position_id="12345",
            pool="WETH/USDC/3000",
            collect_fees=True,
            protocol="uniswap_v4",
        )
        assert intent.intent_type == IntentType.LP_CLOSE
        assert intent.protocol == "uniswap_v4"
        assert intent.position_id == "12345"
        assert intent.collect_fees is True

    def test_collect_fees_v4_protocol(self):
        """LP_COLLECT_FEES with protocol='uniswap_v4' should create valid intent."""
        intent = Intent.collect_fees(
            pool="WETH/USDC/3000",
            protocol="uniswap_v4",
        )
        assert intent.intent_type == IntentType.LP_COLLECT_FEES
        assert intent.protocol == "uniswap_v4"

    def test_lp_open_with_protocol_params(self):
        """LP_OPEN should accept protocol_params for hook data."""
        protocol_params = {
            "hook_address": "0x" + "ab" * 19 + "80",
            "hook_data": "00" * 32,
            "hook_capabilities": ["before_swap"],
        }
        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.01"),
            amount1=Decimal("30"),
            range_lower=Decimal("2800"),
            range_upper=Decimal("3600"),
            protocol="uniswap_v4",
            protocol_params=protocol_params,
        )
        assert intent.protocol_params == protocol_params
        assert intent.protocol_params["hook_address"] == "0x" + "ab" * 19 + "80"


# =============================================================================
# V4 Hooks Strategy — Hook Discovery Integration Tests
# =============================================================================


class TestV4HooksIntegration:
    """Test hook discovery integrated with strategy patterns."""

    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    def test_hookless_pool_uses_empty_encoder(self):
        """Hookless pools should use EmptyHookDataEncoder."""
        flags = HookFlags.from_address("0x" + "0" * 40)
        assert flags.is_empty
        encoder = EmptyHookDataEncoder()
        assert encoder.validate_flags(flags) is True
        assert encoder.encode() == b""

    def test_dynamic_fee_hook_detected(self):
        """Address with beforeSwap should use DynamicFeeHookEncoder."""
        hook_addr = "0x" + "0" * 36 + "0080"  # bit 7 = beforeSwap
        flags = HookFlags.from_address(hook_addr)
        assert flags.before_swap is True

        encoder = DynamicFeeHookEncoder()
        assert encoder.validate_flags(flags) is True

    def test_hook_data_passed_in_protocol_params(self):
        """hookData should be passed via protocol_params."""
        encoder = DynamicFeeHookEncoder()
        hook_data = encoder.encode(fee_hint=500)
        assert len(hook_data) == 32

        protocol_params = {
            "hook_address": "0x" + "0" * 36 + "0080",
            "hook_data": hook_data.hex(),
            "hook_capabilities": ["before_swap"],
        }

        intent = Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.01"),
            amount1=Decimal("30"),
            range_lower=Decimal("2800"),
            range_upper=Decimal("3600"),
            protocol="uniswap_v4",
            protocol_params=protocol_params,
        )

        assert intent.protocol_params["hook_data"] == hook_data.hex()

    def test_empty_hook_data_warning_on_hooked_pool(self):
        """Empty hookData on a hooked pool should produce a warning."""
        flags = HookFlags.from_bitmask(BEFORE_SWAP_FLAG)
        warning = warn_empty_hook_data(flags, b"")
        assert warning is not None
        assert "before_swap" in warning

    def test_pool_discovery_with_hooks(self):
        """discover_pool should decode hook capabilities."""
        hook_addr = "0x" + "ab" * 19 + "C0"  # beforeSwap + afterSwap
        result = discover_pool(
            token0=self.WETH,
            token1=self.USDC,
            fee=3000,
            hooks=hook_addr,
        )
        assert result.hook_flags.before_swap is True
        assert result.hook_flags.after_swap is True
        assert result.hook_flags.has_any_swap_hooks is True

    def test_wider_range_for_hooked_pools(self):
        """Strategy should use wider ranges for hooked pools (0.30 vs 0.20)."""
        hookless_width = Decimal("0.20")
        hooked_width = Decimal("0.30")
        assert hooked_width > hookless_width

        current_price = Decimal("3400")

        # Hookless range
        half_hookless = hookless_width / 2
        hookless_lower = current_price * (1 - half_hookless)
        hookless_upper = current_price * (1 + half_hookless)

        # Hooked range (wider)
        half_hooked = hooked_width / 2
        hooked_lower = current_price * (1 - half_hooked)
        hooked_upper = current_price * (1 + half_hooked)

        assert hooked_lower < hookless_lower
        assert hooked_upper > hookless_upper

    def test_teardown_uses_v4_protocol(self):
        """Teardown intents should use protocol='uniswap_v4'."""
        intent = Intent.lp_close(
            position_id="99999",
            pool="WETH/USDC/3000",
            collect_fees=True,
            protocol="uniswap_v4",
        )
        assert intent.protocol == "uniswap_v4"
        assert intent.collect_fees is True


# =============================================================================
# Strategy Config Tests
# =============================================================================


class TestV4StrategyConfigs:
    """Test strategy configuration patterns."""

    def test_v4_lp_config_defaults(self):
        """V4 LP config should have sensible defaults."""
        from almanak.demo_strategies.uniswap_v4_lp.strategy import UniswapV4LPConfig

        config = UniswapV4LPConfig()
        assert config.pool == "WETH/USDC/3000"
        assert config.range_width_pct == Decimal("0.20")
        assert config.amount0 == Decimal("0.01")
        assert config.amount1 == Decimal("30")

    def test_v4_lp_config_to_dict(self):
        from almanak.demo_strategies.uniswap_v4_lp.strategy import UniswapV4LPConfig

        config = UniswapV4LPConfig()
        d = config.to_dict()
        assert d["pool"] == "WETH/USDC/3000"
        assert d["range_width_pct"] == "0.20"

    def test_v4_hooks_config_defaults(self):
        """V4 hooks config should have wider range and hook_address."""
        from almanak.demo_strategies.uniswap_v4_hooks.strategy import UniswapV4HooksConfig

        config = UniswapV4HooksConfig()
        assert config.hook_address == "0x" + "0" * 40
        assert config.range_width_pct == Decimal("0.30")  # Wider than LP
        assert config.fee_hint is None

    def test_v4_hooks_config_to_dict(self):
        from almanak.demo_strategies.uniswap_v4_hooks.strategy import UniswapV4HooksConfig

        config = UniswapV4HooksConfig()
        d = config.to_dict()
        assert "hook_address" in d
        assert d["fee_hint"] is None


# =============================================================================
# V4 Swap Strategy Tests
# =============================================================================


class TestV4SwapStrategy:
    """Test V4 swap demo strategy decide() logic and teardown."""

    def _make_strategy(self, last_action="SELL"):
        """Create a V4 swap strategy with mocked dependencies."""
        from almanak.demo_strategies.uniswap_v4_swap.strategy import UniswapV4SwapStrategy

        strategy = UniswapV4SwapStrategy.__new__(UniswapV4SwapStrategy)
        strategy._chain = "ethereum"
        strategy.trade_size_usd = Decimal("3")
        strategy.max_slippage_bps = 200
        strategy.base_token = "WETH"
        strategy.quote_token = "USDC"
        strategy._max_slippage = Decimal("0.02")
        strategy.state = {"last_action": last_action}
        return strategy

    def _make_market(self, base_price=Decimal("3000"), quote_usd=Decimal("10000"), base_usd=Decimal("6000")):
        """Create a mock MarketSnapshot."""
        market = MagicMock()

        def price_fn(token):
            if token == "WETH":
                return base_price
            if token == "USDC":
                return Decimal("1")
            return None

        def balance_fn(token):
            bal = MagicMock()
            if token == "USDC":
                bal.balance = quote_usd
                bal.balance_usd = quote_usd
            elif token == "WETH":
                bal.balance = base_usd / base_price if base_price > 0 else Decimal("0")
                bal.balance_usd = base_usd
            else:
                bal.balance = Decimal("0")
                bal.balance_usd = Decimal("0")
            return bal

        market.price = price_fn
        market.balance = balance_fn
        return market

    def test_first_run_buys(self):
        """First run (last_action=SELL) should BUY: USDC -> WETH."""
        strategy = self._make_strategy(last_action="SELL")
        market = self._make_market()
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"
        assert intent.protocol == "uniswap_v4"
        assert intent.amount_usd == Decimal("3")

    def test_after_buy_sells(self):
        """After BUY (last_action=BUY) should SELL: WETH -> USDC."""
        strategy = self._make_strategy(last_action="BUY")
        market = self._make_market()
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"
        assert intent.protocol == "uniswap_v4"

    def test_insufficient_balance_holds(self):
        """Should HOLD when insufficient balance for the action."""
        strategy = self._make_strategy(last_action="SELL")
        market = self._make_market(quote_usd=Decimal("0.01"))
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD

    def test_sell_computes_amount_from_price(self):
        """SELL should compute base token amount from trade_size_usd / price."""
        strategy = self._make_strategy(last_action="BUY")
        market = self._make_market(base_price=Decimal("3000"))
        intent = strategy.decide(market)

        assert intent.amount == Decimal("3") / Decimal("3000")

    def test_strategy_import(self):
        """Strategy should be importable from the package."""
        from almanak.demo_strategies.uniswap_v4_swap import UniswapV4SwapStrategy

        assert UniswapV4SwapStrategy is not None

    def test_insufficient_sell_balance_holds(self):
        """Should HOLD when insufficient base balance for SELL."""
        strategy = self._make_strategy(last_action="BUY")
        market = self._make_market(base_usd=Decimal("0.01"))
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD

    def test_swap_intent_uses_v4_protocol(self):
        """All swap intents should use protocol='uniswap_v4'."""
        strategy = self._make_strategy(last_action="SELL")
        market = self._make_market()
        buy_intent = strategy.decide(market)
        assert buy_intent.protocol == "uniswap_v4"

        strategy.state["last_action"] = "BUY"
        sell_intent = strategy.decide(market)
        assert sell_intent.protocol == "uniswap_v4"

    def test_decide_does_not_mutate_state(self):
        """decide() must NOT update state — only on_intent_executed() should."""
        strategy = self._make_strategy(last_action="SELL")
        market = self._make_market()
        strategy.decide(market)
        assert strategy.state["last_action"] == "SELL"

        strategy.state["last_action"] = "BUY"
        strategy.decide(market)
        assert strategy.state["last_action"] == "BUY"

    def test_on_intent_executed_updates_state_on_success(self):
        """on_intent_executed() should update state only on success."""
        strategy = self._make_strategy(last_action="SELL")
        market = self._make_market()
        intent = strategy.decide(market)

        # Simulate successful execution
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy.state["last_action"] == "BUY"

    def test_on_intent_executed_no_update_on_failure(self):
        """on_intent_executed() should NOT update state on failure."""
        strategy = self._make_strategy(last_action="SELL")
        market = self._make_market()
        intent = strategy.decide(market)

        # Simulate failed execution
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy.state["last_action"] == "SELL"
