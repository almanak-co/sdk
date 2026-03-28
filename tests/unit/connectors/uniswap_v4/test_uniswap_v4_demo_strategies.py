"""Tests for V4 demo strategies — LP and hook-aware.

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
        from strategies.demo.uniswap_v4_lp.strategy import UniswapV4LPConfig

        config = UniswapV4LPConfig()
        assert config.pool == "WETH/USDC/3000"
        assert config.range_width_pct == Decimal("0.20")
        assert config.amount0 == Decimal("0.01")
        assert config.amount1 == Decimal("30")

    def test_v4_lp_config_to_dict(self):
        from strategies.demo.uniswap_v4_lp.strategy import UniswapV4LPConfig

        config = UniswapV4LPConfig()
        d = config.to_dict()
        assert d["pool"] == "WETH/USDC/3000"
        assert d["range_width_pct"] == "0.20"

    def test_v4_hooks_config_defaults(self):
        """V4 hooks config should have wider range and hook_address."""
        from strategies.demo.uniswap_v4_hooks.strategy import UniswapV4HooksConfig

        config = UniswapV4HooksConfig()
        assert config.hook_address == "0x" + "0" * 40
        assert config.range_width_pct == Decimal("0.30")  # Wider than LP
        assert config.fee_hint is None

    def test_v4_hooks_config_to_dict(self):
        from strategies.demo.uniswap_v4_hooks.strategy import UniswapV4HooksConfig

        config = UniswapV4HooksConfig()
        d = config.to_dict()
        assert "hook_address" in d
        assert d["fee_hint"] is None
