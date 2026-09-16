"""Tests for V4 demo strategies — swap, LP, and hook-aware.

Tests validate strategy configuration, intent creation, hook discovery
integration, and teardown support. These strategies are forward-looking
design documents that will run once V4 Phases 0-3 merge.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
from almanak.connectors.uniswap_v4.hooks import (
    BEFORE_SWAP_FLAG,
    DynamicFeeHookEncoder,
    EmptyHookDataEncoder,
    HookFlags,
    discover_pool,
    hook_data_to_wire,
    warn_empty_hook_data,
)
from almanak.demo_strategies.uniswap_v4_hooks.strategy import UniswapV4HooksStrategy, parse_force_action
from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown import TeardownMode
from tests.unit.connectors.uniswap_v4.test_position_observation import KEY, WALLET, PositionGateway

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
            "hooks": "0x" + "ab" * 19 + "80",
            "hook_data": "0x" + "00" * 32,
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
        assert intent.protocol_params["hooks"] == "0x" + "ab" * 19 + "80"


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
            "hooks": "0x" + "0" * 36 + "0080",
            "hook_data": hook_data_to_wire(hook_data),
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

        assert intent.protocol_params["hook_data"] == "0x" + hook_data.hex()

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

    def test_strategy_pool_discovery_uses_configured_addresses(self):
        """Pool identity must not re-resolve the display symbols independently."""
        strategy = UniswapV4HooksStrategy.__new__(UniswapV4HooksStrategy)
        strategy.token0_address = self.WETH
        strategy.token1_address = self.USDC
        strategy.fee_tier = 3000
        strategy.hook_address = "0x" + "0" * 40
        result = SimpleNamespace(
            pool_id="0x" + "1" * 64,
            hook_flags=SimpleNamespace(active_flags=[]),
        )

        with patch(
            "almanak.demo_strategies.uniswap_v4_hooks.strategy.discover_pool",
            return_value=result,
        ) as discover:
            strategy._run_pool_discovery()

        discover.assert_called_once_with(
            token0=self.WETH,
            token1=self.USDC,
            fee=3000,
            hooks="0x" + "0" * 40,
        )

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
    """Test strategy configuration patterns.

    The uniswap_v4_lp config tests live with that internalized demo at
    strategies/internal/tests/unit/connectors/uniswap_v4/ (PR #2954). Only the
    golden uniswap_v4_hooks config tests remain here.
    """

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


# The uniswap_v4_swap demo was internalized by PR #2954; its decide()/teardown
# tests live at strategies/internal/tests/unit/connectors/uniswap_v4/.


def _bare_hooks_strategy(encoder, *, pool: str = "WETH/USDC/3000") -> UniswapV4HooksStrategy:
    """Demo instance carrying only the attributes its intent builders read."""
    strategy = UniswapV4HooksStrategy.__new__(UniswapV4HooksStrategy)
    strategy.pool = pool
    strategy.hook_address = "0x" + "0" * 40
    strategy.hook_flags = HookFlags.from_address(strategy.hook_address)
    strategy._encoder = encoder
    strategy.fee_hint = None
    strategy.range_width_pct = Decimal("0.30")
    strategy.amount0 = Decimal("0.01")
    strategy.amount1 = Decimal("30")
    strategy.max_slippage = Decimal("0.005")
    strategy.token0_symbol = "WETH"
    strategy.token1_symbol = "USDC"
    strategy._current_position_id = "42"
    return strategy


def _offline_base_adapter() -> UniswapV4Adapter:
    """Adapter with no RPC and no gateway: LP_OPEN compiles from the oracle estimate."""
    tokens = {
        "WETH": MagicMock(address="0x4200000000000000000000000000000000000006", decimals=18, is_native=False),
        "USDC": MagicMock(address="0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", decimals=6, is_native=False),
    }
    resolver = MagicMock()
    resolver.resolve_for_swap = lambda symbol, chain: tokens[symbol.upper()]
    return UniswapV4Adapter(
        config=UniswapV4Config(chain="base", wallet_address=WALLET),
        token_resolver=resolver,
    )


def _observing_base_adapter(gateway: PositionGateway) -> UniswapV4Adapter:
    """Adapter that observes the owned NFT through a fake gateway: LP_CLOSE compiles."""
    return UniswapV4Adapter(
        config=UniswapV4Config(chain="base", wallet_address=WALLET),
        venue_verification_gateway_factory=lambda: gateway,
    )


_ORACLE = {"WETH": Decimal("2500"), "USDC": Decimal("1")}


class TestV4HooksDemoHookDataWireFormat:
    """Every LP intent the demo emits carries hook_data as 0x-prefixed hex, "0x" when empty."""

    def test_open_intent_empty_hook_data_is_0x(self):
        intent = _bare_hooks_strategy(EmptyHookDataEncoder())._create_open_intent(Decimal("2500"))
        assert intent.protocol_params["hook_data"] == "0x"

    def test_open_intent_dynamic_fee_hook_data_is_prefixed_hex(self):
        strategy = _bare_hooks_strategy(DynamicFeeHookEncoder())
        strategy.fee_hint = 500
        wire = strategy._create_open_intent(Decimal("2500")).protocol_params["hook_data"]
        assert wire.startswith("0x") and bytes.fromhex(wire[2:]) == DynamicFeeHookEncoder().encode(fee_hint=500)

    def test_adapter_compiles_the_demo_open_intent(self):
        adapter = _offline_base_adapter()
        strategy = _bare_hooks_strategy(EmptyHookDataEncoder())
        intent = strategy._create_open_intent(Decimal("2500"), amount0=Decimal("0.05"), amount1=Decimal("125"))
        bundle = adapter.compile_lp_open_intent(intent, _ORACLE)
        assert bundle.transactions, bundle.metadata
        assert bundle.metadata["price_source"] == "oracle_estimate"

    def test_adapter_compiles_the_demo_close_and_teardown_intents(self):
        gateway = PositionGateway()
        adapter = _observing_base_adapter(gateway)
        strategy = _bare_hooks_strategy(EmptyHookDataEncoder(), pool=KEY.pool_id)
        close = strategy._create_close_intent("42")
        (teardown,) = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert close.max_slippage == strategy.max_slippage and teardown.max_slippage == Decimal("0.03")
        for intent in (close, teardown):
            bundle = adapter.compile_lp_close_intent(intent, gateway.liquidity, KEY.currency0, KEY.currency1)
            assert bundle.transactions, bundle.metadata

    def test_force_action_open_mints_before_any_inventory_read(self):
        """The sidecar cell injects force_action=open: iteration 1 is the mint, not a balancing swap."""
        strategy = _bare_hooks_strategy(EmptyHookDataEncoder())
        strategy._current_position_id = None
        strategy.token0_address = "0x4200000000000000000000000000000000000006"
        strategy.token1_address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        market = MagicMock()
        market.price = lambda address: _ORACLE["WETH"] if address == strategy.token0_address else _ORACLE["USDC"]
        market.balance = MagicMock(side_effect=KeyError("no inventory"))

        strategy.force_action = ""
        assert strategy.decide(market).intent_type == IntentType.HOLD and market.balance.called

        market.balance.reset_mock()
        strategy.force_action = "open"
        intent = strategy.decide(market)
        assert not market.balance.called
        assert intent.intent_type == IntentType.LP_OPEN and (intent.amount0, intent.amount1) == (
            strategy.amount0,
            strategy.amount1,
        )
        bundle = _offline_base_adapter().compile_lp_open_intent(intent, _ORACLE)
        assert bundle.transactions, bundle.metadata

    def test_forced_close_survives_a_price_outage(self):
        """A close needs only the position id, so a dead price feed must not strand the position."""
        strategy = _bare_hooks_strategy(EmptyHookDataEncoder())
        strategy.force_action = "close"
        strategy._current_position_id = "42"
        market = MagicMock()
        market.price = MagicMock(side_effect=KeyError("price feed down"))
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.LP_CLOSE and intent.position_id == "42"
        assert not market.price.called

    def test_forced_open_refuses_to_stack_on_an_open_position(self):
        """Forcing an open on a tracked position would mint a second NFT and strand the first."""
        strategy = _bare_hooks_strategy(EmptyHookDataEncoder())
        strategy.force_action = "open"
        strategy._current_position_id = "42"
        market = MagicMock()
        market.price = lambda address: Decimal("2500") if address == strategy.token0_address else Decimal("1")
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.HOLD and "42" in intent.reason

    def test_unknown_force_action_is_refused(self):
        assert [parse_force_action(v) for v in (None, "", " Open ", "close")] == ["", "", "open", "close"]
        with pytest.raises(ValueError, match="force_action must be one of"):
            parse_force_action("mint")

    def test_adapter_refuses_untyped_hook_data(self):
        """An empty string or raw bytes is not the wire format: the adapter refuses both lanes."""
        open_intent = _bare_hooks_strategy(EmptyHookDataEncoder())._create_open_intent(
            Decimal("2500"), amount0=Decimal("0.05"), amount1=Decimal("125")
        )
        refused = _offline_base_adapter().compile_lp_open_intent(
            open_intent.model_copy(update={"protocol_params": {**open_intent.protocol_params, "hook_data": ""}}),
            _ORACLE,
        )
        assert not refused.transactions and "0x-prefixed hex bytes" in refused.metadata["error"]

        gateway = PositionGateway()
        strategy = _bare_hooks_strategy(EmptyHookDataEncoder(), pool=KEY.pool_id)
        close_intent = strategy._create_close_intent("42").model_copy(update={"protocol_params": {"hook_data": b""}})
        with pytest.raises(ValueError, match="0x-prefixed hex bytes"):
            _observing_base_adapter(gateway).compile_lp_close_intent(
                close_intent, gateway.liquidity, KEY.currency0, KEY.currency1
            )
