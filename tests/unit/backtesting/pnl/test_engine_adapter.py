"""Tests for strategy adapter integration in PnL engine.

Tests verify that the PnLBacktester correctly:
1. Detects strategy types from strategy metadata
2. Loads appropriate adapters during initialization
3. Stores adapter instance for use during backtesting
4. Handles explicit strategy_type configuration
5. Calls adapter's execute_intent when adapter exists
"""
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.adapters import (
    AdapterRegistry,
    StrategyBacktestAdapter,
)
from almanak.framework.backtesting.models import IntentType, TradeRecord
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

# =============================================================================
# Mock Adapters for Testing
# =============================================================================


class MockLPAdapter(StrategyBacktestAdapter):
    """Mock LP adapter for testing."""

    @property
    def adapter_name(self) -> str:
        return "lp"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        return None

    def update_position(self, position: Any, market_state: Any, elapsed_seconds: float) -> None:
        pass

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


class MockPerpAdapter(StrategyBacktestAdapter):
    """Mock perp adapter for testing."""

    @property
    def adapter_name(self) -> str:
        return "perp"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        return None

    def update_position(self, position: Any, market_state: Any, elapsed_seconds: float) -> None:
        pass

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


class MockLendingAdapter(StrategyBacktestAdapter):
    """Mock lending adapter for testing."""

    @property
    def adapter_name(self) -> str:
        return "lending"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        return None

    def update_position(self, position: Any, market_state: Any, elapsed_seconds: float) -> None:
        pass

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


# =============================================================================
# Mock Strategy Classes for Testing
# =============================================================================


@dataclass
class MockStrategyMetadata:
    """Mock strategy metadata for testing."""

    name: str = "test_strategy"
    description: str = "Test strategy"
    version: str = "1.0.0"
    author: str = "test"
    tags: list[str] = field(default_factory=list)
    supported_chains: list[str] = field(default_factory=list)
    supported_protocols: list[str] = field(default_factory=list)
    intent_types: list[str] = field(default_factory=list)


class MockStrategy:
    """Mock strategy for testing."""

    STRATEGY_METADATA: MockStrategyMetadata | None = None

    def __init__(
        self,
        deployment_id: str = "test_strategy",
        tags: list[str] | None = None,
        protocols: list[str] | None = None,
        intent_types: list[str] | None = None,
    ):
        self._deployment_id = deployment_id
        self.STRATEGY_METADATA = MockStrategyMetadata(
            tags=tags or [],
            supported_protocols=protocols or [],
            intent_types=intent_types or [],
        )

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def get_metadata(self) -> MockStrategyMetadata | None:
        return self.STRATEGY_METADATA

    def decide(self, market: Any) -> Any:
        return None


class MockDataProvider:
    """Mock data provider for testing."""

    provider_name = "mock"

    async def iterate(self, config: Any):
        # Yield nothing for testing
        if False:
            yield


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate registry mutations without erasing built-in adapters for later modules."""
    original_registry = AdapterRegistry._registry.copy()
    AdapterRegistry.clear()
    yield
    AdapterRegistry.clear()
    AdapterRegistry._registry.update(original_registry)


@pytest.fixture
def registered_adapters():
    """Register test adapters."""
    AdapterRegistry.register("lp", MockLPAdapter, description="LP adapter", aliases=["liquidity"])
    AdapterRegistry.register("perp", MockPerpAdapter, description="Perp adapter", aliases=["perpetual"])
    AdapterRegistry.register("lending", MockLendingAdapter, description="Lending adapter", aliases=["borrow"])
    return {"lp": MockLPAdapter, "perp": MockPerpAdapter, "lending": MockLendingAdapter}


@pytest.fixture
def backtester():
    """Create a basic PnLBacktester for testing."""
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


# =============================================================================
# Tests: Strategy Type Detection in Engine
# =============================================================================


def test_detect_strategy_type_lp(backtester, registered_adapters):
    """Test detection of LP strategy type."""
    strategy = MockStrategy(tags=["lp", "liquidity"])
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "lp"
    assert hint.source == "tags"
    assert backtester._detected_strategy_type is not None
    assert backtester._detected_strategy_type.strategy_type == "lp"


def test_detect_strategy_type_perp(backtester, registered_adapters):
    """Test detection of perp strategy type."""
    strategy = MockStrategy(tags=["perpetual", "leverage"])
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "perp"


def test_detect_strategy_type_lending(backtester, registered_adapters):
    """Test detection of lending strategy type."""
    strategy = MockStrategy(protocols=["aave_v3", "compound_v3"])
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "lending"
    assert hint.source == "protocols"


def test_detect_strategy_type_from_intents(backtester, registered_adapters):
    """Test detection from intent types."""
    strategy = MockStrategy(intent_types=["PERP_OPEN", "PERP_CLOSE"])
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "perp"
    assert hint.source == "intents"


def test_get_intent_tokens_normalizes_address_without_alias_map(backtester):
    """Raw address intent tokens normalize to TokenRef keys when chain is known."""
    address = "0x4200000000000000000000000000000000000006"
    intent = type("MockIntent", (), {"from_token": address})()

    tokens = backtester._get_intent_tokens(intent, aliases=None, chain="base")

    assert tokens == [("base", address)]


def test_normalize_token_canonicalizes_tuple_token_refs():
    """Tuple TokenRefs collapse to one canonical address key."""
    address = "0x4200000000000000000000000000000000000006"

    assert _engine_helpers._normalize_token(("Base", address.upper())) == ("base", address)


def test_detect_strategy_type_explicit_override(registered_adapters):
    """Test explicit strategy_type overrides detection."""
    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
        strategy_type="perp",  # Explicit type
    )

    # Strategy has LP tags but explicit type is perp
    strategy = MockStrategy(tags=["lp", "liquidity"])
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "perp"
    assert hint.source == "explicit"
    assert hint.confidence == "high"


def test_detect_strategy_type_no_match(backtester):
    """Test detection with no matching metadata."""
    strategy = MockStrategy(tags=["custom", "unknown"])
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type is None
    assert hint.source == "none"


def test_detect_strategy_type_auto_mode(registered_adapters):
    """Test auto mode uses metadata detection."""
    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
        strategy_type="auto",  # Explicit auto mode
    )

    strategy = MockStrategy(tags=["lending", "borrow"])
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "lending"
    assert hint.source == "tags"


# =============================================================================
# Tests: Adapter Loading
# =============================================================================


def test_init_adapter_loads_lp_adapter(backtester, registered_adapters):
    """Test that LP adapter is loaded for LP strategy."""
    strategy = MockStrategy(tags=["lp", "concentrated-liquidity"])
    backtester._init_adapter(strategy)

    assert backtester._adapter is not None
    assert isinstance(backtester._adapter, MockLPAdapter)
    assert backtester._adapter.adapter_name == "lp"


def test_init_adapter_loads_perp_adapter(backtester, registered_adapters):
    """Test that perp adapter is loaded for perp strategy."""
    strategy = MockStrategy(tags=["perpetual", "margin"])
    backtester._init_adapter(strategy)

    assert backtester._adapter is not None
    assert isinstance(backtester._adapter, MockPerpAdapter)
    assert backtester._adapter.adapter_name == "perp"


def test_init_adapter_loads_lending_adapter(backtester, registered_adapters):
    """Test that lending adapter is loaded for lending strategy."""
    strategy = MockStrategy(tags=["lending", "supply"])
    backtester._init_adapter(strategy)

    assert backtester._adapter is not None
    assert isinstance(backtester._adapter, MockLendingAdapter)
    assert backtester._adapter.adapter_name == "lending"


def test_init_adapter_no_adapter_for_unknown_type(backtester, registered_adapters):
    """Test that no adapter is loaded for unknown strategy type."""
    strategy = MockStrategy(tags=["unknown", "custom"])
    backtester._init_adapter(strategy)

    assert backtester._adapter is None


def test_init_adapter_explicit_type(registered_adapters):
    """Test that explicit strategy_type forces adapter selection."""
    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
        strategy_type="lending",  # Explicit type
    )

    # Strategy metadata suggests LP but explicit type is lending
    strategy = MockStrategy(tags=["lp", "amm"])
    backtester._init_adapter(strategy)

    assert backtester._adapter is not None
    assert isinstance(backtester._adapter, MockLendingAdapter)


def test_init_adapter_no_registered_adapters(backtester):
    """Test behavior when no adapters are registered."""
    # No adapters registered (clean_registry fixture)
    strategy = MockStrategy(tags=["lp", "liquidity"])
    backtester._init_adapter(strategy)

    assert backtester._adapter is None


def test_init_adapter_via_alias(backtester, registered_adapters):
    """Test adapter loading uses aliases."""
    # Register adapter with alias
    strategy = MockStrategy(tags=["liquidity"])  # Alias for lp
    backtester._init_adapter(strategy)

    assert backtester._adapter is not None
    assert backtester._adapter.adapter_name == "lp"


# =============================================================================
# Tests: Backtester Configuration
# =============================================================================


def test_backtester_default_strategy_type():
    """Test that default strategy_type is 'auto'."""
    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    assert backtester.strategy_type == "auto"


def test_backtester_explicit_strategy_type():
    """Test backtester with explicit strategy_type."""
    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
        strategy_type="lp",
    )

    assert backtester.strategy_type == "lp"


def test_backtester_none_strategy_type():
    """Test backtester with None strategy_type (disables adapter)."""
    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
        strategy_type=None,
    )

    assert backtester.strategy_type is None


def test_backtester_adapter_initially_none():
    """Test that adapter is None before init_adapter is called."""
    backtester = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    assert backtester._adapter is None
    assert backtester._detected_strategy_type is None


# =============================================================================
# Tests: Multiple Strategies
# =============================================================================


def test_adapter_changes_between_strategies(backtester, registered_adapters):
    """Test that adapter changes when initializing for different strategies."""
    # First strategy: LP
    lp_strategy = MockStrategy(deployment_id="lp_strat", tags=["lp"])
    backtester._init_adapter(lp_strategy)
    assert backtester._adapter is not None
    assert backtester._adapter.adapter_name == "lp"

    # Second strategy: Perp
    perp_strategy = MockStrategy(deployment_id="perp_strat", tags=["perp"])
    backtester._init_adapter(perp_strategy)
    assert backtester._adapter is not None
    assert backtester._adapter.adapter_name == "perp"

    # Third strategy: Unknown (no adapter)
    unknown_strategy = MockStrategy(deployment_id="unknown_strat", tags=["custom"])
    backtester._init_adapter(unknown_strategy)
    assert backtester._adapter is None


# =============================================================================
# Tests: Detection Priority
# =============================================================================


def test_detection_priority_tags_over_protocols(backtester, registered_adapters):
    """Test that tags have priority over protocols in detection."""
    strategy = MockStrategy(
        tags=["lending"],
        protocols=["uniswap_v3"],  # Would suggest LP
    )
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "lending"
    assert hint.source == "tags"


def test_detection_priority_protocols_over_intents(backtester, registered_adapters):
    """Test that protocols have priority over intents in detection."""
    strategy = MockStrategy(
        protocols=["aave_v3"],  # Suggests lending
        intent_types=["SWAP"],  # Would suggest swap
    )
    hint = backtester._detect_strategy_type(strategy)

    assert hint.strategy_type == "lending"
    assert hint.source == "protocols"


# =============================================================================
# Tests: Adapter execute_intent Integration (US-047b)
# =============================================================================


class MockAdapterWithFill(StrategyBacktestAdapter):
    """Mock adapter that returns a SimulatedFill for testing."""

    def __init__(self) -> None:
        self.execute_intent_called = False
        self.execute_intent_call_args: tuple[Any, Any, Any] | None = None

    @property
    def adapter_name(self) -> str:
        return "mock_with_fill"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        """Return a SimulatedFill instead of None."""
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill

        self.execute_intent_called = True
        self.execute_intent_call_args = (intent, portfolio, market_state)

        return SimulatedFill(
            timestamp=datetime.now(UTC),
            intent_type=IntentType.SWAP,
            protocol="test_protocol",
            tokens=["ETH", "USDC"],
            executed_price=Decimal("3000"),
            amount_usd=Decimal("1000"),
            fee_usd=Decimal("3"),
            slippage_usd=Decimal("1"),
            gas_cost_usd=Decimal("5"),
            tokens_in={"ETH": Decimal("0.333")},
            tokens_out={"USDC": Decimal("1000")},
            success=True,
        )

    def update_position(self, position: Any, market_state: Any, elapsed_seconds: float) -> None:
        pass

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


class MockAdapterReturnsNone(StrategyBacktestAdapter):
    """Mock adapter that returns None (fallback to generic execution)."""

    def __init__(self) -> None:
        self.execute_intent_called = False

    @property
    def adapter_name(self) -> str:
        return "mock_returns_none"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        """Return None to trigger fallback to generic execution."""
        self.execute_intent_called = True
        return None

    def update_position(self, position: Any, market_state: Any, elapsed_seconds: float) -> None:
        pass

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


@dataclass
class MockIntent:
    """Mock intent for testing."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "ETH"
    amount: Decimal = field(default_factory=lambda: Decimal("1000"))


@dataclass
class MockMarketState:
    """Mock market state for testing."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    chain: str = "arbitrum"
    _prices: dict[str, Decimal] = field(default_factory=dict)
    gas_price_gwei: Decimal | None = None

    def __post_init__(self) -> None:
        if not self._prices:
            self._prices = {
                "ETH": Decimal("3000"),
                "WETH": Decimal("3000"),
                "USDC": Decimal("1"),
            }

    def get_price(self, token: str) -> Decimal:
        if token in self._prices:
            return self._prices[token]
        raise KeyError(f"Price not found for {token}")


@pytest.mark.asyncio
async def test_execute_intent_calls_adapter_when_available(backtester):
    """Test that adapter's execute_intent is called when adapter exists."""
    # Set up mock adapter that returns a fill
    mock_adapter = MockAdapterWithFill()
    backtester._adapter = mock_adapter
    backtester._current_backtest_id = "test_backtest_123"

    # Create mock inputs
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    market_state = MockMarketState()
    intent = MockIntent()
    config = PnLBacktestConfig(
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC) + timedelta(days=1),
        token_funding=_pnl_token_funding(Decimal("10000")),
    )

    # Execute intent
    _ = await backtester._execute_intent(
        intent=intent,
        portfolio=portfolio,
        market_state=market_state,
        timestamp=datetime.now(UTC),
        config=config,
    )

    # Verify adapter was called
    assert mock_adapter.execute_intent_called is True
    assert mock_adapter.execute_intent_call_args is not None
    assert mock_adapter.execute_intent_call_args[0] is intent
    assert mock_adapter.execute_intent_call_args[1] is portfolio
    assert mock_adapter.execute_intent_call_args[2] is market_state


@pytest.mark.asyncio
async def test_execute_intent_uses_adapter_fill(backtester):
    """Test that adapter's SimulatedFill is used when returned.

    Gas is the exception: the engine owns gas resolution, so the adapter's
    gas_cost_usd is replaced with the engine-resolved value (the LP
    adapter's old flat $20/$15 guesses bypassed the chain-aware gas lane).
    """
    # Set up mock adapter that returns a fill
    mock_adapter = MockAdapterWithFill()
    backtester._adapter = mock_adapter
    backtester._current_backtest_id = "test_backtest_123"

    # Create mock inputs
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    market_state = MockMarketState()
    intent = MockIntent()
    config = PnLBacktestConfig(
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC) + timedelta(days=1),
        token_funding=_pnl_token_funding(Decimal("10000")),
        gas_price_gwei=Decimal("30"),
    )

    # Execute intent
    trade_record = await backtester._execute_intent(
        intent=intent,
        portfolio=portfolio,
        market_state=market_state,
        timestamp=datetime.now(UTC),
        config=config,
    )

    # Verify the trade record matches the adapter's fill
    assert trade_record.protocol == "test_protocol"
    assert trade_record.tokens == ["ETH", "USDC"]
    assert trade_record.amount_usd == Decimal("1000")
    assert trade_record.fee_usd == Decimal("3")
    assert trade_record.slippage_usd == Decimal("1")
    # Engine-stamped gas, not the adapter's Decimal("5"):
    # SWAP 180k gas x 30 gwei x $3000 ETH / 1e9 = $16.20
    assert trade_record.gas_cost_usd == Decimal("16.2")
    assert trade_record.gas_price_gwei == Decimal("30")


@pytest.mark.asyncio
async def test_adapter_fill_gas_zeroed_when_gas_costs_disabled(backtester):
    """include_gas_costs=False zeroes adapter-fabricated gas too.

    Before the engine owned adapter-fill gas, an adapter's hardcoded
    gas_cost_usd was charged even with gas simulation disabled.
    """
    mock_adapter = MockAdapterWithFill()
    backtester._adapter = mock_adapter
    backtester._current_backtest_id = "test_backtest_123"

    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    config = PnLBacktestConfig(
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC) + timedelta(days=1),
        token_funding=_pnl_token_funding(Decimal("10000")),
        include_gas_costs=False,
    )

    trade_record = await backtester._execute_intent(
        intent=MockIntent(),
        portfolio=portfolio,
        market_state=MockMarketState(),
        timestamp=datetime.now(UTC),
        config=config,
    )

    assert trade_record.gas_cost_usd == Decimal("0")
    assert trade_record.gas_price_gwei is None


@pytest.mark.asyncio
async def test_failed_adapter_fill_skips_gas_resolution(backtester):
    """Failed adapter fills do not resolve gas.

    apply_fill zeroes execution costs on failed fills anyway, and gas
    resolution can raise when no ETH/WETH price is available -- a failed
    fill must stay a recorded failure, not become an engine crash.
    """

    class MockAdapterWithFailedFill(MockAdapterWithFill):
        def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
            fill = super().execute_intent(intent, portfolio, market_state)
            fill.success = False
            fill.metadata["failure_reason"] = "position not found"
            return fill

    backtester._adapter = MockAdapterWithFailedFill()
    backtester._current_backtest_id = "test_backtest_123"

    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    # No ETH/WETH price: gas resolution would raise ValueError if attempted
    market_state = MockMarketState(_prices={"USDC": Decimal("1")})
    config = PnLBacktestConfig(
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC) + timedelta(days=1),
        token_funding=_pnl_token_funding(Decimal("10000")),
        gas_price_gwei=Decimal("30"),
    )

    trade_record = await backtester._execute_intent(
        intent=MockIntent(),
        portfolio=portfolio,
        market_state=market_state,
        timestamp=datetime.now(UTC),
        config=config,
    )

    # Costs zeroed by apply_fill's failed-fill handling; no gas stamped
    assert trade_record.success is False
    assert trade_record.gas_cost_usd == Decimal("0")
    assert trade_record.gas_price_gwei is None


@pytest.mark.asyncio
async def test_execute_intent_falls_back_when_adapter_returns_none(backtester):
    """Test fallback to generic execution when adapter returns None."""
    # Set up mock adapter that returns None
    mock_adapter = MockAdapterReturnsNone()
    backtester._adapter = mock_adapter
    backtester._current_backtest_id = "test_backtest_123"

    # Create mock inputs
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    market_state = MockMarketState()
    intent = MockIntent()
    config = PnLBacktestConfig(
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC) + timedelta(days=1),
        token_funding=_pnl_token_funding(Decimal("10000")),
    )

    # Execute intent
    trade_record = await backtester._execute_intent(
        intent=intent,
        portfolio=portfolio,
        market_state=market_state,
        timestamp=datetime.now(UTC),
        config=config,
    )

    # Verify adapter was called
    assert mock_adapter.execute_intent_called is True

    # Verify generic execution was used (protocol should be 'default' from generic logic)
    # and the trade record should be generated (any valid result is fine)
    assert trade_record is not None
    assert trade_record.amount_usd > Decimal("0")


@pytest.mark.asyncio
async def test_execute_intent_no_adapter(backtester):
    """Test that generic execution works when no adapter is set."""
    # Ensure no adapter is set
    backtester._adapter = None
    backtester._current_backtest_id = "test_backtest_123"

    # Create mock inputs
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    market_state = MockMarketState()
    intent = MockIntent()
    config = PnLBacktestConfig(
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC) + timedelta(days=1),
        token_funding=_pnl_token_funding(Decimal("10000")),
    )

    # Execute intent - should work without adapter
    trade_record = await backtester._execute_intent(
        intent=intent,
        portfolio=portfolio,
        market_state=market_state,
        timestamp=datetime.now(UTC),
        config=config,
    )

    # Verify generic execution worked
    assert trade_record is not None
    assert trade_record.amount_usd > Decimal("0")


# =============================================================================
# Tests: VIB-4849 -- missing-data error must PROPAGATE, never be swallowed
# =============================================================================


class _RaisingAdapter(StrategyBacktestAdapter):
    """Adapter whose update_position always fails loud with a missing-data signal."""

    @property
    def adapter_name(self) -> str:
        return "lp"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        return None

    def update_position(
        self,
        position: Any,
        market_state: Any,
        elapsed_seconds: float,
        timestamp: Any = None,
    ) -> None:
        from almanak.framework.backtesting.exceptions import DataSourceUnavailableError

        raise DataSourceUnavailableError(
            data_type="volume",
            identifier="0xdeadbeef",
            remediation="provide explicit_pool_volume_usd_daily or opt into the fallback",
        )

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


def _lp_position():
    """Build a minimal LP position for adapter-update tests."""
    from almanak.framework.backtesting.pnl.portfolio import PositionType, SimulatedPosition

    return SimulatedPosition(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        tokens=["WETH", "USDC"],
        amounts={"WETH": Decimal("1"), "USDC": Decimal("2000")},
        entry_price=Decimal("2000"),
        entry_time=datetime.now(UTC),
        liquidity=Decimal("1000000"),
        fee_tier=Decimal("0.003"),
        metadata={},
    )


def _portfolio_with_position():
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    portfolio.positions.append(_lp_position())
    return portfolio



def _strict(backtester):
    """ALM-2930: the fail-loud missing-data contract now applies in strict mode."""
    from almanak.framework.backtesting.config import BacktestDataConfig

    backtester.data_config = BacktestDataConfig(strict_historical_mode=True)
    return backtester


def test_missing_volume_error_propagates_when_no_handler(backtester):
    _strict(backtester)
    """VIB-4849 (P1): DataSourceUnavailableError must NOT be swallowed to DEBUG.

    With no error handler configured, the engine must re-raise so the backtest
    fails loudly rather than silently accruing zero fees for the tick.
    """
    from almanak.framework.backtesting.exceptions import DataSourceUnavailableError

    backtester._adapter = _RaisingAdapter()
    backtester._error_handler = None
    portfolio = _portfolio_with_position()
    market_state = MockMarketState()

    with pytest.raises(DataSourceUnavailableError):
        backtester._update_positions_via_adapter(portfolio, market_state, datetime.now(UTC))


def test_missing_volume_error_is_not_downgraded_to_debug(backtester, caplog):
    _strict(backtester)
    """VIB-4849 (P1): the missing-data signal is surfaced at ERROR, not hidden at DEBUG."""
    import logging

    from almanak.framework.backtesting.exceptions import DataSourceUnavailableError

    backtester._adapter = _RaisingAdapter()
    backtester._error_handler = None
    portfolio = _portfolio_with_position()
    market_state = MockMarketState()

    with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.pnl.engine"):
        with pytest.raises(DataSourceUnavailableError):
            backtester._update_positions_via_adapter(portfolio, market_state, datetime.now(UTC))

    # Must be surfaced loudly (ERROR), never quietly logged at DEBUG.
    error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("Missing data source" in r.message for r in error_records), (
        "missing-data error must be logged at ERROR, not swallowed"
    )


def test_missing_volume_error_routes_to_handler_stop(backtester):
    _strict(backtester)
    """VIB-4849 (P1): with a stop-policy handler, the error still surfaces (raises)."""
    from almanak.framework.backtesting.exceptions import DataSourceUnavailableError

    class _StopResult:
        should_stop = True

    class _StopHandler:
        def handle_error(self, error, context=""):
            return _StopResult()

        def record_success(self):
            pass

    backtester._adapter = _RaisingAdapter()
    backtester._error_handler = _StopHandler()
    portfolio = _portfolio_with_position()
    market_state = MockMarketState()

    with pytest.raises(DataSourceUnavailableError):
        backtester._update_positions_via_adapter(portfolio, market_state, datetime.now(UTC))


def test_missing_volume_error_handler_continue_does_not_swallow_silently(backtester, caplog):
    _strict(backtester)
    """VIB-4849 (P1): a continue-policy handler still gets a loud ERROR (no silent zero)."""
    import logging

    class _ContinueResult:
        should_stop = False

    class _ContinueHandler:
        def __init__(self):
            self.handled = []

        def handle_error(self, error, context=""):
            self.handled.append(error)
            return _ContinueResult()

        def record_success(self):
            pass

    handler = _ContinueHandler()
    backtester._adapter = _RaisingAdapter()
    backtester._error_handler = handler
    portfolio = _portfolio_with_position()
    market_state = MockMarketState()

    with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.pnl.engine"):
        # Handler explicitly chose to continue -> no raise, but routed + logged loud.
        backtester._update_positions_via_adapter(portfolio, market_state, datetime.now(UTC))

    assert len(handler.handled) == 1
    error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("Missing data source" in r.message for r in error_records)


def test_missing_volume_non_strict_skips_position_and_continues(backtester, caplog):
    """ALM-2930: non-strict mode skips the position's accrual tick with a
    once-per-position WARNING instead of killing the simulation."""
    import logging

    backtester._adapter = _RaisingAdapter()
    backtester._error_handler = None
    portfolio = _portfolio_with_position()
    market_state = MockMarketState()

    with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.pnl.engine"):
        backtester._update_positions_via_adapter(portfolio, market_state, datetime.now(UTC))
        backtester._update_positions_via_adapter(portfolio, market_state, datetime.now(UTC))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "skipping its accrual" in r.message]
    assert len(warnings) == 1, "data-gap warning must fire exactly once per position"


def test_missing_volume_gap_on_one_position_does_not_starve_the_other(backtester, caplog):
    """A data gap on position A must not abort position B's update in the same
    tick — the skip is per-position, the tick's remaining positions still
    update (CodeRabbit review on #3269)."""
    import logging

    updated: list[str] = []

    class _SelectivelyRaisingAdapter(_RaisingAdapter):
        def update_position(self, position, market_state, elapsed_seconds, timestamp=None):
            if position.metadata.get("fail"):
                super().update_position(position, market_state, elapsed_seconds, timestamp)
            updated.append(position.position_id)

    failing = _lp_position()
    failing.metadata["fail"] = True
    healthy = _lp_position()

    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
    portfolio.positions.extend([failing, healthy])

    backtester._adapter = _SelectivelyRaisingAdapter()
    backtester._error_handler = None

    with caplog.at_level(logging.DEBUG, logger="almanak.framework.backtesting.pnl.engine"):
        backtester._update_positions_via_adapter(portfolio, MockMarketState(), datetime.now(UTC))

    assert updated == [healthy.position_id], "the healthy position must still update"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "skipping its accrual" in r.message]
    assert len(warnings) == 1
    assert failing.position_id in warnings[0].message


@pytest.mark.asyncio
async def test_accrual_gap_memo_resets_per_run(backtester, monkeypatch):
    """A reused backtester instance must re-log gaps in every run: parameter
    sweeps rerun identical windows, so position ids collide across runs and a
    stale memo entry would suppress the once-per-position warning (gemini
    review on #3269)."""
    from almanak.framework.backtesting.pnl import engine as engine_module

    backtester._accrual_data_gap_positions.add("stale-position-id")

    class _Sentinel(Exception):
        pass

    async def _abort_immediately(**_kwargs):
        raise _Sentinel()

    monkeypatch.setattr(engine_module._engine_helpers, "run_preflight", _abort_immediately)

    from almanak.framework.backtesting.pnl.engine import PnLBacktestConfig

    config = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        token_funding=_pnl_token_funding(Decimal("1000")),
        tokens=["WETH", "USDC"],
    )
    with pytest.raises(_Sentinel):
        await backtester._run_backtest(
            SimpleNamespace(), config, "run-2", SimpleNamespace(), datetime.now(UTC)
        )

    assert backtester._accrual_data_gap_positions == set(), "memo must clear at run start"


# =============================================================================
# Tests: Post-open prewarm hook wiring
# =============================================================================


class TestPrewarmAfterOpen:
    """_prewarm_after_open hands off to the adapter's prewarm_history hook."""

    @staticmethod
    def _config():
        from almanak.framework.backtesting.pnl.engine import PnLBacktestConfig

        return PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 8, tzinfo=UTC),
            token_funding=_pnl_token_funding(Decimal("1000")),
            tokens=["WETH", "USDC"],
        )

    @staticmethod
    def _trade_record(success: bool):
        return TradeRecord(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            intent_type=IntentType.LP_OPEN,
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            pnl_usd=None,
            success=success,
            amount_usd=Decimal("100"),
        )

    @staticmethod
    def _market_state():
        return MarketState(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            prices={"WETH": Decimal("2000")},
            chain="base",
            block_number=1,
        )

    @pytest.mark.asyncio
    async def test_successful_open_awaits_adapter_prewarm(self, backtester):
        calls: list[dict] = []

        class PrewarmAdapter:
            async def prewarm_history(self, intent, *, chain, start_time, end_time):
                calls.append({"intent": intent, "chain": chain, "start": start_time, "end": end_time})

        backtester._adapter = PrewarmAdapter()
        intent = SimpleNamespace(intent_type=IntentType.LP_OPEN)
        config = self._config()

        await backtester._prewarm_after_open(intent, self._trade_record(True), self._market_state(), config)

        assert len(calls) == 1
        assert calls[0]["chain"] == "base"
        assert calls[0]["start"] == config.start_time
        assert calls[0]["end"] == config.end_time

    @pytest.mark.asyncio
    async def test_failed_open_skips_prewarm(self, backtester):
        calls: list[Any] = []

        class PrewarmAdapter:
            async def prewarm_history(self, intent, *, chain, start_time, end_time):
                calls.append(intent)

        backtester._adapter = PrewarmAdapter()
        intent = SimpleNamespace(intent_type=IntentType.LP_OPEN)

        await backtester._prewarm_after_open(intent, self._trade_record(False), self._market_state(), self._config())

        assert calls == []

    @pytest.mark.asyncio
    async def test_non_open_intent_skips_prewarm(self, backtester):
        calls: list[Any] = []

        class PrewarmAdapter:
            async def prewarm_history(self, intent, *, chain, start_time, end_time):
                calls.append(intent)

        backtester._adapter = PrewarmAdapter()
        intent = SimpleNamespace(intent_type=IntentType.SWAP)

        await backtester._prewarm_after_open(intent, self._trade_record(True), self._market_state(), self._config())

        assert calls == []

    @pytest.mark.asyncio
    async def test_adapter_without_hook_is_noop(self, backtester):
        """No-hook adapters no-op — asserted OBSERVABLY: the engine must probe
        exactly the ``prewarm_history`` attribute and touch nothing else
        (CodeRabbit review on #3269)."""
        lookups: list[str] = []

        class HooklessRecordingAdapter:
            def __getattr__(self, name):  # only invoked for MISSING attributes
                lookups.append(name)
                raise AttributeError(name)

        backtester._adapter = HooklessRecordingAdapter()
        intent = SimpleNamespace(intent_type=IntentType.LP_OPEN)

        await backtester._prewarm_after_open(intent, self._trade_record(True), self._market_state(), self._config())

        assert lookups == ["prewarm_history"]

    @pytest.mark.asyncio
    async def test_perp_open_also_prewarms(self, backtester):
        """PERP_OPEN rides the same post-open hook as LP_OPEN (P5: funding
        prewarm) — pin the branch explicitly (CodeRabbit review on #3269)."""
        calls: list[dict] = []

        class PrewarmAdapter:
            async def prewarm_history(self, intent, *, chain, start_time, end_time):
                calls.append({"intent": intent, "chain": chain})

        backtester._adapter = PrewarmAdapter()
        intent = SimpleNamespace(intent_type=IntentType.PERP_OPEN)
        record = self._trade_record(True)
        record.intent_type = IntentType.PERP_OPEN

        await backtester._prewarm_after_open(intent, record, self._market_state(), self._config())

        assert len(calls) == 1 and calls[0]["chain"] == "base"

    @pytest.mark.asyncio
    async def test_market_state_chain_none_falls_back_to_config_chain(self, backtester):
        """A market_state whose ``chain`` attribute EXISTS but is None must
        fall back to config.chain — str(None) would prewarm chain "None"
        (gemini review on #3269)."""
        calls: list[dict] = []

        class PrewarmAdapter:
            async def prewarm_history(self, intent, *, chain, start_time, end_time):
                calls.append({"chain": chain})

        backtester._adapter = PrewarmAdapter()
        intent = SimpleNamespace(intent_type=IntentType.LP_OPEN)
        config = self._config()
        market_state = SimpleNamespace(chain=None)

        await backtester._prewarm_after_open(intent, self._trade_record(True), market_state, config)

        assert calls == [{"chain": config.chain}]
