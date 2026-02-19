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
from typing import Any

import pytest

from almanak.framework.backtesting.adapters import (
    AdapterRegistry,
    StrategyBacktestAdapter,
)
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)

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
        strategy_id: str = "test_strategy",
        tags: list[str] | None = None,
        protocols: list[str] | None = None,
        intent_types: list[str] | None = None,
    ):
        self._strategy_id = strategy_id
        self.STRATEGY_METADATA = MockStrategyMetadata(
            tags=tags or [],
            supported_protocols=protocols or [],
            intent_types=intent_types or [],
        )

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

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
    """Clean registry before and after each test."""
    AdapterRegistry.clear()
    yield
    AdapterRegistry.clear()


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
    lp_strategy = MockStrategy(strategy_id="lp_strat", tags=["lp"])
    backtester._init_adapter(lp_strategy)
    assert backtester._adapter is not None
    assert backtester._adapter.adapter_name == "lp"

    # Second strategy: Perp
    perp_strategy = MockStrategy(strategy_id="perp_strat", tags=["perp"])
    backtester._init_adapter(perp_strategy)
    assert backtester._adapter is not None
    assert backtester._adapter.adapter_name == "perp"

    # Third strategy: Unknown (no adapter)
    unknown_strategy = MockStrategy(strategy_id="unknown_strat", tags=["custom"])
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
        initial_capital_usd=Decimal("10000"),
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
    """Test that adapter's SimulatedFill is used when returned."""
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
        initial_capital_usd=Decimal("10000"),
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
    assert trade_record.gas_cost_usd == Decimal("5")


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
        initial_capital_usd=Decimal("10000"),
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
        initial_capital_usd=Decimal("10000"),
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
